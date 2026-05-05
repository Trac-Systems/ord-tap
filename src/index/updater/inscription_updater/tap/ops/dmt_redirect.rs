use super::super::super::*;

const NOTICE_BLOCKS: u64 = 144;
const MAX_BUCKETS: usize = 8;
const MAX_BUCKET_NAME_LEN: usize = 32;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_dmt_redirect(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    _output_value_sat: u64,
    parents: &[InscriptionId],
  ) {
    // Only process creation-time inscriptions
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() { return; }
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val = match self.parse_tap_json_value(&s) { Some(v) => v, None => return };

    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    if p != "tap" || op != "dmt-redirect" { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }
    if !self.tap_feature_enabled(TapFeature::Dmt) { return; }
    if inscription_number < 0 { return; }

    let tick_user = json_val.get("tick").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if tick_user.is_empty() { return; }
    if tick_user.to_lowercase().starts_with('-') || tick_user.to_lowercase().starts_with("dmt-") { return; }

    let tick_effective = format!("dmt-{}", tick_user);
    let tick_effective_lower = tick_effective.to_lowercase();
    let tick_key = Self::json_stringify_lower(&tick_effective_lower);

    if tick_effective_lower == "dmt-nat" { return; }

    let Some(deployed) = self.tap_get::<DeployRecord>(&format!("d/{}", tick_key)).ok().flatten() else { return; };
    if !deployed.dmt { return; }

    // V1 restriction: only field-11 deploys without a pattern. The dispatcher
    // pays `u128::from(bits)`, which only matches the dmt-nat / dmt-bit emission
    // semantics. Field 4 / 10 deploys and pattern-based field 11 deploys would
    // be mis-credited.
    let Some(elem_ins) = deployed.elem.clone() else { return; };
    let Some(elem_name) = self.tap_get::<String>(&format!("dmt-{}", elem_ins)).ok().flatten() else { return; };
    let Some(elem) = self.tap_get::<DmtElementRecord>(&format!("dmt-el/{}", Self::json_stringify_lower(&elem_name))).ok().flatten() else { return; };
    if elem.fld != 11 || elem.pat.is_some() { return; }

    // Authorization: the reveal transaction must spend the deploy
    // inscription's UTXO, declared as the Ordinals parent (envelope tag 3) of
    // this inscription. ord-tap filters declared parents to only those whose
    // sat is actually transferred by the reveal, so presence in `parents` is
    // proof of control. Whoever currently holds the deploy inscription is the
    // authority for that tick's redirect rule.
    //
    // Reject same-transaction deploy+redirect: in that case the deploy is a
    // sibling envelope rather than a spent-UTXO parent, so the literal
    // "spend the deploy inscription's UTXO" property would not hold. The
    // deployer can always inscribe the redirect in a follow-up transaction.
    let Ok(deploy_ins_id) = deployed.ins.parse::<InscriptionId>() else { return; };
    if deploy_ins_id.txid == inscription_id.txid { return; }
    if !parents.iter().any(|p| *p == deploy_ins_id) { return; }

    let act = match json_val.get("act") {
      Some(v) => match Self::js_parse_int(v) {
        Some(n) if n > 0 => match u64::try_from(n) { Ok(x) => x, Err(_) => return },
        _ => return,
      },
      None => return,
    };
    let min_act = u64::from(self.height).saturating_add(NOTICE_BLOCKS);
    if act <= min_act { return; }

    let Some(rule_val) = json_val.get("rule") else { return; };
    let mut rule: DmtRedirectRule = match serde_json::from_value(rule_val.clone()) {
      Ok(r) => r,
      Err(_) => return,
    };
    if !self.validate_dmt_redirect_rule(&rule) { return; }
    Self::normalize_dmt_redirect_rule_addresses(&mut rule);

    let record = DmtRedirectRecord {
      tick: tick_effective_lower.clone(),
      act,
      rule,
      inscription_id: inscription_id.to_string(),
      inscriber_addr: owner_address.to_string(),
      inscribed_at_height: self.height,
    };

    // If any rule already exists at r/<tick> (active OR scheduled-but-not-yet-
    // active), queue the new one as pending so its activation is governed by
    // its own `act` block, not by inscription time. The per-block dispatcher
    // promotes pendings whose `act` equals the current block.
    let has_existing = self
      .tap_get::<DmtRedirectRecord>(&format!("r/{}", tick_key))
      .ok()
      .flatten()
      .is_some();

    if has_existing {
      let pending_key = format!("r-pending/{}/{}", tick_key, act);
      let _ = self.tap_put(&pending_key, &record);
    } else {
      let _ = self.tap_put(&format!("r/{}", tick_key), &record);
    }

    let _ = self.tap_set_list_record("redirect-list", "redirect-listi", &tick_effective_lower);
  }

  fn validate_dmt_redirect_rule(&self, rule: &DmtRedirectRule) -> bool {
    if rule.rule_type != "weighted-split" { return false; }
    if rule.must_sum_to != 10000 { return false; }
    if rule.buckets.is_empty() || rule.buckets.len() > MAX_BUCKETS { return false; }

    let mut sum: u32 = 0;
    let mut has_solo = false;
    let mut seen_names: Vec<&str> = Vec::with_capacity(rule.buckets.len());
    for bucket in &rule.buckets {
      if bucket.share_bps == 0 { return false; }
      sum = sum.saturating_add(u32::from(bucket.share_bps));
      if bucket.name.is_empty() || bucket.name.len() > MAX_BUCKET_NAME_LEN { return false; }
      if !bucket
        .name
        .chars()
        .all(|c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-')
      {
        return false;
      }
      // Names are used as storage-key suffixes; duplicates would collide.
      if seen_names.iter().any(|n| n == &bucket.name.as_str()) { return false; }
      seen_names.push(bucket.name.as_str());

      match &bucket.recipient {
        BucketRecipient::CoinbaseOutput => {}
        BucketRecipient::SoloCoinbaseOutput { .. } => {
          has_solo = true;
        }
        BucketRecipient::Address { addr } => {
          if !Self::is_valid_bitcoin_address_mainnet(addr.as_str()) { return false; }
        }
      }
    }
    if sum != rule.must_sum_to { return false; }

    // Cap and address-validate any classifier object that's present, even when
    // no solo bucket consumes it. Otherwise an oversized/invalid unused
    // classifier would be stored, weakening the absolute caps.
    if let Some(sc) = rule.solo_classification.as_ref() {
      if sc.tags_substring.len() > 64
        || sc.addresses.len() > 64
        || sc.pool_tags_blocklist.len() > 64
        || sc.pool_addresses_blocklist.len() > 64
      {
        return false;
      }
      for a in sc.addresses.iter().chain(sc.pool_addresses_blocklist.iter()) {
        if !Self::is_valid_bitcoin_address_mainnet(a.as_str()) { return false; }
      }
    }
    if has_solo && rule.solo_classification.is_none() { return false; }
    true
  }

  // Lowercase bech32 addresses to match `address_from_script` output.
  fn normalize_dmt_redirect_rule_addresses(rule: &mut DmtRedirectRule) {
    for bucket in &mut rule.buckets {
      if let BucketRecipient::Address { addr } = &mut bucket.recipient {
        *addr = Self::normalize_address(addr);
      }
    }
    if let Some(sc) = rule.solo_classification.as_mut() {
      for a in sc.addresses.iter_mut() {
        *a = Self::normalize_address(a);
      }
      for a in sc.pool_addresses_blocklist.iter_mut() {
        *a = Self::normalize_address(a);
      }
    }
  }
}
