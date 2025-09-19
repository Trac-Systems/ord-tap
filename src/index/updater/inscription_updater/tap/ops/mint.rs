use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_mints(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Parse body as JSON text
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);

    let json_val: serde_json::Value = match serde_json::from_str(&s) {
      Ok(v) => v,
      Err(_) => return,
    };

    // Entry guard
    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let tick = json_val.get("tick").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let amt_raw = json_val.get("amt").cloned();
    if p != "tap" || op != "token-mint" || tick.is_empty() || amt_raw.is_none() { return; }
    
    // value_stringify activation: reject numeric JSON for amt at/after height
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) {
      if let Some(v) = json_val.get("amt") { if v.is_number() { return; } }
    }
    let tick_lower = tick.to_lowercase();
    if tick_lower.starts_with('-') || tick_lower.starts_with("dmt-") { return; }

    // visible length guards
    let vis_len = Self::visible_length(&tick);
    if !Self::valid_tap_ticker_visible_len(self.height, vis_len) { return; }

    // jubilee (negative numbers)
    let mut effective_tick = tick_lower.clone();
    let tmp_tick = tick.clone();
    if inscription_number < 0 {
      if self.tap_feature_enabled(TapFeature::Jubilee) {
        return;
      } else {
        effective_tick = format!("-{}", effective_tick);
      }
    }

    // optional dta
    let mut ins_data: Option<String> = None;
    if let Some(dta_val) = json_val.get("dta") { if let Some(s) = dta_val.as_str() { if s.as_bytes().len() > 512 { return; } ins_data = Some(s.to_string()); } }

    // Resolve deployment
    let tick_key = serde_json::to_string(&effective_tick).unwrap_or_else(|_| format!("\"{}\"", effective_tick));
    let d_key = format!("d/{}", tick_key);
    let deployed = match self.tap_get::<DeployRecord>(&d_key).ok().flatten() { Some(d) => d, None => return };
    let mut tokens_left: u128 = match self.tap_get::<String>(&format!("dc/{}", tick_key)).ok().flatten().and_then(|s| s.parse::<u128>().ok()) { Some(v) => v, None => return };

    // Parse amount
    let decimals = deployed.dec;
    let amt_str_input = if let Some(a) = &amt_raw { if a.is_string() { a.as_str().unwrap().to_string() } else { a.to_string() } } else { return };
    let amt_norm = match Self::resolve_number_string(&amt_str_input, decimals) { Some(x) => x, None => return };
    let mut amount: u128 = match amt_norm.parse::<u128>() { Ok(v) => v, Err(_) => return };

    let mut fail = false;
    let limit: u128 = deployed.lim.parse::<u128>().unwrap_or(0);
    if limit > 0 && amount > limit { fail = true; }
    if !fail {
      if tokens_left < amount { amount = tokens_left; }
      if amount == 0 { fail = true; }
    }

    // Privilege check if required by deployment
    let mut used_compact_sig: Option<String> = None;
    if !fail {
      if let Some(prv_dep) = &deployed.prv {
        if let Some(prv_obj) = json_val.get("prv") {
          let prv_salt = prv_obj.get("salt").and_then(|v| v.as_str()).unwrap_or("");
          // Parity: use json.prv.address for message building (not owner_address)
          let prv_addr_for_msg = prv_obj.get("address").and_then(|v| v.as_str()).unwrap_or("");
          let msg_hash = Self::build_mint_privilege_message_hash(
            &p,
            &op,
            &tmp_tick,
            &amt_str_input,
            prv_addr_for_msg,
            ins_data.as_deref(),
            prv_salt,
          );
          if let Some((ok, comp_hex)) = self.verify_privilege_signature_with_msg(prv_dep, prv_obj, &msg_hash, owner_address) {
            if !ok { fail = true; }
            else { used_compact_sig = Some(comp_hex); }
          } else { fail = true; }
        } else { fail = true; }
      }
    }

    // Balance update
    let bal_key = format!("b/{}/{}", owner_address, tick_key);
    let prev_balance: u128 = self.tap_get::<String>(&bal_key).ok().flatten().and_then(|s| s.parse::<u128>().ok()).unwrap_or(0);
    let new_balance = if !fail { prev_balance.saturating_add(amount) } else { prev_balance };

    if !fail {
      tokens_left = tokens_left.saturating_sub(amount);
      let _ = self.tap_put(&format!("dc/{}", tick_key), &tokens_left.to_string());
      let _ = self.tap_put(&bal_key, &new_balance.to_string());
      // holder list
      if self.tap_get::<String>(&format!("he/{}/{}", owner_address, tick_key)).ok().flatten().is_none() {
        let _ = self.tap_put(&format!("he/{}/{}", owner_address, tick_key), &"".to_string());
        let _ = self.tap_set_list_record(&format!("h/{}", tick_key), &format!("hi/{}", tick_key), &owner_address.to_string());
      }
      // account token owned list (atl/atli)
      if self.tap_get::<String>(&format!("ato/{}/{}", owner_address, tick_key)).ok().flatten().is_none() {
        let tick_lower_for_list = serde_json::from_str::<String>(&tick_key).unwrap_or_else(|_| effective_tick.clone());
        let _ = self.tap_set_list_record(&format!("atl/{}", owner_address), &format!("atli/{}", owner_address), &tick_lower_for_list);
        let _ = self.tap_put(&format!("ato/{}/{}", owner_address, tick_key), &"".to_string());
      }
    }

    // Record shapes (typed CBOR structs)
    let rec = MintRecord {
      addr: owner_address.to_string(),
      blck: self.height,
      amt: amount.to_string(),
      bal: new_balance.to_string(),
      tx: Some(satpoint.outpoint.txid.to_string()),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: Some(inscription_id.to_string()),
      num: Some(inscription_number),
      ts: self.timestamp,
      fail,
      dmtblck: None,
      dta: ins_data.clone(),
    };
    let _ = self.tap_set_list_record(&format!("aml/{}/{}", owner_address, tick_key), &format!("amli/{}/{}", owner_address, tick_key), &rec);
    let flat_rec = MintFlatRecord { addr: rec.addr.clone(), blck: rec.blck, amt: rec.amt.clone(), bal: rec.bal.clone(), tx: rec.tx.clone(), vo: rec.vo, val: rec.val.clone(), ins: rec.ins.clone(), num: rec.num, ts: rec.ts, fail: rec.fail, dmtblck: rec.dmtblck, dta: rec.dta.clone() };
    let _ = self.tap_set_list_record(&format!("fml/{}", tick_key), &format!("fmli/{}", tick_key), &flat_rec);
    let super_rec = MintSuperflatRecord { tick: effective_tick.clone(), addr: owner_address.to_string(), blck: self.height, amt: amount.to_string(), bal: new_balance.to_string(), tx: rec.tx.clone(), vo: rec.vo, val: rec.val.clone(), ins: rec.ins.clone(), num: rec.num, ts: rec.ts, fail, dmtblck: None, dta: rec.dta.clone() };
    if let Ok(list_len) = self.tap_set_list_record("sfml", "sfmli", &super_rec) {
      let ptr = format!("sfmli/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/mnt/{}", txs), &format!("txi/mnt/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("txt/mnt/{}/{}", tick_key, txs), &format!("txti/mnt/{}/{}", tick_key, txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/mnt/{}", self.height), &format!("blcki/mnt/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckt/mnt/{}/{}", tick_key, self.height), &format!("blckti/mnt/{}/{}", tick_key, self.height), &ptr);
    }

    // mark signature as used if present/valid
    if let Some(comp) = used_compact_sig { let _ = self.tap_put(&format!("prah/{}", comp), &"".to_string()); }
  }
}

