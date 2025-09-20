use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_token_auth_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Only process creation-time inscriptions
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() { return; }
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };
    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    if p != "tap" || op != "token-auth" { return; }

    if json_val.get("cancel").is_some() {
      let acc = TapAccumulatorEntry { op: "token-auth".to_string(), json: json_val.clone(), ins: inscription_id.to_string(), blck: self.height, tx: satpoint.outpoint.txid.to_string(), vo: u32::from(satpoint.outpoint.vout), num: inscription_number, ts: self.timestamp, addr: owner_address.to_string() };
      let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
      let _ = self.tap_set_list_record(&format!("al/{}", owner_address), &format!("ali/{}", owner_address), &acc);
      if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
        let ptr = format!("ali/{}", list_len - 1);
        let txs = satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(&format!("tx/a-athc/{}", txs), &format!("txi/a-athc/{}", txs), &ptr);
        let _ = self.tap_set_list_record(&format!("blck/a-athc/{}", self.height), &format!("blcki/a-athc/{}", self.height), &ptr);
      }
      // Ensure transfer-time execution isn't skipped by preflight bloom
      if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
      return;
    }

    let Some(sig_obj) = json_val.get("sig") else { return; };
    if !sig_obj.is_object() { return; }
    let Some(hash_str) = json_val.get("hash").and_then(|v| v.as_str()) else { return; };
    let Some(salt_str) = json_val.get("salt").and_then(|v| v.as_str()) else { return; };

    if let Some(redeem) = json_val.get("redeem") {
      let Some(items) = redeem.get("items").and_then(|v| v.as_array()) else { return; };
      if items.is_empty() { return; }
      if redeem.get("data").is_none() { return; }
      let mut items_norm = items.clone();
      for it in items_norm.iter_mut() {
        let Some(tick) = it.get("tick").and_then(|v| v.as_str()) else { return; };
        let t = Self::strip_prefix_for_len_check(tick);
        if !Self::valid_tap_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, Self::visible_length(t)) { return; }
        if let Some(addr) = it.get("address").and_then(|v| v.as_str()) {
        let norm = Self::normalize_address(addr);
        if !self.is_valid_bitcoin_address(&norm) { return; }
          if let Some(v) = it.get_mut("address") { *v = serde_json::Value::String(norm); }
        } else { return; }
      }
      let msg_hash = Self::build_sha256_json_plus_salt(redeem, salt_str);
      let Some((ok, compact_sig, pubkey_hex)) = self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash) else { return; };
      if !ok { return; }
      if self.tap_get::<String>(&format!("tah/{}", compact_sig)).ok().flatten().is_some() { return; }
      let Some(auth_id) = redeem.get("auth").and_then(|v| v.as_str()) else { return; };
      let Some(ptr) = self.tap_get::<String>(&format!("tains/{}", auth_id)).ok().flatten() else { return; };
      let Some(link) = self.tap_get::<TokenAuthCreateRecord>(&ptr).ok().flatten() else { return; };
      let auth_msg_hash = Self::build_sha256_json_plus_salt(&serde_json::Value::Array(link.auth.iter().map(|s| serde_json::Value::String(s.clone())).collect()), &link.slt);
      let Some((auth_ok, _, auth_pub)) = self.verify_sig_obj_against_msg_with_hash(&link.sig, &link.hash, &auth_msg_hash) else { return; };
      if !auth_ok { return; }
      if auth_pub.to_lowercase() != pubkey_hex.to_lowercase() { return; }
      // Enforce redeem items whitelist parity from activation height:
      // if link.auth is non-empty, every redeem item.tick must be included in link.auth
      if self.tap_feature_enabled(TapFeature::TokenAuthWhitelistFixActivation) {
        if !link.auth.is_empty() {
          for it in items_norm.iter() {
            let Some(tick) = it.get("tick").and_then(|v| v.as_str()) else { return; };
            if !link.auth.iter().any(|t| t == tick) { return; }
          }
        }
      }
      if self.tap_get::<String>(&format!("tac/{}", link.ins)).ok().flatten().is_some() { return; }
      for it in items_norm.iter() {
        let tick = it.get("tick").and_then(|v| v.as_str()).unwrap_or("");
        let to_addr = it.get("address").and_then(|v| v.as_str()).unwrap_or("");
        let amt_v = it.get("amt").unwrap();
        let dta = it.get("dta").and_then(|v| v.as_str()).map(|s| s.to_string());
        self.exec_internal_send_one(&link.addr, to_addr, tick, amt_v, dta, &inscription_id.to_string(), inscription_number, satpoint, output_value_sat);
      }
      let rec = TokenAuthRedeemRecord { addr: link.addr.clone(), iaddr: owner_address.to_string(), rdm: redeem.clone(), sig: sig_obj.clone(), hash: hash_str.to_string(), slt: salt_str.to_string(), blck: self.height, tx: satpoint.outpoint.txid.to_string(), vo: u32::from(satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription_id.to_string(), num: inscription_number, ts: self.timestamp };
      if let Ok(list_len) = self.tap_set_list_record(&format!("tr/{}", link.addr), &format!("tri/{}", link.addr), &rec) {
        let _ = self.tap_put(&format!("trins/{}", inscription_id), &format!("tri/{}/{}", link.addr, list_len.saturating_sub(1)));
      }
      if let Ok(list_len) = self.tap_set_list_record("sftr", "sftri", &rec) {
        let ptr = format!("sftri/{}", list_len - 1);
        let txs = satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(&format!("tx/ath-rdm/{}", txs), &format!("txi/ath-rdm/{}", txs), &ptr);
        let _ = self.tap_set_list_record(&format!("blck/ath-rdm/{}", self.height), &format!("blcki/ath-rdm/{}", self.height), &ptr);
      }
      let _ = self.tap_put(&format!("tah/{}", compact_sig), &"".to_string());
      return;
    }

    let acc = TapAccumulatorEntry { op: "token-auth".to_string(), json: json_val, ins: inscription_id.to_string(), blck: self.height, tx: satpoint.outpoint.txid.to_string(), vo: u32::from(satpoint.outpoint.vout), num: inscription_number, ts: self.timestamp, addr: owner_address.to_string() };
    let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
    let _ = self.tap_set_list_record(&format!("al/{}", owner_address), &format!("ali/{}", owner_address), &acc);
    if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
      let ptr = format!("ali/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/a-ath/{}", txs), &format!("txi/a-ath/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/a-ath/{}", self.height), &format!("blcki/a-ath/{}", self.height), &ptr);
    }
    // Ensure transfer-time execution is not skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
  }

  pub(crate) fn index_token_auth_executed(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Only execute on transfer (not creation tx)
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() { return; }
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else { return; };
    if acc.addr != owner_address { return; }
    if acc.op.to_lowercase() != "token-auth" { return; }

    if acc.json.get("cancel").is_some() {
      if let Some(cancel_id) = acc.json.get("cancel").and_then(|v| v.as_str()) {
        if let Some(ptr) = self.tap_get::<String>(&format!("tains/{}", cancel_id)).ok().flatten() {
          if let Some(link) = self.tap_get::<TokenAuthCreateRecord>(&ptr).ok().flatten() {
            if link.addr == acc.addr { let _ = self.tap_put(&format!("tac/{}", link.ins), &"".to_string()); }
          }
        }
      }
      let _ = self.tap_del(&key);
      return;
    }

    let Some(sig_obj) = acc.json.get("sig") else { return; };
    let Some(hash_str) = acc.json.get("hash").and_then(|v| v.as_str()) else { return; };
    let Some(salt_str) = acc.json.get("salt").and_then(|v| v.as_str()) else { return; };
    let Some(auth_arr) = acc.json.get("auth").and_then(|v| v.as_array()) else { return; };
    let msg_hash = Self::build_sha256_json_plus_salt(&serde_json::Value::Array(auth_arr.clone()), salt_str);
    let Some((ok, compact_sig, _pub)) = self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash) else { return; };
    if !ok { return; }
    if self.tap_get::<String>(&format!("tah/{}", compact_sig)).ok().flatten().is_some() { return; }
    for t in auth_arr.iter() { let Some(ts) = t.as_str() else { return; }; if self.tap_get::<DeployRecord>(&format!("d/{}", Self::json_stringify_lower(ts))).ok().flatten().is_none() { return; } }
    let auth_vec: Vec<String> = auth_arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    let rec = TokenAuthCreateRecord { addr: acc.addr.clone(), auth: auth_vec, sig: sig_obj.clone(), hash: hash_str.to_string(), slt: salt_str.to_string(), blck: self.height, tx: new_satpoint.outpoint.txid.to_string(), vo: u32::from(new_satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription_id.to_string(), num: acc.num, ts: self.timestamp };
    if let Ok(list_len) = self.tap_set_list_record(&format!("ta/{}", acc.addr), &format!("tai/{}", acc.addr), &rec) {
      let ptr = format!("tai/{}/{}", acc.addr, list_len - 1);
      let _ = self.tap_put(&format!("tains/{}", inscription_id), &ptr);
      if let Ok(sflen) = self.tap_set_list_record("sfta", "sftai", &rec) {
        let sptr = format!("sftai/{}", sflen - 1);
        let txs = new_satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(&format!("tx/ath/{}", txs), &format!("txi/ath/{}", txs), &sptr);
        let _ = self.tap_set_list_record(&format!("blck/ath/{}", self.height), &format!("blcki/ath/{}", self.height), &sptr);
      }
    }
    let _ = self.tap_put(&format!("tah/{}", compact_sig), &"".to_string());
    let _ = self.tap_del(&key);
  }
}
