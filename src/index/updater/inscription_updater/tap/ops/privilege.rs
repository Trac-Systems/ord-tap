use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_privilege_auth_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    // Only process creation-time inscriptions
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() { return; }
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };
    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    if p != "tap" || op != "privilege-auth" { return; }

    if json_val.get("cancel").is_some() {
      if !self.tap_feature_enabled(TapFeature::TapStart) { return; }
      let acc = TapAccumulatorEntry { op: "privilege-auth".to_string(), json: json_val.clone(), ins: inscription_id.to_string(), blck: self.height, tx: satpoint.outpoint.txid.to_string(), vo: u32::from(satpoint.outpoint.vout), num: inscription_number, ts: self.timestamp, addr: owner_address.to_string() };
      let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
      let _ = self.tap_set_list_record(&format!("al/{}", owner_address), &format!("ali/{}", owner_address), &acc);
      if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
        let ptr = format!("ali/{}", list_len - 1);
        let txs = satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(&format!("tx/a-prathc/{}", txs), &format!("txi/a-prathc/{}", txs), &ptr);
        let _ = self.tap_set_list_record(&format!("blck/a-prathc/{}", self.height), &format!("blcki/a-prathc/{}", self.height), &ptr);
      }
      // Ensure transfer-time execution isn't skipped by preflight bloom
      if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
      return;
    }

    if !self.tap_feature_enabled(TapFeature::PrivilegeActivation) { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }
    let Some(sig_obj) = json_val.get("sig") else { return; };
    if !sig_obj.is_object() { return; }
    if json_val.get("hash").and_then(|v| v.as_str()).is_none() { return; }
    if json_val.get("salt").and_then(|v| v.as_str()).is_none() { return; }
    if !json_val.get("auth").map(|v| v.is_object()).unwrap_or(false) { return; }
    if json_val.get("auth").and_then(|v| v.get("name")).and_then(|v| v.as_str()).is_none() { return; }

    let acc = TapAccumulatorEntry { op: "privilege-auth".to_string(), json: json_val, ins: inscription_id.to_string(), blck: self.height, tx: satpoint.outpoint.txid.to_string(), vo: u32::from(satpoint.outpoint.vout), num: inscription_number, ts: self.timestamp, addr: owner_address.to_string() };
    let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
    let _ = self.tap_set_list_record(&format!("al/{}", owner_address), &format!("ali/{}", owner_address), &acc);
    if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
      let ptr = format!("ali/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/a-prath/{}", txs), &format!("txi/a-prath/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/a-prath/{}", self.height), &format!("blcki/a-prath/{}", self.height), &ptr);
    }
    // Ensure transfer-time execution isn't skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
  }

  pub(crate) fn index_privilege_auth_executed(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    // Only execute on transfer (not creation tx)
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() { return; }
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else { return; };
    if acc.addr != owner_address { return; }
    if acc.op.to_lowercase() != "privilege-auth" { return; }

    if let Some(cancel_id) = acc.json.get("cancel").and_then(|v| v.as_str()) {
      if let Some(_ptr) = self.tap_get::<String>(&format!("prains/{}", cancel_id)).ok().flatten() {
        let _ = self.tap_put(&format!("prac/{}", cancel_id), &"".to_string());
      }
      let _ = self.tap_del(&key);
      return;
    }

    let Some(sig_obj) = acc.json.get("sig") else { return; };
    let Some(hash_str) = acc.json.get("hash").and_then(|v| v.as_str()) else { return; };
    let Some(salt_str) = acc.json.get("salt").and_then(|v| v.as_str()) else { return; };
    let Some(auth_obj) = acc.json.get("auth") else { return; };
    let Some(_name_str) = auth_obj.get("name").and_then(|v| v.as_str()) else { return; };
    // Build message and verify signature
    let msg_hash = Self::build_sha256_json_plus_salt(auth_obj, salt_str);
    let Some((ok, compact_sig, _pubkey_hex)) = self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash) else { return; };
    if !ok { return; }
    if self.tap_get::<String>(&format!("prah/{}", compact_sig)).ok().flatten().is_some() { return; }

    // Persist owner and mark signature used
    let _ = self.tap_put(&format!("prao/{}", inscription_id), &owner_address.to_string());
    let _ = self.tap_put(&format!("prah/{}", compact_sig), &"".to_string());
    // Emit typed record for privilege-auth create (sfpra/pra lists)
    let rec = super::super::PrivilegeAuthCreateRecord {
      addr: owner_address.to_string(),
      auth: auth_obj.clone(),
      sig: sig_obj.clone(),
      hash: hash_str.to_string(),
      slt: salt_str.to_string(),
      blck: self.height,
      tx: new_satpoint.outpoint.txid.to_string(),
      vo: u32::from(new_satpoint.outpoint.vout),
      val: _output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: acc.num,
      ts: self.timestamp,
    };
    // Global list
    let ptr = if let Ok(list_len) = self.tap_set_list_record("sfpra", "sfprai", &rec) {
      Some(format!("sfprai/{}", list_len - 1))
    } else { None };
    if let Some(ptr) = ptr {
      let txs = new_satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/pra/{}", txs), &format!("txi/pra/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/pra/{}", self.height), &format!("blcki/pra/{}", self.height), &ptr);
    }
    // Account-scoped list, track pointer for prains mapping
    if let Ok(acc_len) = self.tap_set_list_record(&format!("pra/{}", owner_address), &format!("prai/{}", owner_address), &rec) {
      // prains/<ins> must point to the account-scoped list entry
      let pr_ptr = format!("prai/{}/{}", owner_address, acc_len - 1);
      let _ = self.tap_put(&format!("prains/{}", inscription_id), &pr_ptr);
    }
    let _ = self.tap_del(&key);
  }

  pub(crate) fn index_privilege_verify_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };
    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    if p != "tap" || op != "privilege-auth" { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }

    let sig_obj = match json_val.get("sig") { Some(v) if v.is_object() => v, _ => return };
    let prv = match json_val.get("prv").and_then(|v| v.as_str()) { Some(v) => v, None => return };
    {
      let parts: Vec<&str> = prv.split('i').collect();
      if parts.len() != 2 { return; }
      if parts[0].len() != 64 { return; }
      if hex::decode(parts[0]).is_err() { return; }
      if parts[1].parse::<i64>().is_err() { return; }
    }
    let verify = match json_val.get("verify").and_then(|v| v.as_str()) { Some(v) => v, None => return };
    if verify.len() != 64 || hex::decode(verify).is_err() { return; }
    let col_raw = match json_val.get("col").and_then(|v| v.as_str()) { Some(v) => v, None => return };
    let mut col_norm = col_raw.to_string();
    let col_len = Self::visible_length(&col_norm);
    if col_len > 512 { return; }
    if col_len == 0 { col_norm = "-".to_string(); }
    let addr_field = match json_val.get("address").and_then(|v| v.as_str()) { Some(v) => v, None => return };
    let seq_str = json_val.get("seq").map(|v| if v.is_string() { v.as_str().unwrap().to_string() } else { v.to_string() }).unwrap_or_default();
    if seq_str.is_empty() { return; }
    let seq_i = match seq_str.parse::<i64>() { Ok(v) => v, Err(_) => return };
    if seq_str != seq_i.to_string() { return; }
    let salt = match json_val.get("salt").and_then(|v| v.as_str()) { Some(v) => v, None => return };

    let msg_hash = Self::build_sha256_privilege_verify(prv, &col_norm, verify, &seq_str, addr_field, salt);
    let Some((is_valid, compact_sig, _pubkey_hex)) = self.verify_sig_obj_against_msg_with_hash(sig_obj, verify, &msg_hash) else { return; };
    if !is_valid { return; }
    if self.tap_get::<String>(&format!("prah/{}", compact_sig)).ok().flatten().is_some() { return; }
    if self.tap_get::<String>(&format!("prains/{}", prv)).ok().flatten().is_none() { return; }
    if self.tap_get::<String>(&format!("prac/{}", prv)).ok().flatten().is_some() { return; }

    // Persist verification
    let rec = PrivilegeVerifiedRecord { ownr: owner_address.to_string(), prv: None, name: col_norm.clone(), privf: prv.to_string(), col: col_norm.clone(), vrf: verify.to_string(), seq: seq_i, slt: salt.to_string(), blck: self.height, tx: satpoint.outpoint.txid.to_string(), vo: u32::from(satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription_id.to_string(), num: inscription_number, ts: self.timestamp };
    if let Ok(list_len) = self.tap_set_list_record("sfprav", "sfpravi", &rec) {
      let ptr = format!("sfpravi/{}", list_len - 1);
      let _ = self.tap_put(&format!("prvvrfd/{}/{}/{}/{}", prv, col_norm, verify, seq_i), &ptr);
      let _ = self.tap_put(&format!("prvins/{}/{}/{}/{}", prv, col_norm, verify, seq_i), &inscription_id.to_string());
      let _ = self.tap_put(&format!("prvins/{}", inscription_id), &format!("prvins/{}/{}/{}/{}", prv, col_norm, verify, seq_i));
      let _ = self.tap_set_list_record(&format!("prv/{}", prv), &format!("prvi/{}", prv), &ptr);
      let _ = self.tap_set_list_record(&format!("prvcol/{}/{}", prv, col_norm), &format!("prvcoli/{}/{}", prv, col_norm), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/pravth/{}", self.height), &format!("blcki/pravth/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckp/pravth/{}/{}", prv, self.height), &format!("blckpi/pravth/{}/{}", prv, self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckpc/pravth/{}/{}/{}", prv, col_norm, self.height), &format!("blckpci/pravth/{}/{}/{}", prv, col_norm, self.height), &ptr);
      let _ = self.tap_put(&format!("prah/{}", compact_sig), &"".to_string());
      if let Some(bloom) = &self.priv_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
      if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
      let _ = self.tap_put(&format!("kind/{}", inscription_id), &"prvins".to_string());
    }
  }

  pub(crate) fn index_privilege_verify_transferred(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Only execute on transfer (not creation tx)
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }
    if let Some(bloom) = &self.priv_bloom {
      let b = bloom.borrow();
      if b.should_skip_negatives(self.height) { if !b.contains_str(&inscription_id.to_string()) { return; } }
    }
    let Some(path) = self.tap_get::<String>(&format!("prvins/{}", inscription_id)).ok().flatten() else { return; };
    if !path.starts_with("prvins/") { return; }
    let suffix = &path[7..];
    let Some(ptr) = self.tap_get::<String>(&format!("prvvrfd/{}", suffix)).ok().flatten() else { return; };
    let Some(prev) = self.tap_get::<PrivilegeVerifiedRecord>(&ptr).ok().flatten() else { return; };
    if let Some(link) = self.tap_get::<String>(&path).ok().flatten() { if link != inscription_id.to_string() { return; } } else { return; }
    let new_owner = if owner_address.trim() == "-" { BURN_ADDRESS.to_string() } else { owner_address.to_string() };
    let rec = PrivilegeVerifiedRecord { ownr: new_owner, prv: Some(prev.ownr.clone()), name: prev.name.clone(), privf: prev.privf.clone(), col: prev.col.clone(), vrf: prev.vrf.clone(), seq: prev.seq, slt: prev.slt.clone(), blck: self.height, tx: new_satpoint.outpoint.txid.to_string(), vo: u32::from(new_satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: prev.ins.clone(), num: prev.num, ts: self.timestamp };
    if let Ok(list_len) = self.tap_set_list_record("sfprav", "sfpravi", &rec) {
      let ptr2 = format!("sfpravi/{}", list_len - 1);
      let _ = self.tap_put(&format!("prvvrfd/{}", suffix), &ptr2);
      let _ = self.tap_set_list_record(&format!("blck/pravth/{}", self.height), &format!("blcki/pravth/{}", self.height), &ptr2);
      let parts: Vec<&str> = suffix.split('/').collect();
      if parts.len() == 4 {
        let prv = parts[0]; let col_key = parts[1];
        let _ = self.tap_set_list_record(&format!("blckp/pravth/{}/{}", prv, self.height), &format!("blckpi/pravth/{}/{}", prv, self.height), &ptr2);
        let _ = self.tap_set_list_record(&format!("blckpc/pravth/{}/{}/{}", prv, col_key, self.height), &format!("blckpci/pravth/{}/{}/{}", prv, col_key, self.height), &ptr2);
      }
    }
  }
}
