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
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() {
      return;
    }
    let Some(body) = payload.body() else {
      return;
    };
    let s = String::from_utf8_lossy(body);
    let json_val = match self.parse_tap_json_value(&s) {
      Some(v) => v,
      None => return,
    };
    let p = json_val
      .get("p")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    let op = json_val
      .get("op")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    if p != "tap" || op != "privilege-auth" {
      return;
    }

    if json_val.get("cancel").is_some() {
      if !self.tap_feature_enabled(TapFeature::TapStart) {
        return;
      }
      let acc = TapAccumulatorEntry {
        op: "privilege-auth".to_string(),
        json: json_val.clone(),
        ins: inscription_id.to_string(),
        blck: self.height,
        tx: satpoint.outpoint.txid.to_string(),
        vo: u32::from(satpoint.outpoint.vout),
        val: Some(_output_value_sat.to_string()),
        num: inscription_number,
        ts: self.timestamp,
        addr: owner_address.to_string(),
      };
      let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
      let _ = self.tap_set_list_record(
        &format!("al/{}", owner_address),
        &format!("ali/{}", owner_address),
        &acc,
      );
      if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
        let ptr = format!("ali/{}", list_len - 1);
        let txs = satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(
          &format!("tx/a-prathc/{}", txs),
          &format!("txi/a-prathc/{}", txs),
          &ptr,
        );
        let _ = self.tap_set_list_record(
          &format!("blck/a-prathc/{}", self.height),
          &format!("blcki/a-prathc/{}", self.height),
          &ptr,
        );
      }
      // Ensure transfer-time execution isn't skipped by preflight bloom
      if let Some(bloom) = &self.any_bloom {
        bloom.borrow_mut().insert_str(&inscription_id.to_string());
      }
      return;
    }

    if !self.tap_feature_enabled(TapFeature::PrivilegeActivation) {
      return;
    }
    if !self.tap_feature_enabled(TapFeature::TapStart) {
      return;
    }
    let Some(sig_obj) = json_val.get("sig") else {
      return;
    };
    if !sig_obj.is_object() {
      return;
    }
    if json_val.get("hash").is_none() {
      return;
    }
    if json_val.get("salt").is_none() {
      return;
    }
    if !json_val.get("auth").map(|v| v.is_object()).unwrap_or(false) {
      return;
    }
    if json_val
      .get("auth")
      .and_then(|v| v.get("name"))
      .and_then(|v| v.as_str())
      .is_none()
    {
      return;
    }

    let acc = TapAccumulatorEntry {
      op: "privilege-auth".to_string(),
      json: json_val,
      ins: inscription_id.to_string(),
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: Some(_output_value_sat.to_string()),
      num: inscription_number,
      ts: self.timestamp,
      addr: owner_address.to_string(),
    };
    let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
    let _ = self.tap_set_list_record(
      &format!("al/{}", owner_address),
      &format!("ali/{}", owner_address),
      &acc,
    );
    if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
      let ptr = format!("ali/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(
        &format!("tx/a-prath/{}", txs),
        &format!("txi/a-prath/{}", txs),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/a-prath/{}", self.height),
        &format!("blcki/a-prath/{}", self.height),
        &ptr,
      );
    }
    // Ensure transfer-time execution isn't skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom {
      bloom.borrow_mut().insert_str(&inscription_id.to_string());
    }
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
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() {
      return;
    }
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else {
      return;
    };
    if acc.addr != owner_address {
      return;
    }
    if acc.op.to_lowercase() != "privilege-auth" {
      return;
    }
    macro_rules! delete_acc_and_return {
      () => {{
        let _ = self.tap_del(&key);
        return;
      }};
    }

    if let Some(cancel_val) = acc.json.get("cancel") {
      let cancel_id = Self::js_value_to_string(cancel_val);
      if let Some(ptr) = self
        .tap_get::<String>(&format!("prains/{}", cancel_id))
        .ok()
        .flatten()
      {
        if let Some(link_rec) = self
          .tap_get::<super::super::PrivilegeAuthCreateRecord>(&ptr)
          .ok()
          .flatten()
        {
          if link_rec.addr != owner_address {
            delete_acc_and_return!();
          }
          if self
            .tap_get::<String>(&format!("prac/{}", link_rec.ins))
            .ok()
            .flatten()
            .is_none()
          {
            let _ = self.tap_put(&format!("prac/{}", link_rec.ins), &"".to_string());
          }
        }
      }
      let _ = self.tap_del(&key);
      return;
    }

    let Some(sig_obj) = acc.json.get("sig") else {
      delete_acc_and_return!();
    };
    let Some(hash_str) = acc.json.get("hash").and_then(|v| v.as_str()) else {
      delete_acc_and_return!();
    };
    let Some(salt_val) = acc.json.get("salt") else {
      delete_acc_and_return!();
    };
    let Some(auth_obj) = acc.json.get("auth") else {
      delete_acc_and_return!();
    };
    let Some(name_str) = auth_obj.get("name").and_then(|v| v.as_str()) else {
      delete_acc_and_return!();
    };
    let name_vis = Self::visible_length(name_str);
    if name_vis == 0 || name_vis > 512 {
      delete_acc_and_return!();
    }
    // Build message and verify signature
    let salt_str = Self::js_value_to_string(salt_val);
    let msg_hash = Self::build_sha256_json_plus_salt(auth_obj, &salt_str);
    let Some((ok, compact_sig, _pubkey_hex)) =
      self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
    else {
      delete_acc_and_return!();
    };
    if !ok {
      delete_acc_and_return!();
    }
    if self
      .tap_get::<String>(&format!("prah/{}", compact_sig))
      .ok()
      .flatten()
      .is_some()
    {
      delete_acc_and_return!();
    }

    // Persist owner and mark signature used
    let _ = self.tap_put(
      &format!("prao/{}", inscription_id),
      &owner_address.to_string(),
    );
    let _ = self.tap_put(&format!("prah/{}", compact_sig), &"".to_string());
    // Emit typed record for privilege-auth create (sfpra/pra lists)
    let rec = super::super::PrivilegeAuthCreateRecord {
      addr: owner_address.to_string(),
      auth: auth_obj.clone(),
      sig: sig_obj.clone(),
      hash: hash_str.to_string(),
      slt: salt_str,
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
    } else {
      None
    };
    if let Some(ptr) = ptr {
      let txs = new_satpoint.outpoint.txid.to_string();
      // Writer keys: prath (privilege-auth create)
      let _ = self.tap_set_list_record(
        &format!("tx/prath/{}", txs),
        &format!("txi/prath/{}", txs),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/prath/{}", self.height),
        &format!("blcki/prath/{}", self.height),
        &ptr,
      );
    }
    // Account-scoped list, track pointer for prains mapping
    if let Ok(acc_len) = self.tap_set_list_record(
      &format!("pra/{}", owner_address),
      &format!("prai/{}", owner_address),
      &rec,
    ) {
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
    let Some(body) = payload.body() else {
      return;
    };
    let s = String::from_utf8_lossy(body);
    let json_val = match self.parse_tap_json_value(&s) {
      Some(v) => v,
      None => return,
    };
    let p = json_val
      .get("p")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    let op = json_val
      .get("op")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    if p != "tap" || op != "privilege-auth" {
      return;
    }
    if !self.tap_feature_enabled(TapFeature::TapStart) {
      return;
    }

    let sig_obj = match json_val.get("sig") {
      Some(v) if v.is_object() => v,
      _ => return,
    };
    // Pubkey recovery must use the provided `hash` field (32-byte hex),
    // matching tap-writer's VerifyPrivilegeAuth parser.
    let hash_str = match json_val.get("hash").and_then(|v| v.as_str()) {
      Some(v) => v,
      None => return,
    };
    let prv = match json_val.get("prv").and_then(|v| v.as_str()) {
      Some(v) => v,
      None => return,
    };
    {
      if !Self::writer_loose_inscription_id_syntax(prv) {
        return;
      }
    }
    let verify = match json_val.get("verify").and_then(|v| v.as_str()) {
      Some(v) => v,
      None => return,
    };
    if !Self::js_word_boundary_hex64_test(verify) {
      return;
    }
    let col_raw = match json_val.get("col").and_then(|v| v.as_str()) {
      Some(v) => v,
      None => return,
    };
    let mut col_norm = col_raw.to_string();
    let col_len = Self::visible_length(&col_norm);
    if col_len > 512 {
      return;
    }
    if col_len == 0 {
      col_norm = "-".to_string();
    }
    let addr_field = match json_val.get("address").and_then(|v| v.as_str()) {
      Some(v) => v,
      None => return,
    };
    let seq_val = match json_val.get("seq") {
      Some(v) => v,
      None => return,
    };
    let Some((seq_parsed, seq_str)) = Self::js_parse_int_with_string(seq_val) else {
      return;
    };
    if seq_parsed.to_string() != seq_str {
      return;
    }
    if seq_parsed < 0 || seq_parsed > 9_007_199_254_740_991 {
      return;
    }
    let seq_i = i64::try_from(seq_parsed).ok().unwrap();
    let salt = match json_val.get("salt") {
      Some(v) => Self::js_value_to_string(v),
      None => return,
    };
    let col_key = Self::js_json_stringify(&serde_json::Value::String(col_norm.clone()));

    // Verify signature and authority link parity (writer behavior)
    let msg_hash =
      Self::build_sha256_privilege_verify(prv, &col_norm, verify, &seq_str, addr_field, &salt);
    let Some((is_valid, compact_sig, pubkey_hex)) =
      self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
    else {
      return;
    };
    if !is_valid {
      return;
    }
    if self
      .tap_get::<String>(&format!("prah/{}", compact_sig))
      .ok()
      .flatten()
      .is_some()
    {
      return;
    }
    // Duplicate verification guard
    if self
      .tap_get::<String>(&format!(
        "prvvrfd/{}/{}/{}/{}",
        prv, col_key, verify, seq_str
      ))
      .ok()
      .flatten()
      .is_some()
    {
      return;
    }
    // Require that JSON address equals inscription owner (parity with writer)
    if addr_field != owner_address {
      return;
    }
    // Load authority link and validate its signature; ensure not cancelled
    let Some(link_ptr) = self
      .tap_get::<String>(&format!("prains/{}", prv))
      .ok()
      .flatten()
    else {
      return;
    };
    if self
      .tap_get::<String>(&format!("prac/{}", prv))
      .ok()
      .flatten()
      .is_some()
    {
      return;
    }
    let mut auth_name: Option<String> = None;
    let mut link_ok = false;
    if let Some(link_rec) = self
      .tap_get::<super::super::PrivilegeAuthCreateRecord>(&link_ptr)
      .ok()
      .flatten()
    {
      // Recover pubkey from authority link
      let sig = &link_rec.sig;
      let v2i = match sig.get("v").and_then(Self::js_parse_int_i32) {
        Some(v) => v,
        None => return,
      };
      let r2b = match sig.get("r").and_then(Self::js_bigint_value_to_32) {
        Some(v) => v,
        None => return,
      };
      let s2b = match sig.get("s").and_then(Self::js_bigint_value_to_32) {
        Some(v) => v,
        None => return,
      };
      let rec_hash2 = match hex::decode(link_rec.hash.trim_start_matches("0x")).ok() {
        Some(v) => v,
        None => return,
      };
      if rec_hash2.len() != 32 {
        return;
      }
      let mut rec2_arr = [0u8; 32];
      rec2_arr.copy_from_slice(&rec_hash2);
      let recid2 = match secp256k1::ecdsa::RecoveryId::from_i32(v2i)
        .or_else(|_| secp256k1::ecdsa::RecoveryId::from_i32(v2i - 27))
      {
        Ok(v) => v,
        Err(_) => return,
      };
      let mut sig2b = [0u8; 64];
      sig2b[..32].copy_from_slice(&r2b);
      sig2b[32..].copy_from_slice(&s2b);
      let rsig2 = match secp256k1::ecdsa::RecoverableSignature::from_compact(&sig2b, recid2) {
        Ok(v) => v,
        Err(_) => return,
      };
      let rmsg2 = match secp256k1::Message::from_digest_slice(&rec2_arr) {
        Ok(v) => v,
        Err(_) => return,
      };
      let secp = secp256k1::Secp256k1::new();
      let auth_pk = match secp.recover_ecdsa(&rmsg2, &rsig2) {
        Ok(v) => v,
        Err(_) => return,
      };
      // Validate link signature itself: sha256(JSON.stringify(link.auth) + link.slt)
      let auth_msg_hash = Self::build_sha256_json_plus_salt(&link_rec.auth, &link_rec.slt);
      let nsig2 = match secp256k1::ecdsa::Signature::from_compact(&sig2b) {
        Ok(v) => v,
        Err(_) => return,
      };
      let vmsg2 = match secp256k1::Message::from_digest_slice(&auth_msg_hash) {
        Ok(v) => v,
        Err(_) => return,
      };
      let auth_ok = secp.verify_ecdsa(&vmsg2, &nsig2, &auth_pk).is_ok();
      let auth_pk_hex = hex::encode(auth_pk.serialize_uncompressed());
      // pubkey recovered from verify must equal authority pubkey
      if auth_ok && auth_pk_hex == pubkey_hex {
        link_ok = true;
      }
      // Name comes from authority link's auth.name
      auth_name = link_rec
        .auth
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    }
    if !link_ok {
      return;
    }

    // Persist verification with authority name (writer uses link.auth.name; no fallback)
    let name_field = match auth_name {
      Some(n) => n,
      None => return,
    };
    let rec = PrivilegeVerifiedRecord {
      ownr: owner_address.to_string(),
      prv: None,
      name: name_field,
      privf: prv.to_string(),
      col: col_norm.clone(),
      vrf: verify.to_string(),
      seq: seq_i,
      slt: salt,
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
    };
    if let Ok(list_len) = self.tap_set_list_record("sfprav", "sfpravi", &rec) {
      let ptr = format!("sfpravi/{}", list_len - 1);
      let _ = self.tap_put(
        &format!("prvvrfd/{}/{}/{}/{}", prv, col_key, verify, seq_str),
        &ptr,
      );
      let _ = self.tap_put(
        &format!("prvins/{}/{}/{}/{}", prv, col_key, verify, seq_str),
        &inscription_id.to_string(),
      );
      let _ = self.tap_put(
        &format!("prvins/{}", inscription_id),
        &format!("prvins/{}/{}/{}/{}", prv, col_key, verify, seq_str),
      );
      let _ = self.tap_set_list_record(&format!("prv/{}", prv), &format!("prvi/{}", prv), &ptr);
      let _ = self.tap_set_list_record(
        &format!("prvcol/{}/{}", prv, col_key),
        &format!("prvcoli/{}/{}", prv, col_key),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/pravth/{}", self.height),
        &format!("blcki/pravth/{}", self.height),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckp/pravth/{}/{}", prv, self.height),
        &format!("blckpi/pravth/{}/{}", prv, self.height),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckpc/pravth/{}/{}/{}", prv, col_key, self.height),
        &format!("blckpci/pravth/{}/{}/{}", prv, col_key, self.height),
        &ptr,
      );
      let _ = self.tap_put(&format!("prah/{}", compact_sig), &"".to_string());
      if let Some(bloom) = &self.priv_bloom {
        bloom.borrow_mut().insert_str(&inscription_id.to_string());
      }
      if let Some(bloom) = &self.any_bloom {
        bloom.borrow_mut().insert_str(&inscription_id.to_string());
      }
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
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() {
      return;
    }
    if !self.tap_feature_enabled(TapFeature::TapStart) {
      return;
    }
    let Some(path) = self
      .tap_get::<String>(&format!("prvins/{}", inscription_id))
      .ok()
      .flatten()
    else {
      return;
    };
    if !path.starts_with("prvins/") {
      return;
    }
    let suffix = &path[7..];
    let Some(ptr) = self
      .tap_get::<String>(&format!("prvvrfd/{}", suffix))
      .ok()
      .flatten()
    else {
      return;
    };
    let Some(prev) = self.tap_get::<PrivilegeVerifiedRecord>(&ptr).ok().flatten() else {
      return;
    };
    if let Some(link) = self.tap_get::<String>(&path).ok().flatten() {
      if link != inscription_id.to_string() {
        return;
      }
    } else {
      return;
    }
    let new_owner = if Self::trim_js_whitespace(owner_address) == "-" {
      BURN_ADDRESS.to_string()
    } else {
      owner_address.to_string()
    };
    let rec = PrivilegeVerifiedRecord {
      ownr: new_owner,
      prv: Some(prev.ownr.clone()),
      name: prev.name.clone(),
      privf: prev.privf.clone(),
      col: prev.col.clone(),
      vrf: prev.vrf.clone(),
      seq: prev.seq,
      slt: prev.slt.clone(),
      blck: self.height,
      tx: new_satpoint.outpoint.txid.to_string(),
      vo: u32::from(new_satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: prev.ins.clone(),
      num: prev.num,
      ts: self.timestamp,
    };
    if let Ok(list_len) = self.tap_set_list_record("sfprav", "sfpravi", &rec) {
      let ptr2 = format!("sfpravi/{}", list_len - 1);
      let _ = self.tap_put(&format!("prvvrfd/{}", suffix), &ptr2);
      let _ = self.tap_set_list_record(
        &format!("blck/pravth/{}", self.height),
        &format!("blcki/pravth/{}", self.height),
        &ptr2,
      );
      if let Some((prv, rest)) = suffix.split_once('/') {
        if let Some((col_and_verify, _seq)) = rest.rsplit_once('/') {
          if let Some((col_key, _verify)) = col_and_verify.rsplit_once('/') {
            let _ = self.tap_set_list_record(
              &format!("blckp/pravth/{}/{}", prv, self.height),
              &format!("blckpi/pravth/{}/{}", prv, self.height),
              &ptr2,
            );
            let _ = self.tap_set_list_record(
              &format!("blckpc/pravth/{}/{}/{}", prv, col_key, self.height),
              &format!("blckpci/pravth/{}/{}/{}", prv, col_key, self.height),
              &ptr2,
            );
          }
        }
      }
    }
  }
}
