use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_block_transferables_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };
    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    if p != "tap" || op != "block-transferables" { return; }
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() { /* creation tx */ }
    let acc = TapAccumulatorEntry {
      op: "block-transferables".to_string(),
      json: json_val,
      ins: inscription_id.to_string(),
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      num: inscription_number,
      ts: self.timestamp,
      addr: owner_address.to_string(),
    };
    let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
    let _ = self.tap_set_list_record(&format!("al/{}", owner_address), &format!("ali/{}", owner_address), &acc);
    if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
      let ptr = format!("ali/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/a-athc/{}", txs), &format!("txi/a-athc/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/a-athc/{}", self.height), &format!("blcki/a-athc/{}", self.height), &ptr);
    }
    // Ensure transfer-time execution is not skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
  }

  pub(crate) fn index_block_transferables_executed(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    _new_satpoint: SatPoint,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else { return; };
    if acc.addr != owner_address { return; }
    if acc.op.to_lowercase() != "block-transferables" { return; }
    if self.tap_get::<String>(&format!("bltr/{}", owner_address)).ok().flatten().is_none() {
      let _ = self.tap_put(&format!("bltr/{}", owner_address), &"".to_string());
    }
    let _ = self.tap_del(&key);
  }

  pub(crate) fn index_unblock_transferables_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };
    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    if p != "tap" || op != "unblock-transferables" { return; }
    let acc = TapAccumulatorEntry {
      op: "unblock-transferables".to_string(),
      json: json_val,
      ins: inscription_id.to_string(),
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      num: inscription_number,
      ts: self.timestamp,
      addr: owner_address.to_string(),
    };
    let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
    let _ = self.tap_set_list_record(&format!("al/{}", owner_address), &format!("ali/{}", owner_address), &acc);
    if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
      let ptr = format!("ali/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/a-athc/{}", txs), &format!("txi/a-athc/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/a-athc/{}", self.height), &format!("blcki/a-athc/{}", self.height), &ptr);
    }
    // Ensure transfer-time execution is not skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
  }

  pub(crate) fn index_unblock_transferables_executed(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    _new_satpoint: SatPoint,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else { return; };
    if acc.addr != owner_address { return; }
    if acc.op.to_lowercase() != "unblock-transferables" { return; }
    if self.tap_get::<String>(&format!("bltr/{}", owner_address)).ok().flatten().is_some() {
      let _ = self.tap_del(&format!("bltr/{}", owner_address));
    }
    let _ = self.tap_del(&key);
  }
}
