use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_token_send_created(
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
    if p != "tap" || op != "token-send" { return; }
    let mut items = match json_val.get("items").and_then(|v| v.as_array()).cloned() { Some(v) if !v.is_empty() => v, _ => return };

    if inscription_number < 0 && self.tap_feature_enabled(TapFeature::Jubilee) { return; }

    for it in items.iter_mut() {
      let tick = match it.get("tick").and_then(|v| v.as_str()) { Some(t) => t.to_string(), None => return };
      let addr_raw = match it.get("address").and_then(|v| v.as_str()) { Some(a) => a, None => return };
      let amt_val = match it.get("amt") { Some(v) => v, None => return };

      if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) {
        if amt_val.is_number() { return; }
      }

      let tick_for_len = Self::strip_prefix_for_len_check(&tick);
      let vis_len = Self::visible_length(tick_for_len);
      if !Self::valid_tap_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, vis_len) { return; }

      let addr_norm = Self::normalize_address(addr_raw);
      if !self.is_valid_bitcoin_address(&addr_norm) { return; }

      if let Some(d) = it.get("dta").and_then(|v| v.as_str()) { if d.as_bytes().len() > 512 { return; } }

      if let Some(addr_field) = it.get_mut("address") { *addr_field = serde_json::Value::String(addr_norm); }
    }

    let mut json_norm = json_val.clone();
    if let Some(items_val) = json_norm.get_mut("items") { *items_val = serde_json::Value::Array(items); }
    let acc = TapAccumulatorEntry {
      op: "token-send".to_string(),
      json: json_norm,
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
      let tx = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/a-snd/{}", tx), &format!("txi/a-snd/{}", tx), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/a-snd/{}", self.height), &format!("blcki/a-snd/{}", self.height), &ptr);
    }
    // Ensure transfer-time execution is not skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
  }

  pub(crate) fn index_token_send_executed(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else { return; };
    if acc.addr != owner_address { return; }
    if acc.op.to_lowercase() != "token-send" { return; }

    let Some(items) = acc.json.get("items").and_then(|v| v.as_array()) else { return; };
    for item in items.iter() {
      let mut ins_data = None;
      if let Some(d) = item.get("dta").and_then(|v| v.as_str()) { if d.as_bytes().len() > 512 { continue; } ins_data = Some(d.to_string()); }
      let Some(tick_str) = item.get("tick").and_then(|v| v.as_str()) else { continue; };
      let Some(receiver) = item.get("address").and_then(|v| v.as_str()) else { continue; };
      let Some(amt_raw) = item.get("amt") else { continue; };
      // Execute using shared helper to preserve parity
      self.exec_internal_send_one(
        owner_address,
        receiver,
        tick_str,
        amt_raw,
        ins_data,
        &inscription_id.to_string(),
        acc.num,
        new_satpoint,
        output_value_sat,
      );
    }

    let _ = self.tap_del(&key);
  }
}
