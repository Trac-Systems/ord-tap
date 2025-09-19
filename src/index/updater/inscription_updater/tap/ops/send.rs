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
      if !Self::valid_tap_ticker_visible_len(self.height, vis_len) { return; }

      let addr_norm = Self::normalize_address(addr_raw);
      if !Self::is_valid_bitcoin_address_mainnet(&addr_norm) { return; }

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
      let tick_key = Self::json_stringify_lower(&tick_str);
      let d_key = format!("d/{}", tick_key);
      let Some(deployed) = self.tap_get::<DeployRecord>(&d_key).ok().flatten() else { continue; };
      let decimals = deployed.dec;

      let Some(receiver) = item.get("address").and_then(|v| v.as_str()) else { continue; };
      let receiver = receiver.to_string();
      if Self::normalize_address(&receiver) != receiver { continue; }
      if !Self::is_valid_bitcoin_address_mainnet(&receiver) { continue; }

      let Some(amt_raw) = item.get("amt") else { continue; };
      let amt_input = if amt_raw.is_string() { amt_raw.as_str().unwrap().to_string() } else { amt_raw.to_string() };
      let amt_norm = match Self::resolve_number_string(&amt_input, decimals) { Some(x) => x, None => continue };
      let amount_i = match amt_norm.parse::<i128>() { Ok(v) => v, Err(_) => continue };
      if amount_i <= 0 { continue; }

      let sender = owner_address.to_string();
      let bal_key = format!("b/{}/{}", sender, tick_key);
      let Some(balance_s) = self.tap_get::<String>(&bal_key).ok().flatten() else { continue; };
      let mut balance = balance_s.parse::<i128>().unwrap_or(0);
      let mut transferable = self.tap_get::<String>(&format!("t/{}/{}", sender, tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let effective_new_balance = balance - amount_i;
      let mut fail = false;
      if effective_new_balance < 0 || transferable - amount_i < 0 { fail = true; }
      if !fail {
        balance = effective_new_balance;
        transferable -= amount_i;
      }

      let recv_bal_key = format!("b/{}/{}", receiver, tick_key);
      let receiver_balance_current = self.tap_get::<String>(&recv_bal_key).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let receiver_balance = if fail { receiver_balance_current } else { receiver_balance_current + amount_i };
      if !fail {
        let _ = self.tap_put(&bal_key, &balance.to_string());
        let _ = self.tap_put(&recv_bal_key, &receiver_balance.to_string());
        if self.tap_get::<String>(&format!("he/{}/{}", receiver, tick_key)).ok().flatten().is_none() {
          let _ = self.tap_put(&format!("he/{}/{}", receiver, tick_key), &"".to_string());
          let _ = self.tap_set_list_record(&format!("h/{}", tick_key), &format!("hi/{}", tick_key), &receiver);
        }
      }

      let srec = TransferSendSenderRecord {
        addr: sender.clone(),
        taddr: receiver.clone(),
        blck: self.height,
        amt: amount_i.to_string(),
        trf: transferable.to_string(),
        bal: balance.to_string(),
        tx: new_satpoint.outpoint.txid.to_string(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: acc.num,
        ts: self.timestamp,
        fail,
        int: true,
        dta: ins_data.clone(),
      };
      let _ = self.tap_set_list_record(&format!("strl/{}/{}", sender, tick_key), &format!("strli/{}/{}", sender, tick_key), &srec);

      let rrec = TransferSendReceiverRecord {
        faddr: sender.clone(),
        addr: receiver.clone(),
        blck: self.height,
        amt: amount_i.to_string(),
        bal: receiver_balance.to_string(),
        tx: new_satpoint.outpoint.txid.to_string(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: acc.num,
        ts: self.timestamp,
        fail,
        int: true,
        dta: ins_data.clone(),
      };
      let _ = self.tap_set_list_record(&format!("rstrl/{}/{}", receiver, tick_key), &format!("rstrli/{}/{}", receiver, tick_key), &rrec);

      let frec = TransferSendFlatRecord {
        addr: sender.clone(),
        taddr: receiver.clone(),
        blck: self.height,
        amt: amount_i.to_string(),
        trf: transferable.to_string(),
        bal: balance.to_string(),
        tbal: receiver_balance.to_string(),
        tx: new_satpoint.outpoint.txid.to_string(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: acc.num,
        ts: self.timestamp,
        fail,
        int: true,
        dta: ins_data.clone(),
      };
      let _ = self.tap_set_list_record(&format!("fstrl/{}", tick_key), &format!("fstrli/{}", tick_key), &frec);

      let tick_label = serde_json::from_str::<String>(&tick_key).unwrap_or_else(|_| tick_str.to_lowercase());
      let sfrec = TransferSendSuperflatRecord {
        tick: tick_label,
        addr: sender,
        taddr: receiver,
        blck: self.height,
        amt: amount_i.to_string(),
        trf: transferable.to_string(),
        bal: balance.to_string(),
        tbal: receiver_balance.to_string(),
        tx: new_satpoint.outpoint.txid.to_string(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: acc.num,
        ts: self.timestamp,
        fail,
        int: true,
        dta: ins_data,
      };
      if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
        let ptr = format!("sfstrli/{}", list_len - 1);
        let txs = new_satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(&format!("tx/snd/{}", txs), &format!("txi/snd/{}", txs), &ptr);
        let _ = self.tap_set_list_record(&format!("txt/snd/{}/{}", tick_key, txs), &format!("txti/snd/{}/{}", tick_key, txs), &ptr);
        let _ = self.tap_set_list_record(&format!("blck/snd/{}", self.height), &format!("blcki/snd/{}", self.height), &ptr);
        let _ = self.tap_set_list_record(&format!("blckt/snd/{}/{}", tick_key, self.height), &format!("blckti/snd/{}/{}", tick_key, self.height), &ptr);
      }
    }

    let _ = self.tap_del(&key);
  }
}
