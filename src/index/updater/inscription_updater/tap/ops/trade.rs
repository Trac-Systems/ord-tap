use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_token_trade_created(
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
    let mut json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };

    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let side = json_val.get("side").and_then(|v| v.as_str().or(v.as_i64().map(|n| if n==0 {"0"} else {"1"}))).unwrap_or("").to_string();
    if p != "tap" || op != "token-trade" { return; }
    if side != "0" && side != "1" { return; }

    // Writer parity: side==0 with a trade id present is admitted without
    // requiring tick/amt/accept/valid (used for cancel/unlock flows).
    if side == "0" {
      if let Some(_tid) = json_val.get("trade").and_then(|v| v.as_str()) {
        // Accept accumulator as-is and return
        if inscription_number < 0 && self.tap_feature_enabled(TapFeature::Jubilee) { return; }
        let acc = TapAccumulatorEntry {
          op: "token-trade".to_string(),
          json: json_val.clone(),
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
          let _ = self.tap_set_list_record(&format!("tx/a-t/{}", txs), &format!("txi/a-t/{}", txs), &ptr);
          let _ = self.tap_set_list_record(&format!("blck/a-t/{}", self.height), &format!("blcki/a-t/{}", self.height), &ptr);
        }
        if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
        return;
      }
    }

    let Some(tick_str) = json_val.get("tick").and_then(|v| v.as_str()).map(|s| s.to_string()) else { return; };
    if tick_str.to_lowercase().starts_with('-') && !self.tap_feature_enabled(TapFeature::Jubilee) { return; }
    if !self.validate_trade_main_ticker_len(&tick_str) { return; }

    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) {
      if let Some(v) = json_val.get("amt") { if v.is_number() { return; } }
    }

    if side == "0" {
      let Some(accept_arr) = json_val.get("accept").and_then(|v| v.as_array()) else { return; };
      if accept_arr.is_empty() { return; }
      if json_val.get("valid").is_none() { return; }
      for it in accept_arr {
        let Some(t) = it.get("tick").and_then(|v| v.as_str()) else { return; };
        if let Some(va) = it.get("amt") { if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) && va.is_number() { return; } }
        if !self.validate_trade_accept_ticker_len(t) { return; }
      }
    } else {
      if json_val.get("trade").and_then(|v| v.as_str()).is_none() { return; }
      if json_val.get("amt").is_none() { return; }
      // fee_rcv parity with tap-writer: if present, must be a valid address; normalize
      if let Some(fr) = json_val.get("fee_rcv") {
        let Some(s) = fr.as_str() else { return; };
        let addr_norm = Self::normalize_address(s);
        if !self.is_valid_bitcoin_address(&addr_norm) { return; }
        if let Some(v) = json_val.get_mut("fee_rcv") { *v = serde_json::Value::String(addr_norm); }
      }
    }

    if inscription_number < 0 && !self.tap_feature_enabled(TapFeature::Jubilee) {
      if let Some(v) = json_val.get_mut("tick") {
        if let Some(s) = v.as_str() { *v = serde_json::Value::String(format!("-{}", s)); }
      }
    }

    let acc = TapAccumulatorEntry {
      op: "token-trade".to_string(),
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
      let _ = self.tap_set_list_record(&format!("tx/a-t/{}", txs), &format!("txi/a-t/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/a-t/{}", self.height), &format!("blcki/a-t/{}", self.height), &ptr);
    }
    // Ensure transfer-time execution is not skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
  }

  pub(crate) fn index_token_trade_executed(
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
    if acc.op.to_lowercase() != "token-trade" { return; }

    let side = acc.json.get("side").and_then(|v| v.as_str().or(v.as_i64().map(|n| if n==0 {"0"} else {"1"}))).unwrap_or("");
    if side == "0" {
      if let Some(trade_id) = acc.json.get("trade").and_then(|v| v.as_str()) {
        if let Some(lock) = self.tap_get::<TapAccumulatorEntry>(&format!("tol/{}", trade_id.trim())).ok().flatten() {
          if lock.addr == acc.addr { let _ = self.tap_del(&format!("tol/{}", trade_id.trim())); }
        }
        let _ = self.tap_del(&key);
        return;
      }

      let Some(offer_tick) = acc.json.get("tick").and_then(|v| v.as_str()) else { return; };
      let offer_tick_key = Self::json_stringify_lower(offer_tick);
      if self.tap_get::<DeployRecord>(&format!("d/{}", offer_tick_key)).ok().flatten().is_none() { return; }
      let dec = self.tap_get::<DeployRecord>(&format!("d/{}", offer_tick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
      let Some(accepts) = acc.json.get("accept").and_then(|v| v.as_array()) else { return; };
      let offer_amt_input = acc.json.get("amt").map(|v| if v.is_string() { v.as_str().unwrap().to_string() } else { v.to_string() }).unwrap_or_default();
      if offer_amt_input.is_empty() { return; }
      let offer_amt_norm = match Self::resolve_number_string(&offer_amt_input, dec) { Some(x) => x, None => return };
      let offer_amount = match offer_amt_norm.parse::<i128>() { Ok(v) => v, Err(_) => return };
      if offer_amount <= 0 { return; }

      // Evaluate offer status
      let trf = self.tap_get::<String>(&format!("t/{}/{}", owner_address, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let bal = self.tap_get::<String>(&format!("b/{}/{}", owner_address, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      // Writer parity: accept `valid` as string or number; parse like parseInt
      let mut vld: i64 = -1;
      if let Some(v) = acc.json.get("valid") {
        if let Some(n) = v.as_i64() { vld = n; }
        else if let Some(u) = v.as_u64() { if u <= i64::MAX as u64 { vld = u as i64; } }
        else if let Some(s) = v.as_str() { if let Ok(n) = s.trim().parse::<i64>() { vld = n; } }
      }
      let mut fail = false;
      if bal - trf <= 0 { fail = true; }
      if vld < 0 || (self.height as i64) > vld { vld = -1; fail = true; }

      // Set offer lock (use inscription id as trade id for parity)
      let trade_id = inscription_id.to_string();
      if !fail {
        if self.tap_get::<TapAccumulatorEntry>(&format!("tol/{}", &trade_id)).ok().flatten().is_none() {
          let lock = TapAccumulatorEntry { op: "token-trade-lock".to_string(), json: acc.json.clone(), ins: inscription_id.to_string(), blck: self.height, tx: acc.tx.clone(), vo: acc.vo, num: acc.num, ts: acc.ts, addr: acc.addr.clone() };
          let _ = self.tap_put(&format!("tol/{}", &trade_id), &lock);
        }
      }

      // Persist offers for each accept item (records always written; mapping only when not fail)
      for ac in accepts {
        let Some(atick) = ac.get("tick").and_then(|v| v.as_str()) else { continue; };
        if !self.validate_trade_accept_ticker_len(atick) { continue; }
        let atick_key = Self::json_stringify_lower(atick);
        if self.tap_get::<DeployRecord>(&format!("d/{}", atick_key)).ok().flatten().is_none() { continue; }
        let dec_acc = self.tap_get::<DeployRecord>(&format!("d/{}", atick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
        let aamt_input = ac.get("amt").map(|v| if v.is_string() { v.as_str().unwrap().to_string() } else { v.to_string() }).unwrap_or_default();
        if aamt_input.is_empty() { continue; }
        let aamt_norm = match Self::resolve_number_string(&aamt_input, dec_acc) { Some(x) => x, None => continue };
        let aamt_i = match aamt_norm.parse::<i128>() { Ok(v) => v, Err(_) => continue };
        if aamt_i <= 0 { continue; }

        let rec = TradeOfferRecord { addr: owner_address.to_string(), blck: self.height, tick: offer_tick.to_string(), amt: offer_amount.to_string(), atick: atick.to_string(), aamt: aamt_i.to_string(), vld: vld, trf: trf.to_string(), bal: bal.to_string(), tx: acc.tx.clone(), vo: acc.vo, val: acc.json.get("val").and_then(|v| v.as_str()).unwrap_or("").to_string(), ins: inscription_id.to_string(), num: acc.num, ts: acc.ts, fail };
        // Account offer list
        let list_len = match self.tap_set_list_record(&format!("atrof/{}/{}", owner_address, offer_tick_key), &format!("atrofi/{}/{}", owner_address, offer_tick_key), &rec) { Ok(n) => n, Err(_) => 0 };
        // Ticker-wide offer list
        let _ = self.tap_set_list_record(&format!("fatrof/{}", offer_tick_key), &format!("fatrofi/{}", offer_tick_key), &rec);
        // Global offer list + pointers
        if let Ok(sflen) = self.tap_set_list_record("sfatrof", "sfatrofi", &rec) {
          let sptr = format!("sfatrofi/{}", sflen - 1);
          let txs = acc.tx.clone();
          let _ = self.tap_set_list_record(&format!("tx/to0/{}", txs), &format!("txi/to0/{}", txs), &sptr);
          let _ = self.tap_set_list_record(&format!("txt/to0/{}/{}", offer_tick_key, txs), &format!("txti/to0/{}/{}", offer_tick_key, txs), &sptr);
          let _ = self.tap_set_list_record(&format!("blck/to0/{}", self.height), &format!("blcki/to0/{}", self.height), &sptr);
          let _ = self.tap_set_list_record(&format!("blckt/to0/{}/{}", offer_tick_key, self.height), &format!("blckti/to0/{}/{}", offer_tick_key, self.height), &sptr);
        }
        // Mapping for execution only if not failed
        if !fail && list_len > 0 {
          let ptr = format!("atrofi/{}/{}/{}", owner_address, offer_tick_key, list_len - 1);
          let _ = self.tap_put(&format!("to/{}/{}", trade_id.trim(), atick_key), &ptr);
          let _ = self.tap_set_list_record(&format!("tor/{}", owner_address), &format!("tori/{}", owner_address), &trade_id);
        }
      }
      let _ = self.tap_del(&key);
    } else if side == "1" {
      // In side 1, acc.json.tick is the accepted token
      let Some(accepted_tick) = acc.json.get("tick").and_then(|v| v.as_str()) else { return; };
      let Some(trade_id) = acc.json.get("trade").and_then(|v| v.as_str()) else { return; };
      let accepted_tick_key = Self::json_stringify_lower(accepted_tick);
      if self.tap_get::<DeployRecord>(&format!("d/{}", accepted_tick_key)).ok().flatten().is_none() { return; }
      let dec_acc = self.tap_get::<DeployRecord>(&format!("d/{}", accepted_tick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
      let Some(ptr) = self.tap_get::<String>(&format!("to/{}/{}", trade_id.trim(), accepted_tick_key)).ok().flatten() else { return; };
      if self.tap_get::<TapAccumulatorEntry>(&format!("tol/{}", trade_id.trim())).ok().flatten().is_none() { return; }
      let Some(offer) = self.tap_get::<TradeOfferRecord>(&ptr).ok().flatten() else { return; };
      if offer.addr == acc.addr { return; }
      // Ensure accepted tick matches offer
      if offer.atick.to_lowercase() != accepted_tick.to_lowercase() { return; }
      // Accept numeric JSON for amt before ValueStringifyActivation; require string at/after activation.
      let amount_input: Option<String> = if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) {
        acc.json.get("amt").and_then(|v| v.as_str()).map(|s| s.to_string())
      } else {
        acc.json.get("amt").map(|v| if v.is_string() { v.as_str().unwrap().to_string() } else { v.to_string() })
      };
      let accepted_amount = match amount_input.and_then(|s| Self::resolve_number_string(&s, dec_acc)).and_then(|s| s.parse::<i128>().ok()) { Some(v) => v, None => return };
      let fee_rcv = acc.json.get("fee_rcv").and_then(|v| v.as_str()).map(|s| s.to_string());
      let valid = offer.vld;

      // admission checks
      let amt_str = if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) {
        acc.json.get("amt").and_then(|v| v.as_str()).unwrap_or("").to_string()
      } else {
        acc.json.get("amt").map(|v| if v.is_string() { v.as_str().unwrap().to_string() } else { v.to_string() }).unwrap_or_default()
      };
      let amt_norm = match Self::resolve_number_string(&amt_str, dec_acc) { Some(x) => x, None => return };
      let amount = match amt_norm.parse::<i128>() { Ok(v) => v, Err(_) => return };
      if amount != accepted_amount { return; }

      // balances
      let seller = offer.addr.clone();
      let buyer = acc.addr.clone();
      let offer_tick = offer.tick.clone();
      let offer_tick_key = Self::json_stringify_lower(&offer_tick);
      let seller_bal_off = self.tap_get::<String>(&format!("b/{}/{}", seller, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let buyer_bal_off = self.tap_get::<String>(&format!("b/{}/{}", buyer, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let seller_trf_off = self.tap_get::<String>(&format!("t/{}/{}", seller, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let buyer_bal_acc = self.tap_get::<String>(&format!("b/{}/{}", buyer, accepted_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let seller_bal_acc = self.tap_get::<String>(&format!("b/{}/{}", seller, accepted_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      let buyer_trf_acc = self.tap_get::<String>(&format!("t/{}/{}", buyer, accepted_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);

      // fee calculation
      let mut fee: i128 = 0;
      let mut fee_bal_acc = self.tap_get::<String>(&format!("b/{}/{}", fee_rcv.clone().unwrap_or_default(), accepted_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      if fee_rcv.is_some() {
        let q = accepted_amount / 10000; let r = accepted_amount % 10000; fee = q * 30 + (r * 30) / 10000;
      }

      let mut fail = false;
      if seller_bal_off - offer.amt.parse::<i128>().unwrap_or(0) - seller_trf_off < 0 { fail = true; }
      if buyer_bal_acc - accepted_amount - fee - buyer_trf_acc < 0 { fail = true; }
      if valid >= 0 && (self.height as i64) > valid { fail = true; }

      let txs = new_satpoint.outpoint.txid.to_string();

      if !fail {
        // Seller -> Buyer (offered token)
        let new_buyer_off = buyer_bal_off + offer.amt.parse::<i128>().unwrap_or(0);
        let new_seller_off = seller_bal_off - offer.amt.parse::<i128>().unwrap_or(0);
        let _ = self.tap_put(&format!("b/{}/{}", buyer, offer_tick_key), &new_buyer_off.to_string());
        let _ = self.tap_put(&format!("b/{}/{}", seller, offer_tick_key), &new_seller_off.to_string());
        if self.tap_get::<String>(&format!("he/{}/{}", buyer, offer_tick_key)).ok().flatten().is_none() {
          let _ = self.tap_put(&format!("he/{}/{}", buyer, offer_tick_key), &"".to_string());
          let _ = self.tap_set_list_record(&format!("h/{}", offer_tick_key), &format!("hi/{}", offer_tick_key), &buyer);
        }
        if self.tap_get::<String>(&format!("ato/{}/{}", buyer, offer_tick_key)).ok().flatten().is_none() {
          let tick_lower = serde_json::from_str::<String>(&offer_tick_key).unwrap_or_else(|_| offer.tick.clone());
          let _ = self.tap_set_list_record(&format!("atl/{}", buyer), &format!("atli/{}", buyer), &tick_lower);
          let _ = self.tap_put(&format!("ato/{}/{}", buyer, offer_tick_key), &"".to_string());
        }
        // Buyer -> Seller (accepted token)
        let new_seller_acc = seller_bal_acc + accepted_amount;
        let new_buyer_acc_wo_fee = buyer_bal_acc - accepted_amount;
        let new_buyer_acc = new_buyer_acc_wo_fee - fee;
        let _ = self.tap_put(&format!("b/{}/{}", seller, accepted_tick_key), &new_seller_acc.to_string());
        let _ = self.tap_put(&format!("b/{}/{}", buyer, accepted_tick_key), &new_buyer_acc.to_string());
        if self.tap_get::<String>(&format!("he/{}/{}", seller, accepted_tick_key)).ok().flatten().is_none() {
          let _ = self.tap_put(&format!("he/{}/{}", seller, accepted_tick_key), &"".to_string());
          let _ = self.tap_set_list_record(&format!("h/{}", accepted_tick_key), &format!("hi/{}", accepted_tick_key), &seller);
        }
        if self.tap_get::<String>(&format!("ato/{}/{}", seller, accepted_tick_key)).ok().flatten().is_none() {
          let tick_lower = serde_json::from_str::<String>(&accepted_tick_key).unwrap_or_else(|_| accepted_tick.to_string());
          let _ = self.tap_set_list_record(&format!("atl/{}", seller), &format!("atli/{}", seller), &tick_lower);
          let _ = self.tap_put(&format!("ato/{}/{}", seller, accepted_tick_key), &"".to_string());
        }
        if let Some(rcv) = &fee_rcv {
          fee_bal_acc += fee;
          let _ = self.tap_put(&format!("b/{}/{}", rcv, accepted_tick_key), &fee_bal_acc.to_string());
          if self.tap_get::<String>(&format!("he/{}/{}", rcv, accepted_tick_key)).ok().flatten().is_none() {
            let _ = self.tap_put(&format!("he/{}/{}", rcv, accepted_tick_key), &"".to_string());
            let _ = self.tap_set_list_record(&format!("h/{}", accepted_tick_key), &format!("hi/{}", accepted_tick_key), &rcv);
          }
          if self.tap_get::<String>(&format!("ato/{}/{}", rcv, accepted_tick_key)).ok().flatten().is_none() {
            let tick_lower = serde_json::from_str::<String>(&accepted_tick_key).unwrap_or_else(|_| accepted_tick.to_string());
            let _ = self.tap_set_list_record(&format!("atl/{}", rcv), &format!("atli/{}", rcv), &tick_lower);
            let _ = self.tap_put(&format!("ato/{}/{}", rcv, accepted_tick_key), &"".to_string());
          }
        }
      }

      // Executed transfer logs (writer parity: applyTransferLogs) for both legs (and fee leg when applicable)
      let vo = u32::from(new_satpoint.outpoint.vout);
      let val_str = output_value_sat.to_string();
      // Leg 1: Seller -> Buyer on offered tick
      {
        let sender_bal = if !fail { seller_bal_off - offer.amt.parse::<i128>().unwrap_or(0) } else { seller_bal_off };
        let receiver_bal = if !fail { buyer_bal_off + offer.amt.parse::<i128>().unwrap_or(0) } else { buyer_bal_off };
        let srec = TransferSendSenderRecord {
          addr: seller.clone(),
          taddr: buyer.clone(),
          blck: self.height,
          amt: offer.amt.clone(),
          trf: seller_trf_off.to_string(),
          bal: sender_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("strl/{}/{}", seller, offer_tick_key), &format!("strli/{}/{}", seller, offer_tick_key), &srec);
        let rrec = TransferSendReceiverRecord {
          faddr: seller.clone(),
          addr: buyer.clone(),
          blck: self.height,
          amt: offer.amt.clone(),
          bal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("rstrl/{}/{}", buyer, offer_tick_key), &format!("rstrli/{}/{}", buyer, offer_tick_key), &rrec);
        let frec = TransferSendFlatRecord {
          addr: seller.clone(),
          taddr: buyer.clone(),
          blck: self.height,
          amt: offer.amt.clone(),
          trf: seller_trf_off.to_string(),
          bal: sender_bal.to_string(),
          tbal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("fstrl/{}", offer_tick_key), &format!("fstrli/{}", offer_tick_key), &frec);
        let tick_str = serde_json::from_str::<String>(&offer_tick_key).unwrap_or_else(|_| offer.tick.to_lowercase());
        let sfrec = TransferSendSuperflatRecord {
          tick: tick_str,
          addr: seller.clone(),
          taddr: buyer.clone(),
          blck: self.height,
          amt: offer.amt.clone(),
          trf: seller_trf_off.to_string(),
          bal: sender_bal.to_string(),
          tbal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
          let ptr = format!("sfstrli/{}", list_len - 1);
          let _ = self.tap_set_list_record(&format!("tx/snd/{}", txs), &format!("txi/snd/{}", txs), &ptr);
          let _ = self.tap_set_list_record(&format!("txt/snd/{}/{}", offer_tick_key, txs), &format!("txti/snd/{}/{}", offer_tick_key, txs), &ptr);
          let _ = self.tap_set_list_record(&format!("blck/snd/{}", self.height), &format!("blcki/snd/{}", self.height), &ptr);
          let _ = self.tap_set_list_record(&format!("blckt/snd/{}/{}", offer_tick_key, self.height), &format!("blckti/snd/{}/{}", offer_tick_key, self.height), &ptr);
        }
      }
      // Leg 2: Buyer -> Seller on accepted tick
      {
        let sender_bal = if !fail { buyer_bal_acc - accepted_amount - fee } else { buyer_bal_acc };
        let receiver_bal = if !fail { seller_bal_acc + accepted_amount } else { seller_bal_acc };
        let amt_str = accepted_amount.to_string();
        let srec = TransferSendSenderRecord {
          addr: buyer.clone(),
          taddr: seller.clone(),
          blck: self.height,
          amt: amt_str.clone(),
          trf: buyer_trf_acc.to_string(),
          bal: sender_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("strl/{}/{}", buyer, accepted_tick_key), &format!("strli/{}/{}", buyer, accepted_tick_key), &srec);
        let rrec = TransferSendReceiverRecord {
          faddr: buyer.clone(),
          addr: seller.clone(),
          blck: self.height,
          amt: amt_str.clone(),
          bal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("rstrl/{}/{}", seller, accepted_tick_key), &format!("rstrli/{}/{}", seller, accepted_tick_key), &rrec);
        let frec = TransferSendFlatRecord {
          addr: buyer.clone(),
          taddr: seller.clone(),
          blck: self.height,
          amt: amt_str.clone(),
          trf: buyer_trf_acc.to_string(),
          bal: sender_bal.to_string(),
          tbal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("fstrl/{}", accepted_tick_key), &format!("fstrli/{}", accepted_tick_key), &frec);
        let tick_str = serde_json::from_str::<String>(&accepted_tick_key).unwrap_or_else(|_| accepted_tick.to_lowercase());
        let sfrec = TransferSendSuperflatRecord {
          tick: tick_str,
          addr: buyer.clone(),
          taddr: seller.clone(),
          blck: self.height,
          amt: amt_str,
          trf: buyer_trf_acc.to_string(),
          bal: sender_bal.to_string(),
          tbal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
          let ptr = format!("sfstrli/{}", list_len - 1);
          let _ = self.tap_set_list_record(&format!("tx/snd/{}", txs), &format!("txi/snd/{}", txs), &ptr);
          let _ = self.tap_set_list_record(&format!("txt/snd/{}/{}", accepted_tick_key, txs), &format!("txti/snd/{}/{}", accepted_tick_key, txs), &ptr);
          let _ = self.tap_set_list_record(&format!("blck/snd/{}", self.height), &format!("blcki/snd/{}", self.height), &ptr);
          let _ = self.tap_set_list_record(&format!("blckt/snd/{}/{}", accepted_tick_key, self.height), &format!("blckti/snd/{}/{}", accepted_tick_key, self.height), &ptr);
        }
      }
      // Fee leg: Buyer -> fee_rcv on accepted tick (if fee > 0 and fee_rcv present)
      if fee > 0 { if let Some(rcv) = &fee_rcv {
        let sender_bal = if !fail { buyer_bal_acc - accepted_amount - fee } else { buyer_bal_acc };
        let receiver_bal = fee_bal_acc;
        let fee_amt = fee.to_string();
        let srec = TransferSendSenderRecord {
          addr: buyer.clone(),
          taddr: rcv.clone(),
          blck: self.height,
          amt: fee_amt.clone(),
          trf: buyer_trf_acc.to_string(),
          bal: sender_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("strl/{}/{}", buyer, accepted_tick_key), &format!("strli/{}/{}", buyer, accepted_tick_key), &srec);
        let rrec = TransferSendReceiverRecord {
          faddr: buyer.clone(),
          addr: rcv.clone(),
          blck: self.height,
          amt: fee_amt.clone(),
          bal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("rstrl/{}/{}", rcv, accepted_tick_key), &format!("rstrli/{}/{}", rcv, accepted_tick_key), &rrec);
        let frec = TransferSendFlatRecord {
          addr: buyer.clone(),
          taddr: rcv.clone(),
          blck: self.height,
          amt: fee_amt.clone(),
          trf: buyer_trf_acc.to_string(),
          bal: sender_bal.to_string(),
          tbal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("fstrl/{}", accepted_tick_key), &format!("fstrli/{}", accepted_tick_key), &frec);
        let tick_str = serde_json::from_str::<String>(&accepted_tick_key).unwrap_or_else(|_| accepted_tick.to_lowercase());
        let sfrec = TransferSendSuperflatRecord {
          tick: tick_str,
          addr: buyer.clone(),
          taddr: rcv.clone(),
          blck: self.height,
          amt: fee_amt,
          trf: buyer_trf_acc.to_string(),
          bal: sender_bal.to_string(),
          tbal: receiver_bal.to_string(),
          tx: txs.clone(),
          vo,
          val: val_str.clone(),
          ins: inscription_id.to_string(),
          num: acc.num,
          ts: self.timestamp,
          fail,
          int: true,
          dta: None,
        };
        if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
          let ptr = format!("sfstrli/{}", list_len - 1);
          let _ = self.tap_set_list_record(&format!("tx/snd/{}", txs), &format!("txi/snd/{}", txs), &ptr);
          let _ = self.tap_set_list_record(&format!("txt/snd/{}/{}", accepted_tick_key, txs), &format!("txti/snd/{}/{}", accepted_tick_key, txs), &ptr);
          let _ = self.tap_set_list_record(&format!("blck/snd/{}", self.height), &format!("blcki/snd/{}", self.height), &ptr);
          let _ = self.tap_set_list_record(&format!("blckt/snd/{}/{}", accepted_tick_key, self.height), &format!("blckti/snd/{}/{}", accepted_tick_key, self.height), &ptr);
        }
      }}

      // Records
      let seller_rec = TradeBuySellerRecord {
        addr: buyer.clone(),
        saddr: seller.clone(),
        blck: self.height,
        tick: offer.tick.clone(),
        amt: offer.amt.clone(),
        stick: accepted_tick.to_string(),
        samt: accepted_amount.to_string(),
        fee: fee.to_string(),
        fee_rcv: fee_rcv.clone(),
        tx: txs.clone(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: acc.num,
        sins: offer.ins.clone(),
        snum: offer.num,
        ts: self.timestamp,
        fail,
      };
      // Account seller-trade (buyer perspective)
      if let Ok(list_len) = self.tap_set_list_record(&format!("btrof/{}/{}", buyer, offer_tick_key), &format!("btrofi/{}/{}", buyer, offer_tick_key), &seller_rec) {
        // Pointers for filled offers
        let _ = self.tap_set_list_record(&format!("tx/to1/{}", txs), &format!("txi/to1/{}", txs), &format!("btrofi/{}/{}/{}", buyer, offer_tick_key, list_len - 1));
        let _ = self.tap_set_list_record(&format!("txt/to1/{}/{}", offer_tick_key, txs), &format!("txti/to1/{}/{}", offer_tick_key, txs), &format!("btrofi/{}/{}/{}", buyer, offer_tick_key, list_len - 1));
        let _ = self.tap_set_list_record(&format!("blck/to1/{}", self.height), &format!("blcki/to1/{}", self.height), &format!("btrofi/{}/{}/{}", buyer, offer_tick_key, list_len - 1));
        let _ = self.tap_set_list_record(&format!("blckt/to1/{}/{}", offer_tick_key, self.height), &format!("blckti/to1/{}/{}", offer_tick_key, self.height), &format!("btrofi/{}/{}/{}", buyer, offer_tick_key, list_len - 1));
      }

      let buyer_rec = TradeBuyBuyerRecord {
        baddr: buyer.clone(),
        addr: seller.clone(),
        blck: self.height,
        btick: accepted_tick.to_string(),
        bamt: accepted_amount.to_string(),
        tick: offer.tick.clone(),
        amt: offer.amt.clone(),
        fee: fee.to_string(),
        fee_rcv: fee_rcv.clone(),
        tx: txs.clone(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        bins: inscription_id.to_string(),
        bnum: acc.num,
        ins: offer.ins.clone(),
        num: offer.num,
        ts: self.timestamp,
        fail,
      };
      // Account buyer-trade (seller perspective)
      let _ = self.tap_set_list_record(&format!("rbtrof/{}/{}", seller, accepted_tick_key), &format!("rbtrofi/{}/{}", seller, accepted_tick_key), &buyer_rec);

      // Flat & superflat
      let f_seller_rec = seller_rec.clone();
      let _ = self.tap_set_list_record(&format!("fbtrof/{}", offer_tick_key), &format!("fbtrofi/{}", offer_tick_key), &f_seller_rec);
      let sf_rec = seller_rec.clone();
      if let Ok(_sflen) = self.tap_set_list_record("sfbtrof", "sfbtrofi", &sf_rec) { /* no extra pointers for global filled list beyond set */ }

      // Clear lock and accumulator
      let _ = self.tap_del(&format!("tol/{}", trade_id.trim()));
      let _ = self.tap_del(&key);
    }
  }
}
