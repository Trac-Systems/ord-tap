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
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let mut json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };

    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let side = json_val.get("side").and_then(|v| v.as_str().or(v.as_i64().map(|n| if n==0 {"0"} else {"1"}))).unwrap_or("").to_string();
    if p != "tap" || op != "token-trade" { return; }
    if side != "0" && side != "1" { return; }

    let Some(tick_str) = json_val.get("tick").and_then(|v| v.as_str()).map(|s| s.to_string()) else { return; };
    if tick_str.to_lowercase().starts_with('-') && self.height < TAP_JUBILEE_HEIGHT { return; }
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
      if let Some(fee_rcv) = json_val.get("fee_rcv").and_then(|v| v.as_str()) {
        if !Self::is_valid_bitcoin_address_mainnet(fee_rcv.trim()) { return; }
        let norm = Self::normalize_address(fee_rcv);
        if let Some(v) = json_val.get_mut("fee_rcv") { *v = serde_json::Value::String(norm); }
      }
    }

    if inscription_number < 0 && self.height < TAP_JUBILEE_HEIGHT {
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
      if !self.validate_trade_main_ticker_len(offer_tick) { return; }
      let offer_tick_key = Self::json_stringify_lower(offer_tick);
      if self.tap_get::<DeployRecord>(&format!("d/{}", offer_tick_key)).ok().flatten().is_none() { return; }
      let dec = self.tap_get::<DeployRecord>(&format!("d/{}", offer_tick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
      let Some(accepts) = acc.json.get("accept").and_then(|v| v.as_array()) else { return; };
      let Some(offer_amt) = acc.json.get("amt").and_then(|v| if v.is_string() { v.as_str() } else { None }) else { return; };
      let offer_amt_norm = match Self::resolve_number_string(offer_amt, dec) { Some(x) => x, None => return };
      let offer_amount = match offer_amt_norm.parse::<i128>() { Ok(v) => v, Err(_) => return };
      if offer_amount <= 0 { return; }

      let fail = false;
      // Set offer lock
      let trade_id = format!("{}:{}:{}:{}:{}", self.height, acc.tx, acc.vo, inscription_id, owner_address);
      let lock = TapAccumulatorEntry { op: "token-trade-lock".to_string(), json: acc.json.clone(), ins: inscription_id.to_string(), blck: self.height, tx: acc.tx.clone(), vo: acc.vo, num: acc.num, ts: acc.ts, addr: acc.addr.clone() };
      let _ = self.tap_put(&format!("tol/{}", &trade_id), &lock);

      // Persist offers for each accept item
      for ac in accepts {
        let Some(atick) = ac.get("tick").and_then(|v| v.as_str()) else { continue; };
        if !self.validate_trade_accept_ticker_len(atick) { continue; }
        let atick_key = Self::json_stringify_lower(atick);
        if self.tap_get::<DeployRecord>(&format!("d/{}", atick_key)).ok().flatten().is_none() { continue; }
        let dec_acc = self.tap_get::<DeployRecord>(&format!("d/{}", atick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
        let Some(aamt) = ac.get("amt").and_then(|v| if v.is_string() { v.as_str() } else { None }) else { continue; };
        let aamt_norm = match Self::resolve_number_string(aamt, dec_acc) { Some(x) => x, None => continue };
        let aamt_i = match aamt_norm.parse::<i128>() { Ok(v) => v, Err(_) => continue };
        if aamt_i <= 0 { continue; }

        let trf = self.tap_get::<String>(&format!("t/{}/{}", owner_address, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
        let bal = self.tap_get::<String>(&format!("b/{}/{}", owner_address, offer_tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);

        let rec = TradeOfferRecord { addr: owner_address.to_string(), blck: self.height, tick: offer_tick.to_string(), amt: offer_amount.to_string(), atick: atick.to_string(), aamt: aamt_i.to_string(), vld: acc.json.get("valid").and_then(|v| v.as_i64()).unwrap_or(-1), trf: trf.to_string(), bal: bal.to_string(), tx: acc.tx.clone(), vo: acc.vo, val: acc.json.get("val").and_then(|v| v.as_str()).unwrap_or("").to_string(), ins: inscription_id.to_string(), num: acc.num, ts: acc.ts, fail };
        let _ = self.tap_set_list_record(&format!("to/{}/{}", trade_id.trim(), atick_key), &format!("toi/{}/{}", trade_id.trim(), atick_key), &rec);
        let _ = self.tap_set_list_record(&format!("tor/{}", owner_address), &format!("tori/{}", owner_address), &trade_id);
      }
      let _ = self.tap_del(&key);
    } else if side == "1" {
      let Some(offer_tick) = acc.json.get("tick").and_then(|v| v.as_str()) else { return; };
      let Some(trade_id) = acc.json.get("trade").and_then(|v| v.as_str()) else { return; };
      let offer_tick_key = Self::json_stringify_lower(offer_tick);
      if self.tap_get::<DeployRecord>(&format!("d/{}", offer_tick_key)).ok().flatten().is_none() { return; }
      let dec_off = self.tap_get::<DeployRecord>(&format!("d/{}", offer_tick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
      let Some(ptr) = self.tap_get::<String>(&format!("to/{}/{}", trade_id.trim(), offer_tick_key)).ok().flatten() else { return; };
      if self.tap_get::<TapAccumulatorEntry>(&format!("tol/{}", trade_id.trim())).ok().flatten().is_none() { return; }
      let Some(offer) = self.tap_get::<TradeOfferRecord>(&ptr).ok().flatten() else { return; };
      if offer.addr == acc.addr { return; }
      let Some(accepted_tick) = acc.json.get("accept_tick").and_then(|v| v.as_str()) else { return; };
      let accepted_tick_key = Self::json_stringify_lower(accepted_tick);
      if self.tap_get::<DeployRecord>(&format!("d/{}", accepted_tick_key)).ok().flatten().is_none() { return; }
      let accepted_amount = match acc.json.get("amt").and_then(|v| if v.is_string() { v.as_str() } else { None }).and_then(|s| Self::resolve_number_string(s, dec_off)).and_then(|s| s.parse::<i128>().ok()) { Some(v) => v, None => return };
      let fee_rcv = acc.json.get("fee_rcv").and_then(|v| v.as_str()).map(|s| s.to_string());
      let valid = offer.vld;

      // admission checks
      let amt_str = acc.json.get("amt").and_then(|v| v.as_str()).unwrap_or("").to_string();
      let dec_acc = self.tap_get::<DeployRecord>(&format!("d/{}", accepted_tick_key)).ok().flatten().map(|d| d.dec).unwrap_or(18);
      let amt_norm = match Self::resolve_number_string(&amt_str, dec_acc) { Some(x) => x, None => return };
      let amount = match amt_norm.parse::<i128>() { Ok(v) => v, Err(_) => return };
      if amount != accepted_amount { return; }

      // balances
      let seller = offer.addr.clone();
      let buyer = acc.addr.clone();
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

      let _txs = new_satpoint.outpoint.txid.to_string();

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
        tx: new_satpoint.outpoint.txid.to_string(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: acc.num,
        sins: offer.ins.clone(),
        snum: offer.num,
        ts: self.timestamp,
        fail,
      };
      let _ = self.tap_set_list_record(&format!("tbsl/{}", buyer), &format!("tbsli/{}", buyer), &seller_rec);

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
        tx: new_satpoint.outpoint.txid.to_string(),
        vo: u32::from(new_satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        bins: inscription_id.to_string(),
        bnum: acc.num,
        ins: offer.ins.clone(),
        num: offer.num,
        ts: self.timestamp,
        fail,
      };
      let _ = self.tap_set_list_record(&format!("tbbl/{}", seller), &format!("tbbli/{}", seller), &buyer_rec);

      // Flat & superflat
      let f_seller_rec = seller_rec.clone();
      let _ = self.tap_set_list_record(&format!("tbfl/{}", offer_tick_key), &format!("tbfli/{}", offer_tick_key), &f_seller_rec);
      let _tick_str = serde_json::from_str::<String>(&offer_tick_key).unwrap_or_else(|_| offer.tick.clone());
      let sf_rec = seller_rec.clone();
      if let Ok(list_len) = self.tap_set_list_record("tbsfl", "tbsfli", &sf_rec) {
        let ptr = format!("tbsfli/{}", list_len - 1);
        let txs = new_satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(&format!("tx/td/{}", txs), &format!("txi/td/{}", txs), &ptr);
        let _ = self.tap_set_list_record(&format!("txt/td/{}/{}", offer_tick_key, txs), &format!("txti/td/{}/{}", offer_tick_key, txs), &ptr);
        let _ = self.tap_set_list_record(&format!("blck/td/{}", self.height), &format!("blcki/td/{}", self.height), &ptr);
        let _ = self.tap_set_list_record(&format!("blckt/td/{}/{}", offer_tick_key, self.height), &format!("blckti/td/{}/{}", offer_tick_key, self.height), &ptr);
      }

      // Clear lock and accumulator
      let _ = self.tap_del(&format!("tol/{}", trade_id.trim()));
      let _ = self.tap_del(&key);
    }
  }
}
