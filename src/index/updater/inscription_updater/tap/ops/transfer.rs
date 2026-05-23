use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_token_transfer_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    output_value_sat: u64,
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
    let mut tick = json_val
      .get("tick")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let amt_raw = json_val.get("amt").cloned();
    if p != "tap" || op != "token-transfer" || tick.is_empty() || amt_raw.is_none() {
      return;
    }

    if Self::js_to_lowercase(&tick).starts_with('-')
      && !self.tap_feature_enabled(TapFeature::Jubilee)
    {
      return;
    }

    let vis_len = Self::visible_length(&tick);
    if !Self::valid_transfer_ticker_visible_len(
      self.feature_height(TapFeature::FullTicker),
      self.height,
      self.feature_height(TapFeature::Jubilee),
      &tick,
      vis_len,
    ) {
      return;
    }

    let mut ins_data: Option<String> = None;
    if let Some(dta) = json_val.get("dta").and_then(|v| v.as_str()) {
      if dta.as_bytes().len() > 512 {
        return;
      }
      ins_data = Some(dta.to_string());
    }

    if inscription_number < 0 {
      if !self.tap_feature_enabled(TapFeature::Jubilee) {
        tick = format!("-{}", tick);
      } else {
        return;
      }
    }

    if self
      .tap_get::<String>(&format!("bltr/{}", owner_address))
      .ok()
      .flatten()
      .is_some()
    {
      return;
    }

    let tick_key = Self::json_stringify_lower(&tick);
    let d_key = format!("d/{}", tick_key);
    let deployed = match self.tap_get::<DeployRecord>(&d_key).ok().flatten() {
      Some(d) => d,
      None => return,
    };
    let bal_key = format!("b/{}/{}", owner_address, tick_key);
    let tokens_left: u128 = match self
      .tap_get::<String>(&bal_key)
      .ok()
      .flatten()
      .and_then(|s| s.parse::<u128>().ok())
    {
      Some(v) => v,
      None => return,
    };
    let dec = deployed.dec;
    let amt_input = if let Some(a) = &amt_raw {
      Self::js_value_to_string(a)
    } else {
      return;
    };
    let amt_norm = match Self::resolve_number_string(&amt_input, dec) {
      Some(x) => x,
      None => return,
    };
    let Some(amount_big) = num_bigint::BigUint::parse_bytes(amt_norm.as_bytes(), 10) else {
      return;
    };

    let tr_key = format!("t/{}/{}", owner_address, tick_key);
    let transferable: u128 = self
      .tap_get::<String>(&tr_key)
      .ok()
      .flatten()
      .and_then(|s| s.parse::<u128>().ok())
      .unwrap_or(0);
    // START TAP-PROOFS
    // Locked and obligation-reserved balances are not available for new transferable inscriptions after activation.
    let locked: u128 =
      u128::try_from(self.tap_get_locked_amount(owner_address, &tick_key)).unwrap_or(0);
    let obligation_locked: u128 =
      u128::try_from(self.tap_get_account_obligation_locked_amount(owner_address, &tick_key))
        .unwrap_or(0);
    // END TAP-PROOFS

    let transferable_big = num_bigint::BigUint::from(transferable);
    let locked_big = num_bigint::BigUint::from(locked);
    let obligation_locked_big = num_bigint::BigUint::from(obligation_locked);
    let tokens_left_big = num_bigint::BigUint::from(tokens_left);
    let fail =
      &transferable_big + &locked_big + &obligation_locked_big + &amount_big > tokens_left_big;

    let new_transferable = if !fail {
      let bytes = amount_big.to_bytes_be();
      if bytes.len() > 16 {
        return;
      }
      let mut amount_u128 = 0u128;
      for byte in bytes {
        amount_u128 = (amount_u128 << 8) | u128::from(byte);
      }
      transferable.saturating_add(amount_u128)
    } else {
      transferable
    };
    if !fail {
      let _ = self.tap_put(&tr_key, &new_transferable.to_string());
      let _ = self.tap_put(&format!("tamt/{}", inscription_id), &amount_big.to_string());
    }

    // Writer parity: trf stores post-add transferable on success; pre-add on fail
    let trf_str = if !fail {
      new_transferable.to_string()
    } else {
      transferable.to_string()
    };
    let atr = TransferInitRecord {
      addr: owner_address.to_string(),
      blck: self.height,
      amt: amount_big.to_string(),
      trf: trf_str.clone(),
      bal: tokens_left.to_string(),
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
      fail,
      int: false,
      dta: ins_data.clone(),
    };
    if let Ok(list_len) = self.tap_set_list_record(
      &format!("atrl/{}/{}", owner_address, tick_key),
      &format!("atrli/{}/{}", owner_address, tick_key),
      &atr,
    ) {
      // Only create a transfer link on success
      if !fail {
        let ptr = format!("atrli/{}/{}/{}", owner_address, tick_key, list_len - 1);
        let _ = self.tap_put(&format!("tl/{}", inscription_id), &ptr);
        let _ = self.tap_put(&format!("kind/{}", inscription_id), &"tl".to_string());
      }
    }

    let ftr = TransferInitFlatRecord {
      addr: owner_address.to_string(),
      blck: self.height,
      amt: amount_big.to_string(),
      trf: trf_str.clone(),
      bal: tokens_left.to_string(),
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
      fail,
      int: false,
      dta: ins_data.clone(),
    };
    let _ = self.tap_set_list_record(
      &format!("ftrl/{}", tick_key),
      &format!("ftrli/{}", tick_key),
      &ftr,
    );

    let sftr = TransferInitSuperflatRecord {
      tick: Self::js_to_lowercase(&tick),
      addr: owner_address.to_string(),
      blck: self.height,
      amt: amount_big.to_string(),
      trf: trf_str,
      bal: tokens_left.to_string(),
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
      fail,
      int: false,
      dta: ins_data,
    };
    if let Ok(list_len) = self.tap_set_list_record("sftrl", "sftrli", &sftr) {
      let ptr = format!("sftrli/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(
        &format!("tx/trf/{}", txs),
        &format!("txi/trf/{}", txs),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("txt/trf/{}/{}", tick_key, txs),
        &format!("txti/trf/{}/{}", tick_key, txs),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/trf/{}", self.height),
        &format!("blcki/trf/{}", self.height),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckt/trf/{}/{}", tick_key, self.height),
        &format!("blckti/trf/{}/{}", tick_key, self.height),
        &ptr,
      );
    }
  }

  pub(crate) fn index_token_transfer_executed(
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
    let Some(ptr) = self
      .tap_get::<String>(&format!("tl/{}", inscription_id))
      .ok()
      .flatten()
    else {
      return;
    };
    if ptr.is_empty() {
      return;
    }
    let Some(atr) = self.tap_get::<TransferInitRecord>(&ptr).ok().flatten() else {
      return;
    };

    let Some((ptr_without_index, index)) = ptr.rsplit_once('/') else {
      return;
    };
    if index.parse::<usize>().is_err() {
      return;
    }
    let Some(ptr_without_prefix) = ptr_without_index.strip_prefix("atrli/") else {
      return;
    };
    let Some((ptr_sender, tick_key)) = ptr_without_prefix.split_once('/') else {
      return;
    };
    if ptr_sender != atr.addr || tick_key.is_empty() {
      return;
    }
    let tick_key = tick_key.to_string();

    if atr.tx == new_satpoint.outpoint.txid.to_string() {
      return;
    }

    if self
      .tap_get::<DeployRecord>(&format!("d/{}", tick_key))
      .ok()
      .flatten()
      .is_none()
    {
      return;
    }

    let sender = atr.addr.clone();
    let receiver = owner_address.to_string();
    // START MINER-REWARD-SHIELD
    if self.tap_blocks_dmt_reward_transfer_execution(&sender) {
      if receiver != sender {
        let transfer_key = format!("t/{}/{}", sender, tick_key);
        if let Some(transferable_s) = self.tap_get::<String>(&transfer_key).ok().flatten() {
          let transferable = transferable_s.parse::<i128>().unwrap_or(0);
          let amount = atr.amt.parse::<i128>().unwrap_or(0);
          let new_transferable = (transferable - amount).max(0);
          let _ = self.tap_put(&transfer_key, &new_transferable.to_string());
        } else {
          let _ = self.tap_del(&transfer_key);
        }
        let _ = self.tap_put(&format!("tamt/{}", inscription_id), &"0".to_string());
        let _ = self.tap_put(&format!("tl/{}", inscription_id), &"".to_string());
        let _ = self.tap_del(&format!("kind/{}", inscription_id));
      }
      return;
    }
    // END MINER-REWARD-SHIELD
    let bal_key = format!("b/{}/{}", sender, tick_key);
    if let Some(balance_s) = self.tap_get::<String>(&bal_key).ok().flatten() {
      let mut balance = balance_s.parse::<i128>().unwrap_or(0);
      let have_transferable = self
        .tap_get::<String>(&format!("t/{}/{}", sender, tick_key))
        .ok()
        .flatten();
      let mut transferable = have_transferable
        .clone()
        .and_then(|s| s.parse::<i128>().ok())
        .unwrap_or(0);
      let amount = atr.amt.parse::<i128>().unwrap_or(0);
      if have_transferable.is_some() {
        let mut fail = false;
        let mut new_balance = balance - amount;
        let mut new_transferable = transferable - amount;
        if new_transferable < 0 || new_balance < 0 {
          // Writer parity: invalid -> clamp transferable to 0 (if negative), keep balance unchanged, mark fail
          if new_transferable < 0 {
            new_transferable = 0;
          }
          new_balance = balance;
          fail = true;
        }
        // apply updates for success path
        balance = new_balance;
        transferable = new_transferable;

        let burn_addr = "1BitcoinEaterAddressDontSendf59kuE".to_string();
        let recv_display = if Self::trim_js_whitespace(&receiver) == "-" {
          &burn_addr
        } else {
          &receiver
        };

        let recv_bal_key = format!("b/{}/{}", receiver, tick_key);
        let receiver_balance_current = self
          .tap_get::<String>(&recv_bal_key)
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let mut receiver_balance = receiver_balance_current;
        if !fail && receiver != sender {
          let _ = self.tap_put(&bal_key, &balance.to_string());
          receiver_balance = receiver_balance_current + amount;
          let _ = self.tap_put(&recv_bal_key, &receiver_balance.to_string());
          if self
            .tap_get::<String>(&format!("he/{}/{}", receiver, tick_key))
            .ok()
            .flatten()
            .is_none()
          {
            let _ = self.tap_put(&format!("he/{}/{}", receiver, tick_key), &"".to_string());
            let _ = self.tap_set_list_record(
              &format!("h/{}", tick_key),
              &format!("hi/{}", tick_key),
              &receiver,
            );
          }
          // Account-owned list: mirror tap-writer setAccountTokenOwned on successful receive
          if self
            .tap_get::<String>(&format!("ato/{}/{}", receiver, tick_key))
            .ok()
            .flatten()
            .is_none()
          {
            let _ = self.tap_put(&format!("ato/{}/{}", receiver, tick_key), &"".to_string());
            // decode JSON string key to lowercased ticker for list storage
            let recv_tick_lower =
              Self::js_json_string_parse_str(&tick_key).unwrap_or_else(|| tick_key.clone());
            let _ = self.tap_set_list_record(
              &format!("atl/{}", receiver),
              &format!("atli/{}", receiver),
              &recv_tick_lower,
            );
          }
        }
        let sender_log_balance = if receiver == sender {
          receiver_balance_current
        } else {
          balance
        };

        let srec = TransferSendSenderRecord {
          addr: sender.clone(),
          taddr: recv_display.clone(),
          at: None,
          tt: None,
          st: None,
          rl: None,
          rf: None,
          blck: self.height,
          amt: atr.amt.clone(),
          trf: transferable.to_string(),
          bal: sender_log_balance.to_string(),
          tx: new_satpoint.outpoint.txid.to_string(),
          vo: u32::from(new_satpoint.outpoint.vout),
          val: output_value_sat.to_string(),
          ins: inscription_id.to_string(),
          num: atr.num,
          ts: self.timestamp,
          fail,
          int: false,
          dta: atr.dta.clone(),
        };
        let _ = self.tap_set_list_record(
          &format!("strl/{}/{}", sender, tick_key),
          &format!("strli/{}/{}", sender, tick_key),
          &srec,
        );

        let rrec = TransferSendReceiverRecord {
          faddr: sender.clone(),
          addr: receiver.clone(),
          at: None,
          tt: None,
          st: None,
          rl: None,
          rf: None,
          blck: self.height,
          amt: atr.amt.clone(),
          bal: receiver_balance.to_string(),
          tx: new_satpoint.outpoint.txid.to_string(),
          vo: u32::from(new_satpoint.outpoint.vout),
          val: output_value_sat.to_string(),
          ins: inscription_id.to_string(),
          num: atr.num,
          ts: self.timestamp,
          fail,
          int: false,
          dta: atr.dta.clone(),
        };
        let _ = self.tap_set_list_record(
          &format!("rstrl/{}/{}", receiver, tick_key),
          &format!("rstrli/{}/{}", receiver, tick_key),
          &rrec,
        );

        let frec = TransferSendFlatRecord {
          tick: None,
          addr: sender.clone(),
          taddr: recv_display.clone(),
          at: None,
          tt: None,
          st: None,
          rl: None,
          rf: None,
          blck: self.height,
          amt: atr.amt.clone(),
          trf: transferable.to_string(),
          bal: sender_log_balance.to_string(),
          tbal: receiver_balance.to_string(),
          tx: new_satpoint.outpoint.txid.to_string(),
          vo: u32::from(new_satpoint.outpoint.vout),
          val: output_value_sat.to_string(),
          ins: inscription_id.to_string(),
          num: atr.num,
          ts: self.timestamp,
          fail,
          int: false,
          dta: atr.dta.clone(),
        };
        let _ = self.tap_set_list_record(
          &format!("fstrl/{}", tick_key),
          &format!("fstrli/{}", tick_key),
          &frec,
        );

        let _ = self.tap_put(
          &format!("t/{}/{}", atr.addr, tick_key),
          &transferable.to_string(),
        );
        let _ = self.tap_put(&format!("tamt/{}", inscription_id), &"0".to_string());

        let tick_str =
          Self::js_json_string_parse_str(&tick_key).unwrap_or_else(|| tick_key.clone());
        let sfrec = TransferSendSuperflatRecord {
          tick: tick_str,
          addr: sender,
          taddr: recv_display.to_string(),
          at: None,
          tt: None,
          st: None,
          rl: None,
          rf: None,
          blck: self.height,
          amt: atr.amt.clone(),
          trf: transferable.to_string(),
          bal: sender_log_balance.to_string(),
          tbal: receiver_balance.to_string(),
          tx: new_satpoint.outpoint.txid.to_string(),
          vo: u32::from(new_satpoint.outpoint.vout),
          val: output_value_sat.to_string(),
          ins: inscription_id.to_string(),
          num: atr.num,
          ts: self.timestamp,
          fail,
          int: false,
          dta: atr.dta.clone(),
        };
        // Superflat executed transfer record + pointers (writer parity: applyTransferLogs)
        if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
          let ptr = format!("sfstrli/{}", list_len - 1);
          let txs = new_satpoint.outpoint.txid.to_string();
          // tx-scoped pointer
          let _ = self.tap_set_list_record(
            &format!("tx/snd/{}", txs),
            &format!("txi/snd/{}", txs),
            &ptr,
          );
          // ticker+tx pointer
          let _ = self.tap_set_list_record(
            &format!("txt/snd/{}/{}", tick_key, txs),
            &format!("txti/snd/{}/{}", tick_key, txs),
            &ptr,
          );
          // block-scoped pointer
          let _ = self.tap_set_list_record(
            &format!("blck/snd/{}", self.height),
            &format!("blcki/snd/{}", self.height),
            &ptr,
          );
          // ticker+block pointer
          let _ = self.tap_set_list_record(
            &format!("blckt/snd/{}/{}", tick_key, self.height),
            &format!("blckti/snd/{}/{}", tick_key, self.height),
            &ptr,
          );
        }
      }
    } else {
      // No balance object: parity with writer — clear transferable link amount and delete transferable key
      let _ = self.tap_del(&format!("t/{}/{}", atr.addr, tick_key));
      let _ = self.tap_put(&format!("tamt/{}", inscription_id), &"0".to_string());
    }
    let _ = self.tap_put(&format!("tl/{}", inscription_id), &"".to_string());
    let _ = self.tap_del(&format!("kind/{}", inscription_id));
  }
}
