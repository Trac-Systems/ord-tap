use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_deployments(
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

    let json_val: serde_json::Value = match serde_json::from_str(&s) {
      Ok(v) => v,
      Err(_) => return,
    };

    let tick_val = json_val.get("tick");
    let max_val = json_val.get("max");
    let p_val = json_val.get("p");
    let op_val = json_val.get("op");
    if tick_val.is_none() || max_val.is_none() || p_val.is_none() || op_val.is_none() { return; }

    let p = p_val.unwrap().as_str().unwrap_or("").to_lowercase();
    let op = op_val.unwrap().as_str().unwrap_or("").to_lowercase();
    let tick = tick_val.unwrap().as_str().unwrap_or("").to_string();
    if tick.is_empty() { return; }

    let is_tap_deploy = p == "tap" && op == "token-deploy";
    let is_brc20_deploy = p == "brc-20" && op == "deploy";
    if !(is_tap_deploy || is_brc20_deploy) { return; }

    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) {
      for k in ["max", "lim"] {
        if let Some(v) = json_val.get(k) {
          if v.is_number() { return; }
        }
      }
    }

    let tick_lower = tick.to_lowercase();
    if tick_lower.starts_with('-') || tick_lower.starts_with("dmt-") { return; }

    if is_tap_deploy && !self.tap_feature_enabled(TapFeature::TapStart) { return; }

    let vis_len = Self::visible_length(&tick);
    if is_tap_deploy {
      if !Self::valid_tap_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, vis_len) { return; }
    } else if is_brc20_deploy {
      if !Self::valid_brc20_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, vis_len) { return; }
    }

    let mut effective_tick = tick_lower.clone();
    if inscription_number < 0 {
      if self.tap_feature_enabled(TapFeature::Jubilee) {
        return;
      } else {
        effective_tick = format!("-{}", effective_tick);
      }
    }

    let mut ins_data: Option<String> = None;
    if let Some(dta_val) = json_val.get("dta") {
      if let Some(dta_str) = dta_val.as_str() {
        if dta_str.as_bytes().len() > 512 { return; }
        ins_data = Some(dta_str.to_string());
      }
    }

    let mut decimals: u32 = 18;
    if let Some(dec_val) = json_val.get("dec") {
      let dec_str = if dec_val.is_string() { dec_val.as_str().unwrap().to_string() } else { dec_val.to_string() };
      if let Ok(parsed) = dec_str.parse::<i64>() {
        if parsed >= 0 && parsed < 18 {
          if !Self::is_valid_number(&dec_str) { return; }
          decimals = parsed as u32;
        }
      }
    }

    let max_str_input = if max_val.unwrap().is_string() { max_val.unwrap().as_str().unwrap().to_string() } else { max_val.unwrap().to_string() };
    let max_s = match Self::resolve_number_string(&max_str_input, decimals) { Some(x) => x, None => return };
    let max = match max_s.parse::<u128>() { Ok(v) => v, Err(_) => return };
    if max == 0 { return; }
    let cap_s = Self::resolve_number_string(MAX_DEC_U64_STR, decimals).unwrap();
    let cap = cap_s.parse::<u128>().unwrap_or(u128::MAX);
    if max > cap { return; }

    let mut limit: u128 = 0;
    if let Some(lim_val) = json_val.get("lim") {
      let lim_str_input = if lim_val.is_string() { lim_val.as_str().unwrap().to_string() } else { lim_val.to_string() };
      let lim_s = match Self::resolve_number_string(&lim_str_input, decimals) { Some(x) => x, None => return };
      let lim = match lim_s.parse::<u128>() { Ok(v) => v, Err(_) => return };
      if lim == 0 { return; }
      if lim > cap { return; }
      limit = lim;
    }

    let mut privilege_auth: Option<String> = None;
    if is_brc20_deploy {
      privilege_auth = Some(BRC20_PRIVILEGE_AUTHORITY.to_string());
    }
    if let Some(prv_val) = json_val.get("prv") {
      if let Some(prv_str) = prv_val.as_str() {
        if p == "tap" {
          let prains_key = format!("prains/{}", prv_str);
          let prac_key = format!("prac/{}", prv_str);
          let exists_prains = self.tap_get::<String>(&prains_key).ok().flatten().is_some();
          let exists_prac = self.tap_get::<String>(&prac_key).ok().flatten().is_some();
          if !exists_prains || exists_prac { return; }
        }
        privilege_auth = Some(prv_str.to_string());
      } else {
        return;
      }
    }

    let tick_key = serde_json::to_string(&effective_tick).unwrap_or_else(|_| format!("\"{}\"", effective_tick));
    let d_key = format!("d/{}", tick_key);
    if self.tap_get::<DeployRecord>(&d_key).ok().flatten().is_some() { return; }

    let record = DeployRecord {
      tick: effective_tick.clone(),
      max: max_s,
      lim: limit.to_string(),
      dec: decimals,
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
      addr: owner_address.to_string(),
      crsd: inscription_number < 0,
      dmt: false,
      elem: None,
      prj: None,
      dim: None,
      dt: None,
      prv: privilege_auth,
      dta: ins_data,
    };

    let _ = self.tap_put(&d_key, &record);
    let _ = self.tap_put(&format!("dc/{}", tick_key), &record.max);
    if let Ok(list_len) = self.tap_set_list_record("dl", "dli", &record.tick) {
      let ptr = format!("dli/{}", list_len - 1);
      let tx = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/dpl/{}", tx), &format!("txi/dpl/{}", tx), &ptr);
      let _ = self.tap_set_list_record(&format!("txt/dpl/{}/{}", tick_key, tx), &format!("txti/dpl/{}/{}", tick_key, tx), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/dpl/{}", self.height), &format!("blcki/dpl/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckt/dpl/{}/{}", tick_key, self.height), &format!("blckti/dpl/{}/{}", tick_key, self.height), &ptr);
    }
  }
}
