use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_dmt_deploy(
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
    if p != "tap" || op != "dmt-deploy" { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }

    // Visible-length check mirrors tap-writer (pre-prefix) and must NOT start with '-' or 'dmt-'
    let Some(user_tick) = json_val.get("tick").and_then(|v| v.as_str()) else { return; };
    let tick_lower = user_tick.to_lowercase();
    if tick_lower.starts_with('-') || tick_lower.starts_with("dmt-") { return; }
    let len = Self::visible_length(user_tick);
    if !Self::valid_tap_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, len) { return; }

    // Only non-cursed allowed for dmt-deploy
    if inscription_number < 0 { return; }
    if !self.tap_feature_enabled(TapFeature::Dmt) { return; }

    // Optional dta
    let mut ins_data: Option<String> = None;
    if let Some(dta_val) = json_val.get("dta") { if let Some(s) = dta_val.as_str() { if s.as_bytes().len() > 512 { return; } ins_data = Some(s.to_string()); } }

    // Dim/dt options
    let mut dim: Option<String> = None;
    if let Some(dim_val) = json_val.get("dim").and_then(|v| v.as_str()) {
      match dim_val { "h"|"v"|"d"|"a" => dim = Some(dim_val.to_string()), _ => return }
    }
    let mut dt: Option<String> = None;
    if let Some(dt_val) = json_val.get("dt").and_then(|v| v.as_str()) {
      match dt_val { "h"|"n"|"x"|"s"|"b" => dt = Some(dt_val.to_string()), _ => return }
    }

    // Resolve element by inscription id → name → dmt-el/<name>
    // Parity with tap-writer: deployment stores elem as the inscription id string
    let Some(elem_id) = json_val.get("elem").and_then(|v| v.as_str()) else { return; };
    let Some(elem_name) = self.tap_get::<String>(&format!("dmt-{}", elem_id)).ok().flatten() else { return; };
    let Some(elem_rec) = self.tap_get::<DmtElementRecord>(&format!("dmt-el/{}", Self::json_stringify_lower(&elem_name))).ok().flatten() else { return; };

    // If element has a pattern, enforce dt compatibility at deploy time (parity with tap-writer)
    if elem_rec.pat.is_some() {
      if let Some(ref dtv) = dt {
        match elem_rec.fld {
          4 | 10 => { if dtv != "n" { return; } }
          11 => { if dtv != "n" && dtv != "h" { return; } }
          _ => { return; }
        }
      } else {
        // dt absent but pattern present → invalid
        return;
      }
    }

    // Optional project (
    let mut prvj: Option<String> = None;
    if let Some(prj_str) = json_val.get("prj").and_then(|v| v.as_str()) {
      if !self.ordinal_available(prj_str) { return; }
      prvj = Some(prj_str.to_string());
    }

    // Optional privilege
    let mut prv: Option<String> = None;
    if let Some(prv_str) = json_val.get("prv").and_then(|v| v.as_str()) {
      // active authority required
      if self.tap_get::<String>(&format!("prains/{}", prv_str)).ok().flatten().is_none() { return; }
      if self.tap_get::<String>(&format!("prac/{}", prv_str)).ok().flatten().is_some() { return; }
      prv = Some(prv_str.to_string());
    }

    // Adjust tick label with dmt-
    let effective_tick = format!("dmt-{}", user_tick);
    let tick_key = Self::json_stringify_lower(&effective_tick);
    if self.tap_get::<DeployRecord>(&format!("d/{}", tick_key)).ok().flatten().is_some() { return; }

    // Fixed decimals and cap (parity with tap-writer): max/lim = u64 cap, dc initialized to cap
    let decimals: u32 = 0;
    let cap_s = Self::resolve_number_string(MAX_DEC_U64_STR, decimals).unwrap_or_else(|| MAX_DEC_U64_STR.to_string());
    let max_s = cap_s.clone();
    let limit = cap_s.clone();

    let record = DeployRecord {
      tick: effective_tick.clone(),
      max: max_s,
      lim: limit,
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
      dmt: true,
      // Store the element as inscription id (writer behavior); dmt-mint will resolve via dmt-<ins> → name
      elem: Some(elem_id.to_string()),
      prj: prvj,
      dim,
      dt,
      prv,
      dta: ins_data,
    };

    let _ = self.tap_put(&format!("d/{}", tick_key), &record);
    // Map deployment inscription -> dmt ticker (parity with tap-writer: dmt-di/<ins> => <tick_lower>)
    let _ = self.tap_put(&format!("dmt-di/{}", inscription_id), &effective_tick.to_lowercase());
    let _ = self.tap_put(&format!("dc/{}", tick_key), &cap_s);
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
