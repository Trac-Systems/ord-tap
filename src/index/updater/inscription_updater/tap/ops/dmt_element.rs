use super::super::super::*;
use regex::Regex;

// DMT Element record shape
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct DmtElementRecord {
  pub tick: String,
  pub blck: u32,
  pub tx: String,
  pub vo: u32,
  pub ins: String,
  pub num: i32,
  pub ts: u32,
  pub addr: String,
  #[serde(default)]
  pub pat: Option<String>,
  pub fld: u32,
}

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_dmt_element_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    _output_value_sat: u64,
  ) {
    // Gates
    if self.height < TAP_DMT_HEIGHT { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }

    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let s_trim = s.trim();
    let s_lower = s_trim.to_lowercase();
    if !s_lower.ends_with(".element") { return; }

    let parts: Vec<&str> = s_trim.split('.').collect();
    if !(parts.len() == 3 || parts.len() >= 4) { return; }

    let (name_lc, mut pattern_opt, field_str, element_tag) = if parts.len() == 3 {
      (parts[0].to_lowercase(), None, parts[1].to_string(), parts[2])
    } else {
      (parts[0].to_lowercase(), Some(parts[1..parts.len()-2].join(".")), parts[parts.len()-2].to_string(), parts[parts.len()-1])
    };

    if element_tag != "element" { return; }
    // name invalid chars
    if name_lc.chars().any(|c| matches!(c, '/' | '.' | '[' | ']' | '{' | '}' | ':' | ';' | '"' | '\'' | ' ' | '\t' | '\n' | '\r')) { return; }

    // field parse and round-trip after activation
    let parsed_field = match field_str.parse::<i64>() { Ok(v) => v, Err(_) => return };
    if self.height >= TAP_DMT_PARSEINT_ACTIVATION_HEIGHT && field_str != parsed_field.to_string() { return; }
    let field_u = if parsed_field >= 0 { parsed_field as u32 } else { return };
    if field_u != 4 && field_u != 10 && field_u != 11 { return; }

    // pattern validation: compile with Rust regex (no backtracking/look-around)
    if let Some(pat) = &pattern_opt {
      if pat.is_empty() { pattern_opt = None; }
      else if Regex::new(pat).is_err() { return; }
    }

    // Uniqueness
    let element_key = Self::json_stringify_lower(&name_lc);
    let sig_concat = format!("{}{}", pattern_opt.clone().unwrap_or_default(), field_str);
    let element_sig = serde_json::to_string(&sig_concat).unwrap_or_else(|_| format!("\"{}\"", sig_concat));

    if self.tap_get::<DmtElementRecord>(&format!("dmt-el/{}", element_key)).ok().flatten().is_some() { return; }
    if self.tap_get::<String>(&format!("dmt-sig/{}", element_sig)).ok().flatten().is_some() { return; }

    let rec = DmtElementRecord {
      tick: name_lc.clone(),
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
      addr: owner_address.to_string(),
      pat: pattern_opt.clone(),
      fld: field_u,
    };

    let _ = self.tap_put(&format!("dmt-el/{}", element_key), &rec);
    let _ = self.tap_put(&format!("dmt-sig/{}", element_sig), &"".to_string());
    let _ = self.tap_put(&format!("dmt-{}", inscription_id), &name_lc);

    if let Ok(list_len) = self.tap_set_list_record("dmt-ell", "dmt-elli", &name_lc) {
      let ptr = format!("dmt-elli/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/dmt-el/{}", txs), &format!("txi/dmt-el/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("tx/dmt-el/{}/{}", element_key, txs), &format!("txi/dmt-el/{}/{}", element_key, txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/dmt-el/{}", self.height), &format!("blcki/dmt-el/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckt/dmt-el/{}/{}", element_key, self.height), &format!("blckti/dmt-el/{}/{}", element_key, self.height), &ptr);
    }
  }
}

