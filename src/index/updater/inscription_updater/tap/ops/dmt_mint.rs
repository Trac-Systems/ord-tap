use super::super::super::*;
use super::super::jsregex::js_count_global_matches;

#[derive(Serialize, Deserialize)]
pub(crate) struct DmtMintMetaRecord {
  pub tick: String,
  // Parity: elem must be the full element object (writer stores JSON.stringify(elem))
  // Keep it flexible to read old records where elem was a string; upgrade on read.
  pub elem: serde_json::Value,
  pub dmtblck: u32,
  pub blckdrp: bool,
  pub dep: String,
  pub prts: Option<String>,
  pub num: i32,
}

impl InscriptionUpdater<'_, '_> {
  // Emulate JS parseInt semantics used by tap-writer for blk pre-activation.
  // Returns (parsed_value, js_string_of_input).
  fn js_parse_int_repr(v: &serde_json::Value) -> Option<(i64, String)> {
    match v {
      serde_json::Value::String(s) => {
        // JS ToString yields the same string
        let js_s = s.clone();
        // Emulate parseInt: skip leading whitespace, optional sign, then base-10 digits until first non-digit
        let mut chars = s.chars().peekable();
        // skip leading whitespace
        while let Some(c) = chars.peek() { if c.is_whitespace() { chars.next(); } else { break; } }
        // optional sign
        let mut sign: i64 = 1;
        if let Some(&c) = chars.peek() {
          if c == '-' { sign = -1; chars.next(); }
          else if c == '+' { chars.next(); }
        }
        // digits
        let mut acc: i64 = 0;
        let mut found = false;
        while let Some(&c) = chars.peek() {
          if c >= '0' && c <= '9' {
            let d = (c as u8 - b'0') as i64;
            acc = acc.saturating_mul(10).saturating_add(d);
            found = true;
            chars.next();
          } else { break; }
        }
        if !found { return None; }
        Some((sign.saturating_mul(acc), js_s))
      }
      serde_json::Value::Number(num) => {
        if let Some(i) = num.as_i64() { return Some((i, i.to_string())); }
        if let Some(u) = num.as_u64() { let i = i64::try_from(u).ok()?; return Some((i, i.to_string())); }
        if let Some(f) = num.as_f64() {
          // Coerce f64 to string then parse like parseInt("<number>") → take leading integer part
          let s = if f.fract() == 0.0 { format!("{:.0}", f) } else { f.to_string() };
          // Recurse into string path to emulate parseInt on the string form
          return Self::js_parse_int_repr(&serde_json::Value::String(s));
        }
        None
      }
      _ => None,
    }
  }
  pub(crate) fn index_dmt_mint(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    output_value_sat: u64,
    parents: &[InscriptionId],
    index: &Index,
  ) {
    // Only process creation-time inscriptions
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() { return; }
    let Some(body) = payload.body() else { return; };
    let s = String::from_utf8_lossy(body);
    let json_val: serde_json::Value = match serde_json::from_str(&s) { Ok(v) => v, Err(_) => return };

    let p = json_val.get("p").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let op = json_val.get("op").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let tick_user = json_val.get("tick").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let blk_v = json_val.get("blk");
    if p != "tap" || op != "dmt-mint" || tick_user.is_empty() || blk_v.is_none() { return; }
    if !self.tap_feature_enabled(TapFeature::TapStart) { return; }
    if !self.tap_feature_enabled(TapFeature::Dmt) { return; }

    if tick_user.to_lowercase().starts_with('-') || tick_user.to_lowercase().starts_with("dmt-") { return; }
    let vis_len = Self::visible_length(&tick_user);
    let full = self.tap_feature_enabled(TapFeature::FullTicker);
    let ok_len = if !full { vis_len == 3 || (vis_len >= 5 && vis_len <= 32) } else { vis_len > 0 && vis_len <= 32 };
    if !ok_len { return; }

    let tick_effective = format!("dmt-{}", tick_user);
    let tick_effective_lower = tick_effective.to_lowercase();
    let tick_key = Self::json_stringify_lower(&tick_effective_lower);

    if self.tap_feature_enabled(TapFeature::DmtNatRewards) && tick_effective_lower == "dmt-nat" { return; }

    let (parsed_blk_i64, blk_js_str) = match Self::js_parse_int_repr(blk_v.unwrap()) { Some(t) => t, None => return };
    if inscription_number < 0 { return; }
    if parsed_blk_i64 < 0 { return; }
    let parsed_blk = parsed_blk_i64 as u32;
    if parsed_blk > self.height { return; }
    // After activation: require exact JS string equality like tap-writer (''+parsed !== ''+json.blk)
    if self.tap_feature_enabled(TapFeature::DmtParseintActivation) && parsed_blk_i64.to_string() != blk_js_str { return; }

    let mut ins_data: Option<String> = None;
    if let Some(dta_val) = json_val.get("dta") { if let Some(ds) = dta_val.as_str() { if ds.as_bytes().len() > 512 { return; } ins_data = Some(ds.to_string()); } }

    if self.tap_get::<String>(&format!("dmt-blk/{}/{}", tick_effective_lower, parsed_blk)).ok().flatten().is_some() { return; }

    let dep_json = json_val.get("dep").and_then(|v| v.as_str()).map(|s| s.to_string());
    if dep_json.is_none() && !self.tap_feature_enabled(TapFeature::FullTicker) { return; }

    let Some(deployed) = self.tap_get::<DeployRecord>(&format!("d/{}", tick_key)).ok().flatten() else { return; };
    if !deployed.dmt { return; }
    let mut tokens_left: i128 = match self.tap_get::<String>(&format!("dc/{}", tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()) { Some(v) => v, None => return };

    let mut the_dep: Option<String> = None;
    let mut is_blockdrop = false;
    let mut is_unat = false;
    let mut is_bitmap = false;
    let mut project_ticker: Option<String> = None;
    let mut project_inscription: Option<String> = None;
    if let Some(prj) = deployed.prj.clone() {
      if let Some(project_tick) = self.tap_get::<String>(&format!("dmt-di/{}", prj)).ok().flatten() {
        let ptk = Self::json_stringify_lower(&project_tick);
        if let Some(project_dep) = self.tap_get::<DeployRecord>(&format!("d/{}", ptk)).ok().flatten() {
          if project_dep.dmt {
            project_ticker = Some(project_dep.tick.clone());
            project_inscription = Some(deployed.ins.clone());
            is_blockdrop = true;
            is_unat = true;
          }
        }
      } else {
        if let Some(bm_ptr) = self.tap_get::<String>(&format!("bmh/{}", prj)).ok().flatten() {
          if bm_ptr == "bm/0" {
            project_inscription = Some(deployed.ins.clone());
            is_blockdrop = true;
            is_bitmap = true;
          }
        }
      }
    }

    if is_blockdrop && dep_json.is_none() {
      if let Some(parent_first) = parents.first() {
        let parent_id = parent_first.to_string();
        if is_unat {
          if let Ok(Some(bytes)) = self.tap_db.get(format!("dmtmh/{}", parent_id).as_bytes()) {
            if let Ok(unat_val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
              if let Some(pt) = &project_ticker { if unat_val.get("tick").and_then(|v| v.as_str()) == Some(pt.as_str()) { the_dep = project_inscription.clone(); } }
            }
          }
        } else if is_bitmap {
          if self.tap_get::<String>(&format!("bmh/{}", parent_id)).ok().flatten().is_some() {
            the_dep = project_inscription.clone();
          }
        }
      }
    } else if !is_blockdrop {
      // Parity: explicit dep is only honored for non-blockdrop mints
      the_dep = dep_json.clone();
    }

    let Some(dep_str) = the_dep else { return; };
    if deployed.ins != dep_str { return; }
    if !dep_str.contains('i') { return; }
    let mut it = dep_str.split('i');
    let (txh, idxs) = (it.next().unwrap_or(""), it.next().unwrap_or(""));
    if it.next().is_some() { return; }
    if txh.len() != 64 || !txh.chars().all(|c| c.is_ascii_hexdigit()) { return; }
    if idxs.parse::<u32>().is_err() { return; }
    if !self.ordinal_available(&dep_str, index) { return; }

    // Resolve element exactly like tap-writer does:
    // - Writer stores deployment.elem as the element inscription id (json.elem)
    // - At mint time, it looks up `dmt-<elem_ins>` → element name, then loads `dmt-el/<name>`
    let Some(elem_ins) = deployed.elem.clone() else { return; };
    let Some(elem_name) = self.tap_get::<String>(&format!("dmt-{}", elem_ins)).ok().flatten() else { return; };
    let Some(elem) = self.tap_get::<DmtElementRecord>(&format!("dmt-el/{}", Self::json_stringify_lower(&elem_name))).ok().flatten() else { return; };

    let mut amount: i128;
    let mut fail = false;
    let limit: i128 = deployed.lim.parse::<i128>().unwrap_or(0);
    match elem.fld {
      4 => {
        // JS uses '' + json.blk (string form)
        let c_val = blk_js_str.clone();
        if let Some(pat) = &elem.pat {
          if deployed.dt.as_deref() != Some("n") { return; }
          if let Some(cnt) = js_count_global_matches(pat, &c_val) { amount = cnt as i128; } else { return; }
        } else { amount = blk_js_str.parse::<i128>().unwrap_or(0); }
      }
      10 => {
        #[derive(Deserialize)]
        struct TapHeaderSnapshot { bits: u32, nonce: u32, ntx: u32, time: u32 }
        let hdr: TapHeaderSnapshot = match self.tap_db.get(format!("hdr/{}", parsed_blk).as_bytes()).ok().flatten().and_then(|b| ciborium::de::from_reader::<TapHeaderSnapshot, _>(std::io::Cursor::new(b)).ok()) { Some(h) => h, None => return };
        let c_val = hdr.nonce.to_string();
        if let Some(pat) = &elem.pat {
          if deployed.dt.as_deref() != Some("n") { return; }
          if let Some(cnt) = js_count_global_matches(pat, &c_val) { amount = cnt as i128; } else { return; }
        } else { amount = hdr.nonce as i128; }
      }
      11 => {
        #[derive(Deserialize)]
        struct TapHeaderSnapshot { bits: u32, nonce: u32, ntx: u32, time: u32 }
        let hdr: TapHeaderSnapshot = match self.tap_db.get(format!("hdr/{}", parsed_blk).as_bytes()).ok().flatten().and_then(|b| ciborium::de::from_reader::<TapHeaderSnapshot, _>(std::io::Cursor::new(b)).ok()) { Some(h) => h, None => return };
        let mut c_val = hdr.bits.to_string();
        if let Some(pat) = &elem.pat {
          // Parity: allow dt 'n' (decimal string) or 'h' (hex string) for field 11
          match deployed.dt.as_deref() {
            Some("n") => { /* already decimal */ }
            Some("h") => { c_val = format!("{:x}", hdr.bits); }
            _ => { return; }
          }
          if let Some(cnt) = js_count_global_matches(pat, &c_val) { amount = cnt as i128; } else { return; }
        } else { amount = hdr.bits as i128; }
      }
      _ => { return; }
    }

    if limit > 0 && amount > limit { fail = true; }
    if !fail {
      if tokens_left - amount < 0 { amount = tokens_left; }
      if amount <= 0 { fail = true; }
    }

    // Privilege check (if deployed.prv present)
    let mut used_compact_sig: Option<String> = None;
    if !fail {
      if let Some(ref prv_dep) = deployed.prv {
        if let Some(prv_obj) = json_val.get("prv") {
          // message: p-op-origtick-blk-dep-addr[-dta]-salt
          use sha2::Digest;
          let salt = prv_obj.get("salt").and_then(|v| v.as_str()).unwrap_or("");
          // Parity: use json.prv.address for message building (not owner_address)
          let prv_addr_for_msg = prv_obj.get("address").and_then(|v| v.as_str()).unwrap_or("");
          let mut msg = format!("{}-{}-{}-{}-{}-{}", p, op, tick_user, blk_js_str, dep_str, prv_addr_for_msg);
          if let Some(d) = &ins_data { msg.push('-'); msg.push_str(d); }
          msg.push('-'); msg.push_str(salt);
          let mut hasher = sha2::Sha256::new(); hasher.update(msg.as_bytes()); let out = hasher.finalize();
          let mut arr = [0u8; 32]; arr.copy_from_slice(&out);
          if let Some((ok, comp_hex)) = self.verify_privilege_signature_with_msg(prv_dep, prv_obj, &arr, owner_address) { if !ok { fail = true; } else { used_compact_sig = Some(comp_hex); } } else { fail = true; }
        } else { fail = true; }
      }
    }

    // Balance update
    let bal_key = format!("b/{}/{}", owner_address, tick_key);
    let mut balance: i128 = self.tap_get::<String>(&bal_key).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
    if !fail {
      tokens_left = tokens_left.saturating_sub(amount);
      let _ = self.tap_put(&format!("dc/{}", tick_key), &tokens_left.to_string());
      balance = balance.saturating_add(amount);
      let _ = self.tap_put(&bal_key, &balance.to_string());
      // Holders list (parity with tap-writer setHolder)
      if self.tap_get::<String>(&format!("he/{}/{}", owner_address, tick_key)).ok().flatten().is_none() {
        let _ = self.tap_put(&format!("he/{}/{}", owner_address, tick_key), &"".to_string());
        let _ = self.tap_set_list_record(&format!("h/{}", tick_key), &format!("hi/{}", tick_key), &owner_address.to_string());
      }
    }

    // Writer stores the full parents string (joined by '|') or null
    let parents_str = if parents.is_empty() {
      None
    } else {
      Some(parents.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("|"))
    };

    // Holder record and pointers are written only on successful mint
    if !fail {
      // Parity: elem must be the full element object
      let elem_json = serde_json::to_value(&elem).unwrap_or(serde_json::Value::Null);
      let holder_json = serde_json::json!({
        "ownr": owner_address,
        "prv": serde_json::Value::Null,
        "tick": tick_effective_lower,
        "elem": elem_json,
        "blck": self.height,
        "tx": satpoint.outpoint.txid.to_string(),
        "vo": u32::from(satpoint.outpoint.vout),
        "val": output_value_sat.to_string(),
        "ins": inscription_id.to_string(),
        "num": inscription_number,
        "ts": self.timestamp,
        "dmtblck": parsed_blk,
        "blckdrp": is_blockdrop,
        "dep": dep_str,
        "prts": parents_str,
      });

      // Holder JSON stored as JSON (parity with writer)
      let _ = self.tap_db.put(format!("dmtmh/{}", inscription_id).as_bytes(), &serde_json::to_vec(&holder_json).unwrap());
      // Map tick/block to holder pointer regardless of address visibility (parity)
      let _ = self.tap_put(&format!("dmtmhb/{}/{}", tick_key, parsed_blk), &format!("dmtmh/{}", inscription_id));
      // Per-inscription history pointer (always on success)
      let len_key = format!("dmtmhl/{}", inscription_id);
      let iter_prefix = format!("dmtmhli/{}", inscription_id);
      let mut cur_len: u64 = self.tap_get::<String>(&len_key).ok().flatten().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
      let _ = self.tap_db.put(format!("{}/{}", iter_prefix, cur_len).as_bytes(), &serde_json::to_vec(&holder_json).unwrap());
      cur_len += 1;
      let _ = self.tap_put(&len_key, &cur_len.to_string());

      let ptr = format!("{}/{}", iter_prefix, cur_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/dmt-md/{}", txs), &format!("txi/dmt-md/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("txt/dmt-md/{}/{}", tick_key, txs), &format!("txti/dmt-md/{}/{}", tick_key, txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/dmt-md/{}", self.height), &format!("blcki/dmt-md/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckt/dmt-md/{}/{}", tick_key, self.height), &format!("blckti/dmt-md/{}/{}", tick_key, self.height), &ptr);
      // Wallet historic list (parity)
      let _ = self.tap_set_list_record(&format!("dmtmwl/{}", owner_address), &format!("dmtmwli/{}", owner_address), &inscription_id.to_string());
      // Mark block consumed for this tick (one per dmt block) — written last (parity with writer)
      let _ = self.tap_put(&format!("dmt-blk/{}/{}", tick_effective_lower, parsed_blk), &"".to_string());
    }

    if !owner_address.trim().eq("-") && !fail {
      let meta = DmtMintMetaRecord {
        tick: tick_effective_lower.clone(),
        elem: serde_json::to_value(&elem).unwrap_or(serde_json::Value::Null),
        dmtblck: parsed_blk,
        blckdrp: is_blockdrop,
        dep: dep_str.clone(),
        prts: parents_str.clone(),
        num: inscription_number,
      };
      let _ = self.tap_put(&format!("dmtmhm/{}", inscription_id), &meta);
      let _ = self.tap_put(&format!("dmtmho/{}", inscription_id), &owner_address.to_string());
      let _ = self.tap_set_list_record(&format!("dmtmwl/{}", owner_address), &format!("dmtmwli/{}", owner_address), &inscription_id.to_string());

      if let Some(bloom) = &self.dmt_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
      if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
      let _ = self.tap_put(&format!("kind/{}", inscription_id), &"dmtmh".to_string());

      if self.tap_get::<String>(&format!("ato/{}/{}", owner_address, tick_key)).ok().flatten().is_none() {
        let tick_lower_for_list = serde_json::from_str::<String>(&tick_key).unwrap_or_else(|_| tick_effective_lower.clone());
        let _ = self.tap_set_list_record(&format!("atl/{}", owner_address), &format!("atli/{}", owner_address), &tick_lower_for_list);
        let _ = self.tap_put(&format!("ato/{}/{}", owner_address, tick_key), &"".to_string());
      }

      // already marked above
    }

    let data_json = serde_json::json!({
      "addr": owner_address,
      "blck": self.height,
      "amt": amount.to_string(),
      "bal": balance.to_string(),
      "tx": satpoint.outpoint.txid.to_string(),
      "vo": u32::from(satpoint.outpoint.vout),
      "val": output_value_sat.to_string(),
      "ins": inscription_id.to_string(),
      "num": inscription_number,
      "ts": self.timestamp,
      "fail": fail,
      "dmtblck": parsed_blk,
      "dta": ins_data,
    });
    // Parity with tap-writer: record mint events (account/ticker/global) regardless of fail
    let _ = self.tap_set_list_record(&format!("aml/{}/{}", owner_address, tick_key), &format!("amli/{}/{}", owner_address, tick_key), &data_json);
    let _ = self.tap_set_list_record(&format!("fml/{}", tick_key), &format!("fmli/{}", tick_key), &data_json);
    let super_json = serde_json::json!({
      "tick": tick_effective_lower,
      "addr": owner_address,
      "blck": self.height,
      "amt": amount.to_string(),
      "bal": balance.to_string(),
      "tx": satpoint.outpoint.txid.to_string(),
      "vo": u32::from(satpoint.outpoint.vout),
      "val": output_value_sat.to_string(),
      "ins": inscription_id.to_string(),
      "num": inscription_number,
      "ts": self.timestamp,
      "fail": fail,
      "dmtblck": parsed_blk,
      "dta": data_json.get("dta").cloned().unwrap_or(serde_json::Value::Null),
    });
    // Global superflat + pointers always recorded (even when fail=true)
    if let Ok(list_len) = self.tap_set_list_record("sfml", "sfmli", &super_json) {
      let ptr = format!("sfmli/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/mnt/{}", txs), &format!("txi/mnt/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("txt/mnt/{}/{}", tick_key, txs), &format!("txti/mnt/{}/{}", tick_key, txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/mnt/{}", self.height), &format!("blcki/mnt/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckt/mnt/{}/{}", tick_key, self.height), &format!("blckti/mnt/{}/{}", tick_key, self.height), &ptr);
    }

    if let Some(comp) = used_compact_sig { let _ = self.tap_put(&format!("prah/{}", comp), &"".to_string()); }
  }

  pub(crate) fn index_dmt_mint_transferred(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Only execute on transfer (not creation tx)
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() { return; }
    if let Some(bloom) = &self.dmt_bloom {
      let b = bloom.borrow();
      if b.should_skip_negatives(self.height) {
        if !b.contains_str(&inscription_id.to_string()) { return; }
      }
    }

    let meta_key = format!("dmtmhm/{}", inscription_id);
    let owner_key = format!("dmtmho/{}", inscription_id);
    let meta = match self.tap_get::<DmtMintMetaRecord>(&meta_key).ok().flatten() { Some(m) => m, None => return };
    let prev_owner = match self.tap_get::<String>(&owner_key).ok().flatten() { Some(o) => o, None => return };

    let new_owner = if owner_address.trim() == "-" { BURN_ADDRESS.to_string() } else { owner_address.to_string() };
    let ins = inscription_id.to_string();

    let tick = serde_json::Value::String(meta.tick.clone());
    let elem = meta.elem.clone();
    let dmtblck = serde_json::Value::from(meta.dmtblck);
    let blckdrp = serde_json::Value::from(meta.blckdrp);
    let dep = serde_json::Value::String(meta.dep.clone());
    let prts = match &meta.prts { Some(s) => serde_json::Value::String(s.clone()), None => serde_json::Value::Null };
    let num = serde_json::Value::from(meta.num);

    let data_json = serde_json::json!({
      "ownr": new_owner,
      "prv": prev_owner,
      "tick": tick,
      "elem": elem,
      "blck": self.height,
      "tx": new_satpoint.outpoint.txid.to_string(),
      "vo": u32::from(new_satpoint.outpoint.vout),
      "val": output_value_sat.to_string(),
      "ins": ins,
      "num": num,
      "ts": self.timestamp,
      "dmtblck": dmtblck,
      "blckdrp": blckdrp,
      "dep": dep,
      "prts": prts,
    });

    let bytes = serde_json::to_vec(&data_json).unwrap_or_default();
    let _ = self.tap_db.put(format!("dmtmh/{}", inscription_id).as_bytes(), &bytes);
    let _ = self.tap_put(&owner_key, &new_owner);
    let list_len = match self.tap_set_list_record(&format!("dmtmhl/{}", inscription_id), &format!("dmtmhli/{}", inscription_id), &data_json) { Ok(n) => n, Err(_) => return };
    let ptr_key = format!("dmtmhli/{}/{}", inscription_id, list_len.saturating_sub(1));

    let tick_lower = meta.tick.to_lowercase();
    let tick_key = serde_json::to_string(&tick_lower).unwrap_or_else(|_| format!("\"{}\"", tick_lower));
    let txs = new_satpoint.outpoint.txid.to_string();
    let _ = self.tap_set_list_record(&format!("tx/dmt-md/{}", txs), &format!("txi/dmt-md/{}", txs), &ptr_key);
    let _ = self.tap_set_list_record(&format!("txt/dmt-md/{}/{}", tick_key, txs), &format!("txti/dmt-md/{}/{}", tick_key, txs), &ptr_key);
    let _ = self.tap_set_list_record(&format!("blck/dmt-md/{}", self.height), &format!("blcki/dmt-md/{}", self.height), &ptr_key);
    let _ = self.tap_set_list_record(&format!("blckt/dmt-md/{}/{}", tick_key, self.height), &format!("blckti/dmt-md/{}/{}", tick_key, self.height), &ptr_key);

    let _ = self.tap_set_list_record(&format!("dmtmwl/{}", new_owner), &format!("dmtmwli/{}", new_owner), &inscription_id.to_string());
  }
}
