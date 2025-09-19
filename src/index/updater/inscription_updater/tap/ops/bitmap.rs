use super::super::super::*;

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn index_bitmap_created(
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
    if !s.ends_with(".bitmap") { return; }
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 || parts[1] != "bitmap" { return; }
    let Ok(block_num_signed) = parts[0].parse::<i64>() else { return; };
    if block_num_signed.to_string() != parts[0] { return; }
    if block_num_signed < 0 { return; }
    let block_num = block_num_signed as u64;
    if block_num > u64::from(self.height) { return; }

    let bm_key = format!("bm/{}", block_num);
    if self.tap_get::<BitmapRecord>(&bm_key).ok().flatten().is_some() { return; }

    let record = BitmapRecord {
      ownr: owner_address.to_string(),
      prv: None,
      bm: block_num,
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: inscription_number,
      ts: self.timestamp,
    };

    let _ = self.tap_put(&bm_key, &record);
    let _ = self.tap_put(&format!("bmh/{}", inscription_id), &bm_key);

    if let Ok(list_len) = self.tap_set_list_record(&format!("bmhl/{}", block_num), &format!("bmhli/{}", block_num), &record) {
      let ptr = format!("bmhli/{}/{}", block_num, list_len - 1);
      let _ = self.tap_set_list_record(&format!("tx/bm/{}", satpoint.outpoint.txid), &format!("txi/bm/{}", satpoint.outpoint.txid), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/bm/{}", self.height), &format!("blcki/bm/{}", self.height), &ptr);
    }

    let _ = self.tap_set_list_record(&format!("bml/{}", owner_address), &format!("bmli/{}", owner_address), &inscription_id.to_string());
    if let Some(bloom) = &self.any_bloom { bloom.borrow_mut().insert_str(&inscription_id.to_string()); }
    let _ = self.tap_put(&format!("kind/{}", inscription_id), &"bm".to_string());
  }

  pub(crate) fn index_bitmap_transferred(
    &mut self,
    inscription_id: InscriptionId,
    sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    let Some(mapped) = self.tap_get::<String>(&format!("bmh/{}", inscription_id)).ok().flatten() else { return; };
    let mut it = mapped.split('/');
    if it.next() != Some("bm") { return; }
    let Some(block_str) = it.next() else { return; };
    let Ok(block_num) = block_str.parse::<u64>() else { return; };

    let Some(prev) = self.tap_get::<BitmapRecord>(&format!("bm/{}", block_num)).ok().flatten() else { return; };

    let entry_val = match self.sequence_number_to_entry.get(&sequence_number) {
      Ok(Some(v)) => v.value(),
      _ => return,
    };
    let entry = InscriptionEntry::load(entry_val);
    if entry.inscription_number < 0 { return; }

    const BURN_ADDRESS: &str = "1BitcoinEaterAddressDontSendf59kuE";
    let owner = if owner_address.trim() == "-" { BURN_ADDRESS } else { owner_address };

    let record = BitmapRecord {
      ownr: owner.to_string(),
      prv: Some(prev.ownr.clone()),
      bm: block_num,
      blck: self.height,
      tx: new_satpoint.outpoint.txid.to_string(),
      vo: u32::from(new_satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: entry.inscription_number,
      ts: self.timestamp,
    };

    let _ = self.tap_put(&format!("bm/{}", block_num), &record);
    if let Ok(list_len) = self.tap_set_list_record(&format!("bmhl/{}", block_num), &format!("bmhli/{}", block_num), &record) {
      let ptr = format!("bmhli/{}/{}", block_num, list_len - 1);
      let _ = self.tap_set_list_record(&format!("tx/bm/{}", new_satpoint.outpoint.txid), &format!("txi/bm/{}", new_satpoint.outpoint.txid), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/bm/{}", self.height), &format!("blcki/bm/{}", self.height), &ptr);
    }

    let _ = self.tap_set_list_record(&format!("bml/{}", owner), &format!("bmli/{}", owner), &inscription_id.to_string());
  }
}
