// Child module of inscription_updater.rs
// Re-exports keep external paths stable: crate::index::updater::inscription_updater::TapBloomFilter

pub(super) mod filters;
pub(super) mod kv;
pub(super) mod records;
pub(super) mod jsregex;
// Shared TAP constants and helpers live here and are re-exported by parent.

// --- TAP feature gating (laddered block heights; mainnet values) ---
pub(crate) const TAP_BITMAP_START_HEIGHT: u32 = 779_832; // mainnet
pub(crate) const TAP_START_HEIGHT: u32 = 801_993; // mainnet

// Additional TAP feature heights (mainnet only)
pub(crate) const TAP_FULL_TICKER_HEIGHT: u32 = 861_576; // mainnet
pub(crate) const TAP_JUBILEE_HEIGHT: u32 = 824_544; // mainnet
pub(crate) const TAP_DMT_HEIGHT: u32 = 817_705; // mainnet
pub(crate) const TAP_DMT_NAT_REWARDS_HEIGHT: u32 = 885_588; // mainnet
pub(crate) const TAP_PRIVILEGE_ACTIVATION_HEIGHT: u32 = 841_682; // mainnet
pub(crate) const TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT: u32 = 885_588; // mainnet
pub(crate) const TAP_DMT_PARSEINT_ACTIVATION_HEIGHT: u32 = 885_588; // mainnet
pub(crate) const TAP_TESTNET_FIX_ACTIVATION_HEIGHT: u32 = 916_233; // mainnet
pub(crate) const TAP_AUTH_ITEM_LENGTH_ACTIVATION_HEIGHT: u32 = 916_233; // mainnet

// TAP Bloom Filter constants
pub(crate) const TAP_BLOOM_K: u8 = 10;
pub(crate) const TAP_BLOOM_DMT_BITS: u64 = 432_000_000;
pub(crate) const TAP_BLOOM_PRIV_BITS: u64 = 2_880_000;
pub(crate) const TAP_BLOOM_ANY_BITS: u64 = 432_000_000;
pub(crate) const TAP_BLOOM_DIR: &str = "tap-filters";

// Shared numeric/string constants
pub(crate) const MAX_DEC_U64_STR: &str = "18446744073709551615";
pub(crate) const BURN_ADDRESS: &str = "1BitcoinEaterAddressDontSendf59kuE";

#[derive(Copy, Clone)]
pub(crate) enum TapFeature {
  Bitmap,
  TapStart,
  FullTicker,
  Jubilee,
  Dmt,
  DmtNatRewards,
  PrivilegeActivation,
  ValueStringifyActivation,
  DmtParseintActivation,
  TokenAuthWhitelistFixActivation,
  TestnetFixActivation,
}
pub(crate) mod ops {
  pub(super) mod bitmap;
  pub(super) mod dmt_element;
  pub(super) mod dmt_mint;
  pub(super) mod dmt_deploy;
  pub(super) mod deploy;
  pub(super) mod mint;
  pub(super) mod transfer;
  pub(super) mod send;
  pub(super) mod trade;
  pub(super) mod auth;
  pub(super) mod privilege;
  pub(super) mod block;
}

// Re-export types for parent visibility
pub(crate) use filters::TapBloomFilter;
pub(crate) use kv::TapBatch;
pub(crate) use ops::dmt_element::DmtElementRecord;
pub(crate) use records::*;

// Helper functions implemented as associated fns on InscriptionUpdater
use super::super::InscriptionUpdater;
use unicode_segmentation::UnicodeSegmentation;
use secp256k1::{Secp256k1, Message, ecdsa::{RecoverableSignature, RecoveryId, Signature as SecpSignature}};
use sha2::{Digest, Sha256};
use bitcoin::{address::NetworkUnchecked, Address as BtcAddress, Network as BtcNetwork};
use std::str::FromStr;
use crate::SatPoint;

impl InscriptionUpdater<'_, '_> {
  // Visible-length and ticker rules
  pub(crate) fn valid_tap_ticker_visible_len(full_height: u32, height: u32, len: usize) -> bool {
    if height < full_height { len == 3 || (len >= 5 && len <= 32) } else { len > 0 && len <= 32 }
  }
  pub(crate) fn valid_brc20_ticker_visible_len(full_height: u32, height: u32, len: usize) -> bool {
    if height < full_height { len == 1 || len == 2 || len == 4 } else { false }
  }
  pub(crate) fn valid_transfer_ticker_visible_len(full_height: u32, height: u32, jubilee: u32, tick: &str, len: usize) -> bool {
    let t = tick.to_lowercase();
    let is_neg = t.starts_with('-');
    let is_dmt = t.starts_with("dmt-");
    if height < full_height {
      if !is_neg && !is_dmt { return len == 3 || (len >= 5 && len <= 32); }
      if is_neg && height >= jubilee { return len == 4 || (len >= 6 && len <= 33); }
      if is_dmt { return len == 7 || (len >= 9 && len <= 36); }
      return false;
    } else {
      if !is_neg && !is_dmt { return len > 0 && len <= 32; }
      if is_neg && height >= jubilee { return len > 1 && len <= 33; }
      if is_dmt { return len > 4 && len <= 36; }
      return false;
    }
  }
  pub(crate) fn strip_prefix_for_len_check(tick: &str) -> &str {
    let tl = tick.to_lowercase();
    if tl.starts_with('-') { &tick[1..] } else if tl.starts_with("dmt-") { &tick[4..] } else { tick }
  }
  pub(crate) fn visible_length(s: &str) -> usize {
    UnicodeSegmentation::graphemes(s, true).count()
  }
  pub(crate) fn is_valid_number(s: &str) -> bool {
    if s.is_empty() { return true; }
    let mut seen_dot = false;
    for c in s.chars() {
      if c.is_ascii_digit() { continue; }
      if c == '.' && !seen_dot { seen_dot = true; continue; }
      return false;
    }
    true
  }
  pub(crate) fn resolve_number_string(num: &str, decimals: u32) -> Option<String> {
    if !Self::is_valid_number(num) { return None; }
    let mut parts = num.split('.');
    let int_part = parts.next().unwrap_or("");
    let mut frac_part = parts.next().unwrap_or("").to_string();
    if parts.next().is_some() { return None; }
    if decimals > 0 && frac_part.is_empty() { frac_part = String::new(); }
    if frac_part.len() < decimals as usize { frac_part.extend(std::iter::repeat('0').take(decimals as usize - frac_part.len())); }
    let frac_trunc: String = frac_part.chars().take(decimals as usize).collect();
    let mut number = String::new();
    if int_part != "0" { number.push_str(int_part); }
    number.push_str(&frac_trunc);
    let is_zero = number.chars().all(|c| c == '0') || number.is_empty();
    if is_zero { number = "0".to_string(); }
    // strip leading zeros
    let mut first_non_zero_index = 0usize;
    for (i, c) in number.chars().enumerate() {
      if c != '0' { first_non_zero_index = i; break; }
      first_non_zero_index = i + 1;
    }
    if first_non_zero_index > 0 {
      let rest = number.get(first_non_zero_index..).unwrap_or("");
      number = if rest.is_empty() { "0".to_string() } else { rest.to_string() };
    }
    if number.is_empty() { number = "0".to_string(); }
    Some(number)
  }
  pub(crate) fn sha256_bytes(s: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    let out = hasher.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
  }
  pub(crate) fn is_valid_bitcoin_address_mainnet(addr: &str) -> bool {
    if let Ok(parsed) = addr.parse::<BtcAddress<NetworkUnchecked>>() { parsed.require_network(BtcNetwork::Bitcoin).is_ok() } else { false }
  }
  pub(crate) fn is_valid_bitcoin_address(&self, addr: &str) -> bool {
    // Absolute parity with tap-writer's isValidBitcoinAddress:
    // - Prefix + type gating (P2PKH/P2SH/P2WPKH/P2TR)
    // - Height-gated network rule: before full_ticker_height accept test/reg prefixes;
    //   at/after cutoff, accept mainnet only.
    let s = addr.trim().to_lowercase();
    let before_testnet_fix = self.height < self.feature_height(TapFeature::TestnetFixActivation);

    let parsed = match addr.parse::<BtcAddress<NetworkUnchecked>>() { Ok(p) => p, Err(_) => return false };

    // Helpers
    let main_ok = parsed.clone().require_network(BtcNetwork::Bitcoin).is_ok();
    let test_ok = parsed.clone().require_network(BtcNetwork::Testnet).is_ok();
    let reg_ok = parsed.clone().require_network(BtcNetwork::Regtest).is_ok();
    let signet_ok = parsed.clone().require_network(BtcNetwork::Signet).is_ok();
    let any_net_ok = main_ok || test_ok || signet_ok || reg_ok;
    let exact_net_ok = match self.btc_network {
      BtcNetwork::Bitcoin => main_ok,
      BtcNetwork::Testnet => test_ok,
      BtcNetwork::Signet  => signet_ok,
      BtcNetwork::Regtest => reg_ok,
      _ => main_ok || test_ok || signet_ok || reg_ok,
    };
    let spk = parsed.assume_checked_ref().script_pubkey();
    let b = spk.as_bytes();
    let is_p2wpkh = b.len() == 22 && b[0] == 0x00 && b[1] == 0x14; // OP_0 PUSH20
    let is_p2wsh  = b.len() == 34 && b[0] == 0x00 && b[1] == 0x20; // OP_0 PUSH32
    let is_p2tr   = b.len() == 34 && b[0] == 0x51 && b[1] == 0x20; // OP_1 PUSH32
    let is_p2pkh  = b.len() == 25 && b[0] == 0x76 && b[1] == 0xa9 && b[2] == 0x14 && b[23] == 0x88 && b[24] == 0xac;
    let is_p2sh   = b.len() == 23 && b[0] == 0xa9 && b[1] == 0x14 && b[22] == 0x87; // OP_HASH160 PUSH20 OP_EQUAL

    // Map writer's exact branches
    if s.starts_with("bc1q") {
      // P2WPKH/P2WSH: network gating per fix activation (writer fix removes separate p2wsh activation)
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && (is_p2wpkh || is_p2wsh);
    } else if s.starts_with("tb1q") || s.starts_with("bcrt1q") {
      // P2WPKH/P2WSH test/reg/signet branch
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && (is_p2wpkh || is_p2wsh);
    } else if s.starts_with("1") {
      // P2PKH mainnet prefix
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && is_p2pkh;
    } else if s.starts_with("m") || s.starts_with("n") {
      // P2PKH test/reg prefix
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && is_p2pkh;
    } else if s.starts_with("3") {
      // P2SH mainnet
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && is_p2sh;
    } else if s.starts_with("2") {
      // P2SH test/reg
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && is_p2sh;
    } else if s.starts_with("tb1p") || s.starts_with("bcrt1p") {
      // P2TR test/reg/signet
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && is_p2tr;
    } else {
      // Fallback: P2TR (e.g., bc1p...)
      let net_ok = if before_testnet_fix { any_net_ok } else { exact_net_ok };
      return net_ok && is_p2tr;
    }
  }
  pub(crate) fn normalize_address(addr: &str) -> String {
    let t = addr.trim();
    let tl = t.to_lowercase();
    if tl.starts_with("bc1") || tl.starts_with("tb1") || tl.starts_with("bcrt1") { tl } else { t.to_string() }
  }
  pub(crate) fn parse_sig_component_to_32(s: &str) -> Option<[u8; 32]> {
    let s = s.trim();
    if s.starts_with("0x") || s.starts_with("0X") {
      let hex_str = &s[2..];
      let mut bytes = hex::decode(hex_str).ok()?;
      if bytes.len() > 32 { return None; }
      if bytes.len() < 32 { let mut v = vec![0u8; 32 - bytes.len()]; v.extend(bytes); bytes = v; }
      let mut out = [0u8; 32]; out.copy_from_slice(&bytes); return Some(out);
    }
    let n = num_bigint::BigUint::from_str(s).ok()?;
    let mut bytes = n.to_bytes_be();
    if bytes.len() > 32 { return None; }
    if bytes.len() < 32 { let mut v = vec![0u8; 32 - bytes.len()]; v.extend(bytes); bytes = v; }
    let mut out = [0u8; 32]; out.copy_from_slice(&bytes); Some(out)
  }
  pub(crate) fn secp_compact_hex(r: &[u8; 32], s: &[u8; 32]) -> String {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(r);
    buf[32..].copy_from_slice(s);
    hex::encode(buf)
  }
  pub(crate) fn build_mint_privilege_message_hash(
    p: &str, op: &str, tmp_tick: &str, amt_str: &str, address: &str, ins_data: Option<&str>, prv_salt: &str,
  ) -> [u8; 32] {
    let msg_str = match ins_data {
      Some(d) => format!("{}-{}-{}-{}-{}-{}-{}", p, op, tmp_tick, amt_str, address, d, prv_salt),
      None => format!("{}-{}-{}-{}-{}-{}", p, op, tmp_tick, amt_str, address, prv_salt),
    };
    Self::sha256_bytes(&msg_str)
  }
  pub(crate) fn build_sha256_privilege_verify(
    prv: &str,
    col: &str,
    verify: &str,
    seq: &str,
    address: &str,
    salt: &str,
  ) -> [u8; 32] {
    let msg = format!("{}-{}-{}-{}-{}-{}", prv, col, verify, seq, address, salt);
    let mut hasher = Sha256::new();
    hasher.update(msg.as_bytes());
    let out = hasher.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
  }

  pub(crate) fn exec_internal_send_one(
    &mut self,
    from_addr: &str,
    to_addr: &str,
    tick: &str,
    amt_val: &serde_json::Value,
    dta: Option<String>,
    inscription: &str,
    num: i32,
    new_satpoint: SatPoint,
    output_value_sat: u64,
  ) {
    let tick_key = Self::json_stringify_lower(tick);
    let Some(deployed) = self.tap_get::<DeployRecord>(&format!("d/{}", tick_key)).ok().flatten() else { return; };
    let dec = deployed.dec;
    let amt_str = if amt_val.is_string() { amt_val.as_str().unwrap().to_string() } else { amt_val.to_string() };
    let amt_norm = match Self::resolve_number_string(&amt_str, dec) { Some(x) => x, None => return };
    // Enforce MAX_DEC_U64_STR cap at token decimals (parity with tap-writer)
    let max_norm = match Self::resolve_number_string(MAX_DEC_U64_STR, dec) { Some(x) => x, None => return };
    let amount = match amt_norm.parse::<i128>() { Ok(v) => v, Err(_) => return };
    let max_amount = match max_norm.parse::<i128>() { Ok(v) => v, Err(_) => return };
    if amount <= 0 || amount > max_amount { return; }
    // Balances
    let bal_key_from = format!("b/{}/{}", from_addr, tick_key);
    let mut from_balance = self.tap_get::<String>(&bal_key_from).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
    let from_trf = self.tap_get::<String>(&format!("t/{}/{}", from_addr, tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
    let mut to_balance = self.tap_get::<String>(&format!("b/{}/{}", to_addr, tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
    let mut fail = false;
    if from_balance - amount - from_trf < 0 { fail = true; }
    if !fail {
      // Avoid double-write when sending to self; in that case balances are unchanged
      if from_addr != to_addr {
        from_balance -= amount;
        to_balance += amount;
        let _ = self.tap_put(&bal_key_from, &from_balance.to_string());
        let _ = self.tap_put(&format!("b/{}/{}", to_addr, tick_key), &to_balance.to_string());
        if self.tap_get::<String>(&format!("he/{}/{}", to_addr, tick_key)).ok().flatten().is_none() {
          let _ = self.tap_put(&format!("he/{}/{}", to_addr, tick_key), &"".to_string());
          let _ = self.tap_set_list_record(&format!("h/{}", tick_key), &format!("hi/{}", tick_key), &to_addr.to_string());
        }
        if self.tap_get::<String>(&format!("ato/{}/{}", to_addr, tick_key)).ok().flatten().is_none() {
          let tick_lower = serde_json::from_str::<String>(&tick_key).unwrap_or_else(|_| tick.to_lowercase());
          let _ = self.tap_set_list_record(&format!("atl/{}", to_addr), &format!("atli/{}", to_addr), &tick_lower);
          let _ = self.tap_put(&format!("ato/{}/{}", to_addr, tick_key), &"".to_string());
        }
      }
    }
    // Writer parity: do not emit logs for self→self token-send
    if from_addr == to_addr {
      return;
    }
    // Logs (sender, receiver, flat, superflat)
    let srec = TransferSendSenderRecord { addr: from_addr.to_string(), taddr: to_addr.to_string(), blck: self.height, amt: amount.to_string(), trf: from_trf.to_string(), bal: from_balance.to_string(), tx: new_satpoint.outpoint.txid.to_string(), vo: u32::from(new_satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription.to_string(), num, ts: self.timestamp, fail, int: true, dta: dta.clone() };
    let _ = self.tap_set_list_record(&format!("strl/{}/{}", from_addr, tick_key), &format!("strli/{}/{}", from_addr, tick_key), &srec);
    let rrec = TransferSendReceiverRecord { faddr: from_addr.to_string(), addr: to_addr.to_string(), blck: self.height, amt: amount.to_string(), bal: to_balance.to_string(), tx: new_satpoint.outpoint.txid.to_string(), vo: u32::from(new_satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription.to_string(), num, ts: self.timestamp, fail, int: true, dta: dta.clone() };
    let _ = self.tap_set_list_record(&format!("rstrl/{}/{}", to_addr, tick_key), &format!("rstrli/{}/{}", to_addr, tick_key), &rrec);
    let frec = TransferSendFlatRecord { addr: from_addr.to_string(), taddr: to_addr.to_string(), blck: self.height, amt: amount.to_string(), trf: from_trf.to_string(), bal: from_balance.to_string(), tbal: to_balance.to_string(), tx: new_satpoint.outpoint.txid.to_string(), vo: u32::from(new_satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription.to_string(), num, ts: self.timestamp, fail, int: true, dta: dta.clone() };
    let _ = self.tap_set_list_record(&format!("fstrl/{}", tick_key), &format!("fstrli/{}", tick_key), &frec);
    let tick_label = serde_json::from_str::<String>(&tick_key).unwrap_or_else(|_| tick.to_lowercase());
    let sfrec = TransferSendSuperflatRecord { tick: tick_label, addr: from_addr.to_string(), taddr: to_addr.to_string(), blck: self.height, amt: amount.to_string(), trf: from_trf.to_string(), bal: from_balance.to_string(), tbal: to_balance.to_string(), tx: new_satpoint.outpoint.txid.to_string(), vo: u32::from(new_satpoint.outpoint.vout), val: output_value_sat.to_string(), ins: inscription.to_string(), num, ts: self.timestamp, fail, int: true, dta };
    if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
      let ptr = format!("sfstrli/{}", list_len - 1);
      let txs = new_satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(&format!("tx/snd/{}", txs), &format!("txi/snd/{}", txs), &ptr);
      let _ = self.tap_set_list_record(&format!("txt/snd/{}/{}", tick_key, txs), &format!("txti/snd/{}/{}", tick_key, txs), &ptr);
      let _ = self.tap_set_list_record(&format!("blck/snd/{}", self.height), &format!("blcki/snd/{}", self.height), &ptr);
      let _ = self.tap_set_list_record(&format!("blckt/snd/{}/{}", tick_key, self.height), &format!("blckti/snd/{}/{}", tick_key, self.height), &ptr);
    }
  }
  pub(crate) fn verify_privilege_signature_with_msg(
    &mut self,
    deployed_prv: &str,
    prv_obj: &serde_json::Value,
    msg_hash: &[u8; 32],
    address: &str,
  ) -> Option<(bool, String)> {
    // Returns (is_valid, compact_sig_hex)
    let sig = prv_obj.get("sig")?;
    let v_val = sig.get("v")?; let r_val = sig.get("r")?; let s_val = sig.get("s")?;
    let hash_val = prv_obj.get("hash")?; let prv_addr = prv_obj.get("address")?.as_str()?; let _prv_salt = prv_obj.get("salt")?.as_str()?;
    let v_i = if v_val.is_string() { v_val.as_str()?.parse::<i32>().ok()? } else { v_val.as_i64()? as i32 };
    let r_str = if r_val.is_string() { r_val.as_str()?.to_string() } else { r_val.to_string() };
    let s_str = if s_val.is_string() { s_val.as_str()?.to_string() } else { s_val.to_string() };
    let hash_hex = hash_val.as_str()?;
    let r_bytes = Self::parse_sig_component_to_32(&r_str)?; let s_bytes = Self::parse_sig_component_to_32(&s_str)?;
    let compact_sig_hex = Self::secp_compact_hex(&r_bytes, &s_bytes).to_lowercase();
    let rec_hash_bytes = hex::decode(hash_hex.trim_start_matches("0x")).ok()?; if rec_hash_bytes.len() != 32 { return None; }
    let mut rec_hash_arr = [0u8; 32]; rec_hash_arr.copy_from_slice(&rec_hash_bytes);
    let secp = Secp256k1::new();
    let rec_id = match RecoveryId::from_i32(v_i) { Ok(id) => id, Err(_) => { if v_i >= 27 { RecoveryId::from_i32(v_i - 27).ok()? } else { return None } } };
    let mut sig_bytes = [0u8; 64]; sig_bytes[..32].copy_from_slice(&r_bytes); sig_bytes[32..].copy_from_slice(&s_bytes);
    let rec_sig = RecoverableSignature::from_compact(&sig_bytes, rec_id).ok()?;
    let rec_msg = Message::from_digest_slice(&rec_hash_arr).ok()?;
    let pubkey = secp.recover_ecdsa(&rec_msg, &rec_sig).ok()?;
    // Recovered pubkey from mint signature (keep as PublicKey for equality)
    let pubkey_uncompressed = pubkey.serialize_uncompressed();
    let norm_sig = SecpSignature::from_compact(&sig_bytes).ok()?;
    let verify_msg = Message::from_digest_slice(msg_hash).ok()?;
    let is_valid = secp.verify_ecdsa(&verify_msg, &norm_sig, &pubkey).is_ok();

    // Validate authority link
    let link_ptr = self.tap_get::<String>(&format!("prains/{}", deployed_prv)).ok().flatten();
    let mut link_ok = false;
    if let Some(ptr) = link_ptr {
      if let Some(link_rec) = self.tap_get::<self::records::PrivilegeAuthCreateRecord>(&ptr).ok().flatten() {
        let sig = &link_rec.sig;
        let r2s = sig.get("r").and_then(|v| v.as_str()).unwrap_or("");
        let s2s = sig.get("s").and_then(|v| v.as_str()).unwrap_or("");
        let v2i = if let Some(sv) = sig.get("v").and_then(|v| v.as_str()) { sv.parse::<i32>().unwrap_or(0) } else { sig.get("v").and_then(|v| v.as_i64()).unwrap_or(0) as i32 };
        let r2b = Self::parse_sig_component_to_32(r2s)?;
        let s2b = Self::parse_sig_component_to_32(s2s)?;
        let rec_hash2 = hex::decode(link_rec.hash.trim_start_matches("0x")).ok()?;
        if rec_hash2.len() != 32 { return None; }
        let mut rec2_arr = [0u8; 32]; rec2_arr.copy_from_slice(&rec_hash2);
        let recid2 = RecoveryId::from_i32(v2i).or_else(|_| RecoveryId::from_i32(v2i - 27)).ok()?;
        let mut sig2b = [0u8; 64]; sig2b[..32].copy_from_slice(&r2b); sig2b[32..].copy_from_slice(&s2b);
        let rsig2 = RecoverableSignature::from_compact(&sig2b, recid2).ok()?;
        let rmsg2 = Message::from_digest_slice(&rec2_arr).ok()?;
        let auth_pk = secp.recover_ecdsa(&rmsg2, &rsig2).ok()?;
        let auth_msg_str = format!("{}{}", link_rec.auth.to_string(), link_rec.slt);
        let auth_msg_hash = Self::sha256_bytes(&auth_msg_str);
        let nsig2 = SecpSignature::from_compact(&sig2b).ok()?;
        let vmsg2 = Message::from_digest_slice(&auth_msg_hash).ok()?;
        let ok2 = secp.verify_ecdsa(&vmsg2, &nsig2, &auth_pk).is_ok();
        let cancelled = self.tap_get::<String>(&format!("prac/{}", link_rec.ins)).ok().flatten().is_some();
        if !cancelled && ok2 && pubkey == auth_pk && prv_addr == address { link_ok = true; }
      }
    }

    let used = self.tap_get::<String>(&format!("prah/{}", compact_sig_hex)).ok().flatten().is_some();
    let overall_ok = is_valid && !used && link_ok;
    Some((overall_ok, compact_sig_hex))
  }
  pub(crate) fn build_sha256_json_plus_salt(obj: &serde_json::Value, salt: &str) -> [u8; 32] {
    let s = serde_json::to_string(obj).unwrap_or_else(|_| String::new());
    let mut hasher = Sha256::new(); hasher.update(s.as_bytes()); hasher.update(salt.as_bytes());
    let out = hasher.finalize(); let mut arr = [0u8; 32]; arr.copy_from_slice(&out); arr
  }
  pub(crate) fn tap_feature_enabled(&self, feature: TapFeature) -> bool {
    self.height >= self.feature_height(feature)
  }
  pub(crate) fn feature_height(&self, feature: TapFeature) -> u32 {
    let is_mainnet = matches!(self.btc_network, BtcNetwork::Bitcoin);
    if !is_mainnet { return 0; }
    match feature {
      TapFeature::Bitmap => TAP_BITMAP_START_HEIGHT,
      TapFeature::TapStart => TAP_START_HEIGHT,
      TapFeature::FullTicker => TAP_FULL_TICKER_HEIGHT,
      TapFeature::Jubilee => TAP_JUBILEE_HEIGHT,
      TapFeature::Dmt => TAP_DMT_HEIGHT,
      TapFeature::DmtNatRewards => TAP_DMT_NAT_REWARDS_HEIGHT,
      TapFeature::PrivilegeActivation => TAP_PRIVILEGE_ACTIVATION_HEIGHT,
      TapFeature::ValueStringifyActivation => TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT,
      TapFeature::DmtParseintActivation => TAP_DMT_PARSEINT_ACTIVATION_HEIGHT,
      TapFeature::TokenAuthWhitelistFixActivation => TAP_AUTH_ITEM_LENGTH_ACTIVATION_HEIGHT,
      TapFeature::TestnetFixActivation => TAP_TESTNET_FIX_ACTIVATION_HEIGHT,
    }
  }
  pub(crate) fn json_stringify_lower(s: &str) -> String {
    serde_json::to_string(&s.to_lowercase()).unwrap_or_else(|_| format!("\"{}\"", s.to_lowercase()))
  }

}
