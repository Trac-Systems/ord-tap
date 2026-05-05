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
// START MINER-REWARD-SHIELD
pub(crate) const TAP_MINER_REWARD_SHIELD_ACTIVATION_HEIGHT: u32 = 941_848; // mainnet
// END MINER-REWARD-SHIELD
// START MINER-REWARD-SHIELD
pub(crate) const TAP_MINER_REWARD_TRANSFER_EXECUTION_SHIELD_ACTIVATION_HEIGHT: u32 = 942_002; // mainnet
// END MINER-REWARD-SHIELD
// START MINER-REWARD-SHIELD
pub(crate) const TAP_DMT_REWARD_ADDRESS_PREFIX: &str = "dmtrwd";
// END MINER-REWARD-SHIELD

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
  // START MINER-REWARD-SHIELD
  MinerRewardShieldActivation,
  MinerRewardTransferExecutionShieldActivation,
  // END MINER-REWARD-SHIELD
}
pub(crate) mod ops {
  pub(super) mod bitmap;
  pub(super) mod dmt_element;
  pub(super) mod dmt_mint;
  pub(super) mod dmt_deploy;
  pub(super) mod dmt_redirect;
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

  pub(crate) fn is_js_whitespace(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
  }

  pub(crate) fn trim_js_whitespace(s: &str) -> &str {
    s.trim_matches(Self::is_js_whitespace)
  }

  pub(crate) fn js_number_to_string(f: f64) -> Option<String> {
    if !f.is_finite() { return None; }
    if f == 0.0 { return Some("0".to_string()); }
    let abs = f.abs();
    if abs >= 1e21 || abs < 1e-6 {
      let mut s = format!("{:e}", f);
      if let Some(e_pos) = s.find('e') {
        let mut mantissa = s[..e_pos].to_string();
        while mantissa.contains('.') && mantissa.ends_with('0') { mantissa.pop(); }
        if mantissa.ends_with('.') { mantissa.pop(); }
        let exp_num = s[e_pos + 1..].parse::<i32>().ok()?;
        let sign = if exp_num >= 0 { "+" } else { "" };
        s = format!("{}e{}{}", mantissa, sign, exp_num);
      }
      return Some(s);
    }
    if f.fract() == 0.0 { return Some(format!("{:.0}", f)); }
    Some(f.to_string())
  }

  fn js_array_to_string(items: &[serde_json::Value]) -> String {
    items.iter().map(|item| match item {
      serde_json::Value::Null => "".to_string(),
      serde_json::Value::Array(inner) => Self::js_array_to_string(inner),
      other => Self::js_value_to_string(other),
    }).collect::<Vec<_>>().join(",")
  }

  pub(crate) fn js_value_to_string(v: &serde_json::Value) -> String {
    match v {
      serde_json::Value::Null => "null".to_string(),
      serde_json::Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
      serde_json::Value::Number(n) => {
        if let Some(i) = n.as_i64() { i.to_string() }
        else if let Some(u) = n.as_u64() { u.to_string() }
        else { n.as_f64().and_then(Self::js_number_to_string).unwrap_or_else(|| n.to_string()) }
      }
      serde_json::Value::String(s) => s.clone(),
      serde_json::Value::Array(items) => Self::js_array_to_string(items),
      serde_json::Value::Object(_) => "[object Object]".to_string(),
    }
  }

  pub(crate) fn js_parse_int_with_string(v: &serde_json::Value) -> Option<(i128, String)> {
    let js_s = Self::js_value_to_string(v);
    let mut s = js_s.as_str();
    while let Some(c) = s.chars().next() {
      if Self::is_js_whitespace(c) { s = &s[c.len_utf8()..]; } else { break; }
    }
    let mut sign = 1i128;
    if let Some(c) = s.chars().next() {
      if c == '-' {
        sign = -1;
        s = &s[1..];
      } else if c == '+' {
        s = &s[1..];
      }
    }
    let (radix, digits) = if s.starts_with("0x") || s.starts_with("0X") {
      (16u32, &s[2..])
    } else {
      (10u32, s)
    };
    let mut found = false;
    let mut acc = 0i128;
    for c in digits.chars() {
      let Some(d) = c.to_digit(radix) else { break; };
      found = true;
      acc = acc.saturating_mul(radix as i128).saturating_add(d as i128);
    }
    if !found { return None; }
    Some((acc.saturating_mul(sign), js_s))
  }

  pub(crate) fn js_parse_int(v: &serde_json::Value) -> Option<i128> {
    Self::js_parse_int_with_string(v).map(|(n, _)| n)
  }

  pub(crate) fn js_parse_int_i32(v: &serde_json::Value) -> Option<i32> {
    let n = Self::js_parse_int(v)?;
    i32::try_from(n).ok()
  }

  fn parse_js_bigint_string_to_biguint(s: &str) -> Option<num_bigint::BigUint> {
    let s = Self::trim_js_whitespace(s);
    if s.is_empty() { return Some(num_bigint::BigUint::from(0u8)); }
    if s.starts_with('-') { return None; }
    let (radix, digits) = if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
      (16u32, rest)
    } else if let Some(rest) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
      (2u32, rest)
    } else if let Some(rest) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
      (8u32, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
      if rest.starts_with("0x") || rest.starts_with("0X") || rest.starts_with("0b") || rest.starts_with("0B") || rest.starts_with("0o") || rest.starts_with("0O") {
        return None;
      }
      (10u32, rest)
    } else {
      (10u32, s)
    };
    if digits.is_empty() { return None; }
    let mut acc = num_bigint::BigUint::from(0u8);
    let base = num_bigint::BigUint::from(radix);
    for c in digits.chars() {
      let d = c.to_digit(radix)?;
      acc = acc * &base + num_bigint::BigUint::from(d);
    }
    Some(acc)
  }

  pub(crate) fn js_bigint_value_to_biguint(v: &serde_json::Value) -> Option<num_bigint::BigUint> {
    match v {
      serde_json::Value::Bool(b) => Some(num_bigint::BigUint::from(if *b { 1u8 } else { 0u8 })),
      serde_json::Value::Number(n) => {
        if let Some(u) = n.as_u64() { return Some(num_bigint::BigUint::from(u)); }
        if let Some(i) = n.as_i64() {
          if i < 0 { return None; }
          return Some(num_bigint::BigUint::from(i as u64));
        }
        let f = n.as_f64()?;
        if !f.is_finite() || f < 0.0 || f.fract() != 0.0 { return None; }
        Self::parse_js_bigint_string_to_biguint(&format!("{:.0}", f))
      }
      serde_json::Value::String(s) => Self::parse_js_bigint_string_to_biguint(s),
      serde_json::Value::Array(_) => Self::parse_js_bigint_string_to_biguint(&Self::js_value_to_string(v)),
      _ => None,
    }
  }

  pub(crate) fn js_bigint_value_to_32(v: &serde_json::Value) -> Option<[u8; 32]> {
    let n = Self::js_bigint_value_to_biguint(v)?;
    let mut bytes = n.to_bytes_be();
    if bytes.len() > 32 { return None; }
    if bytes.len() < 32 {
      let mut padded = vec![0u8; 32 - bytes.len()];
      padded.extend(bytes);
      bytes = padded;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Some(out)
  }

  pub(crate) fn js_bigint_string_to_i128(s: &str) -> Option<i128> {
    let n = Self::parse_js_bigint_string_to_biguint(s)?;
    let bytes = n.to_bytes_be();
    if bytes.len() > 16 { return None; }
    if bytes.len() == 16 && bytes[0] >= 0x80 { return None; }
    let mut out = 0i128;
    for b in bytes { out = (out << 8) | i128::from(b); }
    Some(out)
  }

  pub(crate) fn js_json_stringify(v: &serde_json::Value) -> String {
    match v {
      serde_json::Value::Null => "null".to_string(),
      serde_json::Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
      serde_json::Value::Number(n) => {
        if let Some(i) = n.as_i64() { i.to_string() }
        else if let Some(u) = n.as_u64() { u.to_string() }
        else { n.as_f64().and_then(Self::js_number_to_string).unwrap_or_else(|| "null".to_string()) }
      }
      serde_json::Value::String(s) => serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string()),
      serde_json::Value::Array(items) => {
        let inner = items.iter().map(Self::js_json_stringify).collect::<Vec<_>>().join(",");
        format!("[{}]", inner)
      }
      serde_json::Value::Object(map) => {
        let inner = map.iter().map(|(k, v)| {
          let key = serde_json::to_string(k).unwrap_or_else(|_| "\"\"".to_string());
          format!("{}:{}", key, Self::js_json_stringify(v))
        }).collect::<Vec<_>>().join(",");
        format!("{{{}}}", inner)
      }
    }
  }

  pub(crate) fn js_word_boundary_hex64_test(s: &str) -> bool {
    fn is_word(b: u8) -> bool { b.is_ascii_alphanumeric() || b == b'_' }
    let bytes = s.as_bytes();
    if bytes.len() < 64 { return false; }
    for start in 0..=bytes.len() - 64 {
      let end = start + 64;
      if !bytes[start..end].iter().all(|b| b.is_ascii_hexdigit()) { continue; }
      let before_word = start > 0 && is_word(bytes[start - 1]);
      let first_word = is_word(bytes[start]);
      let last_word = is_word(bytes[end - 1]);
      let after_word = end < bytes.len() && is_word(bytes[end]);
      if before_word != first_word && last_word != after_word { return true; }
    }
    false
  }

  pub(crate) fn writer_loose_inscription_id_syntax(s: &str) -> bool {
    if !s.contains('i') { return false; }
    let parts: Vec<&str> = s.split('i').collect();
    parts.len() == 2
      && parts[0].len() == 64
      && parts[0].chars().all(|c| c.is_ascii_hexdigit())
      && Self::js_parse_int(&serde_json::Value::String(parts[1].to_string())).is_some()
  }

  pub(crate) fn parse_tap_json_value(&self, s: &str) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(s).ok()?;
    // taprest runs tap-writer on Node 20.10.0, where the JSON.parse reviver
    // context argument is undefined. That makes raw numeric max/lim/amt throw.
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation)
      && Self::tap_writer_node20_value_stringify_would_throw(&value)
    {
      return None;
    }
    Some(value)
  }

  fn tap_writer_node20_value_stringify_would_throw(value: &serde_json::Value) -> bool {
    match value {
      serde_json::Value::Object(map) => map.iter().any(|(key, child)| {
        ((key == "max" || key == "lim" || key == "amt") && child.is_number())
          || Self::tap_writer_node20_value_stringify_would_throw(child)
      }),
      serde_json::Value::Array(items) => items
        .iter()
        .any(Self::tap_writer_node20_value_stringify_would_throw),
      _ => false,
    }
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
    let s = Self::trim_js_whitespace(addr).to_lowercase();
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
    let t = Self::trim_js_whitespace(addr);
    let tl = t.to_lowercase();
    if tl.starts_with("bc1") || tl.starts_with("tb1") || tl.starts_with("bcrt1") { tl } else { t.to_string() }
  }
  pub(crate) fn parse_sig_component_to_32(s: &str) -> Option<[u8; 32]> {
    let s = Self::trim_js_whitespace(s);
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
    let amt_str = Self::js_value_to_string(amt_val);
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
    let hash_val = prv_obj.get("hash")?; let prv_addr = prv_obj.get("address")?.as_str()?; let _prv_salt = prv_obj.get("salt")?;
    let hash_hex = hash_val.as_str()?;
    let v_i = Self::js_parse_int_i32(v_val)?;
    let r_bytes = Self::js_bigint_value_to_32(r_val)?; let s_bytes = Self::js_bigint_value_to_32(s_val)?;
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
    let norm_sig = SecpSignature::from_compact(&sig_bytes).ok()?;
    let verify_msg = Message::from_digest_slice(msg_hash).ok()?;
    let is_valid = secp.verify_ecdsa(&verify_msg, &norm_sig, &pubkey).is_ok();

    // Validate authority link
    let link_ptr = self.tap_get::<String>(&format!("prains/{}", deployed_prv)).ok().flatten();
    let mut link_ok = false;
    if let Some(ptr) = link_ptr {
      if let Some(link_rec) = self.tap_get::<self::records::PrivilegeAuthCreateRecord>(&ptr).ok().flatten() {
        let sig = &link_rec.sig;
        let v2i = Self::js_parse_int_i32(sig.get("v")?)?;
        let r2b = Self::js_bigint_value_to_32(sig.get("r")?)?;
        let s2b = Self::js_bigint_value_to_32(sig.get("s")?)?;
        let rec_hash2 = hex::decode(link_rec.hash.trim_start_matches("0x")).ok()?;
        if rec_hash2.len() != 32 { return None; }
        let mut rec2_arr = [0u8; 32]; rec2_arr.copy_from_slice(&rec_hash2);
        let recid2 = RecoveryId::from_i32(v2i).or_else(|_| RecoveryId::from_i32(v2i - 27)).ok()?;
        let mut sig2b = [0u8; 64]; sig2b[..32].copy_from_slice(&r2b); sig2b[32..].copy_from_slice(&s2b);
        let rsig2 = RecoverableSignature::from_compact(&sig2b, recid2).ok()?;
        let rmsg2 = Message::from_digest_slice(&rec2_arr).ok()?;
        let auth_pk = secp.recover_ecdsa(&rmsg2, &rsig2).ok()?;
        let auth_msg_hash = Self::build_sha256_json_plus_salt(&link_rec.auth, &link_rec.slt);
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
    let s = Self::js_json_stringify(obj);
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
      // START MINER-REWARD-SHIELD
      TapFeature::MinerRewardShieldActivation => TAP_MINER_REWARD_SHIELD_ACTIVATION_HEIGHT,
      TapFeature::MinerRewardTransferExecutionShieldActivation => TAP_MINER_REWARD_TRANSFER_EXECUTION_SHIELD_ACTIVATION_HEIGHT,
      // END MINER-REWARD-SHIELD
    }
  }
  pub(crate) fn json_stringify_lower(s: &str) -> String {
    serde_json::to_string(&s.to_lowercase()).unwrap_or_else(|_| format!("\"{}\"", s.to_lowercase()))
  }

  // START MINER-REWARD-SHIELD
  pub(crate) fn tap_dmt_reward_address_key(address: &str) -> String {
    format!("{}/{}", TAP_DMT_REWARD_ADDRESS_PREFIX, address)
  }

  // START MINER-REWARD-SHIELD
  pub(crate) fn tap_has_dmt_reward_address_mark(&mut self, address: &str) -> bool {
    self.tap_get::<String>(&Self::tap_dmt_reward_address_key(address)).ok().flatten().is_some()
  }
  // END MINER-REWARD-SHIELD

  pub(crate) fn tap_is_dmt_reward_address(&mut self, address: &str) -> bool {
    if !self.tap_feature_enabled(TapFeature::MinerRewardShieldActivation) { return false; }
    self.tap_has_dmt_reward_address_mark(address)
  }

  // START MINER-REWARD-SHIELD
  pub(crate) fn tap_blocks_dmt_reward_transfer_execution(&mut self, address: &str) -> bool {
    if !self.tap_feature_enabled(TapFeature::MinerRewardTransferExecutionShieldActivation) { return false; }
    if !self.tap_has_dmt_reward_address_mark(address) { return false; }
    self.tap_get::<String>(&format!("bltr/{}", address)).ok().flatten().is_some()
  }
  // END MINER-REWARD-SHIELD

  pub(crate) fn tap_mark_dmt_reward_address(&mut self, address: &str) {
    if !self.tap_feature_enabled(TapFeature::MinerRewardShieldActivation) { return; }
    if self.tap_has_dmt_reward_address_mark(address) { return; }
    let _ = self.tap_put(&Self::tap_dmt_reward_address_key(address), &"".to_string());
    // Auto-block transferables once on first reward credit, but do not re-block if the miner
    // later unblocks deliberately.
    if self.tap_get::<String>(&format!("bltr/{}", address)).ok().flatten().is_none() {
      let _ = self.tap_put(&format!("bltr/{}", address), &"".to_string());
    }
  }
  // END MINER-REWARD-SHIELD

}

// START MINER-REWARD-SHIELD
#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Chain, Inscription, InscriptionId};
  use crate::index::{
    entry::Entry,
    testing::Context,
    HOME_INSCRIPTIONS, INSCRIPTION_ID_TO_SEQUENCE_NUMBER, INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER,
    SEQUENCE_NUMBER_TO_CHILDREN, SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY, SAT_TO_SEQUENCE_NUMBER,
    TAP_KV, TRANSACTION_ID_TO_TRANSACTION,
  };
  use bitcoin::{
    absolute::LockTime, address::NetworkUnchecked, transaction::Version, Address, Amount,
    OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
  };
  use redb::Database;
  use std::{collections::HashMap, str::FromStr};
  use tempfile::TempDir;

  const MINER_ADDRESS: &str = "tb1q6en7qjxgw4ev8xwx94pzdry6a6ky7wlfeqzunz";
  const USER_ADDRESS: &str = "tb1qjsv26lap3ffssj6hfy8mzn0lg5vte6a42j75ww";
  const RECIPIENT_ADDRESS: &str = "tb1qakxxzv9n7706kc3xdcycrtfv8cqv62hnwexc0l";

  fn with_test_updater<T>(
    network: BtcNetwork,
    height: u32,
    test: impl FnOnce(&mut InscriptionUpdater<'_, '_>) -> T,
  ) -> T {
    let tempdir = TempDir::new().unwrap();
    let db = Database::create(tempdir.path().join("tap-miner-reward-shield.redb")).unwrap();
    let write_tx = db.begin_write().unwrap();

    let mut home_inscriptions = write_tx.open_table(HOME_INSCRIPTIONS).unwrap();
    let mut id_to_sequence_number = write_tx.open_table(INSCRIPTION_ID_TO_SEQUENCE_NUMBER).unwrap();
    let mut inscription_number_to_sequence_number =
      write_tx.open_table(INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER).unwrap();
    let mut transaction_id_to_transaction = write_tx.open_table(TRANSACTION_ID_TO_TRANSACTION).unwrap();
    let mut sat_to_sequence_number = write_tx.open_multimap_table(SAT_TO_SEQUENCE_NUMBER).unwrap();
    let mut sequence_number_to_children =
      write_tx.open_multimap_table(SEQUENCE_NUMBER_TO_CHILDREN).unwrap();
    let mut sequence_number_to_entry =
      write_tx.open_table(SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY).unwrap();
    let mut tap_kv = write_tx.open_table(TAP_KV).unwrap();

    let mut updater = InscriptionUpdater {
      blessed_inscription_count: 0,
      cursed_inscription_count: 0,
      flotsam: Vec::new(),
      height,
      run_start_height: height,
      home_inscription_count: 0,
      home_inscriptions: &mut home_inscriptions,
      id_to_sequence_number: &mut id_to_sequence_number,
      inscription_number_to_sequence_number: &mut inscription_number_to_sequence_number,
      lost_sats: 0,
      next_sequence_number: 0,
      reward: 0,
      transaction_buffer: Vec::new(),
      transaction_id_to_transaction: &mut transaction_id_to_transaction,
      sat_to_sequence_number: &mut sat_to_sequence_number,
      sequence_number_to_children: &mut sequence_number_to_children,
      sequence_number_to_entry: &mut sequence_number_to_entry,
      timestamp: 0,
      unbound_inscriptions: 0,
      tap_db: TapBatch::new(&mut tap_kv),
      dmt_bloom: None,
      priv_bloom: None,
      list_len_cache: HashMap::new(),
      any_bloom: None,
      block_availability_cache: HashMap::new(),
      profile: false,
      prof_bm_tr_ms: 0,
      prof_bm_tr_ct: 0,
      prof_dmt_tr_ms: 0,
      prof_dmt_tr_ct: 0,
      prof_prv_tr_ms: 0,
      prof_prv_tr_ct: 0,
      prof_ttr_ex_ms: 0,
      prof_ttr_ex_ct: 0,
      prof_tsend_ex_ms: 0,
      prof_tsend_ex_ct: 0,
      prof_ttrade_ex_ms: 0,
      prof_ttrade_ex_ct: 0,
      prof_tauth_ex_ms: 0,
      prof_tauth_ex_ct: 0,
      prof_pra_ex_ms: 0,
      prof_pra_ex_ct: 0,
      prof_blk_ex_ms: 0,
      prof_blk_ex_ct: 0,
      prof_unblk_ex_ms: 0,
      prof_unblk_ex_ct: 0,
      prof_created_total_ms: 0,
      prof_created_ct: 0,
      prof_bm_cr_ms: 0,
      prof_bm_cr_ct: 0,
      prof_dmt_el_cr_ms: 0,
      prof_dmt_el_cr_ct: 0,
      prof_dpl_cr_ms: 0,
      prof_dpl_cr_ct: 0,
      prof_dmtmint_cr_ms: 0,
      prof_dmtmint_cr_ct: 0,
      prof_mint_cr_ms: 0,
      prof_mint_cr_ct: 0,
      prof_ttr_cr_ms: 0,
      prof_ttr_cr_ct: 0,
      prof_tsend_cr_ms: 0,
      prof_tsend_cr_ct: 0,
      prof_ttrade_cr_ms: 0,
      prof_ttrade_cr_ct: 0,
      prof_tauth_cr_ms: 0,
      prof_tauth_cr_ct: 0,
      prof_dmtdep_cr_ms: 0,
      prof_dmtdep_cr_ct: 0,
      prof_pra_cr_ms: 0,
      prof_pra_cr_ct: 0,
      prof_prv_cr_ms: 0,
      prof_prv_cr_ct: 0,
      prof_blk_cr_ms: 0,
      prof_blk_cr_ct: 0,
      prof_unblk_cr_ms: 0,
      prof_unblk_cr_ct: 0,
      prof_core_env_ms: 0,
      prof_core_old_ms: 0,
      prof_core_new_ms: 0,
      prof_core_parent_ms: 0,
      prof_core_txdb_ms: 0,
      prof_core_addr_ms: 0,
      prof_core_update_ms: 0,
      prof_core_event_ms: 0,
      prof_core_event_ct: 0,
      prof_core_old_ct: 0,
      prof_core_new_ct: 0,
      prof_core_txdb_ct: 0,
      prof_core_addr_ct: 0,
      prof_core_update_ct: 0,
      prof_core_up_old_ms: 0,
      prof_core_up_old_ct: 0,
      prof_core_up_new_ms: 0,
      prof_core_up_new_ct: 0,
      prof_core_up_new_parents_us: 0,
      prof_core_up_new_entry_us: 0,
      prof_core_up_new_serialize_us: 0,
      prof_core_up_new_maps_us: 0,
      prof_core_up_new_num_us: 0,
      prof_core_up_new_sat_us: 0,
      prof_core_up_new_delegate_us: 0,
      prof_core_up_tap_us: 0,
      prof_core_up_utxo_us: 0,
      delegate_cache: HashMap::new(),
      delegate_payload_cache: HashMap::new(),
      btc_network: network,
    };

    test(&mut updater)
  }

  fn txid_from_seed(seed: u8) -> Txid {
    Txid::from_str(&format!("{seed:064x}")).unwrap()
  }

  fn inscription_id_from_seed(seed: u8) -> InscriptionId {
    InscriptionId {
      txid: txid_from_seed(seed),
      index: 0,
    }
  }

  fn satpoint_from_inscription(inscription_id: InscriptionId, vout: u32) -> SatPoint {
    SatPoint {
      outpoint: OutPoint {
        txid: inscription_id.txid,
        vout,
      },
      offset: 0,
    }
  }

  fn transfer_satpoint(seed: u8, vout: u32) -> SatPoint {
    SatPoint {
      outpoint: OutPoint {
        txid: txid_from_seed(seed),
        vout,
      },
      offset: 0,
    }
  }

  fn inscription_from_json(json: serde_json::Value) -> Inscription {
    Inscription {
      body: Some(serde_json::to_vec(&json).unwrap()),
      ..Default::default()
    }
  }

  fn inscription_from_body(body: &str) -> Inscription {
    Inscription {
      body: Some(body.as_bytes().to_vec()),
      ..Default::default()
    }
  }

  fn deploy_record_with_supply(
    tick: &str,
    addr: &str,
    dec: u32,
    max: &str,
    lim: &str,
  ) -> DeployRecord {
    DeployRecord {
      tick: tick.to_string(),
      max: InscriptionUpdater::resolve_number_string(max, dec).unwrap(),
      lim: InscriptionUpdater::resolve_number_string(lim, dec).unwrap(),
      dec,
      blck: 0,
      tx: txid_from_seed(200).to_string(),
      vo: 0,
      val: "1000".to_string(),
      ins: inscription_id_from_seed(200).to_string(),
      num: 0,
      ts: 0,
      addr: addr.to_string(),
      crsd: false,
      dmt: false,
      elem: None,
      prj: None,
      dim: None,
      dt: None,
      prv: None,
      dta: None,
    }
  }

  fn put_deploy(updater: &mut InscriptionUpdater<'_, '_>, tick: &str, addr: &str) {
    put_deploy_with_dec(updater, tick, addr, 0);
  }

  fn put_deploy_with_dec(updater: &mut InscriptionUpdater<'_, '_>, tick: &str, addr: &str, dec: u32) {
    put_deploy_with_supply(updater, tick, addr, dec, "21000000", "1000");
  }

  fn put_deploy_with_supply(
    updater: &mut InscriptionUpdater<'_, '_>,
    tick: &str,
    addr: &str,
    dec: u32,
    max: &str,
    lim: &str,
  ) {
    updater
      .tap_put(
        &format!("d/{}", InscriptionUpdater::json_stringify_lower(tick)),
        &deploy_record_with_supply(tick, addr, dec, max, lim),
      )
      .unwrap();
  }

  fn put_balance(updater: &mut InscriptionUpdater<'_, '_>, addr: &str, tick: &str, balance: &str) {
    updater
      .tap_put(
        &format!("b/{}/{}", addr, InscriptionUpdater::json_stringify_lower(tick)),
        &balance.to_string(),
      )
      .unwrap();
  }

  fn get_string(updater: &mut InscriptionUpdater<'_, '_>, key: &str) -> Option<String> {
    updater.tap_get::<String>(key).unwrap()
  }

  fn get_acc_addr(updater: &mut InscriptionUpdater<'_, '_>, key: &str) -> Option<String> {
    updater.tap_get::<TapAccumulatorEntry>(key).unwrap().map(|entry| entry.addr)
  }

  // START MINER-REWARD-SHIELD
  fn seed_transferable(
    updater: &mut InscriptionUpdater<'_, '_>,
    addr: &str,
    tick: &str,
    amount: &str,
    inscription_id: InscriptionId,
    record_seed: u8,
  ) {
    let tick_key = InscriptionUpdater::json_stringify_lower(tick);
    let ptr = format!("atrli/{}/{}/0", addr, tick_key);
    let balance = get_string(updater, &format!("b/{}/{}", addr, tick_key)).unwrap_or_else(|| "0".to_string());
    updater.tap_put(&format!("t/{}/{}", addr, tick_key), &amount.to_string()).unwrap();
    updater.tap_put(&format!("tamt/{}", inscription_id), &amount.to_string()).unwrap();
    updater.tap_put(&format!("tl/{}", inscription_id), &ptr).unwrap();
    updater.tap_put(
      &ptr,
      &TransferInitRecord {
        addr: addr.to_string(),
        blck: 1,
        amt: amount.to_string(),
        trf: amount.to_string(),
        bal: balance,
        tx: txid_from_seed(record_seed).to_string(),
        vo: 0,
        val: "1000".to_string(),
        ins: inscription_id.to_string(),
        num: 0,
        ts: 0,
        fail: false,
        int: false,
        dta: None,
      },
    ).unwrap();
  }
  // END MINER-REWARD-SHIELD

  fn build_miner_reward_shield_snapshot() -> serde_json::Value {
    let activation = serde_json::json!({
      "mainnet_active_at_zero": with_test_updater(BtcNetwork::Bitcoin, 0, |updater| updater.tap_feature_enabled(TapFeature::MinerRewardShieldActivation)),
      "mainnet_active_at_one_million": with_test_updater(BtcNetwork::Bitcoin, 1_000_000, |updater| updater.tap_feature_enabled(TapFeature::MinerRewardShieldActivation)),
      "signet_active_at_zero": with_test_updater(BtcNetwork::Signet, 0, |updater| updater.tap_feature_enabled(TapFeature::MinerRewardShieldActivation)),
      // START MINER-REWARD-SHIELD
      "mainnet_transfer_execution_active_at_zero": with_test_updater(BtcNetwork::Bitcoin, 0, |updater| updater.tap_feature_enabled(TapFeature::MinerRewardTransferExecutionShieldActivation)),
      "mainnet_transfer_execution_active_at_one_million": with_test_updater(BtcNetwork::Bitcoin, 1_000_000, |updater| updater.tap_feature_enabled(TapFeature::MinerRewardTransferExecutionShieldActivation)),
      "signet_transfer_execution_active_at_zero": with_test_updater(BtcNetwork::Signet, 0, |updater| updater.tap_feature_enabled(TapFeature::MinerRewardTransferExecutionShieldActivation)),
      // END MINER-REWARD-SHIELD
    });

    let mainnet_inactive_mark = with_test_updater(BtcNetwork::Bitcoin, 1, |updater| {
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      serde_json::json!({
        "reward_marked": updater.tap_is_dmt_reward_address(MINER_ADDRESS),
        "auto_blocked": get_string(updater, &format!("bltr/{}", MINER_ADDRESS)).is_some(),
      })
    });

    let reward_mark = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      let marked = updater.tap_is_dmt_reward_address(MINER_ADDRESS);
      let auto_blocked = get_string(updater, &format!("bltr/{}", MINER_ADDRESS)).is_some();
      updater.tap_del(&format!("bltr/{}", MINER_ADDRESS)).unwrap();
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      let reblocked_after_manual_unblock =
        get_string(updater, &format!("bltr/{}", MINER_ADDRESS)).is_some();

      serde_json::json!({
        "reward_marked": marked,
        "auto_blocked": auto_blocked,
        "reblocked_after_manual_unblock": reblocked_after_manual_unblock,
      })
    });

    let non_miner_transfer = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");

      let transfer_id = inscription_id_from_seed(1);
      updater.index_token_transfer_created(
        transfer_id,
        0,
        satpoint_from_inscription(transfer_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-transfer",
          "tick": "foo",
          "amt": "5"
        })),
        USER_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "tamt": get_string(updater, &format!("tamt/{}", transfer_id)),
        "transferable": get_string(
          updater,
          &format!("t/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))
        ),
      })
    });

    let miner_transfer = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      let transfer_id = inscription_id_from_seed(2);
      updater.index_token_transfer_created(
        transfer_id,
        0,
        satpoint_from_inscription(transfer_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-transfer",
          "tick": "foo",
          "amt": "5"
        })),
        MINER_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "tamt": get_string(updater, &format!("tamt/{}", transfer_id)),
        "transferable": get_string(
          updater,
          &format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))
        ),
      })
    });

    // START MINER-REWARD-SHIELD
    let non_miner_transfer_execution = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");
      seed_transferable(updater, USER_ADDRESS, "foo", "5", inscription_id_from_seed(30), 31);

      updater.index_token_transfer_executed(
        inscription_id_from_seed(30),
        0,
        transfer_satpoint(32, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "sender_balance": get_string(updater, &format!("b/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "recipient_balance": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "transferable": get_string(updater, &format!("t/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "tamt": get_string(updater, &format!("tamt/{}", inscription_id_from_seed(30))),
        "link": get_string(updater, &format!("tl/{}", inscription_id_from_seed(30))),
      })
    });

    let reward_transfer_execution_invalidated_after_foreign_move = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      seed_transferable(updater, MINER_ADDRESS, "foo", "5", inscription_id_from_seed(33), 34);

      updater.index_token_transfer_executed(
        inscription_id_from_seed(33),
        0,
        transfer_satpoint(35, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "sender_balance": get_string(updater, &format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "recipient_balance": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "transferable": get_string(updater, &format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "tamt": get_string(updater, &format!("tamt/{}", inscription_id_from_seed(33))),
        "link": get_string(updater, &format!("tl/{}", inscription_id_from_seed(33))),
      })
    });

    let (reward_transfer_execution_blocked_same_address, reward_transfer_execution_after_unblock) =
      with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      seed_transferable(updater, MINER_ADDRESS, "foo", "5", inscription_id_from_seed(36), 37);

      updater.index_token_transfer_executed(
        inscription_id_from_seed(36),
        0,
        transfer_satpoint(38, 0),
        MINER_ADDRESS,
        1_000,
      );

      let blocked_same_address = serde_json::json!({
        "sender_balance": get_string(updater, &format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "recipient_balance": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "transferable": get_string(updater, &format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "tamt": get_string(updater, &format!("tamt/{}", inscription_id_from_seed(36))),
        "link": get_string(updater, &format!("tl/{}", inscription_id_from_seed(36))),
      });

      updater.tap_del(&format!("bltr/{}", MINER_ADDRESS)).unwrap();
      updater.index_token_transfer_executed(
        inscription_id_from_seed(36),
        0,
        transfer_satpoint(39, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      let after_unblock = serde_json::json!({
        "sender_balance": get_string(updater, &format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "recipient_balance": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "transferable": get_string(updater, &format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "tamt": get_string(updater, &format!("tamt/{}", inscription_id_from_seed(36))),
        "link": get_string(updater, &format!("tl/{}", inscription_id_from_seed(36))),
      });

      (blocked_same_address, after_unblock)
    });
    // END MINER-REWARD-SHIELD

    let non_miner_send_creation = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");

      let send_id = inscription_id_from_seed(3);
      updater.index_token_send_created(
        send_id,
        0,
        satpoint_from_inscription(send_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-send",
          "items": [
            {
              "tick": "foo",
              "amt": "5",
              "address": RECIPIENT_ADDRESS
            }
          ]
        })),
        USER_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "accumulator_addr": get_acc_addr(updater, &format!("a/{}", send_id)),
      })
    });

    let miner_send_creation = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      let send_id = inscription_id_from_seed(4);
      updater.index_token_send_created(
        send_id,
        0,
        satpoint_from_inscription(send_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-send",
          "items": [
            {
              "tick": "foo",
              "amt": "5",
              "address": RECIPIENT_ADDRESS
            }
          ]
        })),
        MINER_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "accumulator_addr": get_acc_addr(updater, &format!("a/{}", send_id)),
      })
    });

    let reward_authorized_outbound = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      updater.exec_internal_send_one(
        MINER_ADDRESS,
        RECIPIENT_ADDRESS,
        "foo",
        &serde_json::json!("5"),
        None,
        &inscription_id_from_seed(99).to_string(),
        0,
        transfer_satpoint(100, 1),
        1_000,
      );

      serde_json::json!({
        "sender_balance": get_string(
          updater,
          &format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))
        ),
        "recipient_balance": get_string(
          updater,
          &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))
        ),
      })
    });

    let non_miner_trade_creation = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let trade_id = inscription_id_from_seed(5);
      updater.index_token_trade_created(
        trade_id,
        0,
        satpoint_from_inscription(trade_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "0",
          "tick": "foo",
          "amt": "5",
          "accept": [
            {
              "tick": "bar",
              "amt": "2"
            }
          ],
          "valid": 100
        })),
        USER_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "accumulator_addr": get_acc_addr(updater, &format!("a/{}", trade_id)),
      })
    });

    let miner_trade_creation = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      let trade_id = inscription_id_from_seed(6);
      updater.index_token_trade_created(
        trade_id,
        0,
        satpoint_from_inscription(trade_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "0",
          "tick": "foo",
          "amt": "5",
          "accept": [
            {
              "tick": "bar",
              "amt": "2"
            }
          ],
          "valid": 100
        })),
        MINER_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "accumulator_addr": get_acc_addr(updater, &format!("a/{}", trade_id)),
      })
    });

    let non_miner_trade_fill = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_deploy(updater, "bar", RECIPIENT_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");
      put_balance(updater, RECIPIENT_ADDRESS, "bar", "100");

      let offer_id = inscription_id_from_seed(7);
      updater.index_token_trade_created(
        offer_id,
        0,
        satpoint_from_inscription(offer_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "0",
          "tick": "foo",
          "amt": "5",
          "accept": [
            {
              "tick": "bar",
              "amt": "2"
            }
          ],
          "valid": 100
        })),
        USER_ADDRESS,
        1_000,
      );
      updater.index_token_trade_executed(
        offer_id,
        0,
        transfer_satpoint(70, 0),
        USER_ADDRESS,
        1_000,
      );

      let accept_id = inscription_id_from_seed(8);
      updater.index_token_trade_created(
        accept_id,
        0,
        satpoint_from_inscription(accept_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "1",
          "trade": offer_id.to_string(),
          "tick": "bar",
          "amt": "2"
        })),
        RECIPIENT_ADDRESS,
        1_000,
      );
      updater.index_token_trade_executed(
        accept_id,
        0,
        transfer_satpoint(71, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "seller_foo": get_string(updater, &format!("b/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "buyer_foo": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "seller_bar": get_string(updater, &format!("b/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("bar"))),
        "buyer_bar": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("bar"))),
      })
    });

    let legacy_reward_trade_fill_blocked = with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_deploy(updater, "bar", RECIPIENT_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      put_balance(updater, RECIPIENT_ADDRESS, "bar", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      let trade_id = "legacy-trade".to_string();
      let accepted_tick_key = InscriptionUpdater::json_stringify_lower("bar");
      let offer_pointer = "legacy-offer-pointer".to_string();

      updater.tap_put(
        &format!("to/{}/{}", trade_id, accepted_tick_key),
        &offer_pointer,
      ).unwrap();
      updater.tap_put(
        &format!("tol/{}", trade_id),
        &TapAccumulatorEntry {
          op: "token-trade-lock".to_string(),
          json: serde_json::json!({}),
          ins: "legacy-offer".to_string(),
          blck: 1,
          tx: txid_from_seed(72).to_string(),
          vo: 0,
          val: None,
          num: 0,
          ts: 0,
          addr: MINER_ADDRESS.to_string(),
        },
      ).unwrap();
      updater.tap_put(
        &offer_pointer,
        &TradeOfferRecord {
          addr: MINER_ADDRESS.to_string(),
          blck: 1,
          tick: "foo".to_string(),
          amt: "5".to_string(),
          atick: "bar".to_string(),
          aamt: "2".to_string(),
          vld: 100,
          trf: "0".to_string(),
          bal: "100".to_string(),
          tx: txid_from_seed(72).to_string(),
          vo: 0,
          val: "1000".to_string(),
          ins: "legacy-offer".to_string(),
          num: 0,
          ts: 0,
          fail: false,
        },
      ).unwrap();

      let accept_id = inscription_id_from_seed(9);
      updater.index_token_trade_created(
        accept_id,
        0,
        satpoint_from_inscription(accept_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "1",
          "trade": trade_id,
          "tick": "bar",
          "amt": "2"
        })),
        RECIPIENT_ADDRESS,
        1_000,
      );
      updater.index_token_trade_executed(
        accept_id,
        0,
        transfer_satpoint(73, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      serde_json::json!({
        "seller_foo": get_string(updater, &format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "buyer_foo": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo"))),
        "seller_bar": get_string(updater, &format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("bar"))),
        "buyer_bar": get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("bar"))),
      })
    });

    serde_json::json!({
      "activation": activation,
      "mainnet_inactive_mark": mainnet_inactive_mark,
      "reward_mark": reward_mark,
      "non_miner_transfer": non_miner_transfer,
      "miner_transfer": miner_transfer,
      // START MINER-REWARD-SHIELD
      "non_miner_transfer_execution": non_miner_transfer_execution,
      "reward_transfer_execution_invalidated_after_foreign_move": reward_transfer_execution_invalidated_after_foreign_move,
      "reward_transfer_execution_blocked_same_address": reward_transfer_execution_blocked_same_address,
      "reward_transfer_execution_after_unblock": reward_transfer_execution_after_unblock,
      // END MINER-REWARD-SHIELD
      "non_miner_send_creation": non_miner_send_creation,
      "miner_send_creation": miner_send_creation,
      "reward_authorized_outbound": reward_authorized_outbound,
      "non_miner_trade_creation": non_miner_trade_creation,
      "miner_trade_creation": miner_trade_creation,
      "non_miner_trade_fill": non_miner_trade_fill,
      "legacy_reward_trade_fill_blocked": legacy_reward_trade_fill_blocked,
    })
  }

  #[test]
  fn node20_value_stringify_rejects_numeric_max_lim_amt() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      assert!(updater.tap_feature_enabled(TapFeature::ValueStringifyActivation));

      for raw_numeric in [
        r#"{"amt":1}"#,
        r#"{"lim":2.500}"#,
        r#"{"max":21000000.0100}"#,
        r#"{"items":[{"amt":74.0100}]}"#,
        r#"{"items":[{"lim":2.500}]}"#,
        r#"{"nested":{"max":21000000.0100}}"#,
        r#"{"\u0061mt":0.0100}"#,
        r#"{"amt":"2","amt":1}"#,
      ] {
        assert!(updater.parse_tap_json_value(raw_numeric).is_none(), "unexpectedly parsed {raw_numeric}");
      }

      let parsed = updater.parse_tap_json_value(
        r#"{"amt":"1","items":[{"amt":"74.0100"},{"lim":"2.500"}],"nested":{"max":"21000000.0100"},"lim":"1000.90","max":"21000000.0100","other":42,"valid":100,"amt":"75"}"#,
      ).unwrap();

      assert_eq!(parsed.get("amt").and_then(|v| v.as_str()), Some("75"));
      assert_eq!(parsed.get("lim").and_then(|v| v.as_str()), Some("1000.90"));
      assert_eq!(parsed.get("max").and_then(|v| v.as_str()), Some("21000000.0100"));
      assert_eq!(parsed.get("other").and_then(|v| v.as_i64()), Some(42));
      assert_eq!(parsed.get("valid").and_then(|v| v.as_i64()), Some(100));
      assert_eq!(
        parsed.get("items").and_then(|v| v.as_array()).and_then(|items| items[0].get("amt")).and_then(|v| v.as_str()),
        Some("74.0100")
      );
      assert_eq!(
        parsed.get("items").and_then(|v| v.as_array()).and_then(|items| items[1].get("lim")).and_then(|v| v.as_str()),
        Some("2.500")
      );
      assert_eq!(
        parsed.get("nested").and_then(|v| v.get("max")).and_then(|v| v.as_str()),
        Some("21000000.0100")
      );

      let escaped_key = updater.parse_tap_json_value(r#"{"\u0061mt":"0.0100"}"#).unwrap();
      assert_eq!(escaped_key.get("amt").and_then(|v| v.as_str()), Some("0.0100"));
      let duplicate_final_string = updater.parse_tap_json_value(r#"{"amt":1,"amt":"2"}"#).unwrap();
      assert_eq!(duplicate_final_string.get("amt").and_then(|v| v.as_str()), Some("2"));

      assert!(updater.parse_tap_json_value(r#"{"amt":01}"#).is_none());
      assert!(updater.parse_tap_json_value(r#"{"amt":1.}"#).is_none());
      assert!(updater.parse_tap_json_value(r#"{"amt":1e}"#).is_none());
    });
  }

  #[test]
  fn number_resolution_matches_writer_edge_cases() {
    assert_eq!(InscriptionUpdater::resolve_number_string("74.0100", 2).as_deref(), Some("7401"));
    assert_eq!(InscriptionUpdater::resolve_number_string("1.239", 2).as_deref(), Some("123"));
    assert_eq!(InscriptionUpdater::resolve_number_string("0001.20", 2).as_deref(), Some("120"));
    assert_eq!(InscriptionUpdater::resolve_number_string("0.0001", 2).as_deref(), Some("0"));
    assert_eq!(InscriptionUpdater::resolve_number_string("", 2).as_deref(), Some("0"));
    assert!(InscriptionUpdater::resolve_number_string("1,200", 2).is_none());
    assert!(InscriptionUpdater::resolve_number_string("1a", 2).is_none());
    assert!(InscriptionUpdater::resolve_number_string("1.2.3", 2).is_none());
    assert!(InscriptionUpdater::resolve_number_string("-1", 2).is_none());
    assert!(InscriptionUpdater::resolve_number_string("1e3", 2).is_none());
  }

  #[test]
  fn js_coercion_helpers_match_node20_protocol_edges() {
    use serde_json::json;

    assert_eq!(InscriptionUpdater::js_value_to_string(&json!([1, 2])), "1,2");
    assert_eq!(InscriptionUpdater::js_value_to_string(&json!([null, 1])), ",1");
    assert_eq!(InscriptionUpdater::js_value_to_string(&json!([[1, 2], 3])), "1,2,3");
    assert_eq!(InscriptionUpdater::js_parse_int(&json!("+0x10")), Some(16));
    assert_eq!(InscriptionUpdater::js_parse_int(&json!([1, 2])), Some(1));
    assert_eq!(InscriptionUpdater::js_parse_int(&json!({ "a": 1 })), None);
    assert_eq!(
      InscriptionUpdater::js_bigint_value_to_biguint(&json!([])).map(|n| n.to_string()).as_deref(),
      Some("0")
    );
    assert_eq!(
      InscriptionUpdater::js_bigint_value_to_biguint(&json!([1])).map(|n| n.to_string()).as_deref(),
      Some("1")
    );
    assert!(InscriptionUpdater::js_bigint_value_to_biguint(&json!(null)).is_none());
    assert!(InscriptionUpdater::js_bigint_value_to_biguint(&json!("+0x10")).is_none());
    assert!(InscriptionUpdater::writer_loose_inscription_id_syntax(
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaai1abc"
    ));
    assert!(!InscriptionUpdater::writer_loose_inscription_id_syntax(
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaiabc"
    ));
  }

  #[test]
  fn deploy_dec_and_array_stringification_follow_tap_writer() {
    with_test_updater(BtcNetwork::Bitcoin, 850_000, |updater| {
      let dec_fraction_id = inscription_id_from_seed(90);
      updater.index_deployments(
        dec_fraction_id,
        0,
        satpoint_from_inscription(dec_fraction_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"decim","max":"100","lim":"10","dec":"0.00000000000000001"}"#),
        USER_ADDRESS,
        1_000,
      );
      let decim = updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("decim"))).unwrap().unwrap();
      assert_eq!(decim.dec, 0);
      assert_eq!(decim.max, "100");
      assert_eq!(decim.lim, "10");

      let bad_dec_id = inscription_id_from_seed(91);
      updater.index_deployments(
        bad_dec_id,
        0,
        satpoint_from_inscription(bad_dec_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"baddec","max":"100","lim":"10","dec":"1abc"}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("baddec"))).unwrap().is_none());

      let default_dec_id = inscription_id_from_seed(92);
      updater.index_deployments(
        default_dec_id,
        0,
        satpoint_from_inscription(default_dec_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"defdc","max":"100","lim":"10","dec":"18abc"}"#),
        USER_ADDRESS,
        1_000,
      );
      let defdc = updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("defdc"))).unwrap().unwrap();
      assert_eq!(defdc.dec, 18);

      let array_id = inscription_id_from_seed(93);
      updater.index_deployments(
        array_id,
        0,
        satpoint_from_inscription(array_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"array","max":[100],"lim":[10],"dec":0}"#),
        USER_ADDRESS,
        1_000,
      );
      let array = updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("array"))).unwrap().unwrap();
      assert_eq!(array.max, "100");
      assert_eq!(array.lim, "10");
    });
  }

  #[test]
  fn transfer_amount_zero_cap_and_array_coercion_match_tap_writer() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy_with_supply(updater, "xfer", USER_ADDRESS, 0, "100", "100");
      put_balance(updater, USER_ADDRESS, "xfer", "100");
      let tick_key = InscriptionUpdater::json_stringify_lower("xfer");

      for (seed, amt) in [(94, r#""""#), (95, r#""0""#), (96, r#""0.0""#)] {
        let transfer_id = inscription_id_from_seed(seed);
        updater.index_token_transfer_created(
          transfer_id,
          0,
          satpoint_from_inscription(transfer_id, 0),
          &inscription_from_body(&format!(r#"{{"p":"tap","op":"token-transfer","tick":"xfer","amt":{}}}"#, amt)),
          USER_ADDRESS,
          1_000,
        );
        assert_eq!(get_string(updater, &format!("tamt/{}", transfer_id)).as_deref(), Some("0"), "unexpected zero transfer for amt {amt}");
      }
      assert_eq!(get_string(updater, &format!("t/{}/{}", USER_ADDRESS, tick_key)).as_deref(), Some("0"));

      let too_big_id = inscription_id_from_seed(97);
      updater.index_token_transfer_created(
        too_big_id,
        0,
        satpoint_from_inscription(too_big_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"xfer","amt":"18446744073709551616"}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(get_string(updater, &format!("tamt/{}", too_big_id)).is_none());
      assert_eq!(get_string(updater, "sftrl").as_deref(), Some("4"));

      let array_id = inscription_id_from_seed(98);
      updater.index_token_transfer_created(
        array_id,
        0,
        satpoint_from_inscription(array_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"xfer","amt":[1]}"#),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, &format!("tamt/{}", array_id)).as_deref(), Some("1"));
      assert_eq!(get_string(updater, &format!("t/{}/{}", USER_ADDRESS, tick_key)).as_deref(), Some("1"));

      let bad_array_id = inscription_id_from_seed(99);
      updater.index_token_transfer_created(
        bad_array_id,
        0,
        satpoint_from_inscription(bad_array_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"xfer","amt":[1,2]}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(get_string(updater, &format!("tamt/{}", bad_array_id)).is_none());
    });
  }

  #[test]
  fn trade_side_valid_amount_caps_and_duplicate_accepts_match_tap_writer() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy_with_supply(updater, "foo", USER_ADDRESS, 0, "100", "100");
      put_deploy_with_supply(updater, "bar", RECIPIENT_ADDRESS, 0, "100", "100");
      put_balance(updater, USER_ADDRESS, "foo", "100");
      put_balance(updater, RECIPIENT_ADDRESS, "bar", "100");

      let invalid_side_id = inscription_id_from_seed(100);
      updater.index_token_trade_created(
        invalid_side_id,
        0,
        satpoint_from_inscription(invalid_side_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"2","tick":"foo","amt":"1","accept":[{"tick":"bar","amt":"2"}],"valid":100}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", invalid_side_id)).unwrap().is_none());

      let parsed_side_id = inscription_id_from_seed(101);
      updater.index_token_trade_created(
        parsed_side_id,
        0,
        satpoint_from_inscription(parsed_side_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"1abc","trade":"missing","tick":"bar","amt":"2"}"#),
        RECIPIENT_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", parsed_side_id)).unwrap().is_some());

      let offer_id = inscription_id_from_seed(102);
      updater.index_token_trade_created(
        offer_id,
        0,
        satpoint_from_inscription(offer_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"foo","amt":"1","accept":[{"tick":"bar","amt":"2"},{"tick":"BAR","amt":"3"}],"valid":"100abc"}"#),
        USER_ADDRESS,
        1_000,
      );
      updater.index_token_trade_executed(
        offer_id,
        0,
        transfer_satpoint(103, 0),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, "sfatrof").as_deref(), Some("1"));
      let offer = updater.tap_get::<TradeOfferRecord>("sfatrofi/0").unwrap().unwrap();
      assert_eq!(offer.vld, 100);
      assert_eq!(offer.aamt, "2");

      let too_big_offer_id = inscription_id_from_seed(104);
      updater.index_token_trade_created(
        too_big_offer_id,
        0,
        satpoint_from_inscription(too_big_offer_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"foo","amt":"18446744073709551616","accept":[{"tick":"bar","amt":"2"}],"valid":100}"#),
        USER_ADDRESS,
        1_000,
      );
      updater.index_token_trade_executed(
        too_big_offer_id,
        0,
        transfer_satpoint(105, 0),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, "sfatrof").as_deref(), Some("1"));

      let malformed_fill_id = inscription_id_from_seed(108);
      updater.tap_put(
        &format!("a/{}", malformed_fill_id),
        &TapAccumulatorEntry {
          op: "token-trade".to_string(),
          json: serde_json::json!({"side":1,"trade":[offer_id.to_string()],"tick":"bar","amt":"2"}),
          ins: malformed_fill_id.to_string(),
          blck: updater.height,
          tx: malformed_fill_id.txid.to_string(),
          vo: 0,
          val: None,
          num: 0,
          ts: updater.timestamp,
          addr: RECIPIENT_ADDRESS.to_string(),
        },
      ).unwrap();
      updater.index_token_trade_executed(
        malformed_fill_id,
        0,
        transfer_satpoint(109, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", malformed_fill_id)).unwrap().is_none());
    });
  }

  #[test]
  fn dmt_element_field_parseint_activation_matches_tap_writer() {
    with_test_updater(BtcNetwork::Bitcoin, 850_000, |updater| {
      let element_id = inscription_id_from_seed(106);
      updater.index_dmt_element_created(
        element_id,
        0,
        satpoint_from_inscription(element_id, 0),
        &inscription_from_body("loose.4abc.element"),
        USER_ADDRESS,
        1_000,
      );
      let rec = updater.tap_get::<DmtElementRecord>(&format!("dmt-el/{}", InscriptionUpdater::json_stringify_lower("loose"))).unwrap().unwrap();
      assert_eq!(rec.fld, 4);
    });

    with_test_updater(BtcNetwork::Bitcoin, 900_000, |updater| {
      let element_id = inscription_id_from_seed(107);
      updater.index_dmt_element_created(
        element_id,
        0,
        satpoint_from_inscription(element_id, 0),
        &inscription_from_body("strict.4abc.element"),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<DmtElementRecord>(&format!("dmt-el/{}", InscriptionUpdater::json_stringify_lower("strict"))).unwrap().is_none());
    });
  }

  #[test]
  fn deploy_mint_and_transfer_accept_quoted_decimal_sources() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let deploy_id = inscription_id_from_seed(40);
      updater.index_deployments(
        deploy_id,
        0,
        satpoint_from_inscription(deploy_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"foo","max":"21000000.0100","lim":"1000.90","dec":2}"#),
        USER_ADDRESS,
        1_000,
      );

      let foo_key = InscriptionUpdater::json_stringify_lower("foo");
      let deployed = updater.tap_get::<DeployRecord>(&format!("d/{}", foo_key)).unwrap().unwrap();
      assert_eq!(deployed.dec, 2);
      assert_eq!(deployed.max, "2100000001");
      assert_eq!(deployed.lim, "100090");
      assert_eq!(get_string(updater, &format!("dc/{}", foo_key)).as_deref(), Some("2100000001"));

      put_deploy_with_supply(updater, "mnt", USER_ADDRESS, 2, "1000.00", "1000.00");
      let mnt_key = InscriptionUpdater::json_stringify_lower("mnt");
      updater.tap_put(&format!("dc/{}", mnt_key), &"100000".to_string()).unwrap();
      let mint_id = inscription_id_from_seed(41);
      updater.index_mints(
        mint_id,
        0,
        satpoint_from_inscription(mint_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-mint","tick":"mnt","amt":"74.0100"}"#),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, mnt_key)).as_deref(), Some("7401"));
      assert_eq!(get_string(updater, &format!("dc/{}", mnt_key)).as_deref(), Some("92599"));

      put_deploy_with_supply(updater, "trf", USER_ADDRESS, 2, "100.00", "100.00");
      put_balance(updater, USER_ADDRESS, "trf", "10000");
      let transfer_id = inscription_id_from_seed(42);
      updater.index_token_transfer_created(
        transfer_id,
        0,
        satpoint_from_inscription(transfer_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"trf","amt":"74.0100"}"#),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, &format!("tamt/{}", transfer_id)).as_deref(), Some("7401"));
      assert_eq!(
        get_string(updater, &format!("t/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("trf"))).as_deref(),
        Some("7401")
      );
    });
  }

  #[test]
	  fn send_and_trade_accept_quoted_decimal_sources() {
	    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy_with_supply(updater, "snd", USER_ADDRESS, 2, "100.00", "100.00");
      put_balance(updater, USER_ADDRESS, "snd", "10000");

      let send_id = inscription_id_from_seed(50);
      updater.index_token_send_created(
        send_id,
        0,
        satpoint_from_inscription(send_id, 0),
        &inscription_from_body(&format!(
          r#"{{"p":"tap","op":"token-send","items":[{{"tick":"snd","amt":"1.25","address":"{}"}}]}}"#,
          RECIPIENT_ADDRESS
        )),
        USER_ADDRESS,
        1_000,
      );
      let send_acc = updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", send_id)).unwrap().unwrap();
      assert_eq!(
        send_acc.json.get("items").and_then(|v| v.as_array()).and_then(|items| items[0].get("amt")).and_then(|v| v.as_str()),
        Some("1.25")
      );
      updater.index_token_send_executed(
        send_id,
        0,
        transfer_satpoint(51, 0),
        USER_ADDRESS,
        1_000,
      );
      let snd_key = InscriptionUpdater::json_stringify_lower("snd");
      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, snd_key)).as_deref(), Some("9875"));
      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, snd_key)).as_deref(), Some("125"));

      put_deploy_with_supply(updater, "foo", USER_ADDRESS, 2, "100.00", "100.00");
      put_deploy_with_supply(updater, "bar", RECIPIENT_ADDRESS, 2, "100.00", "100.00");
      put_balance(updater, USER_ADDRESS, "foo", "10000");
      put_balance(updater, RECIPIENT_ADDRESS, "bar", "10000");

      let offer_id = inscription_id_from_seed(52);
      updater.index_token_trade_created(
        offer_id,
        0,
        satpoint_from_inscription(offer_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"foo","amt":"1.25","accept":[{"tick":"bar","amt":"2.50"}],"valid":100}"#),
        USER_ADDRESS,
        1_000,
      );
      let offer_acc = updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", offer_id)).unwrap().unwrap();
      assert_eq!(offer_acc.json.get("amt").and_then(|v| v.as_str()), Some("1.25"));
      assert_eq!(
        offer_acc.json.get("accept").and_then(|v| v.as_array()).and_then(|items| items[0].get("amt")).and_then(|v| v.as_str()),
        Some("2.50")
      );
      updater.index_token_trade_executed(
        offer_id,
        0,
        transfer_satpoint(53, 0),
        USER_ADDRESS,
        1_000,
      );
      let offer_lock = updater.tap_get::<TapAccumulatorEntry>(&format!("tol/{}", offer_id)).unwrap().unwrap();
      assert_eq!(offer_lock.op, "token-trade");
      assert_eq!(offer_lock.blck, offer_acc.blck);
      assert_eq!(offer_lock.ins, offer_acc.ins);
      assert_eq!(offer_lock.addr, offer_acc.addr);

      let accept_id = inscription_id_from_seed(54);
      updater.index_token_trade_created(
        accept_id,
        0,
        satpoint_from_inscription(accept_id, 0),
        &inscription_from_body(&format!(
          r#"{{"p":"tap","op":"token-trade","side":"1","trade":"{}","tick":"bar","amt":"2.50"}}"#,
          offer_id
        )),
        RECIPIENT_ADDRESS,
        1_000,
      );
      updater.index_token_trade_executed(
        accept_id,
        0,
        transfer_satpoint(55, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      let foo_key = InscriptionUpdater::json_stringify_lower("foo");
      let bar_key = InscriptionUpdater::json_stringify_lower("bar");
      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, foo_key)).as_deref(), Some("9875"));
      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, foo_key)).as_deref(), Some("125"));
      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, bar_key)).as_deref(), Some("250"));
      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, bar_key)).as_deref(), Some("9750"));
	    });
	  }

	  #[test]
	  fn token_trade_records_use_writer_timestamp_and_filled_side_metadata() {
	    with_test_updater(BtcNetwork::Signet, 1, |updater| {
	      put_deploy_with_supply(updater, "FoO", USER_ADDRESS, 2, "100.00", "100.00");
	      put_deploy_with_supply(updater, "BaR", RECIPIENT_ADDRESS, 2, "100.00", "100.00");
	      put_balance(updater, USER_ADDRESS, "FoO", "10000");
	      put_balance(updater, RECIPIENT_ADDRESS, "BaR", "10000");

	      updater.timestamp = 11;
	      let offer_id = inscription_id_from_seed(60);
	      updater.index_token_trade_created(
	        offer_id,
	        0,
	        satpoint_from_inscription(offer_id, 0),
	        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"FoO","amt":"1.25","accept":[{"tick":"BaR","amt":"2.50"}],"valid":100}"#),
	        USER_ADDRESS,
	        1_000,
	      );
	      updater.timestamp = 22;
	      updater.index_token_trade_executed(
	        offer_id,
	        0,
	        transfer_satpoint(61, 0),
	        USER_ADDRESS,
	        1_000,
	      );

	      let offer_record = updater
	        .tap_get::<TradeOfferRecord>("sfatrofi/0")
	        .unwrap()
	        .unwrap();
	      assert_eq!(offer_record.ts, 22);
	      assert_eq!(offer_record.tick, "foo");
	      assert_eq!(offer_record.amt, "125");
	      assert_eq!(offer_record.atick, "bar");
	      assert_eq!(offer_record.aamt, "250");

	      updater.timestamp = 33;
	      let accept_id = inscription_id_from_seed(62);
	      updater.index_token_trade_created(
	        accept_id,
	        0,
	        satpoint_from_inscription(accept_id, 0),
	        &inscription_from_body(&format!(
	          r#"{{"p":"tap","op":"token-trade","side":"1","trade":"{}","tick":"bar","amt":"2.50"}}"#,
	          offer_id
	        )),
	        RECIPIENT_ADDRESS,
	        1_000,
	      );
	      updater.timestamp = 44;
	      updater.index_token_trade_executed(
	        accept_id,
	        0,
	        transfer_satpoint(63, 0),
	        RECIPIENT_ADDRESS,
	        1_000,
	      );

	      let filled = updater
	        .tap_get::<TradeBuySellerRecord>("sfbtrofi/0")
	        .unwrap()
	        .unwrap();
	      assert!(!filled.fail);
	      assert_eq!(filled.addr, RECIPIENT_ADDRESS);
	      assert_eq!(filled.saddr, USER_ADDRESS);
	      assert_eq!(filled.tick, "bar");
	      assert_eq!(filled.amt, "250");
	      assert_eq!(filled.stick, "foo");
	      assert_eq!(filled.samt, "125");
	      assert_eq!(filled.ts, 44);

	      let accepted_key = InscriptionUpdater::json_stringify_lower("BaR");
	      let seller_receive = updater
	        .tap_get::<TradeBuyBuyerRecord>(&format!("rbtrofi/{}/{}/0", USER_ADDRESS, accepted_key))
	        .unwrap()
	        .unwrap();
	      assert_eq!(seller_receive.btick, "bar");
	      assert_eq!(seller_receive.bamt, "250");
	      assert_eq!(seller_receive.tick, "foo");
	      assert_eq!(seller_receive.amt, "125");

	      let offer_key = InscriptionUpdater::json_stringify_lower("FoO");
	      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, offer_key)).as_deref(), Some("9875"));
	      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, offer_key)).as_deref(), Some("125"));
	      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, accepted_key)).as_deref(), Some("250"));
	      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, accepted_key)).as_deref(), Some("9750"));
	    });
	  }

	  #[test]
	  fn token_trade_metadata_lowercases_offer_and_fill_tickers_like_writer() {
	    with_test_updater(BtcNetwork::Signet, 1, |updater| {
	      put_deploy_with_supply(updater, "Punk #7523", USER_ADDRESS, 0, "100", "100");
	      put_deploy_with_supply(updater, "GUCCI", RECIPIENT_ADDRESS, 0, "100", "100");
	      put_balance(updater, USER_ADDRESS, "Punk #7523", "100");
	      put_balance(updater, RECIPIENT_ADDRESS, "GUCCI", "100");

	      let offer_id = inscription_id_from_seed(68);
	      updater.index_token_trade_created(
	        offer_id,
	        0,
	        satpoint_from_inscription(offer_id, 0),
	        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"Punk #7523","amt":"7","accept":[{"tick":"GUCCI","amt":"9"}],"valid":100}"#),
	        USER_ADDRESS,
	        1_000,
	      );
	      updater.index_token_trade_executed(
	        offer_id,
	        0,
	        transfer_satpoint(69, 0),
	        USER_ADDRESS,
	        1_000,
	      );

	      let offer_record = updater
	        .tap_get::<TradeOfferRecord>("sfatrofi/0")
	        .unwrap()
	        .unwrap();
	      assert_eq!(offer_record.tick, "punk #7523");
	      assert_eq!(offer_record.atick, "gucci");

	      let accept_id = inscription_id_from_seed(70);
	      updater.index_token_trade_created(
	        accept_id,
	        0,
	        satpoint_from_inscription(accept_id, 0),
	        &inscription_from_body(&format!(
	          r#"{{"p":"tap","op":"token-trade","side":"1","trade":"{}","tick":"GUCCI","amt":"9"}}"#,
	          offer_id
	        )),
	        RECIPIENT_ADDRESS,
	        1_000,
	      );
	      updater.index_token_trade_executed(
	        accept_id,
	        0,
	        transfer_satpoint(71, 0),
	        RECIPIENT_ADDRESS,
	        1_000,
	      );

	      let filled = updater
	        .tap_get::<TradeBuySellerRecord>("sfbtrofi/0")
	        .unwrap()
	        .unwrap();
	      assert_eq!(filled.tick, "gucci");
	      assert_eq!(filled.stick, "punk #7523");

	      let gucci_key = InscriptionUpdater::json_stringify_lower("GUCCI");
	      let seller_receive = updater
	        .tap_get::<TradeBuyBuyerRecord>(&format!("rbtrofi/{}/{}/0", USER_ADDRESS, gucci_key))
	        .unwrap()
	        .unwrap();
	      assert_eq!(seller_receive.btick, "gucci");
	      assert_eq!(seller_receive.tick, "punk #7523");
	    });
	  }

	  #[test]
	  fn token_trade_failed_fill_keeps_writer_metadata_without_balance_changes() {
	    with_test_updater(BtcNetwork::Signet, 1, |updater| {
	      put_deploy_with_supply(updater, "FoO", USER_ADDRESS, 2, "100.00", "100.00");
	      put_deploy_with_supply(updater, "BaR", RECIPIENT_ADDRESS, 2, "100.00", "100.00");
	      put_balance(updater, USER_ADDRESS, "FoO", "10000");
	      put_balance(updater, USER_ADDRESS, "BaR", "0");
	      put_balance(updater, RECIPIENT_ADDRESS, "FoO", "0");
	      put_balance(updater, RECIPIENT_ADDRESS, "BaR", "100");

	      let offer_id = inscription_id_from_seed(64);
	      updater.timestamp = 55;
	      updater.index_token_trade_created(
	        offer_id,
	        0,
	        satpoint_from_inscription(offer_id, 0),
	        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"FoO","amt":"1.25","accept":[{"tick":"BaR","amt":"2.50"}],"valid":100}"#),
	        USER_ADDRESS,
	        1_000,
	      );
	      updater.timestamp = 66;
	      updater.index_token_trade_executed(
	        offer_id,
	        0,
	        transfer_satpoint(65, 0),
	        USER_ADDRESS,
	        1_000,
	      );

	      let accept_id = inscription_id_from_seed(66);
	      updater.timestamp = 77;
	      updater.index_token_trade_created(
	        accept_id,
	        0,
	        satpoint_from_inscription(accept_id, 0),
	        &inscription_from_body(&format!(
	          r#"{{"p":"tap","op":"token-trade","side":"1","trade":"{}","tick":"bar","amt":"2.50"}}"#,
	          offer_id
	        )),
	        RECIPIENT_ADDRESS,
	        1_000,
	      );
	      updater.timestamp = 88;
	      updater.index_token_trade_executed(
	        accept_id,
	        0,
	        transfer_satpoint(67, 0),
	        RECIPIENT_ADDRESS,
	        1_000,
	      );

	      let filled = updater
	        .tap_get::<TradeBuySellerRecord>("sfbtrofi/0")
	        .unwrap()
	        .unwrap();
	      assert!(filled.fail);
	      assert_eq!(filled.tick, "bar");
	      assert_eq!(filled.amt, "250");
	      assert_eq!(filled.stick, "foo");
	      assert_eq!(filled.samt, "125");
	      assert_eq!(filled.ts, 88);

	      let offer_key = InscriptionUpdater::json_stringify_lower("FoO");
	      let accepted_key = InscriptionUpdater::json_stringify_lower("BaR");
	      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, offer_key)).as_deref(), Some("10000"));
	      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, offer_key)).as_deref(), Some("0"));
	      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, accepted_key)).as_deref(), Some("0"));
	      assert_eq!(get_string(updater, &format!("b/{}/{}", RECIPIENT_ADDRESS, accepted_key)).as_deref(), Some("100"));
	    });
	  }

	  #[test]
	  fn invalid_writer_stringified_number_forms_do_not_create_balance_effects() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy_with_supply(updater, "bad", USER_ADDRESS, 2, "100.00", "100.00");
      put_balance(updater, USER_ADDRESS, "bad", "10000");

      for (offset, amt) in [
        r#"1e3"#,
        r#""1,200""#,
        r#""1a""#,
        r#""1.2.3""#,
        r#"-1"#,
      ].iter().enumerate() {
        let transfer_id = inscription_id_from_seed(60 + offset as u8);
        updater.index_token_transfer_created(
          transfer_id,
          0,
          satpoint_from_inscription(transfer_id, 0),
          &inscription_from_body(&format!(r#"{{"p":"tap","op":"token-transfer","tick":"bad","amt":{}}}"#, amt)),
          USER_ADDRESS,
          1_000,
        );
        assert!(get_string(updater, &format!("tamt/{}", transfer_id)).is_none(), "unexpected transfer for amt {amt}");
      }

      let zero_id = inscription_id_from_seed(66);
      updater.index_token_transfer_created(
        zero_id,
        0,
        satpoint_from_inscription(zero_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"bad","amt":""}"#),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, &format!("tamt/{}", zero_id)).as_deref(), Some("0"));

      let trunc_id = inscription_id_from_seed(67);
      updater.index_token_transfer_created(
        trunc_id,
        0,
        satpoint_from_inscription(trunc_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"bad","amt":1.239}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(get_string(updater, &format!("tamt/{}", trunc_id)).is_none());

      let quoted_trunc_id = inscription_id_from_seed(71);
      updater.index_token_transfer_created(
        quoted_trunc_id,
        0,
        satpoint_from_inscription(quoted_trunc_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"bad","amt":"1.239"}"#),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, &format!("tamt/{}", quoted_trunc_id)).as_deref(), Some("123"));

      let deploy_id = inscription_id_from_seed(68);
      updater.index_deployments(
        deploy_id,
        0,
        satpoint_from_inscription(deploy_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"exp","max":1e3,"lim":1,"dec":2}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("exp"))).unwrap().is_none());

      let bad_max_id = inscription_id_from_seed(69);
      updater.index_deployments(
        bad_max_id,
        0,
        satpoint_from_inscription(bad_max_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"badmax","max":"1,200","lim":1.25,"dec":2}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("badmax"))).unwrap().is_none());

      let bad_lim_id = inscription_id_from_seed(70);
      updater.index_deployments(
        bad_lim_id,
        0,
        satpoint_from_inscription(bad_lim_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"badlim","max":100.00,"lim":"1a","dec":2}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("badlim"))).unwrap().is_none());
    });
  }

  #[test]
  fn raw_numeric_max_lim_amt_skips_all_tap_surfaces_after_activation() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let deploy_id = inscription_id_from_seed(80);
      updater.index_deployments(
        deploy_id,
        0,
        satpoint_from_inscription(deploy_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-deploy","tick":"rawdep","max":100,"lim":"10","dec":2}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<DeployRecord>(&format!("d/{}", InscriptionUpdater::json_stringify_lower("rawdep"))).unwrap().is_none());

      put_deploy_with_supply(updater, "raw", USER_ADDRESS, 2, "100.00", "100.00");
      put_balance(updater, USER_ADDRESS, "raw", "10000");
      updater.tap_put(&format!("dc/{}", InscriptionUpdater::json_stringify_lower("raw")), &"10000".to_string()).unwrap();

      let mint_id = inscription_id_from_seed(81);
      updater.index_mints(
        mint_id,
        0,
        satpoint_from_inscription(mint_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-mint","tick":"raw","amt":1.25}"#),
        USER_ADDRESS,
        1_000,
      );
      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("raw"))).as_deref(), Some("10000"));
      assert!(get_string(updater, &format!("tx/mnt/{}", mint_id)).is_none());

      let transfer_id = inscription_id_from_seed(82);
      updater.index_token_transfer_created(
        transfer_id,
        0,
        satpoint_from_inscription(transfer_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"raw","amt":1.25}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(get_string(updater, &format!("tamt/{}", transfer_id)).is_none());
      assert!(get_string(updater, &format!("t/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("raw"))).is_none());

      let send_id = inscription_id_from_seed(83);
      updater.index_token_send_created(
        send_id,
        0,
        satpoint_from_inscription(send_id, 0),
        &inscription_from_body(&format!(
          r#"{{"p":"tap","op":"token-send","items":[{{"tick":"raw","amt":1.25,"address":"{}"}}]}}"#,
          RECIPIENT_ADDRESS
        )),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", send_id)).unwrap().is_none());

      let trade_id = inscription_id_from_seed(84);
      updater.index_token_trade_created(
        trade_id,
        0,
        satpoint_from_inscription(trade_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"raw","amt":1.25,"accept":[{"tick":"raw","amt":"1.25"}],"valid":100}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", trade_id)).unwrap().is_none());

      let nested_trade_id = inscription_id_from_seed(85);
      updater.index_token_trade_created(
        nested_trade_id,
        0,
        satpoint_from_inscription(nested_trade_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-trade","side":"0","tick":"raw","amt":"1.25","accept":[{"tick":"raw","amt":1.25}],"valid":100}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", nested_trade_id)).unwrap().is_none());

      let element_id = inscription_id_from_seed(86);
      updater.index_dmt_element_created(
        element_id,
        0,
        satpoint_from_inscription(element_id, 0),
        &inscription_from_body("rawel.4.element"),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.parse_tap_json_value(&format!(
        r#"{{"p":"tap","op":"dmt-deploy","tick":"rawdm","elem":"{}","amt":1}}"#,
        element_id
      )).is_none());

      let privilege_id = inscription_id_from_seed(88);
      updater.index_privilege_auth_created(
        privilege_id,
        0,
        satpoint_from_inscription(privilege_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"privilege-auth","sig":{"v":"0","r":"1","s":"1"},"hash":"0000000000000000000000000000000000000000000000000000000000000000","salt":"s","auth":{"name":"raw"},"amt":1}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", privilege_id)).unwrap().is_none());

      let block_id = inscription_id_from_seed(89);
      updater.index_block_transferables_created(
        block_id,
        0,
        satpoint_from_inscription(block_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"block-transferables","amt":1}"#),
        USER_ADDRESS,
        1_000,
      );
      assert!(updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", block_id)).unwrap().is_none());
    });
  }

  #[test]
  fn accumulator_metadata_and_salt_coercion_match_tap_writer() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let token_auth_id = inscription_id_from_seed(110);
      updater.index_token_auth_created(
        token_auth_id,
        0,
        satpoint_from_inscription(token_auth_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-auth","sig":{"v":"0","r":"1","s":"1"},"hash":"0000000000000000000000000000000000000000000000000000000000000000","salt":5,"auth":["foo"]}"#),
        USER_ADDRESS,
        1_000,
      );
      let token_auth_acc = updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", token_auth_id)).unwrap().unwrap();
      assert_eq!(token_auth_acc.val.as_deref(), Some("1000"));
      assert_eq!(token_auth_acc.json.get("salt").and_then(|v| v.as_i64()), Some(5));

      let token_cancel_id = inscription_id_from_seed(111);
      updater.index_token_auth_created(
        token_cancel_id,
        0,
        satpoint_from_inscription(token_cancel_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-auth","cancel":5}"#),
        USER_ADDRESS,
        1_000,
      );
      let token_cancel_acc = updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", token_cancel_id)).unwrap().unwrap();
      assert_eq!(token_cancel_acc.val, None);

      let privilege_id = inscription_id_from_seed(112);
      updater.index_privilege_auth_created(
        privilege_id,
        0,
        satpoint_from_inscription(privilege_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"privilege-auth","sig":{"v":"0","r":"1","s":"1"},"hash":"0000000000000000000000000000000000000000000000000000000000000000","salt":5,"auth":{"name":"raw"}}"#),
        USER_ADDRESS,
        1_000,
      );
      let privilege_acc = updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", privilege_id)).unwrap().unwrap();
      assert_eq!(privilege_acc.val.as_deref(), Some("1000"));
      assert_eq!(privilege_acc.json.get("salt").and_then(|v| v.as_i64()), Some(5));

      let block_id = inscription_id_from_seed(113);
      updater.index_block_transferables_created(
        block_id,
        0,
        satpoint_from_inscription(block_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"block-transferables"}"#),
        USER_ADDRESS,
        1_000,
      );
      let block_acc = updater.tap_get::<TapAccumulatorEntry>(&format!("a/{}", block_id)).unwrap().unwrap();
      assert_eq!(block_acc.val.as_deref(), Some("1000"));
    });
  }

  #[test]
  fn transfer_zero_and_failed_oversize_rows_match_tap_writer() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy_with_supply(updater, "zro", USER_ADDRESS, 0, "100", "100");
      put_balance(updater, USER_ADDRESS, "zro", "100");
      let tick_key = InscriptionUpdater::json_stringify_lower("zro");

      let zero_id = inscription_id_from_seed(114);
      updater.index_token_transfer_created(
        zero_id,
        0,
        satpoint_from_inscription(zero_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"zro","amt":""}"#),
        USER_ADDRESS,
        1_000,
      );
      let zero_row = updater
        .tap_get::<TransferInitRecord>(&format!("sftrli/{}", 0))
        .unwrap()
        .unwrap();
      assert_eq!(zero_row.amt, "0");
      assert_eq!(zero_row.trf, "0");
      assert_eq!(zero_row.bal, "100");
      assert!(!zero_row.fail);
      assert_eq!(get_string(updater, &format!("tamt/{}", zero_id)).as_deref(), Some("0"));
      assert_eq!(get_string(updater, &format!("t/{}/{}", USER_ADDRESS, tick_key)).as_deref(), Some("0"));

      let oversize_id = inscription_id_from_seed(115);
      updater.index_token_transfer_created(
        oversize_id,
        0,
        satpoint_from_inscription(oversize_id, 0),
        &inscription_from_body(r#"{"p":"tap","op":"token-transfer","tick":"zro","amt":"18446744073709551616"}"#),
        USER_ADDRESS,
        1_000,
      );
      let oversize_row = updater
        .tap_get::<TransferInitRecord>(&format!("sftrli/{}", 1))
        .unwrap()
        .unwrap();
      assert_eq!(oversize_row.amt, "18446744073709551616");
      assert_eq!(oversize_row.trf, "0");
      assert_eq!(oversize_row.bal, "100");
      assert!(oversize_row.fail);
      assert!(get_string(updater, &format!("tamt/{}", oversize_id)).is_none());
      assert!(get_string(updater, &format!("tl/{}", oversize_id)).is_none());
      assert_eq!(get_string(updater, "sftrl").as_deref(), Some("2"));
    });
  }

  #[test]
  fn dmt_mint_holder_elem_is_stringified_json_like_tap_writer() {
    let context = Context::builder().chain(Chain::Signet).build();
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let elem_id = inscription_id_from_seed(116);
      updater.index_dmt_element_created(
        elem_id,
        0,
        satpoint_from_inscription(elem_id, 0),
        &inscription_from_body("edge.4.element"),
        USER_ADDRESS,
        1_000,
      );

      let deploy_id = inscription_id_from_seed(117);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut deploy = deploy_record_with_supply("dmt-edge", USER_ADDRESS, 0, "100", "100");
      deploy.dmt = true;
      deploy.elem = Some(elem_id.to_string());
      deploy.ins = deploy_id.to_string();
      updater.tap_put(&format!("d/{}", InscriptionUpdater::json_stringify_lower("dmt-edge")), &deploy).unwrap();
      updater.tap_put(&format!("dc/{}", InscriptionUpdater::json_stringify_lower("dmt-edge")), &"100".to_string()).unwrap();

      let mint_id = inscription_id_from_seed(118);
      updater.index_dmt_mint(
        mint_id,
        0,
        satpoint_from_inscription(mint_id, 0),
        &inscription_from_body(&format!(
          r#"{{"p":"tap","op":"dmt-mint","tick":"edge","blk":"1","dep":"{}"}}"#,
          deploy_id
        )),
        USER_ADDRESS,
        1_000,
        &[],
        &context.index,
      );

      let elem_string = updater
        .tap_get::<ops::dmt_element::DmtElementRecord>(&format!("dmt-el/{}", InscriptionUpdater::json_stringify_lower("edge")))
        .unwrap()
        .map(|elem| InscriptionUpdater::js_json_stringify(&serde_json::to_value(elem).unwrap()))
        .unwrap();
      let holder_bytes = updater.tap_db.get(format!("dmtmh/{}", mint_id).as_bytes()).unwrap().unwrap();
      let holder_json: serde_json::Value = serde_json::from_slice(&holder_bytes).unwrap();
      assert_eq!(holder_json.get("elem").and_then(|v| v.as_str()), Some(elem_string.as_str()));

      let history_bytes = updater.tap_db.get(format!("dmtmhli/{}/0", mint_id).as_bytes()).unwrap().unwrap();
      let history_json: serde_json::Value = serde_json::from_slice(&history_bytes).unwrap();
      assert_eq!(history_json.get("elem").and_then(|v| v.as_str()), Some(elem_string.as_str()));

      let meta = updater
        .tap_get::<ops::dmt_mint::DmtMintMetaRecord>(&format!("dmtmhm/{}", mint_id))
        .unwrap()
        .unwrap();
      assert_eq!(meta.elem.as_str(), Some(elem_string.as_str()));

      updater.index_dmt_mint_transferred(
        mint_id,
        0,
        transfer_satpoint(119, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );
      let transferred_bytes = updater.tap_db.get(format!("dmtmh/{}", mint_id).as_bytes()).unwrap().unwrap();
      let transferred_json: serde_json::Value = serde_json::from_slice(&transferred_bytes).unwrap();
      assert_eq!(transferred_json.get("elem").and_then(|v| v.as_str()), Some(elem_string.as_str()));
      assert_eq!(transferred_json.get("prv").and_then(|v| v.as_str()), Some(USER_ADDRESS));
    });
  }

  #[test]
  fn nat_reward_zero_amount_still_logs_failed_mint_row_like_tap_writer() {
    let context = Context::builder().chain(Chain::Signet).build();
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let deploy_id = inscription_id_from_seed(120);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut deploy = deploy_record_with_supply("dmt-nat", USER_ADDRESS, 0, "100", "100");
      deploy.dmt = true;
      deploy.ins = deploy_id.to_string();
      updater.tap_put(&format!("d/{}", InscriptionUpdater::json_stringify_lower("dmt-nat")), &deploy).unwrap();
      updater.tap_put(&format!("dc/{}", InscriptionUpdater::json_stringify_lower("dmt-nat")), &"100".to_string()).unwrap();

      let address = USER_ADDRESS
        .parse::<Address<NetworkUnchecked>>()
        .unwrap()
        .assume_checked();
      let coinbase = Transaction {
        version: Version(2),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
          previous_output: OutPoint::null(),
          script_sig: ScriptBuf::new(),
          sequence: Sequence::MAX,
          witness: Witness::new(),
        }],
        output: vec![TxOut {
          value: Amount::from_sat(50_000),
          script_pubkey: address.script_pubkey(),
        }],
      };

      updater.index_dmt_nat_rewards_for_block(&coinbase, 0, &context.index).unwrap();

      let row = updater
        .tap_get::<MintSuperflatRecord>("sfmli/0")
        .unwrap()
        .unwrap();
      assert_eq!(row.tick, "dmt-nat");
      assert_eq!(row.addr, USER_ADDRESS);
      assert_eq!(row.amt, "0");
      assert_eq!(row.bal, "0");
      assert!(row.fail);
      assert_eq!(row.dmtblck, Some(1));
      assert_eq!(get_string(updater, "sfml").as_deref(), Some("1"));
      assert!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("dmt-nat"))).is_none());
    });
  }

  #[test]
  fn miner_reward_shield_activation_height_matches_network_rules() {
    with_test_updater(BtcNetwork::Bitcoin, 0, |updater| {
      assert_eq!(
        updater.feature_height(TapFeature::MinerRewardShieldActivation),
        TAP_MINER_REWARD_SHIELD_ACTIVATION_HEIGHT
      );
      // START MINER-REWARD-SHIELD
      assert_eq!(
        updater.feature_height(TapFeature::MinerRewardTransferExecutionShieldActivation),
        TAP_MINER_REWARD_TRANSFER_EXECUTION_SHIELD_ACTIVATION_HEIGHT
      );
      // END MINER-REWARD-SHIELD
    });

    with_test_updater(BtcNetwork::Signet, 0, |updater| {
      assert_eq!(updater.feature_height(TapFeature::MinerRewardShieldActivation), 0);
      assert!(updater.tap_feature_enabled(TapFeature::MinerRewardShieldActivation));
      // START MINER-REWARD-SHIELD
      assert_eq!(updater.feature_height(TapFeature::MinerRewardTransferExecutionShieldActivation), 0);
      assert!(updater.tap_feature_enabled(TapFeature::MinerRewardTransferExecutionShieldActivation));
      // END MINER-REWARD-SHIELD
    });
  }

  #[test]
  fn reward_marking_sets_dmtrwd_and_only_auto_blocks_once() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      assert!(updater.tap_is_dmt_reward_address(MINER_ADDRESS));
      assert!(
        updater
          .tap_get::<String>(&format!("bltr/{}", MINER_ADDRESS))
          .unwrap()
          .is_some()
      );

      updater.tap_del(&format!("bltr/{}", MINER_ADDRESS)).unwrap();
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      assert!(
        updater
          .tap_get::<String>(&format!("bltr/{}", MINER_ADDRESS))
          .unwrap()
          .is_none()
      );
    });
  }

  #[test]
  fn token_transfer_stays_normal_for_non_reward_addresses_and_is_blocked_for_rewards() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");

      let user_transfer_id = inscription_id_from_seed(1);
      updater.index_token_transfer_created(
        user_transfer_id,
        0,
        satpoint_from_inscription(user_transfer_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-transfer",
          "tick": "foo",
          "amt": "5"
        })),
        USER_ADDRESS,
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!("tamt/{}", user_transfer_id))
          .unwrap()
          .as_deref(),
        Some("5")
      );

      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");

      let miner_transfer_id = inscription_id_from_seed(2);
      updater.index_token_transfer_created(
        miner_transfer_id,
        0,
        satpoint_from_inscription(miner_transfer_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-transfer",
          "tick": "foo",
          "amt": "5"
        })),
        MINER_ADDRESS,
        1_000,
      );

      assert!(
        updater
          .tap_get::<String>(&format!("tamt/{}", miner_transfer_id))
          .unwrap()
          .is_none()
      );
    });
  }

  // START MINER-REWARD-SHIELD
  #[test]
  fn token_transfer_execution_stays_normal_for_non_reward_addresses_invalidates_off_address_reward_transferables_while_blocked_and_works_again_after_a_same_address_unblock_flow() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");
      seed_transferable(updater, USER_ADDRESS, "foo", "5", inscription_id_from_seed(30), 31);

      updater.index_token_transfer_executed(
        inscription_id_from_seed(30),
        0,
        transfer_satpoint(32, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("95")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("5")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("t/{}/{}", USER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tamt/{}", inscription_id_from_seed(30)))
          .unwrap()
          .as_deref(),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tl/{}", inscription_id_from_seed(30)))
          .unwrap()
          .as_deref(),
        Some("")
      );
    });

    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      seed_transferable(updater, MINER_ADDRESS, "foo", "5", inscription_id_from_seed(33), 34);

      updater.index_token_transfer_executed(
        inscription_id_from_seed(33),
        0,
        transfer_satpoint(35, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("100")
      );
      assert!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .is_none()
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tamt/{}", inscription_id_from_seed(33)))
          .unwrap()
          .as_deref(),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tl/{}", inscription_id_from_seed(33)))
          .unwrap()
          .as_deref(),
        Some("")
      );

      updater.tap_del(&format!("bltr/{}", MINER_ADDRESS)).unwrap();
      updater.index_token_transfer_executed(
        inscription_id_from_seed(33),
        0,
        transfer_satpoint(36, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("100")
      );
      assert!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .is_none()
      );
    });

    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");
      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      seed_transferable(updater, MINER_ADDRESS, "foo", "5", inscription_id_from_seed(36), 37);
      let expected_link = format!(
        "atrli/{}/{}/0",
        MINER_ADDRESS,
        InscriptionUpdater::json_stringify_lower("foo")
      );

      updater.index_token_transfer_executed(
        inscription_id_from_seed(36),
        0,
        transfer_satpoint(38, 0),
        MINER_ADDRESS,
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("5")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tamt/{}", inscription_id_from_seed(36)))
          .unwrap()
          .as_deref(),
        Some("5")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tl/{}", inscription_id_from_seed(36)))
          .unwrap()
          .as_deref(),
        Some(expected_link.as_str())
      );

      updater.tap_del(&format!("bltr/{}", MINER_ADDRESS)).unwrap();
      updater.index_token_transfer_executed(
        inscription_id_from_seed(36),
        0,
        transfer_satpoint(39, 0),
        RECIPIENT_ADDRESS,
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("95")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("b/{}/{}", RECIPIENT_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("5")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("t/{}/{}", MINER_ADDRESS, InscriptionUpdater::json_stringify_lower("foo")))
          .unwrap()
          .as_deref(),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tamt/{}", inscription_id_from_seed(36)))
          .unwrap()
          .as_deref(),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!("tl/{}", inscription_id_from_seed(36)))
          .unwrap()
          .as_deref(),
        Some("")
      );
    });
  }
  // END MINER-REWARD-SHIELD

  #[test]
  fn token_transfer_self_execution_logs_unchanged_balance_like_tap_writer() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "1604799");
      let transfer_id = inscription_id_from_seed(90);
      seed_transferable(updater, USER_ADDRESS, "foo", "1000000", transfer_id, 91);

      updater.index_token_transfer_executed(
        transfer_id,
        0,
        transfer_satpoint(92, 0),
        USER_ADDRESS,
        82_820,
      );

      let tick_key = InscriptionUpdater::json_stringify_lower("foo");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("1604799")
      );
      assert_eq!(
        get_string(updater, &format!("t/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("0")
      );
      assert_eq!(get_string(updater, &format!("tamt/{}", transfer_id)).as_deref(), Some("0"));
      assert_eq!(get_string(updater, &format!("tl/{}", transfer_id)).as_deref(), Some(""));

      let sender_record = updater
        .tap_get::<TransferSendSenderRecord>(&format!("strli/{}/{}/0", USER_ADDRESS, tick_key))
        .unwrap()
        .unwrap();
      assert_eq!(sender_record.taddr, USER_ADDRESS);
      assert_eq!(sender_record.trf, "0");
      assert_eq!(sender_record.bal, "1604799");

      let receiver_record = updater
        .tap_get::<TransferSendReceiverRecord>(&format!("rstrli/{}/{}/0", USER_ADDRESS, tick_key))
        .unwrap()
        .unwrap();
      assert_eq!(receiver_record.addr, USER_ADDRESS);
      assert_eq!(receiver_record.bal, "1604799");

      let flat_record = updater
        .tap_get::<TransferSendFlatRecord>(&format!("fstrli/{}/0", tick_key))
        .unwrap()
        .unwrap();
      assert_eq!(flat_record.addr, USER_ADDRESS);
      assert_eq!(flat_record.taddr, USER_ADDRESS);
      assert_eq!(flat_record.trf, "0");
      assert_eq!(flat_record.bal, "1604799");
      assert_eq!(flat_record.tbal, "1604799");

      let superflat_record = updater
        .tap_get::<TransferSendSuperflatRecord>("sfstrli/0")
        .unwrap()
        .unwrap();
      assert_eq!(superflat_record.addr, USER_ADDRESS);
      assert_eq!(superflat_record.taddr, USER_ADDRESS);
      assert_eq!(superflat_record.trf, "0");
      assert_eq!(superflat_record.bal, "1604799");
      assert_eq!(superflat_record.tbal, "1604799");
    });
  }

  #[test]
  fn token_send_stays_normal_for_non_reward_addresses_and_reward_addresses_use_internal_send() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "foo", USER_ADDRESS);
      put_balance(updater, USER_ADDRESS, "foo", "100");

      let send_id = inscription_id_from_seed(3);
      updater.index_token_send_created(
        send_id,
        0,
        satpoint_from_inscription(send_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-send",
          "items": [
            {
              "tick": "foo",
              "amt": "5",
              "address": RECIPIENT_ADDRESS
            }
          ]
        })),
        USER_ADDRESS,
        1_000,
      );

      let send_acc = updater
        .tap_get::<TapAccumulatorEntry>(&format!("a/{}", send_id))
        .unwrap()
        .unwrap();
      assert_eq!(send_acc.addr, USER_ADDRESS);

      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);
      put_balance(updater, MINER_ADDRESS, "foo", "100");

      let miner_send_id = inscription_id_from_seed(4);
      updater.index_token_send_created(
        miner_send_id,
        0,
        satpoint_from_inscription(miner_send_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-send",
          "items": [
            {
              "tick": "foo",
              "amt": "5",
              "address": RECIPIENT_ADDRESS
            }
          ]
        })),
        MINER_ADDRESS,
        1_000,
      );

      assert!(
        updater
          .tap_get::<TapAccumulatorEntry>(&format!("a/{}", miner_send_id))
          .unwrap()
          .is_none()
      );

      updater.exec_internal_send_one(
        MINER_ADDRESS,
        RECIPIENT_ADDRESS,
        "foo",
        &serde_json::json!("5"),
        None,
        &inscription_id_from_seed(99).to_string(),
        0,
        transfer_satpoint(100, 1),
        1_000,
      );

      assert_eq!(
        updater
          .tap_get::<String>(&format!(
            "b/{}/{}",
            MINER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("foo")
          ))
          .unwrap()
          .as_deref(),
        Some("95")
      );
      assert_eq!(
        updater
          .tap_get::<String>(&format!(
            "b/{}/{}",
            RECIPIENT_ADDRESS,
            InscriptionUpdater::json_stringify_lower("foo")
          ))
          .unwrap()
          .as_deref(),
        Some("5")
      );
    });
  }

  #[test]
  fn token_trade_stays_normal_for_non_reward_addresses_and_is_blocked_for_rewards() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let trade_id = inscription_id_from_seed(5);
      updater.index_token_trade_created(
        trade_id,
        0,
        satpoint_from_inscription(trade_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "0",
          "tick": "foo",
          "amt": "5",
          "accept": [
            {
              "tick": "bar",
              "amt": "2"
            }
          ],
          "valid": 100
        })),
        USER_ADDRESS,
        1_000,
      );

      let trade_acc = updater
        .tap_get::<TapAccumulatorEntry>(&format!("a/{}", trade_id))
        .unwrap()
        .unwrap();
      assert_eq!(trade_acc.addr, USER_ADDRESS);

      updater.tap_mark_dmt_reward_address(MINER_ADDRESS);

      let miner_trade_id = inscription_id_from_seed(6);
      updater.index_token_trade_created(
        miner_trade_id,
        0,
        satpoint_from_inscription(miner_trade_id, 0),
        &inscription_from_json(serde_json::json!({
          "p": "tap",
          "op": "token-trade",
          "side": "0",
          "tick": "foo",
          "amt": "5",
          "accept": [
            {
              "tick": "bar",
              "amt": "2"
            }
          ],
          "valid": 100
        })),
        MINER_ADDRESS,
        1_000,
      );

      assert!(
        updater
          .tap_get::<TapAccumulatorEntry>(&format!("a/{}", miner_trade_id))
          .unwrap()
          .is_none()
      );
    });
  }

  #[test]
  fn miner_reward_shield_snapshot_json() {
    println!(
      "SNAPSHOT_JSON:{}",
      serde_json::to_string(&build_miner_reward_shield_snapshot()).unwrap()
    );
  }

  // Real mainnet addresses so is_valid_bitcoin_address_mainnet passes.
  const MAIN_DEPLOYER_ADDRESS: &str = "bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f";
  const MAIN_OTHER_ADDRESS: &str = "bc1qreaftg3lr53nv84dnxhcvchmswevzlp9tdj2jd";
  const MAIN_TRIBUTE_ADDRESS: &str = "bc1qxhmdufsvnuaaaer4ynz88fspdsxq2h9e9cetdj";
  const MAIN_MINER_ADDRESS_A: &str = "bc1qx9t2l3pyny2spqpqlye8svce70nppwtaxwdrp4";

  fn put_dmt_deploy(
    updater: &mut InscriptionUpdater<'_, '_>,
    tick_lower: &str, // e.g. "dmt-bit"
    addr: &str,
    cap: &str,
  ) {
    put_dmt_deploy_with_element(updater, tick_lower, addr, cap, 11, None);
  }

  fn put_dmt_deploy_with_element(
    updater: &mut InscriptionUpdater<'_, '_>,
    tick_lower: &str,
    addr: &str,
    cap: &str,
    fld: u32,
    pat: Option<String>,
  ) {
    let mut dep = deploy_record_with_supply(tick_lower, addr, 0, cap, MAX_DEC_U64_STR);
    dep.dmt = true;
    dep.tick = tick_lower.to_string();
    let elem_ins = inscription_id_from_seed(210).to_string();
    dep.elem = Some(elem_ins.clone());
    let tick_key = InscriptionUpdater::json_stringify_lower(tick_lower);
    updater.tap_put(&format!("d/{}", tick_key), &dep).unwrap();
    updater.tap_put(&format!("dc/{}", tick_key), &cap.to_string()).unwrap();
    let elem_name = tick_lower.trim_start_matches("dmt-").to_string();
    updater.tap_put(&format!("dmt-{}", elem_ins), &elem_name).unwrap();
    let elem = ops::dmt_element::DmtElementRecord {
      tick: elem_name.clone(),
      blck: 0,
      tx: txid_from_seed(210).to_string(),
      vo: 0,
      ins: elem_ins,
      num: 0,
      ts: 0,
      addr: addr.to_string(),
      pat,
      fld,
    };
    updater
      .tap_put(&format!("dmt-el/{}", InscriptionUpdater::json_stringify_lower(&elem_name)), &elem)
      .unwrap();
  }

  fn redirect_inscription(
    tick: &str,
    act: u64,
    rule: serde_json::Value,
  ) -> Inscription {
    inscription_from_json(serde_json::json!({
      "p": "tap",
      "op": "dmt-redirect",
      "tick": tick,
      "act": act,
      "rule": rule,
    }))
  }

  fn one_bucket_pool() -> serde_json::Value {
    serde_json::json!({
      "type": "weighted-split",
      "must_sum_to": 10000,
      "buckets": [
        { "name": "miners", "share_bps": 10000, "recipient": { "type": "coinbase-output" } }
      ]
    })
  }

  fn solo_classification_minimal() -> serde_json::Value {
    serde_json::json!({
      "tags_substring": ["/solo.ckpool.org/"],
      "addresses": [],
      "pool_tags_blocklist": [],
      "pool_addresses_blocklist": [],
      "ambiguous_block_policy": "treat_as_pool"
    })
  }

  fn one_bucket_solo() -> serde_json::Value {
    serde_json::json!({
      "type": "weighted-split",
      "must_sum_to": 10000,
      "buckets": [{
        "name": "solo",
        "share_bps": 10000,
        "recipient": { "type": "solo-coinbase-output", "accumulate_when_no_solo": true }
      }],
      "solo_classification": solo_classification_minimal()
    })
  }

  fn hybrid_70_20_10(tribute: &str) -> serde_json::Value {
    serde_json::json!({
      "type": "weighted-split",
      "must_sum_to": 10000,
      "buckets": [
        { "name": "pool", "share_bps": 7000, "recipient": { "type": "coinbase-output" } },
        { "name": "solo", "share_bps": 2000, "recipient": { "type": "solo-coinbase-output", "accumulate_when_no_solo": true } },
        { "name": "tribute", "share_bps": 1000, "recipient": { "type": "address", "addr": tribute } }
      ],
      "solo_classification": solo_classification_minimal()
    })
  }

  // Default deploy parent: the inscription id `put_dmt_deploy` uses for
  // DeployRecord.ins (seed 200). Tests that exercise the standard happy-path
  // authorization use this; tests that exercise rejection paths
  // (missing/wrong parent) call `dmt_redirect_call_with_parents` directly.
  fn deploy_parents() -> Vec<InscriptionId> {
    vec![inscription_id_from_seed(200)]
  }

  fn dmt_redirect_call(
    updater: &mut InscriptionUpdater<'_, '_>,
    inscription_id: InscriptionId,
    inscriber: &str,
    payload: &Inscription,
  ) {
    let parents = deploy_parents();
    dmt_redirect_call_with_parents(updater, inscription_id, inscriber, payload, &parents);
  }

  fn dmt_redirect_call_with_parents(
    updater: &mut InscriptionUpdater<'_, '_>,
    inscription_id: InscriptionId,
    inscriber: &str,
    payload: &Inscription,
    parents: &[InscriptionId],
  ) {
    updater.index_dmt_redirect(
      inscription_id,
      0,
      satpoint_from_inscription(inscription_id, 0),
      payload,
      inscriber,
      1_000,
      parents,
    );
  }

  fn redirect_active(
    updater: &mut InscriptionUpdater<'_, '_>,
    tick_lower: &str,
  ) -> Option<DmtRedirectRecord> {
    updater
      .tap_get::<DmtRedirectRecord>(&format!(
        "r/{}",
        InscriptionUpdater::json_stringify_lower(tick_lower)
      ))
      .unwrap()
  }

  fn redirect_pending(
    updater: &mut InscriptionUpdater<'_, '_>,
    tick_lower: &str,
    act: u64,
  ) -> Option<DmtRedirectRecord> {
    updater
      .tap_get::<DmtRedirectRecord>(&format!(
        "r-pending/{}/{}",
        InscriptionUpdater::json_stringify_lower(tick_lower),
        act
      ))
      .unwrap()
  }

  fn coinbase_with_outputs(
    btc_outputs: Vec<(&str, u64)>,
    script_sig_bytes: Vec<u8>,
  ) -> Transaction {
    let outputs = btc_outputs
      .into_iter()
      .map(|(addr_str, sat)| {
        let address = addr_str
          .parse::<Address<NetworkUnchecked>>()
          .unwrap()
          .assume_checked();
        TxOut {
          value: Amount::from_sat(sat),
          script_pubkey: address.script_pubkey(),
        }
      })
      .collect();
    Transaction {
      version: Version(2),
      lock_time: LockTime::ZERO,
      input: vec![TxIn {
        previous_output: OutPoint::null(),
        script_sig: ScriptBuf::from_bytes(script_sig_bytes),
        sequence: Sequence::MAX,
        witness: Witness::new(),
      }],
      output: outputs,
    }
  }

  fn coinbase_script_sig_with_tag(tag: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(tag.len() + 2);
    if tag.len() <= 0x4b {
      bytes.push(tag.len() as u8);
      bytes.extend_from_slice(tag);
    } else {
      bytes.push(0x4c);
      bytes.push(tag.len() as u8);
      bytes.extend_from_slice(tag);
    }
    bytes
  }

  #[test]
  fn dmt_redirect_deployer_valid_accepted() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(50);
      let payload = redirect_inscription("bit", 947_300 + 200, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      let active = redirect_active(updater, "dmt-bit").expect("redirect stored");
      assert_eq!(active.tick, "dmt-bit");
      assert_eq!(active.act, 947_300 + 200);
      assert_eq!(active.inscription_id, id.to_string());
      // appended to the iterable list
      assert_eq!(get_string(updater, "redirect-list").as_deref(), Some("1"));
      assert_eq!(get_string(updater, "redirect-listi/0").as_deref(), Some("dmt-bit"));
    });
  }

  #[test]
  fn dmt_redirect_missing_deploy_parent_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(51);
      let payload = redirect_inscription("bit", 947_300 + 200, one_bucket_pool());
      // No parents declared — even with the deployer address as receiver, the
      // deploy inscription's UTXO was not spent in the reveal, so the rule
      // must be rejected.
      dmt_redirect_call_with_parents(updater, id, MAIN_DEPLOYER_ADDRESS, &payload, &[]);
      assert!(redirect_active(updater, "dmt-bit").is_none());
      assert!(get_string(updater, "redirect-list").is_none());
    });
  }

  #[test]
  fn dmt_redirect_wrong_parent_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(56);
      let payload = redirect_inscription("bit", 947_300 + 200, one_bucket_pool());
      // Some unrelated inscription as parent (not the deploy).
      let unrelated = inscription_id_from_seed(99);
      dmt_redirect_call_with_parents(
        updater,
        id,
        MAIN_DEPLOYER_ADDRESS,
        &payload,
        &[unrelated],
      );
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_same_tx_as_deploy_rejected() {
    // Same-tx deploy+redirect: if both inscriptions live in the same reveal
    // transaction the deploy was never on a UTXO that this tx spent — it's a
    // sibling envelope. Reject so the spec wording "must spend the deploy
    // inscription's UTXO" holds literally for every accepted case.
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      // put_dmt_deploy uses inscription_id_from_seed(200) for DeployRecord.ins.
      // Manufacture a redirect inscription_id with the same txid (different
      // index) so deploy.ins.txid == redirect.txid.
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let deploy_ins = inscription_id_from_seed(200);
      let same_tx_redirect = InscriptionId { txid: deploy_ins.txid, index: 1 };
      let payload = redirect_inscription("bit", 947_300 + 200, one_bucket_pool());
      dmt_redirect_call_with_parents(
        updater,
        same_tx_redirect,
        MAIN_DEPLOYER_ADDRESS,
        &payload,
        &[deploy_ins],
      );
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_owner_address_match_without_parent_rejected() {
    // Receiving-output-address equality is not an authorization proof: an
    // attacker can craft a reveal tx whose output goes to DeployRecord.addr
    // without controlling that address. The reveal must additionally spend
    // the deploy inscription's UTXO (declared as Ordinals parent).
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(57);
      let payload = redirect_inscription("bit", 947_300 + 200, one_bucket_pool());
      dmt_redirect_call_with_parents(updater, id, MAIN_DEPLOYER_ADDRESS, &payload, &[]);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_replacement_pre_first_act_b_after_a_stores_pending() {
    // When rule A is scheduled but not yet in force, a later rule B must NOT
    // overwrite A in r/<tick>; it must queue under r-pending so each promotes
    // at its own act block.
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id_a = inscription_id_from_seed(60);
      let act_a: u64 = 947_300 + 145;
      let p_a = redirect_inscription("bit", act_a, one_bucket_pool());
      dmt_redirect_call(updater, id_a, MAIN_DEPLOYER_ADDRESS, &p_a);

      // B inscribed at the same height as A, with later act. B.act > A.act,
      // both still in the future relative to current_height.
      let id_b = inscription_id_from_seed(61);
      let act_b: u64 = act_a + 100;
      let p_b = redirect_inscription("bit", act_b, one_bucket_pool());
      dmt_redirect_call(updater, id_b, MAIN_DEPLOYER_ADDRESS, &p_b);

      // r/<tick> still holds A; B is in r-pending/<tick>/<act_b>.
      let active = redirect_active(updater, "dmt-bit").expect("A still scheduled");
      assert_eq!(active.act, act_a);
      assert_eq!(active.inscription_id, id_a.to_string());
      let pending = redirect_pending(updater, "dmt-bit", act_b).expect("B queued pending");
      assert_eq!(pending.inscription_id, id_b.to_string());
    });
  }

  #[test]
  fn dmt_redirect_replacement_pre_first_act_b_before_a_stores_pending() {
    // Same MED scenario but with B.act < A.act — both inscribed before either
    // activates. B must still queue as pending (under its own act), and the
    // dispatcher's normal promotion path will replace A with B at B.act.
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id_a = inscription_id_from_seed(62);
      let act_a: u64 = 947_300 + 200;
      let p_a = redirect_inscription("bit", act_a, one_bucket_pool());
      dmt_redirect_call(updater, id_a, MAIN_DEPLOYER_ADDRESS, &p_a);

      let id_b = inscription_id_from_seed(63);
      let act_b: u64 = act_a - 50; // still > current+144 since act_a was current+200
      let p_b = redirect_inscription("bit", act_b, one_bucket_pool());
      dmt_redirect_call(updater, id_b, MAIN_DEPLOYER_ADDRESS, &p_b);

      let active = redirect_active(updater, "dmt-bit").expect("A still scheduled");
      assert_eq!(active.act, act_a);
      assert_eq!(active.inscription_id, id_a.to_string());
      let pending = redirect_pending(updater, "dmt-bit", act_b).expect("B queued pending");
      assert_eq!(pending.inscription_id, id_b.to_string());
    });
  }

  #[test]
  fn dmt_redirect_act_at_boundary_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(52);
      let payload = redirect_inscription("bit", 947_300 + 144, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_act_one_above_boundary_accepted() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(53);
      let payload = redirect_inscription("bit", 947_300 + 145, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_some());
    });
  }

  #[test]
  fn dmt_redirect_replacement_stores_pending() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      // First rule: act = current + 145 — becomes active.
      let id1 = inscription_id_from_seed(54);
      let p1 = redirect_inscription("bit", 947_300 + 145, one_bucket_pool());
      dmt_redirect_call(updater, id1, MAIN_DEPLOYER_ADDRESS, &p1);
      assert_eq!(redirect_active(updater, "dmt-bit").unwrap().act, 947_300 + 145);
      // Advance updater height past the first act so the old rule is in force.
      updater.height = 947_300 + 200;
      // Second rule: act must be > current_height + 144 = 947_644.
      let new_act: u64 = 947_300 + 200 + 145;
      let id2 = inscription_id_from_seed(55);
      let p2 = redirect_inscription("bit", new_act, one_bucket_pool());
      dmt_redirect_call(updater, id2, MAIN_DEPLOYER_ADDRESS, &p2);
      // Old still active, new is pending.
      assert_eq!(redirect_active(updater, "dmt-bit").unwrap().act, 947_300 + 145);
      assert!(redirect_pending(updater, "dmt-bit", new_act).is_some());
    });
  }

  #[test]
  fn dmt_redirect_multiple_pendings_stack() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id1 = inscription_id_from_seed(56);
      let p1 = redirect_inscription("bit", 947_445, one_bucket_pool());
      dmt_redirect_call(updater, id1, MAIN_DEPLOYER_ADDRESS, &p1);
      // Move past act so first rule is in force.
      updater.height = 947_500;
      let id2 = inscription_id_from_seed(57);
      let p2 = redirect_inscription("bit", 947_700, one_bucket_pool());
      dmt_redirect_call(updater, id2, MAIN_DEPLOYER_ADDRESS, &p2);
      let id3 = inscription_id_from_seed(58);
      let p3 = redirect_inscription("bit", 947_700, one_bucket_solo());
      dmt_redirect_call(updater, id3, MAIN_DEPLOYER_ADDRESS, &p3);
      // Both pending writes target the same key — latest wins.
      let pending = redirect_pending(updater, "dmt-bit", 947_700).unwrap();
      assert_eq!(pending.inscription_id, id3.to_string());
      // Solo rule has solo classification.
      assert!(pending.rule.solo_classification.is_some());
    });
  }

  #[test]
  fn dmt_redirect_bad_sum_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(60);
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "a", "share_bps": 4000, "recipient": { "type": "coinbase-output" } },
          { "name": "b", "share_bps": 4000, "recipient": { "type": "coinbase-output" } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_empty_buckets_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(61);
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": []
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_nine_buckets_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(62);
      let mut buckets = Vec::new();
      for i in 0..9 {
        buckets.push(serde_json::json!({
          "name": format!("b{i}"), "share_bps": 1111,
          "recipient": { "type": "coinbase-output" }
        }));
      }
      // Sum != 10000 too; defends the test if the bucket-count and sum
      // checks ever reorder.
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": buckets,
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_zero_share_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(63);
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "a", "share_bps": 10000, "recipient": { "type": "coinbase-output" } },
          { "name": "b", "share_bps": 0, "recipient": { "type": "coinbase-output" } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_bad_address_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(64);
      // Testnet address — not mainnet.
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": "tb1q6en7qjxgw4ev8xwx94pzdry6a6ky7wlfeqzunz" } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_solo_without_classification_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(65);
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "solo", "share_bps": 10000,
            "recipient": { "type": "solo-coinbase-output", "accumulate_when_no_solo": true } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_present_but_unused_classifier_oversized_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(66);
      let oversized: Vec<String> = (0..65).map(|i| format!("tag-{}", i)).collect();
      let bad_rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "miners", "share_bps": 10000, "recipient": { "type": "coinbase-output" } }
        ],
        "solo_classification": {
          "tags_substring": oversized,
          "addresses": [],
          "pool_tags_blocklist": [],
          "pool_addresses_blocklist": [],
          "ambiguous_block_policy": "treat_as_pool"
        }
      });
      let payload = redirect_inscription("bit", 947_500, bad_rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  #[test]
  fn dmt_redirect_solo_tag_classifies_solo() {
    let sc = SoloClassification {
      tags_substring: vec!["/solo.ckpool.org/".to_string()],
      addresses: vec![],
      pool_tags_blocklist: vec![],
      pool_addresses_blocklist: vec![],
      ambiguous_block_policy: NoRecipientPolicy::TreatAsPool,
    };
    let cb_addrs: std::collections::HashSet<String> = std::collections::HashSet::new();
    assert!(InscriptionUpdater::classify_block(
      "tag prefix /solo.ckpool.org/ suffix",
      &cb_addrs,
      &sc,
    ));
  }

  #[test]
  fn dmt_redirect_pool_takes_precedence() {
    let sc = SoloClassification {
      tags_substring: vec!["NiceHash".to_string()],
      addresses: vec![],
      pool_tags_blocklist: vec!["Foundry USA".to_string()],
      pool_addresses_blocklist: vec![],
      ambiguous_block_policy: NoRecipientPolicy::TreatAsPool,
    };
    let cb_addrs: std::collections::HashSet<String> = std::collections::HashSet::new();
    assert!(!InscriptionUpdater::classify_block(
      "Foundry USA + NiceHash",
      &cb_addrs,
      &sc,
    ));
  }

  #[test]
  fn dmt_redirect_no_match_falls_back_to_pool_default() {
    let sc = SoloClassification {
      tags_substring: vec!["/solo.ckpool.org/".to_string()],
      addresses: vec![],
      pool_tags_blocklist: vec![],
      pool_addresses_blocklist: vec![],
      ambiguous_block_policy: NoRecipientPolicy::TreatAsPool,
    };
    let cb_addrs: std::collections::HashSet<String> = std::collections::HashSet::new();
    assert!(!InscriptionUpdater::classify_block("random tag", &cb_addrs, &sc));
  }

  #[test]
  fn dmt_redirect_allowlist_address_solo() {
    let sc = SoloClassification {
      tags_substring: vec![],
      addresses: vec![MAIN_OTHER_ADDRESS.to_string()],
      pool_tags_blocklist: vec![],
      pool_addresses_blocklist: vec![],
      ambiguous_block_policy: NoRecipientPolicy::TreatAsPool,
    };
    let cb_addrs: std::collections::HashSet<String> =
      vec![MAIN_OTHER_ADDRESS.to_string()].into_iter().collect();
    assert!(InscriptionUpdater::classify_block("", &cb_addrs, &sc));
  }

  #[test]
  fn dmt_redirect_blocklist_address_pool() {
    let sc = SoloClassification {
      tags_substring: vec![],
      addresses: vec![MAIN_OTHER_ADDRESS.to_string()],
      pool_tags_blocklist: vec![],
      pool_addresses_blocklist: vec![MAIN_TRIBUTE_ADDRESS.to_string()],
      ambiguous_block_policy: NoRecipientPolicy::TreatAsSolo,
    };
    let cb_addrs: std::collections::HashSet<String> = vec![
      MAIN_OTHER_ADDRESS.to_string(),
      MAIN_TRIBUTE_ADDRESS.to_string(),
    ]
    .into_iter()
    .collect();
    assert!(!InscriptionUpdater::classify_block("", &cb_addrs, &sc));
  }

  #[test]
  fn dmt_redirect_pool_single_output_full_credit() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(120);
      updater
        .id_to_sequence_number
        .insert(&deploy_id.store(), &0)
        .unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "10000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      updater
        .tap_put(&format!("d/{}", InscriptionUpdater::json_stringify_lower("dmt-bit")), &dep)
        .unwrap();
      updater
        .tap_put(&format!("dc/{}", InscriptionUpdater::json_stringify_lower("dmt-bit")), &"10000000".to_string())
        .unwrap();

      // Install the redirect manually so we can drive the per-block dispatcher.
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_pool()).unwrap(),
        inscription_id: inscription_id_from_seed(121).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater
        .tap_put(&format!("r/{}", InscriptionUpdater::json_stringify_lower("dmt-bit")), &rec)
        .unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let coinbase = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      // bits = 100_000 → mint_amount = 100_000.
      updater
        .index_active_redirects_for_block(&coinbase, 100_000, &context.index)
        .unwrap();

      // Single-output coinbase, single 10000bps bucket → 100% to that addr.
      let bal = updater
        .tap_get::<String>(&format!(
          "b/{}/{}",
          MAIN_MINER_ADDRESS_A,
          InscriptionUpdater::json_stringify_lower("dmt-bit"),
        ))
        .unwrap()
        .unwrap();
      assert_eq!(bal, "100000");
    });
  }

  #[test]
  fn dmt_redirect_pool_multi_output_pro_rata() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(122);
      updater
        .id_to_sequence_number
        .insert(&deploy_id.store(), &0)
        .unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      updater
        .tap_put(&format!("d/{}", InscriptionUpdater::json_stringify_lower("dmt-bit")), &dep)
        .unwrap();
      updater
        .tap_put(&format!("dc/{}", InscriptionUpdater::json_stringify_lower("dmt-bit")), &"1000000".to_string())
        .unwrap();

      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_pool()).unwrap(),
        inscription_id: inscription_id_from_seed(123).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater
        .tap_put(&format!("r/{}", InscriptionUpdater::json_stringify_lower("dmt-bit")), &rec)
        .unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let coinbase = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 75_000), (MAIN_OTHER_ADDRESS, 25_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      // bits = 1000, share_bps = 10000 → share = 1000.
      // 75/100 → 750 to A, 25/100 → 250 to other.
      updater
        .index_active_redirects_for_block(&coinbase, 1000, &context.index)
        .unwrap();

      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      let a = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_MINER_ADDRESS_A, key)).unwrap().unwrap();
      let b = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(a, "750");
      assert_eq!(b, "250");
    });
  }

  #[test]
  fn dmt_redirect_solo_accumulator_releases() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(124);
      updater
        .id_to_sequence_number
        .insert(&deploy_id.store(), &0)
        .unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_solo()).unwrap(),
        inscription_id: inscription_id_from_seed(125).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      // 5 pool blocks (random tag) — each adds `share` to the solo accumulator.
      let pool_cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      for _ in 0..5 {
        updater.index_active_redirects_for_block(&pool_cb, 1_000, &context.index).unwrap();
      }
      // After 5 pool blocks, accumulator = 5_000 (1000 * 5 * 100% bps).
      let acc = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap().unwrap();
      assert_eq!(acc, "5000");
      // No miner balance has been credited.
      assert!(updater.tap_get::<String>(&format!("b/{}/{}", MAIN_MINER_ADDRESS_A, key)).unwrap().is_none());

      // Solo block — coinbase has the `/solo.ckpool.org/` tag.
      let solo_cb = coinbase_with_outputs(
        vec![(MAIN_OTHER_ADDRESS, 50_000)],
        coinbase_script_sig_with_tag(b"prefix /solo.ckpool.org/ suffix"),
      );
      updater.index_active_redirects_for_block(&solo_cb, 1_000, &context.index).unwrap();
      // 6× share = 6000 to OTHER addr (5000 accumulated + 1000 this block).
      let bal = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal, "6000");
      // Accumulator reset.
      let acc2 = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap();
      assert_eq!(acc2.as_deref(), Some("0"));
    });
  }

  #[test]
  fn dmt_redirect_solo_no_accumulator_burns_pool_blocks() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(126);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [{
          "name": "solo", "share_bps": 10000,
          "recipient": { "type": "solo-coinbase-output", "accumulate_when_no_solo": false }
        }],
        "solo_classification": solo_classification_minimal()
      });
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule).unwrap(),
        inscription_id: inscription_id_from_seed(127).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let pool_cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      for _ in 0..5 {
        updater.index_active_redirects_for_block(&pool_cb, 1_000, &context.index).unwrap();
      }
      // No accumulator written — burn semantics.
      assert!(updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap().is_none());

      let solo_cb = coinbase_with_outputs(
        vec![(MAIN_OTHER_ADDRESS, 50_000)],
        coinbase_script_sig_with_tag(b"/solo.ckpool.org/"),
      );
      updater.index_active_redirects_for_block(&solo_cb, 1_000, &context.index).unwrap();
      // Just 1× share = 1000.
      let bal = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal, "1000");
    });
  }

  #[test]
  fn dmt_redirect_hybrid_pool_block() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(128);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rule = hybrid_70_20_10(MAIN_TRIBUTE_ADDRESS);
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule).unwrap(),
        inscription_id: inscription_id_from_seed(129).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let pool_cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      // bits=10_000 → pool=7000, solo=2000, tribute=1000.
      updater.index_active_redirects_for_block(&pool_cb, 10_000, &context.index).unwrap();
      let bal_pool = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_MINER_ADDRESS_A, key)).unwrap().unwrap();
      let bal_tribute = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal_pool, "7000");
      assert_eq!(bal_tribute, "1000");
      // Solo accumulator holds 2000 for the unaccumulated bucket.
      let acc = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap().unwrap();
      assert_eq!(acc, "2000");
    });
  }

  #[test]
  fn dmt_redirect_hybrid_solo_block() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(130);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rule = hybrid_70_20_10(MAIN_TRIBUTE_ADDRESS);
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule).unwrap(),
        inscription_id: inscription_id_from_seed(131).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();
      // Pre-load the solo accumulator with 4000 (from prior pool blocks).
      updater.tap_put(&format!("redirect-acc/{}/solo", key), &"4000".to_string()).unwrap();

      let solo_cb = coinbase_with_outputs(
        vec![(MAIN_OTHER_ADDRESS, 50_000)],
        coinbase_script_sig_with_tag(b"/solo.ckpool.org/ banner"),
      );
      // bits=10_000 → pool=7000 to coinbase, solo=2000+4000=6000 to coinbase, tribute=1000.
      updater.index_active_redirects_for_block(&solo_cb, 10_000, &context.index).unwrap();
      // Both pool and solo buckets target the same coinbase output (single-output here).
      // Coinbase gets 7000 (pool) + 6000 (solo + acc) = 13000.
      let bal_coinbase = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal_coinbase, "13000");
      let bal_tribute = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal_tribute, "1000");
      // Accumulator zeroed out.
      let acc = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap().unwrap();
      assert_eq!(acc, "0");
    });
  }

  #[test]
  fn dmt_redirect_coinbase_recipient_shielded() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(132);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_pool()).unwrap(),
        inscription_id: inscription_id_from_seed(133).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      assert!(updater.tap_is_dmt_reward_address(MAIN_MINER_ADDRESS_A));
    });
  }

  #[test]
  fn dmt_redirect_address_recipient_not_shielded() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(134);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": MAIN_TRIBUTE_ADDRESS } }
        ]
      });
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule).unwrap(),
        inscription_id: inscription_id_from_seed(135).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      // Address recipient gets the credit, but is NOT shielded.
      let bal = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal, "1000");
      assert!(!updater.tap_is_dmt_reward_address(MAIN_TRIBUTE_ADDRESS));
    });
  }

  #[test]
  fn dmt_redirect_address_bucket_exhausted_dc_writes_failed_row() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(170);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      // dc starts at 1 — first block credits 1 and exhausts; second block fails.
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1".to_string()).unwrap();
      let rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": MAIN_TRIBUTE_ADDRESS } }
        ]
      });
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule).unwrap(),
        inscription_id: inscription_id_from_seed(171).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      // First block: dc=1, share=1000 → amount clamped to 1, credit succeeds.
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      let bal = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal, "1");
      assert_eq!(updater.tap_get::<String>(&format!("dc/{}", key)).unwrap().unwrap(), "0");
      let len_after_first: usize = updater
        .tap_get::<String>(&format!("aml/{}/{}", MAIN_TRIBUTE_ADDRESS, key))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
      assert_eq!(len_after_first, 1);

      // Second block: dc=0, must still write a fail=true mint row.
      updater.height = 947_501;
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      let bal2 = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal2, "1", "balance unchanged when dc exhausted");
      let len_after_second: usize = updater
        .tap_get::<String>(&format!("aml/{}/{}", MAIN_TRIBUTE_ADDRESS, key))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
      assert_eq!(len_after_second, 2, "failed-mint row written even with exhausted dc");
      let last = updater
        .tap_get::<MintRecord>(&format!("amli/{}/{}/1", MAIN_TRIBUTE_ADDRESS, key))
        .unwrap()
        .unwrap();
      assert!(last.fail);
      assert_eq!(last.amt, "0");
    });
  }

  #[test]
  fn dmt_redirect_nat_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      // dmt-nat exemption — even with a deploy at the tick, the rule is rejected
      put_dmt_deploy(updater, "dmt-nat", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(70);
      let payload = redirect_inscription("nat", 947_500, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-nat").is_none());
    });
  }

  #[test]
  fn dmt_redirect_nat_uppercase_rejected() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-nat", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(71);
      let payload = redirect_inscription("NAT", 947_500, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-nat").is_none());
    });
  }

  #[test]
  fn dmt_redirect_rejects_field_4_deploy() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy_with_element(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000", 4, None);
      let id = inscription_id_from_seed(72);
      let payload = redirect_inscription("bit", 947_500, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
      assert!(get_string(updater, "redirect-list").is_none());
    });
  }

  #[test]
  fn dmt_redirect_rejects_pattern_field_11_deploy() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy_with_element(
        updater,
        "dmt-bit",
        MAIN_DEPLOYER_ADDRESS,
        "1000000000",
        11,
        Some("3b".to_string()),
      );
      let id = inscription_id_from_seed(73);
      let payload = redirect_inscription("bit", 947_500, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
      assert!(get_string(updater, "redirect-list").is_none());
    });
  }

  #[test]
  fn dmt_redirect_accepts_no_pattern_field_11_deploy() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy_with_element(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000", 11, None);
      let id = inscription_id_from_seed(74);
      let payload = redirect_inscription("bit", 947_500, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_some());
    });
  }

  #[test]
  fn dmt_redirect_mint_guard_suppresses_inscriber_credit() {
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      // The redirect guard fires before element resolution, so we can skip
      // the element fixture.
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_pool()).unwrap(),
        inscription_id: inscription_id_from_seed(140).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();

      let pre = updater
        .tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key))
        .unwrap();
      assert!(pre.is_none());
      let mint_payload = inscription_from_json(serde_json::json!({
        "p": "tap", "op": "dmt-mint", "tick": "bit", "blk": 947_500
      }));
      let mint_id = inscription_id_from_seed(141);
      updater.index_dmt_mint(
        mint_id,
        0,
        satpoint_from_inscription(mint_id, 0),
        &mint_payload,
        MAIN_OTHER_ADDRESS,
        1_000,
        &[],
        &Context::builder().chain(Chain::Mainnet).build().index,
      );
      // Guard suppressed the inscriber credit
      let post = updater
        .tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key))
        .unwrap();
      assert!(post.is_none());
    });
  }

  #[test]
  fn dmt_redirect_dmt_prefix_form_rejected_by_sanity_check() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(72);
      let payload = redirect_inscription("dmt-bit", 947_500, one_bucket_pool());
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none());
    });
  }

  // The spec README's worked example must round-trip through the validator.
  // If this drifts, either the spec or the validator changed.
  #[test]
  fn dmt_redirect_spec_example_parity() {
    // Verbatim JSON from the spec README amendment. Do not edit this string
    // without also editing the spec — that is the entire point of this test.
    let spec_json = r#"{
  "p": "tap",
  "op": "dmt-redirect",
  "tick": "bit",
  "act": 948888,
  "rule": {
    "type": "weighted-split",
    "must_sum_to": 10000,
    "buckets": [
      {
        "name": "pool",
        "share_bps": 7000,
        "recipient": { "type": "coinbase-output" }
      },
      {
        "name": "solo",
        "share_bps": 2000,
        "recipient": {
          "type": "solo-coinbase-output",
          "accumulate_when_no_solo": true
        }
      },
      {
        "name": "tribute",
        "share_bps": 1000,
        "recipient": {
          "type": "address",
          "addr": "bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f"
        }
      }
    ],
    "solo_classification": {
      "tags_substring": ["/solo.ckpool.org/", "Public-Pool", "NiceHash"],
      "addresses": ["bc1qreaftg3lr53nv84dnxhcvchmswevzlp9tdj2jd"],
      "pool_tags_blocklist": ["Foundry USA", "/AntPool/", "F2Pool"],
      "pool_addresses_blocklist": [],
      "ambiguous_block_policy": "treat_as_pool"
    }
  }
}"#;

    // Spec tribute address == test deployer, so the inscriber-auth check passes.
    // act=948888 needs current_height < 948744.
    let current_height: u32 = 948_700;
    with_test_updater(BtcNetwork::Bitcoin, current_height, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(99);
      let payload = inscription_from_body(spec_json);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);

      // The spec's worked example must be accepted as-is.
      let active = redirect_active(updater, "dmt-bit")
        .expect("spec example must be accepted by the validator");
      assert_eq!(active.tick, "dmt-bit");
      assert_eq!(active.act, 948_888);
      assert_eq!(active.inscription_id, id.to_string());
      assert_eq!(active.inscriber_addr, MAIN_DEPLOYER_ADDRESS);
      assert_eq!(active.inscribed_at_height, current_height);

      // Rule-level fields.
      assert_eq!(active.rule.rule_type, "weighted-split");
      assert_eq!(active.rule.must_sum_to, 10_000);
      assert_eq!(active.rule.buckets.len(), 3);
      let share_sum: u32 = active
        .rule
        .buckets
        .iter()
        .map(|b| u32::from(b.share_bps))
        .sum();
      assert_eq!(share_sum, 10_000);

      // Bucket 0: pool / 7000 / coinbase-output.
      assert_eq!(active.rule.buckets[0].name, "pool");
      assert_eq!(active.rule.buckets[0].share_bps, 7_000);
      assert!(matches!(
        active.rule.buckets[0].recipient,
        BucketRecipient::CoinbaseOutput
      ));

      // Bucket 1: solo / 2000 / solo-coinbase-output { accumulate_when_no_solo: true }.
      assert_eq!(active.rule.buckets[1].name, "solo");
      assert_eq!(active.rule.buckets[1].share_bps, 2_000);
      match &active.rule.buckets[1].recipient {
        BucketRecipient::SoloCoinbaseOutput { accumulate_when_no_solo } => {
          assert!(*accumulate_when_no_solo, "spec example sets accumulate_when_no_solo: true");
        }
        other => panic!("expected SoloCoinbaseOutput, got {other:?}"),
      }

      // Bucket 2: tribute / 1000 / address.
      assert_eq!(active.rule.buckets[2].name, "tribute");
      assert_eq!(active.rule.buckets[2].share_bps, 1_000);
      match &active.rule.buckets[2].recipient {
        BucketRecipient::Address { addr } => {
          assert_eq!(addr, "bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f");
        }
        other => panic!("expected Address, got {other:?}"),
      }

      // Solo classifier well-formed.
      let sc = active
        .rule
        .solo_classification
        .as_ref()
        .expect("solo bucket present, classifier must be stored");
      assert_eq!(
        sc.tags_substring,
        vec![
          "/solo.ckpool.org/".to_string(),
          "Public-Pool".to_string(),
          "NiceHash".to_string(),
        ]
      );
      assert_eq!(
        sc.addresses,
        vec!["bc1qreaftg3lr53nv84dnxhcvchmswevzlp9tdj2jd".to_string()]
      );
      assert_eq!(
        sc.pool_tags_blocklist,
        vec![
          "Foundry USA".to_string(),
          "/AntPool/".to_string(),
          "F2Pool".to_string(),
        ]
      );
      assert!(sc.pool_addresses_blocklist.is_empty());
      assert_eq!(sc.ambiguous_block_policy, NoRecipientPolicy::TreatAsPool);

      // Iterable redirect-list updated.
      assert_eq!(get_string(updater, "redirect-list").as_deref(), Some("1"));
      assert_eq!(
        get_string(updater, "redirect-listi/0").as_deref(),
        Some("dmt-bit")
      );
    });
  }

  // F1 reproducer: a SOLO-classified block whose coinbase has no addressable
  // outputs (all OP_RETURN) must not drain the solo accumulator. Before the
  // fix, the dispatcher zeroed the accumulator before calling pay_coinbase_share,
  // which returned early on tot_btc==0 and silently burned the accumulated value.
  #[test]
  fn dmt_redirect_op_return_coinbase_preserves_accumulator() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(140);
      updater
        .id_to_sequence_number
        .insert(&deploy_id.store(), &0)
        .unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_solo()).unwrap(),
        inscription_id: inscription_id_from_seed(141).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      // 3 pool blocks accumulate 3 emissions (1000 each).
      let pool_cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      for _ in 0..3 {
        updater.index_active_redirects_for_block(&pool_cb, 1_000, &context.index).unwrap();
      }
      let acc = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap().unwrap();
      assert_eq!(acc, "3000");

      // SOLO-classified block (carries the solo tag) but coinbase has only
      // an OP_RETURN output — no addressable recipient. Accumulator must
      // survive untouched and the block's own share must accumulate too
      // since accumulate_when_no_solo=true.
      let solo_op_return_cb = Transaction {
        version: Version(2),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
          previous_output: OutPoint::null(),
          script_sig: ScriptBuf::from_bytes(coinbase_script_sig_with_tag(b"prefix /solo.ckpool.org/ suffix")),
          sequence: Sequence::MAX,
          witness: Witness::new(),
        }],
        output: vec![TxOut {
          value: Amount::from_sat(0),
          script_pubkey: bitcoin::Script::builder()
            .push_opcode(bitcoin::opcodes::all::OP_RETURN)
            .into_script(),
        }],
      };
      updater.index_active_redirects_for_block(&solo_op_return_cb, 1_000, &context.index).unwrap();
      let acc_after = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap().unwrap();
      assert_eq!(acc_after, "4000",
        "OP_RETURN-only coinbase must not silently burn the accumulator");

      // A real solo block now credits the recipient the full accumulated total.
      let solo_cb = coinbase_with_outputs(
        vec![(MAIN_OTHER_ADDRESS, 50_000)],
        coinbase_script_sig_with_tag(b"prefix /solo.ckpool.org/ suffix"),
      );
      updater.index_active_redirects_for_block(&solo_cb, 1_000, &context.index).unwrap();
      let bal = updater.tap_get::<String>(&format!("b/{}/{}", MAIN_OTHER_ADDRESS, key)).unwrap().unwrap();
      assert_eq!(bal, "5000",
        "5 emissions must reach the solo recipient (3 pool + 1 OP_RETURN solo + 1 paid solo)");
      let acc_final = updater.tap_get::<String>(&format!("redirect-acc/{}/solo", key)).unwrap();
      assert_eq!(acc_final.as_deref(), Some("0"));
    });
  }

  // F2 reproducer: classifier addresses are normalized at validation time, so
  // an inscriber writing uppercase bech32 still matches the canonical
  // (lowercase) form produced by address_from_script.
  #[test]
  fn dmt_redirect_uppercase_bech32_in_classifier_matches() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(151);
      // MAIN_OTHER_ADDRESS is lowercase bech32 ("bc1q...").
      // Uppercase form per BIP173 is still a valid bech32 encoding of the
      // same script.
      let upper = MAIN_OTHER_ADDRESS.to_uppercase();
      let rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [{
          "name": "solo",
          "share_bps": 10000,
          "recipient": { "type": "solo-coinbase-output", "accumulate_when_no_solo": false }
        }],
        "solo_classification": {
          "tags_substring": [],
          "addresses": [upper.clone()],
          "pool_tags_blocklist": [],
          "pool_addresses_blocklist": [],
          "ambiguous_block_policy": "treat_as_pool"
        }
      });
      let payload = redirect_inscription("bit", 947_300 + 200, rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      let active = redirect_active(updater, "dmt-bit").expect("redirect stored");
      let sc = active.rule.solo_classification.as_ref().unwrap();
      assert_eq!(sc.addresses, vec![MAIN_OTHER_ADDRESS.to_string()],
        "uppercase bech32 must be canonicalized to lowercase at storage time");

      // address_from_script always emits lowercase bech32; classifier should match.
      let cb_addrs: std::collections::HashSet<String> =
        vec![MAIN_OTHER_ADDRESS.to_string()].into_iter().collect();
      assert!(InscriptionUpdater::classify_block("", &cb_addrs, sc),
        "lowercase coinbase address must match a classifier list that the inscriber wrote in uppercase");
    });
  }

  // Classifier extractor reproducer: Braiins Solo (mainnet block 947128)
  // encodes `/braiinssolo/` such that part of the brand string lives outside
  // pushdata wrappers. The whole-scriptSig UTF-8-lossy decode surfaces it;
  // the prior pushdata-walker dropped the trailing bytes and misclassified
  // the block as POOL.
  #[test]
  fn dmt_redirect_braiins_solo_classifies_as_solo() {
    // scriptSig hex from mempool.space for block 947128 (Braiins Solo).
    let hex_str = "03b8730e0004dfcef1690485f02c0f0c4e23686d00000000000000000a636b706f6f6c0d2f62726169696e73736f6c6f2f";
    let bytes = hex::decode(hex_str).expect("valid hex");

    let coinbase = Transaction {
      version: Version(2),
      lock_time: LockTime::ZERO,
      input: vec![TxIn {
        previous_output: OutPoint::null(),
        script_sig: ScriptBuf::from_bytes(bytes),
        sequence: Sequence::MAX,
        witness: Witness::new(),
      }],
      output: vec![],
    };

    let tag = InscriptionUpdater::extract_coinbase_tag(&coinbase);
    assert!(tag.contains("/braiinssolo/"),
      "whole-scriptSig UTF-8-lossy decode must surface /braiinssolo/, got {:?}", tag);

    let sc = SoloClassification {
      tags_substring: vec!["/braiinssolo/".to_string()],
      addresses: vec![],
      pool_tags_blocklist: vec![],
      pool_addresses_blocklist: vec![],
      ambiguous_block_policy: NoRecipientPolicy::TreatAsPool,
    };
    let cb_addrs: std::collections::HashSet<String> = std::collections::HashSet::new();
    assert!(InscriptionUpdater::classify_block(&tag, &cb_addrs, &sc),
      "Braiins Solo block 947128 must classify as SOLO once the extractor sees the whole scriptSig");
  }

  // Two pendings B (act=X) and C (act=Y > X) on the same tick. After the
  // dispatcher runs at X, B promotes; at Y, C replaces B in the active slot.
  #[test]
  fn dmt_redirect_cross_act_pending_promotion() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(180);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();

      // Active rule A is in force from the start so subsequent rules become pending.
      let rec_a = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_pool()).unwrap(),
        inscription_id: inscription_id_from_seed(181).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec_a).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      // Pending B at act=X with the address-only rule.
      let rule_b = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": MAIN_TRIBUTE_ADDRESS } }
        ]
      });
      let act_b = 947_600u64;
      let rec_b = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: act_b,
        rule: serde_json::from_value(rule_b).unwrap(),
        inscription_id: inscription_id_from_seed(182).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_500,
      };
      updater.tap_put(&format!("r-pending/{}/{}", key, act_b), &rec_b).unwrap();

      // Pending C at act=Y > X with a different address recipient.
      let rule_c = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": MAIN_OTHER_ADDRESS } }
        ]
      });
      let act_c = 947_700u64;
      let rec_c = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: act_c,
        rule: serde_json::from_value(rule_c).unwrap(),
        inscription_id: inscription_id_from_seed(183).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_500,
      };
      updater.tap_put(&format!("r-pending/{}/{}", key, act_c), &rec_c).unwrap();

      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );

      // Run at height X: B should promote, replacing A.
      updater.height = act_b as u32;
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      let active = redirect_active(updater, "dmt-bit").unwrap();
      assert_eq!(active.act, act_b);
      assert_eq!(active.inscription_id, inscription_id_from_seed(182).to_string());
      // Pending B is consumed; pending C still queued.
      assert!(redirect_pending(updater, "dmt-bit", act_b).is_none());
      assert!(redirect_pending(updater, "dmt-bit", act_c).is_some());

      // Run at height Y: C should promote, replacing B.
      updater.height = act_c as u32;
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      let active = redirect_active(updater, "dmt-bit").unwrap();
      assert_eq!(active.act, act_c);
      assert_eq!(active.inscription_id, inscription_id_from_seed(183).to_string());
      assert!(redirect_pending(updater, "dmt-bit", act_c).is_none());
    });
  }

  // Base58 P2PKH (Genesis address) as the fixed-address recipient. Verify the
  // credit lands on the bare base58 form (normalize_address only lowercases bech32).
  #[test]
  fn dmt_redirect_base58_address_recipient_credited() {
    const GENESIS_ADDRESS: &str = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa";
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      let deploy_id = inscription_id_from_seed(190);
      updater.id_to_sequence_number.insert(&deploy_id.store(), &0).unwrap();
      let mut dep = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep.dmt = true;
      dep.tick = "dmt-bit".to_string();
      dep.ins = deploy_id.to_string();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.tap_put(&format!("dc/{}", key), &"1000000".to_string()).unwrap();

      let rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": GENESIS_ADDRESS } }
        ]
      });
      let rec = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule).unwrap(),
        inscription_id: inscription_id_from_seed(191).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key), &rec).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();

      let bal = updater
        .tap_get::<String>(&format!("b/{}/{}", GENESIS_ADDRESS, key))
        .unwrap()
        .unwrap();
      assert_eq!(bal, "1000");
    });
  }

  // Uppercase bech32 fixed-address recipient: validation lowercases at storage,
  // so the credit lands on the lowercase form.
  #[test]
  fn dmt_redirect_uppercase_bech32_fixed_address_normalized() {
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(195);
      let upper = MAIN_TRIBUTE_ADDRESS.to_uppercase();
      let rule = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": upper } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, rule);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      let active = redirect_active(updater, "dmt-bit").expect("redirect stored");
      let stored_addr = match &active.rule.buckets[0].recipient {
        BucketRecipient::Address { addr } => addr.clone(),
        _ => panic!("expected address recipient"),
      };
      assert_eq!(stored_addr, MAIN_TRIBUTE_ADDRESS,
        "uppercase bech32 must be normalized to lowercase at storage time");

      // Drive a block and verify the credit lands on the lowercase form.
      let context = Context::builder().chain(Chain::Mainnet).build();
      let deploy_id = active.inscription_id.clone();
      // Mark the deploy inscription as available so the dispatcher proceeds.
      let parsed_id: InscriptionId = deploy_id.parse().unwrap();
      updater.id_to_sequence_number.insert(&parsed_id.store(), &0).unwrap();
      let key = InscriptionUpdater::json_stringify_lower("dmt-bit");
      // Ensure dmt-bit deploy.ins points at an inscription marked available.
      let mut dep = updater.tap_get::<DeployRecord>(&format!("d/{}", key)).unwrap().unwrap();
      dep.ins = deploy_id;
      updater.tap_put(&format!("d/{}", key), &dep).unwrap();
      updater.height = 947_500;
      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();
      let bal_lower = updater
        .tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key))
        .unwrap()
        .unwrap();
      assert_eq!(bal_lower, "1000");
      // No balance keyed under uppercase form.
      assert!(updater.tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS.to_uppercase(), key)).unwrap().is_none());
    });
  }

  // Two ticks each with their own active redirect. One block, both fire.
  #[test]
  fn dmt_redirect_simultaneous_active_redirects_independent() {
    let context = Context::builder().chain(Chain::Mainnet).build();
    with_test_updater(BtcNetwork::Bitcoin, 947_500, |updater| {
      // Tick A — credits coinbase output bucket.
      let deploy_id_a = inscription_id_from_seed(200);
      updater.id_to_sequence_number.insert(&deploy_id_a.store(), &0).unwrap();
      let mut dep_a = deploy_record_with_supply("dmt-bit", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep_a.dmt = true;
      dep_a.tick = "dmt-bit".to_string();
      dep_a.ins = deploy_id_a.to_string();
      let key_a = InscriptionUpdater::json_stringify_lower("dmt-bit");
      updater.tap_put(&format!("d/{}", key_a), &dep_a).unwrap();
      updater.tap_put(&format!("dc/{}", key_a), &"1000000".to_string()).unwrap();
      let rec_a = DmtRedirectRecord {
        tick: "dmt-bit".to_string(),
        act: 947_500,
        rule: serde_json::from_value(one_bucket_pool()).unwrap(),
        inscription_id: inscription_id_from_seed(201).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key_a), &rec_a).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-bit".to_string()).unwrap();

      // Tick B — credits the tribute fixed address.
      let deploy_id_b = inscription_id_from_seed(202);
      updater.id_to_sequence_number.insert(&deploy_id_b.store(), &0).unwrap();
      let mut dep_b = deploy_record_with_supply("dmt-foo", MAIN_DEPLOYER_ADDRESS, 0, "1000000", MAX_DEC_U64_STR);
      dep_b.dmt = true;
      dep_b.tick = "dmt-foo".to_string();
      dep_b.ins = deploy_id_b.to_string();
      let key_b = InscriptionUpdater::json_stringify_lower("dmt-foo");
      updater.tap_put(&format!("d/{}", key_b), &dep_b).unwrap();
      updater.tap_put(&format!("dc/{}", key_b), &"1000000".to_string()).unwrap();
      let rule_b = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "tribute", "share_bps": 10000,
            "recipient": { "type": "address", "addr": MAIN_TRIBUTE_ADDRESS } }
        ]
      });
      let rec_b = DmtRedirectRecord {
        tick: "dmt-foo".to_string(),
        act: 947_500,
        rule: serde_json::from_value(rule_b).unwrap(),
        inscription_id: inscription_id_from_seed(203).to_string(),
        inscriber_addr: MAIN_DEPLOYER_ADDRESS.to_string(),
        inscribed_at_height: 947_300,
      };
      updater.tap_put(&format!("r/{}", key_b), &rec_b).unwrap();
      updater.tap_set_list_record("redirect-list", "redirect-listi", &"dmt-foo".to_string()).unwrap();

      let cb = coinbase_with_outputs(
        vec![(MAIN_MINER_ADDRESS_A, 50_000)],
        coinbase_script_sig_with_tag(b"random"),
      );
      updater.index_active_redirects_for_block(&cb, 1_000, &context.index).unwrap();

      // Both ticks should have credited their respective recipients.
      let bal_a = updater
        .tap_get::<String>(&format!("b/{}/{}", MAIN_MINER_ADDRESS_A, key_a))
        .unwrap()
        .unwrap();
      let bal_b = updater
        .tap_get::<String>(&format!("b/{}/{}", MAIN_TRIBUTE_ADDRESS, key_b))
        .unwrap()
        .unwrap();
      assert_eq!(bal_a, "1000");
      assert_eq!(bal_b, "1000");
    });
  }

  // Bucket-name validator edge cases bundled into one test: invalid chars,
  // overlong names, and exact duplicates all reject.
  #[test]
  fn dmt_redirect_bucket_name_validation_edge_cases() {
    // Invalid characters reject.
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(220);
      let bad = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "miners!@#", "share_bps": 10000, "recipient": { "type": "coinbase-output" } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none(),
        "name with !@# must reject");
    });

    // Name longer than 32 chars rejects.
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(221);
      let too_long: String = "a".repeat(33);
      let bad = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": too_long, "share_bps": 10000, "recipient": { "type": "coinbase-output" } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none(),
        "name >32 chars must reject");
    });

    // Exact duplicate names reject (storage-key collision risk).
    with_test_updater(BtcNetwork::Bitcoin, 947_300, |updater| {
      put_dmt_deploy(updater, "dmt-bit", MAIN_DEPLOYER_ADDRESS, "1000000000");
      let id = inscription_id_from_seed(222);
      let bad = serde_json::json!({
        "type": "weighted-split",
        "must_sum_to": 10000,
        "buckets": [
          { "name": "miners", "share_bps": 5000, "recipient": { "type": "coinbase-output" } },
          { "name": "miners", "share_bps": 5000, "recipient": { "type": "coinbase-output" } }
        ]
      });
      let payload = redirect_inscription("bit", 947_500, bad);
      dmt_redirect_call(updater, id, MAIN_DEPLOYER_ADDRESS, &payload);
      assert!(redirect_active(updater, "dmt-bit").is_none(),
        "duplicate names must reject");
    });
  }
}
// END MINER-REWARD-SHIELD
