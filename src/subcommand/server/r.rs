use super::*;
use crate::index::{
  tap_js_json_stringify_str, tap_js_json_stringify_value, tap_js_preprocess_json_for_serde,
  tap_js_to_lowercase,
};
use ciborium::de::from_reader as cbor_from_reader;
use std::io::{BufRead, BufReader};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapBitmapRecord {
  ownr: String,
  #[serde(default)]
  prv: Option<String>,
  bm: u64,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

fn tap_decode_bitmap_record(bytes: &[u8]) -> Option<TapBitmapRecord> {
  tap_decode_record(bytes)
}

fn tap_decode_json_value(bytes: &[u8]) -> Option<serde_json::Value> {
  let value = serde_json::from_slice::<serde_json::Value>(bytes)
    .ok()
    .or_else(|| {
      let raw = std::str::from_utf8(bytes).ok()?;
      serde_json::from_str::<serde_json::Value>(&tap_js_preprocess_json_for_serde(raw)).ok()
    })
    .or_else(|| cbor_from_reader::<serde_json::Value, _>(std::io::Cursor::new(bytes)).ok())?;
  value.is_object().then_some(value)
}

fn tap_decode_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
  cbor_from_reader::<T, _>(std::io::Cursor::new(bytes))
    .ok()
    .or_else(|| {
      let raw = std::str::from_utf8(bytes).ok()?;
      serde_json::from_str::<T>(&tap_js_preprocess_json_for_serde(raw)).ok()
    })
}

fn tap_record_json_text<T: serde::de::DeserializeOwned + serde::Serialize>(
  bytes: &[u8],
) -> Option<String> {
  if let Ok(raw) = std::str::from_utf8(bytes) {
    let trimmed = raw.trim_start();
    if trimmed.starts_with('{') {
      return Some(raw.to_string());
    }
  }
  let record: T = tap_decode_record(bytes)?;
  let value = serde_json::to_value(record).ok()?;
  Some(tap_js_json_stringify_value(&value))
}

fn tap_raw_json_response(body: String) -> Response {
  (
    [(
      header::CONTENT_TYPE,
      HeaderValue::from_static("application/json"),
    )],
    body,
  )
    .into_response()
}

fn tap_result_array_response(items: Vec<String>) -> Response {
  tap_raw_json_response(format!("{{\"result\":[{}]}}", items.join(",")))
}

#[cfg(test)]
mod tests {
  use super::{tap_decode_json_value, tap_reader_dmt_holder_shape, TapAccumulatorEntry};

  #[test]
  fn tap_decode_json_value_accepts_raw_json_and_cbor_json_rows() {
    let row = serde_json::json!({
      "ownr": "tb1qowner",
      "prv": "tb1qprevious",
      "tick": "dmt-nat",
      "elem": {"name": "nat"},
      "blck": 1,
      "ins": "abc123i0",
      "dmtblck": 42
    });

    let raw_json = serde_json::to_vec(&row).unwrap();
    assert_eq!(tap_decode_json_value(&raw_json), Some(row.clone()));

    let mut cbor_json = Vec::new();
    ciborium::into_writer(&row, &mut cbor_json).unwrap();
    assert_eq!(tap_decode_json_value(&cbor_json), Some(row));

    assert!(tap_decode_json_value(b"not json or cbor").is_none());
  }

  #[test]
  fn accumulator_entry_serializes_val_when_present_like_tap_reader() {
    let with_val = serde_json::to_value(TapAccumulatorEntry {
      op: "token-auth".to_string(),
      json: serde_json::json!({"p": "tap", "op": "token-auth"}),
      ins: "abc123i0".to_string(),
      blck: 1,
      tx: "abc123".to_string(),
      vo: 0,
      val: Some("10000".to_string()),
      num: 7,
      ts: 42,
      addr: "tb1qowner".to_string(),
    })
    .unwrap();
    assert_eq!(with_val.get("val").and_then(|v| v.as_str()), Some("10000"));

    let without_val = serde_json::to_value(TapAccumulatorEntry {
      op: "token-auth".to_string(),
      json: serde_json::json!({"p": "tap", "op": "token-auth", "cancel": "abc123i0"}),
      ins: "def456i0".to_string(),
      blck: 2,
      tx: "def456".to_string(),
      vo: 0,
      val: None,
      num: 8,
      ts: 43,
      addr: "tb1qowner".to_string(),
    })
    .unwrap();
    assert!(without_val.get("val").is_none());
  }

  #[test]
  fn dmt_holder_rest_shape_parses_elem_string_like_tap_reader() {
    let shaped = tap_reader_dmt_holder_shape(serde_json::json!({
      "tick": "dmt-nat",
      "elem": "{\"name\":\"nat\",\"fld\":4}",
      "ins": "abc123i0"
    }));
    assert_eq!(
      shaped.get("elem"),
      Some(&serde_json::json!({"name": "nat", "fld": 4}))
    );
  }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapDeployRecord {
  tick: String,
  max: String,
  lim: String,
  dec: u32,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  addr: String,
  crsd: bool,
  dmt: bool,
  #[serde(default)]
  elem: Option<String>,
  #[serde(default)]
  prj: Option<String>,
  #[serde(default)]
  dim: Option<String>,
  #[serde(default)]
  dt: Option<String>,
  #[serde(default)]
  prv: Option<String>,
  #[serde(default)]
  dta: Option<String>,
}

fn tap_decode_deploy_record(bytes: &[u8]) -> Option<TapDeployRecord> {
  tap_decode_record(bytes)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapMintRecord {
  addr: String,
  blck: u32,
  amt: String,
  bal: String,
  #[serde(default)]
  tx: Option<String>,
  vo: u32,
  val: String,
  #[serde(default)]
  ins: Option<String>,
  #[serde(default)]
  num: Option<i32>,
  ts: u32,
  fail: bool,
  #[serde(default)]
  dmtblck: Option<u32>,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapMintFlatRecord {
  addr: String,
  blck: u32,
  amt: String,
  bal: String,
  #[serde(default)]
  tx: Option<String>,
  vo: u32,
  val: String,
  #[serde(default)]
  ins: Option<String>,
  #[serde(default)]
  num: Option<i32>,
  ts: u32,
  fail: bool,
  #[serde(default)]
  dmtblck: Option<u32>,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapMintSuperflatRecord {
  tick: String,
  addr: String,
  blck: u32,
  amt: String,
  bal: String,
  #[serde(default)]
  tx: Option<String>,
  vo: u32,
  val: String,
  #[serde(default)]
  ins: Option<String>,
  #[serde(default)]
  num: Option<i32>,
  ts: u32,
  fail: bool,
  #[serde(default)]
  dmtblck: Option<u32>,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferInitRecord {
  addr: String,
  blck: u32,
  amt: String,
  trf: String,
  bal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferInitFlatRecord {
  addr: String,
  blck: u32,
  amt: String,
  trf: String,
  bal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferInitSuperflatRecord {
  tick: String,
  addr: String,
  blck: u32,
  amt: String,
  trf: String,
  bal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferSendSenderRecord {
  addr: String,
  taddr: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  at: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  tt: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  st: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rl: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rf: Option<String>,
  blck: u32,
  amt: String,
  trf: String,
  bal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferSendReceiverRecord {
  faddr: String,
  addr: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  at: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  tt: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  st: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rl: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rf: Option<String>,
  blck: u32,
  amt: String,
  bal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferSendFlatRecord {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  tick: Option<String>,
  addr: String,
  taddr: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  at: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  tt: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  st: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rl: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rf: Option<String>,
  blck: u32,
  amt: String,
  trf: String,
  bal: String,
  tbal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTransferSendSuperflatRecord {
  tick: String,
  addr: String,
  taddr: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  at: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  tt: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  st: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rl: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rf: Option<String>,
  blck: u32,
  amt: String,
  trf: String,
  bal: String,
  tbal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
  int: bool,
  #[serde(default)]
  dta: Option<String>,
}

// Accumulator entry returned by `/r/tap/getAccumulator*` endpoints
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapAccumulatorEntry {
  op: String,
  json: serde_json::Value,
  ins: String,
  blck: u32,
  tx: String,
  vo: u32,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  val: Option<String>,
  num: i32,
  ts: u32,
  addr: String,
}

fn tap_decode_accumulator_entry(bytes: &[u8]) -> Option<TapAccumulatorEntry> {
  tap_decode_record(bytes)
}

fn tap_reader_dmt_holder_shape(mut value: serde_json::Value) -> serde_json::Value {
  if let Some(elem) = value.get("elem").and_then(|v| v.as_str()) {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(elem) {
      value["elem"] = parsed;
    }
  }
  value
}

fn tap_decode_transfer_init_record(bytes: &[u8]) -> Option<TapTransferInitRecord> {
  tap_decode_record(bytes)
}

// Trade records decoders
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTradeOfferRecord {
  addr: String,
  blck: u32,
  tick: String,
  amt: String,
  atick: String,
  aamt: String,
  vld: i64,
  trf: String,
  bal: String,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTradeBuySellerRecord {
  addr: String,
  saddr: String,
  blck: u32,
  tick: String,
  amt: String,
  stick: String,
  samt: String,
  fee: String,
  #[serde(default)]
  fee_rcv: Option<String>,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  sins: String,
  snum: i32,
  ts: u32,
  fail: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTradeBuyBuyerRecord {
  baddr: String,
  addr: String,
  blck: u32,
  btick: String,
  bamt: String,
  tick: String,
  amt: String,
  fee: String,
  #[serde(default)]
  fee_rcv: Option<String>,
  tx: String,
  vo: u32,
  val: String,
  bins: String,
  bnum: i32,
  ins: String,
  num: i32,
  ts: u32,
  fail: bool,
}

fn tap_decode_trade_offer_record(bytes: &[u8]) -> Option<TapTradeOfferRecord> {
  tap_decode_record(bytes)
}
fn tap_decode_trade_buy_seller_record(bytes: &[u8]) -> Option<TapTradeBuySellerRecord> {
  tap_decode_record(bytes)
}
fn tap_decode_trade_buy_buyer_record(bytes: &[u8]) -> Option<TapTradeBuyBuyerRecord> {
  tap_decode_record(bytes)
}

// --- Token-auth records decoders ---
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenAuthCreateRecord {
  addr: String,
  auth: Vec<String>,
  sig: serde_json::Value,
  hash: String,
  slt: String,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenAuthRedeemRecord {
  addr: String,
  iaddr: String,
  rdm: serde_json::Value,
  sig: serde_json::Value,
  hash: String,
  slt: String,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

fn tap_decode_token_auth_create_record(bytes: &[u8]) -> Option<TapTokenAuthCreateRecord> {
  tap_decode_record(bytes)
}
fn tap_decode_token_auth_redeem_record(bytes: &[u8]) -> Option<TapTokenAuthRedeemRecord> {
  tap_decode_record(bytes)
}

// START TAP-PROOFS
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenLockFeeRecord {
  addr: String,
  amt: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenAllocationRecord {
  tt: String,
  to: String,
  amt: String,
  rl: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenLockRecord {
  id: String,
  owner: String,
  auth: String,
  kind: String,
  tick: String,
  amt: String,
  remaining: String,
  claim: String,
  #[serde(default)]
  refund: Option<String>,
  condition: serde_json::Value,
  #[serde(default)]
  refund_after: Option<u32>,
  #[serde(default)]
  data: Option<serde_json::Value>,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  fee: Option<TapTokenLockFeeRecord>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  al: Option<Vec<TapTokenAllocationRecord>>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  total: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenLockConsumeRecord {
  lock: String,
  action: String,
  kind: String,
  owner: String,
  target: String,
  tick: String,
  amt: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  fee: Option<TapTokenLockFeeRecord>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  al: Option<Vec<TapTokenAllocationRecord>>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  total: Option<String>,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapAuthorityConfigRecord {
  id: String,
  k: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  n: Option<String>,
  #[serde(default, skip_serializing_if = "String::is_empty")]
  stk: String,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  rt: Vec<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  st: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pt: Option<String>,
  ctl: serde_json::Value,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  tre: Option<serde_json::Value>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  a: Vec<serde_json::Value>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  ak: Vec<String>,
  #[serde(default, skip_serializing_if = "String::is_empty")]
  sh: String,
  #[serde(default, skip_serializing_if = "String::is_empty")]
  fee: String,
  #[serde(default, skip_serializing_if = "String::is_empty")]
  pf: String,
  #[serde(default, skip_serializing_if = "String::is_empty")]
  min: String,
  #[serde(default, skip_serializing_if = "std::ops::Not::not")]
  p: bool,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pp: Option<serde_json::Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  att: Option<serde_json::Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  xs: Option<serde_json::Value>,
  seq: u32,
  #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
  r: serde_json::Value,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  s: Option<serde_json::Value>,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapStakePositionRecord {
  id: String,
  auth: String,
  addr: String,
  claim: String,
  tick: String,
  amt: String,
  tier: String,
  shares: String,
  uh: u32,
  debt: serde_json::Value,
  status: String,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  closed_blck: Option<u32>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  closed_tx: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapRewardClaimRecord {
  auth: String,
  pos: String,
  rt: String,
  claim: String,
  amt: String,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapTokenDelegationCancelRecord {
  auth: String,
  nonce: String,
  addr: String,
  iaddr: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  rdm: Option<serde_json::Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  sig: Option<serde_json::Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  hash: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  slt: Option<String>,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

fn tap_decode_token_lock_record(bytes: &[u8]) -> Option<TapTokenLockRecord> {
  tap_decode_record(bytes)
}

fn tap_decode_token_lock_consume_record(bytes: &[u8]) -> Option<TapTokenLockConsumeRecord> {
  tap_decode_record(bytes)
}

fn tap_decode_token_delegation_cancel_record(
  bytes: &[u8],
) -> Option<TapTokenDelegationCancelRecord> {
  tap_decode_record(bytes)
}

fn tap_decode_authority_config_record(bytes: &[u8]) -> Option<TapAuthorityConfigRecord> {
  tap_decode_record(bytes)
}

fn tap_authority_config_record_to_value(record: TapAuthorityConfigRecord) -> serde_json::Value {
  let is_staking = record.k == "stk";
  let mut value = serde_json::to_value(record).unwrap_or(serde_json::Value::Null);
  if is_staking {
    if let Some(map) = value.as_object_mut() {
      map
        .entry("rt".to_string())
        .or_insert_with(|| serde_json::json!([]));

      // tap-writer serializes authority config fields in insertion order.
      // Keep REST parity exact for consumers that compare raw JSON shapes.
      let order = [
        "id", "k", "n", "stk", "rt", "st", "pt", "ctl", "tre", "seq", "r", "s", "blck", "tx", "vo",
        "val", "ins", "num", "ts",
      ];
      let mut source = std::mem::take(map);
      for key in order {
        if let Some(value) = source.remove(key) {
          map.insert(key.to_string(), value);
        }
      }
      for (key, value) in source {
        map.insert(key, value);
      }
    }
  }
  value
}

fn tap_decode_stake_position_record(bytes: &[u8]) -> Option<TapStakePositionRecord> {
  tap_decode_record(bytes)
}

fn tap_decode_reward_claim_record(bytes: &[u8]) -> Option<TapRewardClaimRecord> {
  tap_decode_record(bytes)
}

fn tap_collect_token_lock_records(
  index: &Index,
  length_key: &str,
  item_prefix: &str,
  offset: u64,
  max: u64,
) -> Result<Vec<TapTokenLockRecord>> {
  let length = index.tap_get_length(length_key)?;
  let end = std::cmp::min(length, offset.saturating_add(max));
  let mut out = Vec::new();
  for i in offset..end {
    if let Some(bytes) = index.tap_get_raw(&format!("{}/{}", item_prefix, i))? {
      if let Some(rec) = tap_decode_token_lock_record(&bytes) {
        out.push(rec);
      }
    }
  }
  Ok(out)
}

fn tap_collect_token_lock_consume_records(
  index: &Index,
  length_key: &str,
  item_prefix: &str,
  offset: u64,
  max: u64,
) -> Result<Vec<TapTokenLockConsumeRecord>> {
  let length = index.tap_get_length(length_key)?;
  let end = std::cmp::min(length, offset.saturating_add(max));
  let mut out = Vec::new();
  for i in offset..end {
    if let Some(bytes) = index.tap_get_raw(&format!("{}/{}", item_prefix, i))? {
      if let Some(rec) = tap_decode_token_lock_consume_record(&bytes) {
        out.push(rec);
      }
    }
  }
  Ok(out)
}

fn tap_collect_json_records(
  index: &Index,
  length_key: &str,
  item_prefix: &str,
  offset: u64,
  max: u64,
) -> Result<Vec<serde_json::Value>> {
  let length = index.tap_get_length(length_key)?;
  let end = std::cmp::min(length, offset.saturating_add(max));
  let mut out = Vec::new();
  for i in offset..end {
    if let Some(bytes) = index.tap_get_raw(&format!("{}/{}", item_prefix, i))? {
      if let Some(rec) = tap_decode_json_value(&bytes) {
        out.push(rec);
      }
    }
  }
  Ok(out)
}

fn tap_decode_string_value(bytes: &[u8]) -> Option<String> {
  cbor_from_reader::<String, _>(std::io::Cursor::new(bytes))
    .ok()
    .or_else(|| {
      let raw = std::str::from_utf8(bytes).ok()?;
      serde_json::from_str::<String>(&tap_js_preprocess_json_for_serde(raw))
        .ok()
        .or_else(|| Some(raw.to_string()))
    })
}

fn tap_get_json_record(index: &Index, key: &str) -> Result<Option<serde_json::Value>> {
  Ok(
    index
      .tap_get_raw(key)?
      .and_then(|bytes| tap_decode_json_value(&bytes)),
  )
}

fn tap_collect_json_records_or_pointers(
  index: &Index,
  length_key: &str,
  item_prefix: &str,
  offset: u64,
  max: u64,
) -> Result<Vec<serde_json::Value>> {
  let length = index.tap_get_length(length_key)?;
  let end = std::cmp::min(length, offset.saturating_add(max));
  let mut out = Vec::new();
  for i in offset..end {
    if let Some(bytes) = index.tap_get_raw(&format!("{}/{}", item_prefix, i))? {
      if let Some(value) = tap_decode_json_value(&bytes) {
        out.push(value);
        continue;
      }
      if let Some(ptr) = tap_decode_string_value(&bytes) {
        if let Some(value) = tap_get_json_record(index, &ptr)? {
          out.push(value);
        }
      }
    }
  }
  Ok(out)
}

fn tap_amm_max(q: &TapListQuery) -> u64 {
  q.max.unwrap_or(25).min(25)
}

fn tap_obligation_max(q: &TapListQuery) -> u64 {
  q.max.unwrap_or(25).min(25)
}

fn tap_obligation_entity_key(entity_type: &str, entity_id: &str) -> String {
  format!("{}/{}", entity_type, entity_id)
}

fn tap_amm_obligation_entity_key(pool_id: &str, side: u8) -> String {
  format!("amm/{}/{}", pool_id, side)
}
// END TAP-PROOFS

// --- Privilege-auth records decoders ---
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapPrivilegeAuthCreateRecord {
  addr: String,
  auth: serde_json::Value,
  sig: serde_json::Value,
  hash: String,
  slt: String,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

fn tap_decode_privilege_auth_create_record(bytes: &[u8]) -> Option<TapPrivilegeAuthCreateRecord> {
  tap_decode_record(bytes)
}

// Verified privilege events (sfprav/sfpravi)
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapPrivilegeVerifiedRecord {
  ownr: String,
  #[serde(default)]
  prv: Option<String>,
  name: String,
  #[serde(rename = "priv")]
  privf: String,
  col: String,
  vrf: String,
  seq: i64,
  slt: String,
  blck: u32,
  tx: String,
  vo: u32,
  val: String,
  ins: String,
  num: i32,
  ts: u32,
}

fn tap_decode_privilege_verified_record(bytes: &[u8]) -> Option<TapPrivilegeVerifiedRecord> {
  tap_decode_record(bytes)
}

// --- DMT Elements ---
#[derive(Serialize, Deserialize, Clone, Debug)]
struct TapDmtElementRecord {
  tick: String,
  blck: u32,
  tx: String,
  vo: u32,
  ins: String,
  num: i32,
  ts: u32,
  addr: String,
  #[serde(default)]
  pat: Option<String>,
  fld: u32,
}

fn tap_decode_dmt_element_record(bytes: &[u8]) -> Option<TapDmtElementRecord> {
  tap_decode_record(bytes)
}

pub(super) async fn tap_get_dmt_elements_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("dmt-ell")? }),
    ))
  })
}

pub(super) async fn tap_get_dmt_elements_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("dmt-ell")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("dmt-elli/{}", i);
      if let Some(name_bytes) = index.tap_get_raw(&key)? {
        // dmt-elli stores element name as string
        if let Some(name) = tap_decode_string_value(&name_bytes) {
          let elkey = format!(
            "dmt-el/{}",
            serde_json::to_string(&name).unwrap_or_else(|_| format!("\"{}\"", name))
          );
          if let Some(bytes) = index.tap_get_raw(&elkey)? {
            if let Some(rec) = tap_decode_dmt_element_record(&bytes) {
              out.push(rec);
            }
          }
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// --- DMT Events by Block (mint events; records will populate once DMT mint is implemented) ---
pub(super) async fn tap_get_dmt_event_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/dmt-md/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_dmt_event_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/dmt-md/{}", block),
      &format!("blcki/dmt-md/{}", block),
      offset,
      max,
    )?;
    // These pointers reference stored records (JSON/CBOR) — return raw decoded values if available
    let mut out = Vec::<serde_json::Value>::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(val) = tap_decode_json_value(&bytes) {
          out.push(val);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// --- DMT Mint Holders: history + wallet + single holder ---
pub(super) async fn tap_get_dmt_mint_holders_history_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("dmtmhl/{}", inscription))? }),
    ))
  })
}

pub(super) async fn tap_get_dmt_mint_holders_history_list(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("dmtmhl/{}", inscription))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::<serde_json::Value>::new();
    for i in offset..end {
      let key = format!("dmtmhli/{}/{}", inscription, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(val) = tap_decode_json_value(&bytes) {
          out.push(tap_reader_dmt_holder_shape(val));
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_dmt_mint_holder(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("dmtmh/{}", inscription))?
      .and_then(|b| tap_decode_json_value(&b))
      .map(tap_reader_dmt_holder_shape);
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_dmt_mint_holder_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    if let Some(ptr) = index.tap_get_string(&format!("dmtmhb/{}/{}", tkey, block))? {
      if let Some(bytes) = index.tap_get_raw(&ptr)? {
        if let Some(val) = tap_decode_json_value(&bytes) {
          return Ok(Json(
            serde_json::json!({"result": Some(tap_reader_dmt_holder_shape(val))}),
          ));
        }
      }
    }
    Ok(Json(
      serde_json::json!({"result": Option::<serde_json::Value>::None}),
    ))
  })
}

pub(super) async fn tap_get_dmt_mint_wallet_historic_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("dmtmwl/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_dmt_mint_wallet_historic_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let list = index.tap_list_strings(
      &format!("dmtmwl/{}", address),
      &format!("dmtmwli/{}", address),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": list})))
  })
}

// --- Account tokens: list/length/balances/details ---
pub(super) async fn tap_get_account_tokens_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("atl/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_account_tokens(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let records = index.tap_list_strings(
      &format!("atl/{}", &address),
      &format!("atli/{}", &address),
      offset,
      max,
    )?;
    let out: Vec<String> = records.into_iter().map(|s| s.to_lowercase()).collect();
    Ok(Json(serde_json::json!({"result": out})))
  })
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AccountTokenBalanceItem {
  ticker: String,
  overallBalance: Option<String>,
  transferableBalance: String,
}

pub(super) async fn tap_get_account_tokens_balance(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let total = index.tap_get_length(&format!("atl/{}", &address))?;
    let tokens = index.tap_list_strings(
      &format!("atl/{}", &address),
      &format!("atli/{}", &address),
      offset,
      max,
    )?;
    let mut list: Vec<AccountTokenBalanceItem> = Vec::new();
    for t in tokens {
      let tkey = json_stringify_lower(&t);
      let overall = index.tap_get_string(&format!("b/{}/{}", &address, &tkey))?;
      let tr = index.tap_get_string(&format!("t/{}/{}", &address, &tkey))?;
      list.push(AccountTokenBalanceItem {
        ticker: t.to_lowercase(),
        overallBalance: overall,
        transferableBalance: tr.unwrap_or_default(),
      });
    }
    Ok(Json(
      serde_json::json!({"data": {"total": total, "list": list} }),
    ))
  })
}

pub(super) async fn tap_get_account_token_detail(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    // tokenInfo
    let token_info = index
      .tap_get_raw(&format!("d/{}", &tkey))?
      .and_then(|b| tap_decode_deploy_record(&b));
    if token_info.is_none() {
      return Ok(Json(serde_json::json!({"data": serde_json::Value::Null})));
    }
    // balances
    let overall = index.tap_get_string(&format!("b/{}/{}", &address, &tkey))?;
    let tr = index
      .tap_get_string(&format!("t/{}/{}", &address, &tkey))?
      .unwrap_or_default();
    // transfers for this account/ticker
    let len = index.tap_get_length(&format!("atrl/{}/{}", &address, &tkey))?;
    let mut transfer_list = Vec::<serde_json::Value>::new();
    for i in 0..len {
      if let Some(bytes) = index.tap_get_raw(&format!("atrli/{}/{}/{}", &address, &tkey, i))? {
        if let Some(rec) = tap_decode_transfer_init_record(&bytes) {
          transfer_list.push(serde_json::to_value(rec).unwrap_or(serde_json::json!({})));
        }
      }
    }
    Ok(Json(serde_json::json!({
      "data": {
        "tokenInfo": token_info,
        "tokenBalance": { "ticker": ticker.to_lowercase(), "overallBalance": overall, "transferableBalance": tr },
        "transferList": transfer_list,
      }
    })))
  })
}

// --- Generic helpers ---
#[derive(Deserialize)]
pub(super) struct TapGenericListQuery {
  length_key: String,
  iterator_key: String,
  offset: Option<u64>,
  max: Option<u64>,
  return_json: Option<bool>,
}

pub(super) async fn tap_get_list_records(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapGenericListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500);
    if max > 500 {
      return Ok(Json(serde_json::json!({"result": "request too large"})));
    }
    // length
    let length = index.tap_get_length(&q.length_key)?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::<serde_json::Value>::new();
    for i in offset..end {
      let key = format!("{}/{}", q.iterator_key, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if q.return_json.unwrap_or(true) {
          if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            out.push(val);
          }
        } else if let Some(s) = tap_decode_string_value(&bytes) {
          out.push(serde_json::json!(s));
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_length_generic(
  Extension(index): Extension<Arc<Index>>,
  Path(length_key): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&length_key)? }),
    ))
  })
}

// Node-like: current block height
pub(super) async fn tap_get_current_block(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let h = index.block_height()?.unwrap_or(Height(0)).n();
    Ok(Json(serde_json::json!({"result": h})))
  })
}

// Report which backend is used for DMT regex validation (RE2 vs stub)
pub(super) async fn tap_get_regex_backend() -> ServerResult<Json<serde_json::Value>> {
  Ok(Json(
    serde_json::json!({ "result": tap_re2::backend_name() }),
  ))
}

#[derive(Deserialize)]
pub(super) struct TapReorgsQuery {
  #[serde(default)]
  pub(super) limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(super) struct TapReorgRecord {
  pub block: u32,
  pub blockhash: String,
}

// Returns recent reorg records stored off-DB in settings.data_dir()/tap-reorgs.jsonl
pub(super) async fn tap_get_reorgs(
  Extension(settings): Extension<Arc<Settings>>,
  Query(query): Query<TapReorgsQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let mut result: Vec<TapReorgRecord> = Vec::new();
    let path = settings.data_dir().join("tap-reorgs.jsonl");
    if let Ok(file) = std::fs::File::open(&path) {
      let reader = BufReader::new(file);
      for line in reader.lines() {
        if let Ok(line) = line {
          if line.trim().is_empty() {
            continue;
          }
          if let Ok(rec) = serde_json::from_str::<TapReorgRecord>(&line) {
            result.push(rec);
          }
        }
      }
    }
    // Keep most recently recorded reorgs first by using append order:
    // take the last `limit` records (default 100), then reverse so newest is on top.
    let limit = query.limit.unwrap_or(100);
    if result.len() > limit {
      result = result.split_off(result.len() - limit);
    }
    result.reverse();
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_bitmap(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("bm/{}", block);
    let result = index
      .tap_get_raw(&key)?
      .and_then(|b| tap_decode_bitmap_record(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_bitmap_by_inscription(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    if let Some(ptr) = index.tap_get_string(&format!("bmh/{}", inscription))? {
      let result = index
        .tap_get_raw(&ptr)?
        .and_then(|b| tap_decode_bitmap_record(&b));
      return Ok(Json(serde_json::json!({"result": result})));
    }
    Ok(Json(serde_json::json!({"result": null})))
  })
}

#[derive(Deserialize)]
pub(super) struct TapListQuery {
  #[serde(default)]
  offset: Option<u64>,
  #[serde(default)]
  max: Option<u64>,
}

pub(super) async fn tap_get_bitmap_event_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("blck/bm/{}", block))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_bitmap_event_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/bm/{}", block),
      &format!("blcki/bm/{}", block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(b) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_bitmap_record(&b) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_bitmap_wallet_historic_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("bml/{}", address))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_bitmap_wallet_historic_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let list = index.tap_list_strings(
      &format!("bml/{}", address),
      &format!("bmli/{}", address),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": list})))
  })
}

fn json_stringify_lower(s: &str) -> String {
  tap_js_json_stringify_str(&tap_js_to_lowercase(s))
}

fn tap_fetch_deployment_json_texts_by_pointers(
  index: &Index,
  ptrs: Vec<String>,
) -> ServerResult<Vec<String>> {
  let mut out = Vec::new();
  for ptr in ptrs {
    if let Some(ticker) = index.tap_get_string(&ptr)? {
      let key = format!("d/{}", json_stringify_lower(&ticker));
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(json) = tap_record_json_text::<TapDeployRecord>(&bytes) {
          out.push(json);
        }
      }
    }
  }
  Ok(out)
}

fn tap_fetch_record_json_texts_by_pointers<T>(
  index: &Index,
  ptrs: Vec<String>,
) -> ServerResult<Vec<String>>
where
  T: serde::de::DeserializeOwned + serde::Serialize,
{
  let mut out = Vec::new();
  for ptr in ptrs {
    if let Some(bytes) = index.tap_get_raw(&ptr)? {
      if let Some(json) = tap_record_json_text::<T>(&bytes) {
        out.push(json);
      }
    }
  }
  Ok(out)
}

fn tap_collect_record_json_texts<T>(
  index: &Index,
  length_key: &str,
  item_prefix: &str,
  offset: u64,
  max: u64,
) -> ServerResult<Vec<String>>
where
  T: serde::de::DeserializeOwned + serde::Serialize,
{
  let length = index.tap_get_length(length_key)?;
  let end = std::cmp::min(length, offset.saturating_add(max));
  let mut out = Vec::new();
  for i in offset..end {
    let mut item = "null".to_string();
    if let Some(bytes) = index.tap_get_raw(&format!("{}/{}", item_prefix, i))? {
      if let Some(json) = tap_record_json_text::<T>(&bytes) {
        item = json;
      }
    }
    out.push(item);
  }
  Ok(out)
}

pub(super) async fn tap_get_deployments_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length("dl")?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

// TapListQuery is already defined above; reuse it

pub(super) async fn tap_get_deployments(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    // list of tickers
    let ticks = index.tap_list_strings("dl", "dli", offset, max)?;
    let mut out = Vec::new();
    for t in ticks {
      let mut item = "null".to_string();
      let key = format!("d/{}", json_stringify_lower(&t));
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(json) = tap_record_json_text::<TapDeployRecord>(&bytes) {
          item = json;
        }
      }
      out.push(item);
    }
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_deployment(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("d/{}", json_stringify_lower(&ticker));
    let result = index
      .tap_get_raw(&key)?
      .and_then(|b| tap_decode_deploy_record(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_mint_tokens_left(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("dc/{}", json_stringify_lower(&ticker));
    let result = index.tap_get_string(&key)?;
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_deployed_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("tx/dpl/{}", tx))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_deployed_list(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/dpl/{}", tx),
      &format!("txi/dpl/{}", tx),
      offset,
      max,
    )?;
    let out = tap_fetch_deployment_json_texts_by_pointers(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_deployed_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let len = index.tap_get_length(&format!("txt/dpl/{}/{}", tkey, tx))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_ticker_deployed_list(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("txt/dpl/{}/{}", tkey, tx),
      &format!("txti/dpl/{}/{}", tkey, tx),
      offset,
      max,
    )?;
    let out = tap_fetch_deployment_json_texts_by_pointers(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_deployed_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("blck/dpl/{}", block))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_deployed_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/dpl/{}", block),
      &format!("blcki/dpl/{}", block),
      offset,
      max,
    )?;
    let out = tap_fetch_deployment_json_texts_by_pointers(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_deployed_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let len = index.tap_get_length(&format!("blckt/dpl/{}/{}", tkey, block))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_ticker_deployed_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("blckt/dpl/{}/{}", tkey, block),
      &format!("blckti/dpl/{}/{}", tkey, block),
      offset,
      max,
    )?;
    let out = tap_fetch_deployment_json_texts_by_pointers(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

// --- Mint endpoints ---

pub(super) async fn tap_get_account_mint_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let len = index.tap_get_length(&format!("aml/{}/{}", address, tkey))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_account_mint_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapMintRecord>(
      &index,
      &format!("aml/{}/{}", address, tkey),
      &format!("amli/{}/{}", address, tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_mint_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let len = index.tap_get_length(&format!("fml/{}", tkey))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_ticker_mint_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapMintFlatRecord>(
      &index,
      &format!("fml/{}", tkey),
      &format!("fmli/{}", tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_mint_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length("sfml")?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_mint_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let out = tap_collect_record_json_texts::<TapMintSuperflatRecord>(
      &index, "sfml", "sfmli", offset, max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_minted_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("tx/mnt/{}", tx))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_minted_list(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/mnt/{}", tx),
      &format!("txi/mnt/{}", tx),
      offset,
      max,
    )?;
    let out = tap_fetch_record_json_texts_by_pointers::<TapMintSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_minted_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let len = index.tap_get_length(&format!("txt/mnt/{}/{}", tkey, tx))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_ticker_minted_list(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("txt/mnt/{}/{}", tkey, tx),
      &format!("txti/mnt/{}/{}", tkey, tx),
      offset,
      max,
    )?;
    let out = tap_fetch_record_json_texts_by_pointers::<TapMintSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_minted_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("blck/mnt/{}", block))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_minted_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/mnt/{}", block),
      &format!("blcki/mnt/{}", block),
      offset,
      max,
    )?;
    let out = tap_fetch_record_json_texts_by_pointers::<TapMintSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_minted_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let len = index.tap_get_length(&format!("blckt/mnt/{}/{}", tkey, block))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_ticker_minted_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("blckt/mnt/{}/{}", tkey, block),
      &format!("blckti/mnt/{}/{}", tkey, block),
      offset,
      max,
    )?;
    let out = tap_fetch_record_json_texts_by_pointers::<TapMintSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_balance(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("b/{}/{}", address, json_stringify_lower(&ticker));
    let result = index.tap_get_string(&key)?;
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_transferable(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("t/{}/{}", address, json_stringify_lower(&ticker));
    let result = index.tap_get_string(&key)?;
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_single_transferable(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("tamt/{}", inscription);
    let result = index.tap_get_string(&key)?;
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_holders_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("h/{}", json_stringify_lower(&ticker)))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_historic_holders_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  tap_get_holders_length(Extension(index), Path(ticker)).await
}

pub(super) async fn tap_get_holders(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(100).min(500);
    let tkey = json_stringify_lower(&ticker);
    let addrs =
      index.tap_list_strings(&format!("h/{}", tkey), &format!("hi/{}", tkey), offset, max)?;
    let mut out = Vec::new();
    for a in addrs {
      let bal = index.tap_get_string(&format!("b/{}/{}", a, tkey))?;
      let tr = index.tap_get_string(&format!("t/{}/{}", a, tkey))?;
      out.push(serde_json::json!({"address": a, "balance": bal, "transferable": tr}));
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_historic_holders(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  tap_get_holders(Extension(index), Path(ticker), Query(q)).await
}

// --- Inscribe transfer (initial transfer inscriptions) ---

pub(super) async fn tap_get_inscribe_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/trf/{}", tx))? }),
    ))
  })
}

pub(super) async fn tap_get_inscribe_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/trf/{}", tx),
      &format!("txi/trf/{}", tx),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferInitSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("txt/trf/{}/{}", tkey, tx))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("txt/trf/{}/{}", tkey, tx),
      &format!("txti/trf/{}/{}", tkey, tx),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferInitSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_inscribe_transfer_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/trf/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_inscribe_transfer_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/trf/{}", block),
      &format!("blcki/trf/{}", block),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferInitSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blckt/trf/{}/{}", tkey, block))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("blckt/trf/{}/{}", tkey, block),
      &format!("blckti/trf/{}/{}", tkey, block),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferInitSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

// --- Initial transfer account/ticker/global lists ---

pub(super) async fn tap_get_account_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("atrl/{}/{}", address, tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_account_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapTransferInitRecord>(
      &index,
      &format!("atrl/{}/{}", address, tkey),
      &format!("atrli/{}/{}", address, tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("ftrl/{}", tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapTransferInitFlatRecord>(
      &index,
      &format!("ftrl/{}", tkey),
      &format!("ftrli/{}", tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sftrl")? }),
    ))
  })
}

pub(super) async fn tap_get_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let out = tap_collect_record_json_texts::<TapTransferInitSuperflatRecord>(
      &index, "sftrl", "sftrli", offset, max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

// --- Executed transfers (send) ---

pub(super) async fn tap_get_transferred_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/snd/{}", tx))? }),
    ))
  })
}

pub(super) async fn tap_get_transferred_list(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/snd/{}", tx),
      &format!("txi/snd/{}", tx),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferSendSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_transferred_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("txt/snd/{}/{}", tkey, tx))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_transferred_list(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("txt/snd/{}/{}", tkey, tx),
      &format!("txti/snd/{}/{}", tkey, tx),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferSendSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_transferred_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/snd/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_transferred_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/snd/{}", block),
      &format!("blcki/snd/{}", block),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferSendSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_transferred_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blckt/snd/{}/{}", tkey, block))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_transferred_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(
      &format!("blckt/snd/{}/{}", tkey, block),
      &format!("blckti/snd/{}/{}", tkey, block),
      offset,
      max,
    )?;
    let out =
      tap_fetch_record_json_texts_by_pointers::<TapTransferSendSuperflatRecord>(&index, ptrs)?;
    Ok(tap_result_array_response(out))
  })
}

// Sent/Receive lists and global sent

pub(super) async fn tap_get_account_sent_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("strl/{}/{}", address, tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_account_sent_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapTransferSendSenderRecord>(
      &index,
      &format!("strl/{}/{}", address, tkey),
      &format!("strli/{}/{}", address, tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_ticker_sent_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("fstrl/{}", tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_sent_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapTransferSendFlatRecord>(
      &index,
      &format!("fstrl/{}", tkey),
      &format!("fstrli/{}", tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

pub(super) async fn tap_get_sent_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sfstrl")? }),
    ))
  })
}

pub(super) async fn tap_get_sent_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let out = tap_collect_record_json_texts::<TapTransferSendSuperflatRecord>(
      &index, "sfstrl", "sfstrli", offset, max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

// --- Accumulator endpoints ---

pub(super) async fn tap_get_accumulator(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let key = format!("a/{}", inscription);
    let result = index
      .tap_get_raw(&key)?
      .and_then(|b| tap_decode_accumulator_entry(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_account_accumulator_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length(&format!("al/{}", address))?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_account_accumulator_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("al/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("ali/{}/{}", address, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_accumulator_entry(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_accumulator_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let len = index.tap_get_length("al")?;
    Ok(Json(serde_json::json!({"result": len})))
  })
}

pub(super) async fn tap_get_accumulator_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("al")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("ali/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_accumulator_entry(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_receive_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("rstrl/{}/{}", address, tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_account_receive_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_record_json_texts::<TapTransferSendReceiverRecord>(
      &index,
      &format!("rstrl/{}/{}", address, tkey),
      &format!("rstrli/{}/{}", address, tkey),
      offset,
      max,
    )?;
    Ok(tap_result_array_response(out))
  })
}

// --- Trade endpoints ---

pub(super) async fn tap_get_trade(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    // Returns tol/<inscription> content if present
    let result = index
      .tap_get_raw(&format!("tol/{}", inscription))?
      .and_then(|b| tap_decode_accumulator_entry(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_account_trades_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("atrof/{}/{}", address, tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_account_trades_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("atrof/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("atrofi/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_offer_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_trades_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("fatrof/{}", tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_trades_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("fatrof/{}", tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("fatrofi/{}/{}", tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_offer_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_trades_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sfatrof")? }),
    ))
  })
}

pub(super) async fn tap_get_trades_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sfatrof")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sfatrofi/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_offer_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Filled trades
pub(super) async fn tap_get_account_receive_trades_filled_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("rbtrof/{}/{}", address, tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_account_receive_trades_filled_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("rbtrof/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("rbtrofi/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_buy_buyer_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_trades_filled_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("btrof/{}/{}", address, tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_account_trades_filled_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("btrof/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("btrofi/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_buy_seller_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_trades_filled_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("fbtrof/{}", tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_trades_filled_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("fbtrof/{}", tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("fbtrofi/{}/{}", tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_buy_seller_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_trades_filled_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sfbtrof")? }),
    ))
  })
}

pub(super) async fn tap_get_trades_filled_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sfbtrof")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sfbtrofi/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_trade_buy_seller_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// --- Token-auth endpoints ---

// Check if a token-auth inscription has been cancelled
pub(super) async fn tap_get_auth_cancelled(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let exists = index
      .tap_get_raw(&format!("tac/{}", inscription))?
      .is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

// Check if a token-auth compact signature hex exists (replay-protection key)
pub(super) async fn tap_get_auth_hash_exists(
  Extension(index): Extension<Arc<Index>>,
  Path(hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let h = hash.trim().to_lowercase();
    let exists = index.tap_get_raw(&format!("tah/{}", h))?.is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

// Alias for getAuthHashExists (parity with tap-reader)
pub(super) async fn tap_get_auth_compact_hex_exists(
  Extension(index): Extension<Arc<Index>>,
  Path(hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let h = hash.trim().to_lowercase();
    let exists = index.tap_get_raw(&format!("tah/{}", h))?.is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

// Global token-auth list length
pub(super) async fn tap_get_auth_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sfta")? }),
    ))
  })
}

// Global token-auth list
pub(super) async fn tap_get_auth_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sfta")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sftai/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_auth_create_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Account-scoped token-auth list length
pub(super) async fn tap_get_account_auth_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("ta/{}", address))? }),
    ))
  })
}

// Account-scoped token-auth list
pub(super) async fn tap_get_account_auth_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("ta/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("tai/{}/{}", address, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_auth_create_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Redeem: global list length
pub(super) async fn tap_get_redeem_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sftr")? }),
    ))
  })
}

// Redeem: global list
pub(super) async fn tap_get_redeem_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sftr")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sftri/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_auth_redeem_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Redeem: account-scoped list length
pub(super) async fn tap_get_account_redeem_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tr/{}", address))? }),
    ))
  })
}

// Redeem: account-scoped list
pub(super) async fn tap_get_account_redeem_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("tr/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("tri/{}/{}", address, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_auth_redeem_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// START TAP-PROOFS
pub(super) async fn tap_get_lock(
  Extension(index): Extension<Arc<Index>>,
  Path(lock_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("l/{}", lock_id))?
      .and_then(|b| tap_decode_token_lock_record(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_locked_balance(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let result = index
      .tap_get_string(&format!("ll/{}/{}", address, tkey))?
      .unwrap_or_else(|| "0".to_string());
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_lock_consume(
  Extension(index): Extension<Arc<Index>>,
  Path(lock_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("lc/{}", lock_id))?
      .and_then(|b| tap_decode_token_lock_consume_record(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_lock_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sl")? }),
    ))
  })
}

pub(super) async fn tap_get_lock_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sl")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sli/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_lock_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_lock_consume_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("slc")? }),
    ))
  })
}

pub(super) async fn tap_get_lock_consume_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("slc")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("slci/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_lock_consume_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_locks_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path(kind): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lk/{}", kind.to_lowercase()))? }),
    ))
  })
}

pub(super) async fn tap_get_locks_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path(kind): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let kind = kind.to_lowercase();
    let out = tap_collect_token_lock_records(
      &index,
      &format!("lk/{}", kind),
      &format!("lki/{}", kind),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_locks_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, kind)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lak/{}/{}", address, kind.to_lowercase()))? }),
    ))
  })
}

pub(super) async fn tap_get_account_locks_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path((address, kind)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let kind = kind.to_lowercase();
    let out = tap_collect_token_lock_records(
      &index,
      &format!("lak/{}/{}", address, kind),
      &format!("laki/{}/{}", address, kind),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_locks_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, kind)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("ltk/{}/{}", tkey, kind.to_lowercase()))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_locks_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, kind)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let kind = kind.to_lowercase();
    let out = tap_collect_token_lock_records(
      &index,
      &format!("ltk/{}/{}", tkey, kind),
      &format!("ltki/{}/{}", tkey, kind),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_lock_consumes_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lca/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_account_lock_consumes(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let out = tap_collect_token_lock_consume_records(
      &index,
      &format!("lca/{}", address),
      &format!("lcai/{}", address),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_lock_consumes_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lct/{}", tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_lock_consumes(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let out = tap_collect_token_lock_consume_records(
      &index,
      &format!("lct/{}", tkey),
      &format!("lcti/{}", tkey),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_lock_consumes_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path(kind): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lck/{}", kind.to_lowercase()))? }),
    ))
  })
}

pub(super) async fn tap_get_lock_consumes_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path(kind): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let kind = kind.to_lowercase();
    let out = tap_collect_token_lock_consume_records(
      &index,
      &format!("lck/{}", kind),
      &format!("lcki/{}", kind),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_lock_consumes_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, kind)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lcak/{}/{}", address, kind.to_lowercase()))? }),
    ))
  })
}

pub(super) async fn tap_get_account_lock_consumes_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path((address, kind)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let kind = kind.to_lowercase();
    let out = tap_collect_token_lock_consume_records(
      &index,
      &format!("lcak/{}/{}", address, kind),
      &format!("lcaki/{}/{}", address, kind),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_lock_consumes_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, kind)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lctk/{}/{}", tkey, kind.to_lowercase()))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_lock_consumes_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, kind)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let kind = kind.to_lowercase();
    let out = tap_collect_token_lock_consume_records(
      &index,
      &format!("lctk/{}/{}", tkey, kind),
      &format!("lctki/{}/{}", tkey, kind),
      offset,
      max,
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_delegation_cancel(
  Extension(index): Extension<Arc<Index>>,
  Path((auth, nonce)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let rec = index
      .tap_get_raw(&format!("tdcr/{}/{}", auth, nonce))?
      .and_then(|b| tap_decode_token_delegation_cancel_record(&b));
    Ok(Json(serde_json::json!({"result": rec})))
  })
}

pub(super) async fn tap_get_delegation_cancel_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sftdc")? }),
    ))
  })
}

pub(super) async fn tap_get_delegation_cancel_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sftdc")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sftdci/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_delegation_cancel_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_locks_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("la/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_account_locks(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("la/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("lai/{}/{}", address, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_lock_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_delegation_cancel_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tdca/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_account_delegation_cancel_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("tdca/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("tdcai/{}/{}", address, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_delegation_cancel_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_auth_delegation_cancel_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(auth): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tdcath/{}", auth))? }),
    ))
  })
}

pub(super) async fn tap_get_auth_delegation_cancel_list(
  Extension(index): Extension<Arc<Index>>,
  Path(auth): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("tdcath/{}", auth))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("tdcathi/{}/{}", auth, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_delegation_cancel_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_locks_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("lt/{}", tkey))? }),
    ))
  })
}

pub(super) async fn tap_get_ticker_locks(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("lt/{}", tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("lti/{}/{}", tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_token_lock_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_lock_events_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/lck/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_lock_events_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/lck/{}", block),
      &format!("blcki/lck/{}", block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_token_lock_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_lock_consume_events_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/lckc/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_lock_consume_events_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/lckc/{}", block),
      &format!("blcki/lckc/{}", block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_token_lock_consume_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_delegation_cancel_events_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/tdc/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_delegation_cancel_events_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/tdc/{}", block),
      &format!("blcki/tdc/{}", block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_token_delegation_cancel_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_lock_events_by_transaction_length(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/lck/{}", transaction_hash))? }),
    ))
  })
}

pub(super) async fn tap_get_lock_events_by_transaction(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/lck/{}", transaction_hash),
      &format!("txi/lck/{}", transaction_hash),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_token_lock_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_lock_consume_events_by_transaction_length(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/lckc/{}", transaction_hash))? }),
    ))
  })
}

pub(super) async fn tap_get_lock_consume_events_by_transaction(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/lckc/{}", transaction_hash),
      &format!("txi/lckc/{}", transaction_hash),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_token_lock_consume_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_delegation_cancel_events_by_transaction_length(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/tdc/{}", transaction_hash))? }),
    ))
  })
}

pub(super) async fn tap_get_delegation_cancel_events_by_transaction(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("tx/tdc/{}", transaction_hash),
      &format!("txi/tdc/{}", transaction_hash),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_token_delegation_cancel_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_authority_by_id(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("ah/{}", authority_id))?
      .and_then(|b| tap_decode_authority_config_record(&b))
      .map(tap_authority_config_record_to_value);
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_authority_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("ahl")? }),
    ))
  })
}

pub(super) async fn tap_get_authority_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("ahl")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("ahli/{}", i))? {
        if let Some(rec) = tap_decode_authority_config_record(&bytes) {
          out.push(tap_authority_config_record_to_value(rec));
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_authorities_by_kind_length(
  Extension(index): Extension<Arc<Index>>,
  Path(kind): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("ahk/{}", kind))? }),
    ))
  })
}

pub(super) async fn tap_get_authorities_by_kind(
  Extension(index): Extension<Arc<Index>>,
  Path(kind): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("ahk/{}", kind))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("ahki/{}/{}", kind, i))? {
        if let Some(rec) = tap_decode_authority_config_record(&bytes) {
          out.push(tap_authority_config_record_to_value(rec));
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_authority_balance_by_tick(
  Extension(index): Extension<Arc<Index>>,
  Path((authority_id, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    let result = index
      .tap_get_string(&format!("ab/{}/{}", authority_id, tkey))?
      .unwrap_or_else(|| "0".to_string());
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_authority_balances_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("abl/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_authority_balances(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ticks = index.tap_list_strings(
      &format!("abl/{}", authority_id),
      &format!("abli/{}", authority_id),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for tick in ticks {
      let balance = index
        .tap_get_string(&format!(
          "ab/{}/{}",
          authority_id,
          json_stringify_lower(&tick)
        ))?
        .unwrap_or_else(|| "0".to_string());
      out.push(serde_json::json!({"tick": tick, "bal": balance}));
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_pool(
  Extension(index): Extension<Arc<Index>>,
  Path(pool_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": tap_get_json_record(&index, &format!("amm/{}", pool_id))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_pool_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("amml")? }),
    ))
  })
}

pub(super) async fn tap_get_amm_pool_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      "amml",
      "ammli",
      q.offset.unwrap_or(0),
      tap_amm_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_pools_by_asset_length(
  Extension(index): Extension<Arc<Index>>,
  Path(asset_key): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("ammat/{}", asset_key))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_pools_by_asset(
  Extension(index): Extension<Arc<Index>>,
  Path(asset_key): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("ammat/{}", asset_key),
      &format!("ammati/{}", asset_key),
      q.offset.unwrap_or(0),
      tap_amm_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_position(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, target_type, target)): Path<(String, String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({
      "result": tap_get_json_record(
        &index,
        &format!("ammpr/{}/{}/{}", pool_id, target_type, target)
      )?
    })))
  })
}

pub(super) async fn tap_get_amm_positions_by_target_length(
  Extension(index): Extension<Arc<Index>>,
  Path((target_type, target)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("amma/{}/{}", target_type, target))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_positions_by_target(
  Extension(index): Extension<Arc<Index>>,
  Path((target_type, target)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("amma/{}/{}", target_type, target),
      &format!("ammai/{}/{}", target_type, target),
      q.offset.unwrap_or(0),
      tap_amm_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_events_by_pool_length(
  Extension(index): Extension<Arc<Index>>,
  Path(pool_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("amme/{}", pool_id))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_events_by_pool(
  Extension(index): Extension<Arc<Index>>,
  Path(pool_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("amme/{}", pool_id),
      &format!("ammei/{}", pool_id),
      q.offset.unwrap_or(0),
      tap_amm_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_events_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("ammbe/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_events_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("ammbe/{}", block),
      &format!("ammbei/{}", block),
      q.offset.unwrap_or(0),
      tap_amm_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_events_by_transaction_length(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/amm/{}", transaction_hash))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_events_by_transaction(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("tx/amm/{}", transaction_hash),
      &format!("txi/amm/{}", transaction_hash),
      q.offset.unwrap_or(0),
      tap_amm_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_external_snapshot(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, snapshot_id)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({
      "result": tap_get_json_record(&index, &format!("amms/{}/{}", pool_id, snapshot_id))?
    })))
  })
}

pub(super) async fn tap_get_obligation(
  Extension(index): Extension<Arc<Index>>,
  Path(obligation_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": tap_get_json_record(&index, &format!("ob/{}", obligation_id))? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_consume(
  Extension(index): Extension<Arc<Index>>,
  Path(obligation_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": tap_get_json_record(&index, &format!("obc/{}", obligation_id))? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_locked_balance(
  Extension(index): Extension<Arc<Index>>,
  Path((source_type, source_id, ticker)): Path<(String, String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let source_key = tap_obligation_entity_key(&source_type, &source_id);
    let tick_key = json_stringify_lower(&ticker);
    let result = index
      .tap_get_string(&format!("oll/{}/{}", source_key, tick_key))?
      .unwrap_or_else(|| "0".to_string());
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_amm_obligation_locked_balance(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, side, ticker)): Path<(String, u8, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let source_key = tap_amm_obligation_entity_key(&pool_id, side);
    let tick_key = json_stringify_lower(&ticker);
    let result = index
      .tap_get_string(&format!("oll/{}/{}", source_key, tick_key))?
      .unwrap_or_else(|| "0".to_string());
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_obligation_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("obl")? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "obl",
      "obli",
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligation_consume_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("obcl")? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_consume_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "obcl",
      "obcli",
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligations_by_source_length(
  Extension(index): Extension<Arc<Index>>,
  Path((source_type, source_id)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let source_key = tap_obligation_entity_key(&source_type, &source_id);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("obsrc/{}", source_key))? }),
    ))
  })
}

pub(super) async fn tap_get_obligations_by_source(
  Extension(index): Extension<Arc<Index>>,
  Path((source_type, source_id)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let source_key = tap_obligation_entity_key(&source_type, &source_id);
    let out = tap_collect_json_records(
      &index,
      &format!("obsrc/{}", source_key),
      &format!("obsrci/{}", source_key),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_obligations_by_source_length(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, side)): Path<(String, u8)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let source_key = tap_amm_obligation_entity_key(&pool_id, side);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("obsrc/{}", source_key))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_obligations_by_source(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, side)): Path<(String, u8)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let source_key = tap_amm_obligation_entity_key(&pool_id, side);
    let out = tap_collect_json_records(
      &index,
      &format!("obsrc/{}", source_key),
      &format!("obsrci/{}", source_key),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligations_by_target_length(
  Extension(index): Extension<Arc<Index>>,
  Path((target_type, target_id)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let target_key = tap_obligation_entity_key(&target_type, &target_id);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("oba/{}", target_key))? }),
    ))
  })
}

pub(super) async fn tap_get_obligations_by_target(
  Extension(index): Extension<Arc<Index>>,
  Path((target_type, target_id)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let target_key = tap_obligation_entity_key(&target_type, &target_id);
    let out = tap_collect_json_records(
      &index,
      &format!("oba/{}", target_key),
      &format!("obai/{}", target_key),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_amm_obligations_by_target_length(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, side)): Path<(String, u8)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let target_key = tap_amm_obligation_entity_key(&pool_id, side);
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("oba/{}", target_key))? }),
    ))
  })
}

pub(super) async fn tap_get_amm_obligations_by_target(
  Extension(index): Extension<Arc<Index>>,
  Path((pool_id, side)): Path<(String, u8)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let target_key = tap_amm_obligation_entity_key(&pool_id, side);
    let out = tap_collect_json_records(
      &index,
      &format!("oba/{}", target_key),
      &format!("obai/{}", target_key),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligations_by_context_length(
  Extension(index): Extension<Arc<Index>>,
  Path(context_key): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("obctx/{}", context_key))? }),
    ))
  })
}

pub(super) async fn tap_get_obligations_by_context(
  Extension(index): Extension<Arc<Index>>,
  Path(context_key): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("obctx/{}", context_key),
      &format!("obctxi/{}", context_key),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligation_events_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/ob/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_events_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("blck/ob/{}", block),
      &format!("blcki/ob/{}", block),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligation_consume_events_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/obc/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_consume_events_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("blck/obc/{}", block),
      &format!("blcki/obc/{}", block),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligation_events_by_transaction_length(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/ob/{}", transaction_hash))? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_events_by_transaction(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("tx/ob/{}", transaction_hash),
      &format!("txi/ob/{}", transaction_hash),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_obligation_consume_events_by_transaction_length(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("tx/obc/{}", transaction_hash))? }),
    ))
  })
}

pub(super) async fn tap_get_obligation_consume_events_by_transaction(
  Extension(index): Extension<Arc<Index>>,
  Path(transaction_hash): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records_or_pointers(
      &index,
      &format!("tx/obc/{}", transaction_hash),
      &format!("txi/obc/{}", transaction_hash),
      q.offset.unwrap_or(0),
      tap_obligation_max(&q),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_stake_position_by_id(
  Extension(index): Extension<Arc<Index>>,
  Path(position_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("sp/{}", position_id))?
      .and_then(|b| tap_decode_stake_position_record(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_stake_positions_by_address_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("spa/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_stake_positions_by_address(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("spa/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("spai/{}/{}", address, i))? {
        if let Some(rec) = tap_decode_stake_position_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_stake_positions_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("sph/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_stake_positions_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("sph/{}", authority_id))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("sphi/{}/{}", authority_id, i))? {
        if let Some(rec) = tap_decode_stake_position_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_reward_claim_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("rcl")? }),
    ))
  })
}

pub(super) async fn tap_get_reward_claim_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("rcl")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("rcli/{}", i))? {
        if let Some(rec) = tap_decode_reward_claim_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_reward_claims_by_address_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("rca/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_reward_claims_by_address(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("rca/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("rcai/{}/{}", address, i))? {
        if let Some(rec) = tap_decode_reward_claim_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_reward_claims_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("rch/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_reward_claims_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("rch/{}", authority_id))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      if let Some(bytes) = index.tap_get_raw(&format!("rchi/{}/{}", authority_id, i))? {
        if let Some(rec) = tap_decode_reward_claim_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_pending_rewards_by_position(
  Extension(index): Extension<Arc<Index>>,
  Path(position_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let Some(position) = index
      .tap_get_raw(&format!("sp/{}", position_id))?
      .and_then(|b| tap_decode_stake_position_record(&b))
    else {
      return Ok(Json(serde_json::json!({"result": []})));
    };
    if position.status != "open" {
      return Ok(Json(serde_json::json!({"result": []})));
    }
    let Some(authority) = index
      .tap_get_raw(&format!("ah/{}", position.auth))?
      .and_then(|b| tap_decode_authority_config_record(&b))
    else {
      return Ok(Json(serde_json::json!({"result": []})));
    };
    let precision = num_bigint::BigInt::from(1_000_000_000_000_000_000i128);
    let shares = position
      .shares
      .parse::<num_bigint::BigInt>()
      .unwrap_or_else(|_| num_bigint::BigInt::from(0));
    let mut out = Vec::new();
    let reward_ticks = if authority.rt.is_empty() {
      let length = index.tap_get_length(&format!("abl/{}", position.auth))?;
      let mut ticks = Vec::new();
      for i in 0..length {
        if let Some(tick) = index
          .tap_get_raw(&format!("abli/{}/{}", position.auth, i))?
          .and_then(|bytes| tap_decode_string_value(&bytes))
        {
          ticks.push(tick);
        }
      }
      ticks
    } else {
      authority.rt
    };
    for reward_tick in reward_ticks {
      let reward_key = json_stringify_lower(&reward_tick);
      let acc = index
        .tap_get_string(&format!("ahrps/{}/{}", position.auth, reward_key))?
        .and_then(|s| s.parse::<num_bigint::BigInt>().ok())
        .unwrap_or_else(|| num_bigint::BigInt::from(0));
      let paid = position
        .debt
        .get(&reward_tick)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<num_bigint::BigInt>().ok())
        .unwrap_or_else(|| num_bigint::BigInt::from(0));
      let mut pending = &shares * acc / &precision - paid;
      if pending < num_bigint::BigInt::from(0) {
        pending = num_bigint::BigInt::from(0);
      }
      out.push(serde_json::json!({
        "auth": position.auth,
        "pos": position.id,
        "rt": reward_tick,
        "amt": pending.to_string()
      }));
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_status(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("sale/{}", authority_id))?
      .and_then(|b| tap_decode_json_value(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_sale_contributions_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sconl")? }),
    ))
  })
}

pub(super) async fn tap_get_sale_contribution(
  Extension(index): Extension<Arc<Index>>,
  Path(id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index
      .tap_get_raw(&format!("scon/{}", id))?
      .and_then(|b| tap_decode_json_value(&b));
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_sale_contributions(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "sconl",
      "sconli",
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_contributions_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("scona/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_contributions_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("scona/{}", authority_id),
      &format!("sconai/{}", authority_id),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_contributions_by_address_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("sconaddr/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_contributions_by_address(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("sconaddr/{}", address),
      &format!("sconaddri/{}", address),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_contributions_by_claim_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("sconcl/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_contributions_by_claim(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("sconcl/{}", address),
      &format!("sconcli/{}", address),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_claims_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sclaiml")? }),
    ))
  })
}

pub(super) async fn tap_get_sale_claims(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "sclaiml",
      "sclaimli",
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_claims_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("scla/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_claims_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("scla/{}", authority_id),
      &format!("sclai/{}", authority_id),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_claims_by_address_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("scladdr/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_claims_by_address(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("scladdr/{}", address),
      &format!("scladdri/{}", address),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_refunds_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("srefl")? }),
    ))
  })
}

pub(super) async fn tap_get_sale_refunds(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "srefl",
      "srefli",
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_refunds_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("srefa/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_refunds_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("srefa/{}", authority_id),
      &format!("srefai/{}", authority_id),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_refunds_by_address_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("srefaddr/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_refunds_by_address(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("srefaddr/{}", address),
      &format!("srefaddri/{}", address),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_cancels_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("scanl")? }),
    ))
  })
}

pub(super) async fn tap_get_sale_cancels(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "scanl",
      "scanli",
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_cancels_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("scana/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_cancels_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("scana/{}", authority_id),
      &format!("scanai/{}", authority_id),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_withdrawals_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("swdrl")? }),
    ))
  })
}

pub(super) async fn tap_get_sale_withdrawals(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      "swdrl",
      "swdrli",
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sale_withdrawals_by_authority_length(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("swdra/{}", authority_id))? }),
    ))
  })
}

pub(super) async fn tap_get_sale_withdrawals_by_authority(
  Extension(index): Extension<Arc<Index>>,
  Path(authority_id): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let out = tap_collect_json_records(
      &index,
      &format!("swdra/{}", authority_id),
      &format!("swdrai/{}", authority_id),
      q.offset.unwrap_or(0),
      q.max.unwrap_or(500).min(500),
    )?;
    Ok(Json(serde_json::json!({"result": out})))
  })
}
// END TAP-PROOFS

// --- Privilege-auth endpoints ---

pub(super) async fn tap_get_privilege_auth_cancelled(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let exists = index
      .tap_get_raw(&format!("prac/{}", inscription))?
      .is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

pub(super) async fn tap_get_privilege_auth_hash_exists(
  Extension(index): Extension<Arc<Index>>,
  Path(hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let h = hash.trim().to_lowercase();
    let exists = index.tap_get_raw(&format!("prah/{}", h))?.is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

pub(super) async fn tap_get_privilege_auth_compact_hex_exists(
  Extension(index): Extension<Arc<Index>>,
  Path(hash): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let h = hash.trim().to_lowercase();
    let exists = index.tap_get_raw(&format!("prah/{}", h))?.is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

pub(super) async fn tap_get_privilege_auth_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length("sfpra")? }),
    ))
  })
}

pub(super) async fn tap_get_privilege_auth_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sfpra")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sfprai/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_privilege_auth_create_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_privilege_auth_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("pra/{}", address))? }),
    ))
  })
}

pub(super) async fn tap_get_account_privilege_auth_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("pra/{}", address))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("prai/{}/{}", address, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_privilege_auth_create_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Account blocked transferables
pub(super) async fn tap_get_account_blocked_transferables(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let exists = index.tap_get_raw(&format!("bltr/{}", address))?.is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

// --- Privilege verification REST ---

pub(super) async fn tap_get_privilege_authority_verified_inscription(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name, verified_hash, sequence)): Path<(String, String, String, i64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let col_key = serde_json::to_string(&collection_name)
      .unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let result = index.tap_get_string(&format!(
      "prvins/{}/{}/{}/{}",
      priv_ins, col_key, verified_hash, sequence
    ))?;
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_privilege_authority_verified_by_inscription(
  Extension(index): Extension<Arc<Index>>,
  Path(verified_inscription_id): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let result = index.tap_get_string(&format!("prvins/{}", verified_inscription_id))?;
    Ok(Json(serde_json::json!({"result": result})))
  })
}

pub(super) async fn tap_get_privilege_authority_is_verified(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name, verified_hash, sequence)): Path<(String, String, String, i64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let col_key = serde_json::to_string(&collection_name)
      .unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let exists = index
      .tap_get_raw(&format!(
        "prvvrfd/{}/{}/{}/{}",
        priv_ins, col_key, verified_hash, sequence
      ))?
      .is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

pub(super) async fn tap_get_privilege_authority_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(priv_ins): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("prv/{}", priv_ins))? }),
    ))
  })
}

pub(super) async fn tap_get_privilege_authority_list(
  Extension(index): Extension<Arc<Index>>,
  Path(priv_ins): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length(&format!("prv/{}", priv_ins))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("prvi/{}/{}", priv_ins, i);
      if let Some(ptr) = index.tap_get_string(&key)? {
        if let Some(bytes) = index.tap_get_raw(&ptr)? {
          if let Some(rec) = tap_decode_privilege_verified_record(&bytes) {
            out.push(rec);
          }
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_collection_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let col_key = serde_json::to_string(&collection_name)
      .unwrap_or_else(|_| format!("\"{}\"", collection_name));
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("prvcol/{}/{}", priv_ins, col_key))? }),
    ))
  })
}

pub(super) async fn tap_get_privilege_authority_collection_list(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let col_key = serde_json::to_string(&collection_name)
      .unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let length = index.tap_get_length(&format!("prvcol/{}/{}", priv_ins, col_key))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("prvcoli/{}/{}/{}", priv_ins, col_key, i);
      if let Some(ptr) = index.tap_get_string(&key)? {
        if let Some(bytes) = index.tap_get_raw(&ptr)? {
          if let Some(rec) = tap_decode_privilege_verified_record(&bytes) {
            out.push(rec);
          }
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blckp/pravth/{}/{}", priv_ins, block))? }),
    ))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_block(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blckp/pravth/{}/{}", priv_ins, block),
      &format!("blckpi/pravth/{}/{}", priv_ins, block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_privilege_verified_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blck/pravth/{}", block))? }),
    ))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(
      &format!("blck/pravth/{}", block),
      &format!("blcki/pravth/{}", block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_privilege_verified_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_col_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name, block)): Path<(String, String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let col_key = serde_json::to_string(&collection_name)
      .unwrap_or_else(|_| format!("\"{}\"", collection_name));
    Ok(Json(
      serde_json::json!({"result": index.tap_get_length(&format!("blckpc/pravth/{}/{}/{}", priv_ins, col_key, block))? }),
    ))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_col_block(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name, block)): Path<(String, String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let col_key = serde_json::to_string(&collection_name)
      .unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let ptrs = index.tap_list_strings(
      &format!("blckpc/pravth/{}/{}/{}", priv_ins, col_key, block),
      &format!("blckpci/pravth/{}/{}/{}", priv_ins, col_key, block),
      offset,
      max,
    )?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_privilege_verified_record(&bytes) {
          out.push(rec);
        }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn blockhash(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<String>> {
  task::block_in_place(|| {
    Ok(Json(
      index
        .block_hash(None)?
        .ok_or_not_found(|| "blockhash")?
        .to_string(),
    ))
  })
}

pub(super) async fn blockhash_at_height(
  Extension(index): Extension<Arc<Index>>,
  Path(height): Path<u32>,
) -> ServerResult<Json<String>> {
  task::block_in_place(|| {
    Ok(Json(
      index
        .block_hash(Some(height))?
        .ok_or_not_found(|| "blockhash")?
        .to_string(),
    ))
  })
}

pub(super) async fn block_hash_from_height_string(
  Extension(index): Extension<Arc<Index>>,
  Path(height): Path<u32>,
) -> ServerResult<String> {
  task::block_in_place(|| {
    Ok(
      index
        .block_hash(Some(height))?
        .ok_or_not_found(|| "blockhash")?
        .to_string(),
    )
  })
}

pub(super) async fn blockhash_string(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<String> {
  task::block_in_place(|| {
    Ok(
      index
        .block_hash(None)?
        .ok_or_not_found(|| "blockhash")?
        .to_string(),
    )
  })
}

pub(super) async fn blockheight_string(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<String> {
  task::block_in_place(|| {
    Ok(
      index
        .block_height()?
        .ok_or_not_found(|| "blockheight")?
        .to_string(),
    )
  })
}

pub(super) async fn blockinfo(
  Extension(index): Extension<Arc<Index>>,
  Path(DeserializeFromStr(query)): Path<DeserializeFromStr<query::Block>>,
) -> ServerResult<Json<api::BlockInfo>> {
  task::block_in_place(|| {
    let hash = match query {
      query::Block::Hash(hash) => hash,
      query::Block::Height(height) => index
        .block_hash(Some(height))?
        .ok_or_not_found(|| format!("block {height}"))?,
    };

    let header = index
      .block_header(hash)?
      .ok_or_not_found(|| format!("block {hash}"))?;

    let info = index
      .block_header_info(hash)?
      .ok_or_not_found(|| format!("block {hash}"))?;

    let stats = index
      .block_stats(info.height.try_into().unwrap())?
      .ok_or_not_found(|| format!("block {hash}"))?;

    Ok(Json(api::BlockInfo {
      average_fee: stats.avg_fee.to_sat(),
      average_fee_rate: stats.avg_fee_rate.to_sat(),
      bits: header.bits.to_consensus(),
      chainwork: info.chainwork.try_into().unwrap(),
      confirmations: info.confirmations,
      difficulty: info.difficulty,
      hash,
      feerate_percentiles: [
        stats.fee_rate_percentiles.fr_10th.to_sat(),
        stats.fee_rate_percentiles.fr_25th.to_sat(),
        stats.fee_rate_percentiles.fr_50th.to_sat(),
        stats.fee_rate_percentiles.fr_75th.to_sat(),
        stats.fee_rate_percentiles.fr_90th.to_sat(),
      ],
      height: info.height.try_into().unwrap(),
      max_fee: stats.max_fee.to_sat(),
      max_fee_rate: stats.max_fee_rate.to_sat(),
      max_tx_size: stats.max_tx_size,
      median_fee: stats.median_fee.to_sat(),
      median_time: info
        .median_time
        .map(|median_time| median_time.try_into().unwrap()),
      merkle_root: info.merkle_root,
      min_fee: stats.min_fee.to_sat(),
      min_fee_rate: stats.min_fee_rate.to_sat(),
      next_block: info.next_block_hash,
      nonce: info.nonce,
      previous_block: info.previous_block_hash,
      subsidy: stats.subsidy.to_sat(),
      target: target_as_block_hash(header.target()),
      timestamp: info.time.try_into().unwrap(),
      total_fee: stats.total_fee.to_sat(),
      total_size: stats.total_size,
      total_weight: stats.total_weight,
      transaction_count: info.n_tx.try_into().unwrap(),
      #[allow(clippy::cast_sign_loss)]
      version: info.version.to_consensus() as u32,
    }))
  })
}

pub(super) async fn blocktime_string(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<String> {
  task::block_in_place(|| {
    Ok(
      index
        .block_time(index.block_height()?.ok_or_not_found(|| "blocktime")?)?
        .unix_timestamp()
        .to_string(),
    )
  })
}

pub(super) async fn children(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription_id): Path<InscriptionId>,
) -> ServerResult {
  children_paginated(Extension(index), Path((inscription_id, 0))).await
}

pub(super) async fn children_inscriptions(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription_id): Path<InscriptionId>,
) -> ServerResult {
  children_inscriptions_paginated(Extension(index), Path((inscription_id, 0))).await
}

pub(super) async fn children_inscriptions_paginated(
  Extension(index): Extension<Arc<Index>>,
  Path((parent, page)): Path<(InscriptionId, usize)>,
) -> ServerResult {
  task::block_in_place(|| {
    let parent_sequence_number = index
      .get_inscription_entry(parent)?
      .ok_or_not_found(|| format!("inscription {parent}"))?
      .sequence_number;

    let (ids, more) =
      index.get_children_by_sequence_number_paginated(parent_sequence_number, 100, page)?;

    let children = ids
      .into_iter()
      .map(|inscription_id| get_relative_inscription(&index, inscription_id))
      .collect::<ServerResult<Vec<api::RelativeInscriptionRecursive>>>()?;

    Ok(
      Json(api::ChildInscriptions {
        children,
        more,
        page,
      })
      .into_response(),
    )
  })
}

pub(super) async fn children_paginated(
  Extension(index): Extension<Arc<Index>>,
  Path((parent, page)): Path<(InscriptionId, usize)>,
) -> ServerResult {
  task::block_in_place(|| {
    let Some(parent) = index.get_inscription_entry(parent)? else {
      return Err(ServerError::NotFound(format!(
        "inscription {} not found",
        parent
      )));
    };

    let parent_sequence_number = parent.sequence_number;

    let (ids, more) =
      index.get_children_by_sequence_number_paginated(parent_sequence_number, 100, page)?;

    Ok(Json(api::Children { ids, more, page }).into_response())
  })
}

pub(super) async fn content(
  Extension(index): Extension<Arc<Index>>,
  Extension(settings): Extension<Arc<Settings>>,
  Extension(server_config): Extension<Arc<ServerConfig>>,
  Path(inscription_id): Path<InscriptionId>,
  accept_encoding: AcceptEncoding,
) -> ServerResult {
  task::block_in_place(|| {
    if settings.is_hidden(inscription_id) {
      return Ok(PreviewUnknownHtml.into_response());
    }

    let Some(mut inscription) = index.get_inscription_by_id(inscription_id)? else {
      return Err(ServerError::NotFound(format!(
        "inscription {inscription_id} not found"
      )));
    };

    if let Some(delegate) = inscription.delegate() {
      inscription = index
        .get_inscription_by_id(delegate)?
        .ok_or_not_found(|| format!("delegate {inscription_id}"))?
    }

    Ok(
      content_response(inscription, accept_encoding, &server_config)?
        .ok_or_not_found(|| format!("inscription {inscription_id} content"))?
        .into_response(),
    )
  })
}

pub(super) fn content_response(
  inscription: Inscription,
  accept_encoding: AcceptEncoding,
  server_config: &ServerConfig,
) -> ServerResult<Option<(HeaderMap, Vec<u8>)>> {
  let mut headers = HeaderMap::new();

  match &server_config.csp_origin {
    None => {
      headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self' 'unsafe-eval' 'unsafe-inline' data: blob:"),
      );
      headers.append(
          header::CONTENT_SECURITY_POLICY,
          HeaderValue::from_static("default-src *:*/content/ *:*/blockheight *:*/blockhash *:*/blockhash/ *:*/blocktime *:*/r/ 'unsafe-eval' 'unsafe-inline' data: blob:"),
        );
    }
    Some(origin) => {
      let csp = format!("default-src {origin}/content/ {origin}/blockheight {origin}/blockhash {origin}/blockhash/ {origin}/blocktime {origin}/r/ 'unsafe-eval' 'unsafe-inline' data: blob:");
      headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_str(&csp).map_err(|err| ServerError::Internal(Error::from(err)))?,
      );
    }
  }

  headers.insert(
    header::CACHE_CONTROL,
    HeaderValue::from_static("public, max-age=1209600, immutable"),
  );

  headers.insert(
    header::CONTENT_TYPE,
    inscription
      .content_type()
      .and_then(|content_type| content_type.parse().ok())
      .unwrap_or(HeaderValue::from_static("application/octet-stream")),
  );

  if let Some(content_encoding) = inscription.content_encoding() {
    if accept_encoding.is_acceptable(&content_encoding) {
      headers.insert(header::CONTENT_ENCODING, content_encoding);
    } else if server_config.decompress && content_encoding == "br" {
      let Some(body) = inscription.into_body() else {
        return Ok(None);
      };

      let mut decompressed = Vec::new();

      Decompressor::new(body.as_slice(), 4096)
        .read_to_end(&mut decompressed)
        .map_err(|err| ServerError::Internal(err.into()))?;

      return Ok(Some((headers, decompressed)));
    } else {
      return Err(ServerError::NotAcceptable {
        accept_encoding,
        content_encoding,
      });
    }
  }

  let Some(body) = inscription.into_body() else {
    return Ok(None);
  };

  Ok(Some((headers, body)))
}

pub(super) async fn inscription(
  Extension(index): Extension<Arc<Index>>,
  Extension(server_config): Extension<Arc<ServerConfig>>,
  Path(inscription_id): Path<InscriptionId>,
) -> ServerResult {
  task::block_in_place(|| {
    let Some(inscription) = index.get_inscription_by_id(inscription_id)? else {
      return Err(ServerError::NotFound(format!(
        "inscription {} not found",
        inscription_id
      )));
    };

    let entry = index
      .get_inscription_entry(inscription_id)
      .unwrap()
      .unwrap();

    let satpoint = index
      .get_inscription_satpoint_by_id(inscription_id)
      .ok()
      .flatten()
      .unwrap();

    let output = if satpoint.outpoint == unbound_outpoint() {
      None
    } else {
      Some(
        index
          .get_transaction(satpoint.outpoint.txid)?
          .ok_or_not_found(|| format!("inscription {inscription_id} current transaction"))?
          .output
          .into_iter()
          .nth(satpoint.outpoint.vout.try_into().unwrap())
          .ok_or_not_found(|| format!("inscription {inscription_id} current transaction output"))?,
      )
    };

    let address = output.as_ref().and_then(|output| {
      server_config
        .chain
        .address_from_script(&output.script_pubkey)
        .ok()
        .map(|address| address.to_string())
    });

    Ok(
      Json(api::InscriptionRecursive {
        charms: Charm::charms(entry.charms),
        content_type: inscription.content_type().map(|s| s.to_string()),
        content_length: inscription.content_length(),
        delegate: inscription.delegate(),
        fee: entry.fee,
        height: entry.height,
        id: inscription_id,
        number: entry.inscription_number,
        output: satpoint.outpoint,
        value: output.as_ref().map(|o| o.value.to_sat()),
        sat: entry.sat,
        satpoint,
        timestamp: timestamp(entry.timestamp.into()).timestamp(),
        address,
      })
      .into_response(),
    )
  })
}

pub(super) async fn metadata(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription_id): Path<InscriptionId>,
) -> ServerResult {
  task::block_in_place(|| {
    let Some(inscription) = index.get_inscription_by_id(inscription_id)? else {
      return Err(ServerError::NotFound(format!(
        "inscription {} not found",
        inscription_id
      )));
    };

    let metadata = inscription
      .metadata
      .ok_or_not_found(|| format!("inscription {inscription_id} metadata"))?;

    Ok(Json(hex::encode(metadata)).into_response())
  })
}

pub(super) async fn parents(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription_id): Path<InscriptionId>,
) -> ServerResult {
  parents_paginated(Extension(index), Path((inscription_id, 0))).await
}

pub async fn parent_inscriptions(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription_id): Path<InscriptionId>,
) -> ServerResult {
  parent_inscriptions_paginated(Extension(index), Path((inscription_id, 0))).await
}

pub async fn parent_inscriptions_paginated(
  Extension(index): Extension<Arc<Index>>,
  Path((child, page)): Path<(InscriptionId, usize)>,
) -> ServerResult {
  task::block_in_place(|| {
    let entry = index
      .get_inscription_entry(child)?
      .ok_or_not_found(|| format!("inscription {child}"))?;

    let (ids, more) = index.get_parents_by_sequence_number_paginated(entry.parents, 100, page)?;

    let parents = ids
      .into_iter()
      .map(|inscription_id| get_relative_inscription(&index, inscription_id))
      .collect::<ServerResult<Vec<api::RelativeInscriptionRecursive>>>()?;

    Ok(
      Json(api::ParentInscriptions {
        parents,
        more,
        page,
      })
      .into_response(),
    )
  })
}

pub(super) async fn parents_paginated(
  Extension(index): Extension<Arc<Index>>,
  Path((inscription_id, page)): Path<(InscriptionId, usize)>,
) -> ServerResult {
  task::block_in_place(|| {
    let child = index
      .get_inscription_entry(inscription_id)?
      .ok_or_not_found(|| format!("inscription {inscription_id}"))?;

    let (ids, more) = index.get_parents_by_sequence_number_paginated(child.parents, 100, page)?;

    let page_index =
      u32::try_from(page).map_err(|_| anyhow!("page index {} out of range", page))?;

    Ok(
      Json(api::Inscriptions {
        ids,
        more,
        page_index,
      })
      .into_response(),
    )
  })
}

pub(super) async fn sat(
  Extension(index): Extension<Arc<Index>>,
  Path(sat): Path<u64>,
) -> ServerResult<Json<api::SatInscriptions>> {
  sat_paginated(Extension(index), Path((sat, 0))).await
}

pub(super) async fn sat_at_index(
  Extension(index): Extension<Arc<Index>>,
  Path((DeserializeFromStr(sat), inscription_index)): Path<(DeserializeFromStr<Sat>, isize)>,
) -> ServerResult<Json<api::SatInscription>> {
  task::block_in_place(|| {
    if !index.has_sat_index() {
      return Err(ServerError::NotFound(
        "this server has no sat index".to_string(),
      ));
    }

    let id = index.get_inscription_id_by_sat_indexed(sat, inscription_index)?;

    Ok(Json(api::SatInscription { id }))
  })
}

pub(super) async fn sat_paginated(
  Extension(index): Extension<Arc<Index>>,
  Path((sat, page)): Path<(u64, u64)>,
) -> ServerResult<Json<api::SatInscriptions>> {
  task::block_in_place(|| {
    if !index.has_sat_index() {
      return Err(ServerError::NotFound("this server has no sat index".into()));
    }

    let (ids, more) = index.get_inscription_ids_by_sat_paginated(Sat(sat), 100, page)?;

    Ok(Json(api::SatInscriptions { ids, more, page }))
  })
}

pub(super) async fn sat_at_index_content(
  index: Extension<Arc<Index>>,
  settings: Extension<Arc<Settings>>,
  server_config: Extension<Arc<ServerConfig>>,
  Path((DeserializeFromStr(sat), inscription_index)): Path<(DeserializeFromStr<Sat>, isize)>,
  accept_encoding: AcceptEncoding,
) -> ServerResult {
  let inscription_id = task::block_in_place(|| {
    if !index.has_sat_index() {
      return Err(ServerError::NotFound("this server has no sat index".into()));
    }

    index
      .get_inscription_id_by_sat_indexed(sat, inscription_index)?
      .ok_or_not_found(|| format!("inscription on sat {sat}"))
  })?;

  content(
    index,
    settings,
    server_config,
    Path(inscription_id),
    accept_encoding,
  )
  .await
}

fn get_relative_inscription(
  index: &Index,
  id: InscriptionId,
) -> ServerResult<api::RelativeInscriptionRecursive> {
  let entry = index
    .get_inscription_entry(id)?
    .ok_or_not_found(|| format!("inscription {id}"))?;

  let satpoint = index
    .get_inscription_satpoint_by_id(id)?
    .ok_or_not_found(|| format!("satpoint for inscription {id}"))?;

  Ok(api::RelativeInscriptionRecursive {
    charms: Charm::charms(entry.charms),
    fee: entry.fee,
    height: entry.height,
    id,
    number: entry.inscription_number,
    output: satpoint.outpoint,
    sat: entry.sat,
    satpoint,
    timestamp: timestamp(entry.timestamp.into()).timestamp(),
  })
}

pub(super) async fn tx(
  Extension(index): Extension<Arc<Index>>,
  Path(txid): Path<Txid>,
) -> ServerResult<Json<String>> {
  task::block_in_place(|| {
    Ok(Json(
      index
        .get_transaction_hex_recursive(txid)?
        .ok_or_not_found(|| format!("transaction {txid}"))?,
    ))
  })
}

pub(super) async fn undelegated_content(
  Extension(index): Extension<Arc<Index>>,
  Extension(settings): Extension<Arc<Settings>>,
  Extension(server_config): Extension<Arc<ServerConfig>>,
  Path(inscription_id): Path<InscriptionId>,
  accept_encoding: AcceptEncoding,
) -> ServerResult {
  task::block_in_place(|| {
    if settings.is_hidden(inscription_id) {
      return Ok(PreviewUnknownHtml.into_response());
    }

    let inscription = index
      .get_inscription_by_id(inscription_id)?
      .ok_or_not_found(|| format!("inscription {inscription_id}"))?;

    Ok(
      r::content_response(inscription, accept_encoding, &server_config)?
        .ok_or_not_found(|| format!("inscription {inscription_id} content"))?
        .into_response(),
    )
  })
}

pub(super) async fn utxo(
  Extension(index): Extension<Arc<Index>>,
  Path(outpoint): Path<OutPoint>,
) -> ServerResult {
  task::block_in_place(|| {
    Ok(
      Json(
        index
          .get_utxo_recursive(outpoint)?
          .ok_or_not_found(|| format!("output {outpoint}"))?,
      )
      .into_response(),
    )
  })
}
