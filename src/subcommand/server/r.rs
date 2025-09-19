use super::*;
use ciborium::de::from_reader as cbor_from_reader;

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
  cbor_from_reader::<TapBitmapRecord, _>(std::io::Cursor::new(bytes)).ok()
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
  cbor_from_reader::<TapDeployRecord, _>(std::io::Cursor::new(bytes)).ok()
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

fn tap_decode_mint_record(bytes: &[u8]) -> Option<TapMintRecord> {
  cbor_from_reader::<TapMintRecord, _>(std::io::Cursor::new(bytes)).ok()
}

fn tap_decode_mint_flat_record(bytes: &[u8]) -> Option<TapMintFlatRecord> {
  cbor_from_reader::<TapMintFlatRecord, _>(std::io::Cursor::new(bytes)).ok()
}

fn tap_decode_mint_superflat_record(bytes: &[u8]) -> Option<TapMintSuperflatRecord> {
  cbor_from_reader::<TapMintSuperflatRecord, _>(std::io::Cursor::new(bytes)).ok()
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
  addr: String,
  taddr: String,
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
  num: i32,
  ts: u32,
  addr: String,
}

fn tap_decode_accumulator_entry(bytes: &[u8]) -> Option<TapAccumulatorEntry> {
  cbor_from_reader::<TapAccumulatorEntry, _>(std::io::Cursor::new(bytes)).ok()
}

fn tap_decode_transfer_init_record(bytes: &[u8]) -> Option<TapTransferInitRecord> {
  cbor_from_reader::<TapTransferInitRecord, _>(std::io::Cursor::new(bytes)).ok()
}
fn tap_decode_transfer_init_flat_record(bytes: &[u8]) -> Option<TapTransferInitFlatRecord> {
  cbor_from_reader::<TapTransferInitFlatRecord, _>(std::io::Cursor::new(bytes)).ok()
}
fn tap_decode_transfer_init_superflat_record(bytes: &[u8]) -> Option<TapTransferInitSuperflatRecord> {
  cbor_from_reader::<TapTransferInitSuperflatRecord, _>(std::io::Cursor::new(bytes)).ok()
}
fn tap_decode_transfer_send_superflat_record(bytes: &[u8]) -> Option<TapTransferSendSuperflatRecord> {
  cbor_from_reader::<TapTransferSendSuperflatRecord, _>(std::io::Cursor::new(bytes)).ok()
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
  cbor_from_reader::<TapTradeOfferRecord, _>(std::io::Cursor::new(bytes)).ok()
}
fn tap_decode_trade_buy_seller_record(bytes: &[u8]) -> Option<TapTradeBuySellerRecord> {
  cbor_from_reader::<TapTradeBuySellerRecord, _>(std::io::Cursor::new(bytes)).ok()
}
fn tap_decode_trade_buy_buyer_record(bytes: &[u8]) -> Option<TapTradeBuyBuyerRecord> {
  cbor_from_reader::<TapTradeBuyBuyerRecord, _>(std::io::Cursor::new(bytes)).ok()
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
  cbor_from_reader::<TapTokenAuthCreateRecord, _>(std::io::Cursor::new(bytes)).ok()
}
fn tap_decode_token_auth_redeem_record(bytes: &[u8]) -> Option<TapTokenAuthRedeemRecord> {
  cbor_from_reader::<TapTokenAuthRedeemRecord, _>(std::io::Cursor::new(bytes)).ok()
}

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
  cbor_from_reader::<TapPrivilegeAuthCreateRecord, _>(std::io::Cursor::new(bytes)).ok()
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
  cbor_from_reader::<TapPrivilegeVerifiedRecord, _>(std::io::Cursor::new(bytes)).ok()
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
  cbor_from_reader::<TapDmtElementRecord, _>(std::io::Cursor::new(bytes)).ok()
}

pub(super) async fn tap_get_dmt_elements_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length("dmt-ell")? }))))
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
        if let Ok(name) = ciborium::de::from_reader::<String, _>(std::io::Cursor::new(name_bytes)) {
          let elkey = format!("dmt-el/{}", serde_json::to_string(&name).unwrap_or_else(|_| format!("\"{}\"", name)));
          if let Some(bytes) = index.tap_get_raw(&elkey)? { if let Some(rec) = tap_decode_dmt_element_record(&bytes) { out.push(rec); } }
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
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blck/dmt-md/{}", block))? }))))
}

pub(super) async fn tap_get_dmt_event_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blck/dmt-md/{}", block), &format!("blcki/dmt-md/{}", block), offset, max)?;
    // These pointers reference stored records (JSON/CBOR) — return raw decoded values if available
    let mut out = Vec::<serde_json::Value>::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? {
        // Try decode to JSON first, fallback: none.
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) { out.push(val); }
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
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("dmtmhl/{}", inscription))? }))))
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
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) { out.push(val); }
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
      .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
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
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
          return Ok(Json(serde_json::json!({"result": Some(val)})));
        }
      }
    }
    Ok(Json(serde_json::json!({"result": Option::<serde_json::Value>::None})))
  })
}

pub(super) async fn tap_get_dmt_mint_wallet_historic_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("dmtmwl/{}", address))? }))))
}

pub(super) async fn tap_get_dmt_mint_wallet_historic_list(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let list = index.tap_list_strings(&format!("dmtmwl/{}", address), &format!("dmtmwli/{}", address), offset, max)?;
    Ok(Json(serde_json::json!({"result": list})))
  })
}

// --- Account tokens: list/length/balances/details ---
pub(super) async fn tap_get_account_tokens_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("atl/{}", address))? }))))
}

pub(super) async fn tap_get_account_tokens(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let records = index.tap_list_strings(&format!("atl/{}", &address), &format!("atli/{}", &address), offset, max)?;
    let out: Vec<String> = records.into_iter().map(|s| s.to_lowercase()).collect();
    Ok(Json(serde_json::json!({"result": out})))
  })
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AccountTokenBalanceItem { ticker: String, overallBalance: Option<String>, transferableBalance: Option<String> }

pub(super) async fn tap_get_account_tokens_balance(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let total = index.tap_get_length(&format!("atl/{}", &address))?;
    let tokens = index.tap_list_strings(&format!("atl/{}", &address), &format!("atli/{}", &address), offset, max)?;
    let mut list: Vec<AccountTokenBalanceItem> = Vec::new();
    for t in tokens {
      let tkey = json_stringify_lower(&t);
      let overall = index.tap_get_string(&format!("b/{}/{}", &address, &tkey))?;
      let tr = index.tap_get_string(&format!("t/{}/{}", &address, &tkey))?;
      list.push(AccountTokenBalanceItem { ticker: t.to_lowercase(), overallBalance: overall, transferableBalance: tr });
    }
    Ok(Json(serde_json::json!({"data": {"total": total, "list": list} })))
  })
}

pub(super) async fn tap_get_account_token_detail(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    // tokenInfo
    let token_info = index.tap_get_raw(&format!("d/{}", &tkey))?.and_then(|b| tap_decode_deploy_record(&b));
    if token_info.is_none() {
      return Ok(Json(serde_json::json!({"data": serde_json::Value::Null})));
    }
    // balances
    let overall = index.tap_get_string(&format!("b/{}/{}", &address, &tkey))?;
    let tr = index.tap_get_string(&format!("t/{}/{}", &address, &tkey))?;
    // transfers for this account/ticker
    let len = index.tap_get_length(&format!("atrl/{}/{}", &address, &tkey))?;
    let mut transfer_list = Vec::<serde_json::Value>::new();
    for i in 0..len { if let Some(bytes) = index.tap_get_raw(&format!("atrli/{}/{}/{}", &address, &tkey, i))? { if let Some(rec) = tap_decode_transfer_init_record(&bytes) { transfer_list.push(serde_json::to_value(rec).unwrap_or(serde_json::json!({}))); } } }
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
pub(super) struct TapGenericListQuery { length_key: String, iterator_key: String, offset: Option<u64>, max: Option<u64>, return_json: Option<bool> }

pub(super) async fn tap_get_list_records(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapGenericListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500);
    if max > 500 { return Ok(Json(serde_json::json!({"result": "request too large"}))); }
    // length
    let length = index.tap_get_length(&q.length_key)?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::<serde_json::Value>::new();
    for i in offset..end {
      let key = format!("{}/{}", q.iterator_key, i);
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if q.return_json.unwrap_or(true) {
          if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) { out.push(val); }
        } else {
          if let Ok(s) = ciborium::de::from_reader::<String, _>(std::io::Cursor::new(bytes)) { out.push(serde_json::json!(s)); }
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
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&length_key)? }))))
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
    let ptrs = index.tap_list_strings(&format!("blck/bm/{}", block), &format!("blcki/bm/{}", block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(b) = index.tap_get_raw(&p)? {
        if let Some(rec) = tap_decode_bitmap_record(&b) { out.push(rec); }
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
    let list = index.tap_list_strings(&format!("bml/{}", address), &format!("bmli/{}", address), offset, max)?;
    Ok(Json(serde_json::json!({"result": list})))
  })
}

fn json_stringify_lower(s: &str) -> String {
  serde_json::to_string(&s.to_lowercase()).unwrap_or_else(|_| format!("\"{}\"", s.to_lowercase()))
}

fn tap_fetch_deployments_by_pointers(
  index: &Index,
  ptrs: Vec<String>,
) -> ServerResult<Vec<TapDeployRecord>> {
  let mut out = Vec::new();
  for ptr in ptrs {
    if let Some(ticker) = index.tap_get_string(&ptr)? {
      let key = format!("d/{}", json_stringify_lower(&ticker));
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_deploy_record(&bytes) {
          out.push(rec);
        }
      }
    }
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    // list of tickers
    let ticks = index.tap_list_strings("dl", "dli", offset, max)?;
    let mut out = Vec::new();
    for t in ticks {
      let key = format!("d/{}", json_stringify_lower(&t));
      if let Some(bytes) = index.tap_get_raw(&key)? {
        if let Some(rec) = tap_decode_deploy_record(&bytes) { out.push(rec); }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("tx/dpl/{}", tx), &format!("txi/dpl/{}", tx), offset, max)?;
    let out = tap_fetch_deployments_by_pointers(&index, ptrs)?;
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("txt/dpl/{}/{}", tkey, tx), &format!("txti/dpl/{}/{}", tkey, tx), offset, max)?;
    let out = tap_fetch_deployments_by_pointers(&index, ptrs)?;
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blck/dpl/{}", block), &format!("blcki/dpl/{}", block), offset, max)?;
    let out = tap_fetch_deployments_by_pointers(&index, ptrs)?;
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("blckt/dpl/{}/{}", tkey, block), &format!("blckti/dpl/{}/{}", tkey, block), offset, max)?;
    let out = tap_fetch_deployments_by_pointers(&index, ptrs)?;
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("aml/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("amli/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_mint_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("fml/{}", tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("fmli/{}/{}", tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_mint_flat_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sfml")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sfmli/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_mint_superflat_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("tx/mnt/{}", tx), &format!("txi/mnt/{}", tx), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs {
      if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_mint_superflat_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("txt/mnt/{}/{}", tkey, tx), &format!("txti/mnt/{}/{}", tkey, tx), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_mint_superflat_record(&bytes) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blck/mnt/{}", block), &format!("blcki/mnt/{}", block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_mint_superflat_record(&bytes) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
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
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("blckt/mnt/{}/{}", tkey, block), &format!("blckti/mnt/{}/{}", tkey, block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_mint_superflat_record(&bytes) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
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
    let addrs = index.tap_list_strings(&format!("h/{}", tkey), &format!("hi/{}", tkey), offset, max)?;
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("tx/trf/{}", tx))? })))
  })
}

pub(super) async fn tap_get_inscribe_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("tx/trf/{}", tx), &format!("txi/trf/{}", tx), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_init_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("txt/trf/{}/{}", tkey, tx))? })))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("txt/trf/{}/{}", tkey, tx), &format!("txti/trf/{}/{}", tkey, tx), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_init_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_inscribe_transfer_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blck/trf/{}", block))? })))
  })
}

pub(super) async fn tap_get_inscribe_transfer_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blck/trf/{}", block), &format!("blcki/trf/{}", block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_init_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blckt/trf/{}/{}", tkey, block))? })))
  })
}

pub(super) async fn tap_get_ticker_inscribe_transfer_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("blckt/trf/{}/{}", tkey, block), &format!("blckti/trf/{}/{}", tkey, block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_init_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// --- Initial transfer account/ticker/global lists ---

pub(super) async fn tap_get_account_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("atrl/{}/{}", address, tkey))? })))
  })
}

pub(super) async fn tap_get_account_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("atrl/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("atrli/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_transfer_init_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("ftrl/{}", tkey))? })))
  })
}

pub(super) async fn tap_get_ticker_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("ftrl/{}", tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("ftrli/{}/{}", tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_transfer_init_flat_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_transfer_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({"result": index.tap_get_length("sftrl")? })))
  })
}

pub(super) async fn tap_get_transfer_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sftrl")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sftrli/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_transfer_init_superflat_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// --- Executed transfers (send) ---

pub(super) async fn tap_get_transferred_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("tx/snd/{}", tx))? })))
  })
}

pub(super) async fn tap_get_transferred_list(
  Extension(index): Extension<Arc<Index>>,
  Path(tx): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("tx/snd/{}", tx), &format!("txi/snd/{}", tx), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_send_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_transferred_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("txt/snd/{}/{}", tkey, tx))? })))
  })
}

pub(super) async fn tap_get_ticker_transferred_list(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, tx)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("txt/snd/{}/{}", tkey, tx), &format!("txti/snd/{}/{}", tkey, tx), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_send_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_transferred_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blck/snd/{}", block))? })))
  })
}

pub(super) async fn tap_get_transferred_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blck/snd/{}", block), &format!("blcki/snd/{}", block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_send_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_transferred_list_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blckt/snd/{}/{}", tkey, block))? })))
  })
}

pub(super) async fn tap_get_ticker_transferred_list_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path((ticker, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let ptrs = index.tap_list_strings(&format!("blckt/snd/{}/{}", tkey, block), &format!("blckti/snd/{}/{}", tkey, block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(b) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_transfer_send_superflat_record(&b) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Sent/Receive lists and global sent

pub(super) async fn tap_get_account_sent_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("strl/{}/{}", address, tkey))? })))
  })
}

pub(super) async fn tap_get_account_sent_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("strl/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("strli/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = cbor_from_reader::<TapTransferSendSenderRecord, _>(std::io::Cursor::new(bytes)).ok() { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_ticker_sent_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let tkey = json_stringify_lower(&ticker);
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("fstrl/{}", tkey))? })))
  })
}

pub(super) async fn tap_get_ticker_sent_list(
  Extension(index): Extension<Arc<Index>>,
  Path(ticker): Path<String>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("fstrl/{}", tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("fstrli/{}/{}", tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = cbor_from_reader::<TapTransferSendFlatRecord, _>(std::io::Cursor::new(bytes)).ok() { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_sent_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    Ok(Json(serde_json::json!({"result": index.tap_get_length("sfstrl")? })))
  })
}

pub(super) async fn tap_get_sent_list(
  Extension(index): Extension<Arc<Index>>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let length = index.tap_get_length("sfstrl")?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("sfstrli/{}", i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_transfer_send_superflat_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
        if let Some(rec) = tap_decode_accumulator_entry(&bytes) { out.push(rec); }
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
        if let Some(rec) = tap_decode_accumulator_entry(&bytes) { out.push(rec); }
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("rstrl/{}/{}", address, tkey))? })))
  })
}

pub(super) async fn tap_get_account_receive_list(
  Extension(index): Extension<Arc<Index>>,
  Path((address, ticker)): Path<(String, String)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let tkey = json_stringify_lower(&ticker);
    let length = index.tap_get_length(&format!("rstrl/{}/{}", address, tkey))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("rstrli/{}/{}/{}", address, tkey, i);
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = cbor_from_reader::<TapTransferSendReceiverRecord, _>(std::io::Cursor::new(bytes)).ok() { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("atrof/{}/{}", address, tkey))? })))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_offer_record(&bytes) { out.push(rec); } }
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("fatrof/{}", tkey))? })))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_offer_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_trades_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length("sfatrof")? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_offer_record(&bytes) { out.push(rec); } }
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("rbtrof/{}/{}", address, tkey))? })))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_buy_buyer_record(&bytes) { out.push(rec); } }
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("btrof/{}/{}", address, tkey))? })))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_buy_seller_record(&bytes) { out.push(rec); } }
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
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("fbtrof/{}", tkey))? })))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_buy_seller_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_trades_filled_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length("sfbtrof")? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_trade_buy_seller_record(&bytes) { out.push(rec); } }
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
    let exists = index.tap_get_raw(&format!("tac/{}", inscription))?.is_some();
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
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length("sfta")? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_token_auth_create_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Account-scoped token-auth list length
pub(super) async fn tap_get_account_auth_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("ta/{}", address))? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_token_auth_create_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Redeem: global list length
pub(super) async fn tap_get_redeem_list_length(
  Extension(index): Extension<Arc<Index>>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length("sftr")? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_token_auth_redeem_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// Redeem: account-scoped list length
pub(super) async fn tap_get_account_redeem_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("tr/{}", address))? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_token_auth_redeem_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

// --- Privilege-auth endpoints ---

pub(super) async fn tap_get_privilege_auth_cancelled(
  Extension(index): Extension<Arc<Index>>,
  Path(inscription): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let exists = index.tap_get_raw(&format!("prac/{}", inscription))?.is_some();
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
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length("sfpra")? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_privilege_auth_create_record(&bytes) { out.push(rec); } }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_account_privilege_auth_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(address): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("pra/{}", address))? }))))
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
      if let Some(bytes) = index.tap_get_raw(&key)? { if let Some(rec) = tap_decode_privilege_auth_create_record(&bytes) { out.push(rec); } }
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
    let col_key = serde_json::to_string(&collection_name).unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let result = index.tap_get_string(&format!("prvins/{}/{}/{}/{}", priv_ins, col_key, verified_hash, sequence))?;
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
    let col_key = serde_json::to_string(&collection_name).unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let exists = index.tap_get_raw(&format!("prvvrfd/{}/{}/{}/{}", priv_ins, col_key, verified_hash, sequence))?.is_some();
    Ok(Json(serde_json::json!({"result": exists})))
  })
}

pub(super) async fn tap_get_privilege_authority_list_length(
  Extension(index): Extension<Arc<Index>>,
  Path(priv_ins): Path<String>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("prv/{}", priv_ins))? }))))
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
        if let Some(bytes) = index.tap_get_raw(&ptr)? { if let Some(rec) = tap_decode_privilege_verified_record(&bytes) { out.push(rec); } }
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
    let col_key = serde_json::to_string(&collection_name).unwrap_or_else(|_| format!("\"{}\"", collection_name));
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("prvcol/{}/{}", priv_ins, col_key))? })))
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
    let col_key = serde_json::to_string(&collection_name).unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let length = index.tap_get_length(&format!("prvcol/{}/{}", priv_ins, col_key))?;
    let end = std::cmp::min(length, offset.saturating_add(max));
    let mut out = Vec::new();
    for i in offset..end {
      let key = format!("prvcoli/{}/{}/{}", priv_ins, col_key, i);
      if let Some(ptr) = index.tap_get_string(&key)? {
        if let Some(bytes) = index.tap_get_raw(&ptr)? { if let Some(rec) = tap_decode_privilege_verified_record(&bytes) { out.push(rec); } }
      }
    }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, block)): Path<(String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blckp/pravth/{}/{}", priv_ins, block))? }))))
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_block(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, block)): Path<(String, u64)>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blckp/pravth/{}/{}", priv_ins, block), &format!("blckpi/pravth/{}/{}", priv_ins, block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_privilege_verified_record(&bytes) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blck/pravth/{}", block))? }))))
}

pub(super) async fn tap_get_privilege_authority_event_by_block(
  Extension(index): Extension<Arc<Index>>,
  Path(block): Path<u64>,
  Query(q): Query<TapListQuery>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let offset = q.offset.unwrap_or(0);
    let max = q.max.unwrap_or(500).min(500);
    let ptrs = index.tap_list_strings(&format!("blck/pravth/{}", block), &format!("blcki/pravth/{}", block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_privilege_verified_record(&bytes) { out.push(rec); } } }
    Ok(Json(serde_json::json!({"result": out})))
  })
}

pub(super) async fn tap_get_privilege_authority_event_by_priv_col_block_length(
  Extension(index): Extension<Arc<Index>>,
  Path((priv_ins, collection_name, block)): Path<(String, String, u64)>,
) -> ServerResult<Json<serde_json::Value>> {
  task::block_in_place(|| {
    let col_key = serde_json::to_string(&collection_name).unwrap_or_else(|_| format!("\"{}\"", collection_name));
    Ok(Json(serde_json::json!({"result": index.tap_get_length(&format!("blckpc/pravth/{}/{}/{}", priv_ins, col_key, block))? })))
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
    let col_key = serde_json::to_string(&collection_name).unwrap_or_else(|_| format!("\"{}\"", collection_name));
    let ptrs = index.tap_list_strings(&format!("blckpc/pravth/{}/{}/{}", priv_ins, col_key, block), &format!("blckpci/pravth/{}/{}/{}", priv_ins, col_key, block), offset, max)?;
    let mut out = Vec::new();
    for p in ptrs { if let Some(bytes) = index.tap_get_raw(&p)? { if let Some(rec) = tap_decode_privilege_verified_record(&bytes) { out.push(rec); } } }
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
