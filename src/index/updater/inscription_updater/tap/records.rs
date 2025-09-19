use serde::{Deserialize, Serialize};

// Bitmap
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct BitmapRecord {
  pub(crate) ownr: String,
  pub(crate) prv: Option<String>,
  pub(crate) bm: u64,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
}

// Deploy
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct DeployRecord {
  pub(crate) tick: String,
  pub(crate) max: String,
  pub(crate) lim: String,
  pub(crate) dec: u32,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) addr: String,
  pub(crate) crsd: bool,
  pub(crate) dmt: bool,
  pub(crate) elem: Option<String>,
  pub(crate) prj: Option<String>,
  pub(crate) dim: Option<String>,
  pub(crate) dt: Option<String>,
  pub(crate) prv: Option<String>,
  pub(crate) dta: Option<String>,
}

// Mint
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct MintRecord {
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) bal: String,
  #[serde(default)]
  pub(crate) tx: Option<String>,
  pub(crate) vo: u32,
  pub(crate) val: String,
  #[serde(default)]
  pub(crate) ins: Option<String>,
  #[serde(default)]
  pub(crate) num: Option<i32>,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  #[serde(default)]
  pub(crate) dmtblck: Option<u32>,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct MintFlatRecord {
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) bal: String,
  #[serde(default)]
  pub(crate) tx: Option<String>,
  pub(crate) vo: u32,
  pub(crate) val: String,
  #[serde(default)]
  pub(crate) ins: Option<String>,
  #[serde(default)]
  pub(crate) num: Option<i32>,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  #[serde(default)]
  pub(crate) dmtblck: Option<u32>,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct MintSuperflatRecord {
  pub(crate) tick: String,
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) bal: String,
  #[serde(default)]
  pub(crate) tx: Option<String>,
  pub(crate) vo: u32,
  pub(crate) val: String,
  #[serde(default)]
  pub(crate) ins: Option<String>,
  #[serde(default)]
  pub(crate) num: Option<i32>,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  #[serde(default)]
  pub(crate) dmtblck: Option<u32>,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

// Transfer (init + executed)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferInitRecord {
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferInitFlatRecord {
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferInitSuperflatRecord {
  pub(crate) tick: String,
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferSendSenderRecord {
  pub(crate) addr: String,
  pub(crate) taddr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferSendReceiverRecord {
  pub(crate) faddr: String,
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) bal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferSendFlatRecord {
  pub(crate) addr: String,
  pub(crate) taddr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tbal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TransferSendSuperflatRecord {
  pub(crate) tick: String,
  pub(crate) addr: String,
  pub(crate) taddr: String,
  pub(crate) blck: u32,
  pub(crate) amt: String,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tbal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
  pub(crate) int: bool,
  #[serde(default)]
  pub(crate) dta: Option<String>,
}

// Trade
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TradeOfferRecord {
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) tick: String,
  pub(crate) amt: String,
  pub(crate) atick: String,
  pub(crate) aamt: String,
  pub(crate) vld: i64,
  pub(crate) trf: String,
  pub(crate) bal: String,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TradeBuySellerRecord {
  pub(crate) addr: String,
  pub(crate) saddr: String,
  pub(crate) blck: u32,
  pub(crate) tick: String,
  pub(crate) amt: String,
  pub(crate) stick: String,
  pub(crate) samt: String,
  pub(crate) fee: String,
  #[serde(default)]
  pub(crate) fee_rcv: Option<String>,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) sins: String,
  pub(crate) snum: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TradeBuyBuyerRecord {
  pub(crate) baddr: String,
  pub(crate) addr: String,
  pub(crate) blck: u32,
  pub(crate) btick: String,
  pub(crate) bamt: String,
  pub(crate) tick: String,
  pub(crate) amt: String,
  pub(crate) fee: String,
  #[serde(default)]
  pub(crate) fee_rcv: Option<String>,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) bins: String,
  pub(crate) bnum: i32,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) fail: bool,
}

// Privilege verify
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PrivilegeVerifiedRecord {
  pub(crate) ownr: String,
  #[serde(default)]
  pub(crate) prv: Option<String>,
  pub(crate) name: String,
  #[serde(rename = "priv")]
  pub(crate) privf: String,
  pub(crate) col: String,
  pub(crate) vrf: String,
  pub(crate) seq: i64,
  pub(crate) slt: String,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
}

// Accumulator
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TapAccumulatorEntry {
  pub(crate) op: String,
  pub(crate) json: serde_json::Value,
  pub(crate) ins: String,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) num: i32,
  pub(crate) ts: u32,
  pub(crate) addr: String,
}

// Token auth
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TokenAuthCreateRecord {
  pub(crate) addr: String,
  pub(crate) auth: Vec<String>,
  pub(crate) sig: serde_json::Value,
  pub(crate) hash: String,
  pub(crate) slt: String,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TokenAuthRedeemRecord {
  pub(crate) addr: String,
  pub(crate) iaddr: String,
  pub(crate) rdm: serde_json::Value,
  pub(crate) sig: serde_json::Value,
  pub(crate) hash: String,
  pub(crate) slt: String,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
}

// Privilege auth
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PrivilegeAuthCreateRecord {
  pub(crate) addr: String,
  pub(crate) auth: serde_json::Value,
  pub(crate) sig: serde_json::Value,
  pub(crate) hash: String,
  pub(crate) slt: String,
  pub(crate) blck: u32,
  pub(crate) tx: String,
  pub(crate) vo: u32,
  pub(crate) val: String,
  pub(crate) ins: String,
  pub(crate) num: i32,
  pub(crate) ts: u32,
}

