use super::super::super::*;
// START TAP-PROOFS
use crate::index::updater::inscription_updater::tap::{
  AuthorityConfigRecord, RewardClaimRecord, StakePositionRecord, TokenAllocationRecord,
  TokenDelegationCancelRecord, TokenLockConsumeRecord, TokenLockRecord,
};
use num_bigint::BigInt;
use sha2::{Digest, Sha256};
// END TAP-PROOFS

// START TAP-PROOFS
#[derive(Clone)]
struct TokenProofLockValidation {
  kind: String,
  tick_key: String,
  tick: String,
  amount: i128,
  allocations: Vec<TokenAllocationRecord>,
  allocation_amount: i128,
  total_amount: i128,
  control: Option<serde_json::Value>,
}

struct TokenProofReleaseValidation {
  lock: TokenLockRecord,
  tick_key: String,
  amount: i128,
  allocations: Vec<TokenAllocationRecord>,
  allocation_amount: i128,
  total_amount: i128,
  target: String,
  action_name: String,
  owner_balance: i128,
  cert_nonce_key: Option<String>,
  cert: Option<serde_json::Value>,
}

// START TAP-DELEGATED-LOCKS
struct TokenDelegatedLockValidation {
  action: serde_json::Value,
  link: TokenAuthCreateRecord,
  normalized: TokenProofLockValidation,
  nonce_key: String,
}

struct TokenDelegatedActionValidation {
  family: String,
  action: serde_json::Value,
  link: TokenAuthCreateRecord,
  join: Option<PerpJoinValidation>,
  position: Option<PerpPositionValidation>,
  nonce_key: String,
}

struct TokenDelegationCancelValidation {
  cancel_key: String,
  auth: String,
  nonce: String,
}

struct TokenStakeValidation {
  id: String,
  auth: String,
  addr: String,
  claim: String,
  tick: String,
  tick_key: String,
  amt: i128,
  tier: String,
  shares: String,
  uh: u32,
  debt: serde_json::Value,
}

#[derive(Clone)]
struct TokenDeployInfo {
  tick: String,
  tick_key: String,
  record: DeployRecord,
}

#[derive(Clone)]
struct SaleTarget {
  tt: String,
  to: String,
}

struct SaleFundValidation {
  config: AuthorityConfigRecord,
  tick: String,
  tick_key: String,
  amount: i128,
}

struct SaleContributionValidation {
  id: String,
  config: AuthorityConfigRecord,
  status: serde_json::Value,
  claim: String,
  tick: String,
  tick_key: String,
  amount: i128,
  allocation: i128,
  existing_amount: i128,
}

struct SaleFinalizeValidation {
  config: AuthorityConfigRecord,
  payment_key: String,
  amount: i128,
}

struct SaleResolveValidation {
  config: AuthorityConfigRecord,
  payment_key: String,
  amount: i128,
  outcome: String,
  reason: Option<String>,
}

#[derive(Clone)]
struct AmmTarget {
  tt: String,
  to: String,
}

#[derive(Clone)]
struct AmmAsset {
  value: serde_json::Value,
  key: String,
}

#[derive(Clone)]
struct AmmSwapCalc {
  amount_in: BigInt,
  amount_out: BigInt,
  gross_fee: BigInt,
  protocol_fee: BigInt,
}

struct AmmAddValidation {
  pool: AuthorityConfigRecord,
  to: AmmTarget,
  auth_target: AmmTarget,
  reference: Option<String>,
  amounts: [BigInt; 2],
  minted: BigInt,
  reserves_after: [BigInt; 2],
  shares_after: BigInt,
}

struct AmmRemoveValidation {
  pool: AuthorityConfigRecord,
  owner: AmmTarget,
  to: AmmTarget,
  auth_target: AmmTarget,
  reference: Option<String>,
  shares: BigInt,
  outputs: [BigInt; 2],
  reserves_after: [BigInt; 2],
  shares_after: BigInt,
}

struct AmmSwapValidation {
  pool: AuthorityConfigRecord,
  side: usize,
  out_side: usize,
  to: AmmTarget,
  auth_target: AmmTarget,
  reference: Option<String>,
  amount_in: BigInt,
  amount_out: BigInt,
  gross_fee: BigInt,
  protocol_fee: BigInt,
  reserves_after: [BigInt; 2],
  shares_after: BigInt,
  mode: String,
}

struct AmmSnapshotValidation {
  pool: AuthorityConfigRecord,
  sid: String,
  ext: serde_json::Value,
  exp: u32,
  asset_index: usize,
  signers: Vec<String>,
}

struct TokenObligationOpenValidation {
  id: String,
  source: serde_json::Value,
  tick: String,
  tick_key: String,
  amount: BigInt,
  claim: serde_json::Value,
  refund: serde_json::Value,
  condition: serde_json::Value,
  refund_after: u32,
  exp: u32,
  ctx: Option<serde_json::Value>,
  authz: Option<serde_json::Value>,
}

struct TokenObligationSettleValidation {
  obligation: serde_json::Value,
  action_name: String,
  target: serde_json::Value,
  tick_key: String,
  amount: BigInt,
}

#[derive(Clone)]
struct PerpAsset {
  value: serde_json::Value,
  key: String,
}

#[derive(Clone)]
struct PerpCertificateValidation {
  price: serde_json::Value,
  cert: serde_json::Value,
  nonce_key: String,
  sequence_key: Option<String>,
  signed: bool,
}

struct PerpJoinValidation {
  value: serde_json::Value,
  owner: String,
  tick: String,
  tick_key: String,
  collateral: BigInt,
  notional: BigInt,
}

struct PerpExternalEvidenceValidation {
  kind: String,
  id: String,
  position_id: String,
  position: Option<serde_json::Value>,
  group: serde_json::Value,
  collateral: serde_json::Value,
  mode: String,
  surface: serde_json::Value,
  ext: serde_json::Value,
  amount: BigInt,
  notional: BigInt,
  leverage: (BigInt, BigInt),
  equity: BigInt,
  bounty: BigInt,
  nonce_key: String,
  sequence_key: String,
  evidence: serde_json::Value,
}

struct PerpActivateValidation {
  group: serde_json::Value,
  certificate: PerpCertificateValidation,
  bounty: BigInt,
}

struct PerpPositionValidation {
  group: serde_json::Value,
  position: serde_json::Value,
  amount: BigInt,
  equity: BigInt,
  certificate: PerpCertificateValidation,
}

struct PerpLiquidateValidation {
  group: serde_json::Value,
  position: serde_json::Value,
  equity: BigInt,
  certificate: PerpCertificateValidation,
  bounty: BigInt,
}

struct PerpSettleValidation {
  group: serde_json::Value,
  certificate: PerpCertificateValidation,
  aggregate: PerpSettlementAggregate,
  total_equity: BigInt,
  bounty: BigInt,
}

struct PerpSettlementAggregate {
  external_fallback: bool,
  long_open_collateral: BigInt,
  short_open_collateral: BigInt,
  long_open_equity: BigInt,
  short_open_equity: BigInt,
  closed_equity: BigInt,
  total_equity: BigInt,
}

struct PerpPayoutValidation {
  group: serde_json::Value,
  position: serde_json::Value,
  equity: BigInt,
  basis: BigInt,
  amount: BigInt,
}
// END TAP-DELEGATED-LOCKS
// END TAP-PROOFS

impl InscriptionUpdater<'_, '_> {
  // START TAP-PROOFS
  fn token_proof_action_tick(action: &serde_json::Value) -> Option<&str> {
    action.get("tick").and_then(|v| v.as_str())
  }

  fn token_proof_post_cancel_settlement_actions(
    items: &[serde_json::Value],
    redeem: &serde_json::Value,
  ) -> bool {
    if !items.is_empty() {
      return false;
    }
    let Some(actions) = redeem.get("actions").and_then(|v| v.as_array()) else {
      return false;
    };
    if actions.is_empty() {
      return false;
    }
    actions.iter().all(|action| {
      let Some(op) = action.get("op").and_then(|v| v.as_str()) else {
        return false;
      };
      matches!(
        op.to_ascii_lowercase().as_str(),
        "claim"
          | "refund"
          | "claim-rwd"
          | "unstake"
          | "claim-sale"
	          | "refund-sale"
	          | "cancel-sale"
	          | "resolve-sale"
	          | "finalize-sale"
	          | "withdraw-sale"
          | "rm-liq"
          | "ob-claim"
          | "ob-refund"
          | "ob-final"
          | "cancel-delegation"
          | "perp-cancel"
          | "perp-refund"
          | "perp-activate"
          | "perp-close"
          | "perp-liquidate"
          | "perp-settle"
          | "perp-claim"
      )
    })
  }

  fn credit_amm_target(
    &mut self,
    target: &AmmTarget,
    tick_key: &str,
    tick: &str,
    amount: &BigInt,
  ) -> bool {
    if amount <= &BigInt::from(0) {
      return true;
    }
    if target.tt == "a" {
      let next = self.tap_get_address_balance_bigint(&target.to, tick_key) + amount;
      return self.tap_put_address_balance_bigint(&target.to, tick_key, tick, &next);
    }
    if target.tt == "h" {
      return self.tap_add_authority_balance_bigint(&target.to, tick_key, amount);
    }
    target.tt == "b"
  }

  fn obligation_source_key(source: &serde_json::Value) -> Option<String> {
    let tt = source.get("tt")?.as_str()?;
    if tt == "a" || tt == "h" {
      return Some(format!("{}/{}", tt, source.get("to")?.as_str()?));
    }
    if tt == "amm" {
      return Some(format!(
        "amm/{}/{}",
        source.get("pid")?.as_str()?,
        source.get("i")?.as_u64()?
      ));
    }
    None
  }

  fn obligation_target_key(target: &serde_json::Value) -> Option<String> {
    let tt = target.get("tt")?.as_str()?;
    if tt == "a" || tt == "h" {
      return Some(format!("{}/{}", tt, target.get("to")?.as_str()?));
    }
    if tt == "amm" {
      return Some(format!(
        "amm/{}/{}",
        target.get("pid")?.as_str()?,
        target.get("i")?.as_u64()?
      ));
    }
    if tt == "b" {
      return Some(format!("b/{}", BURN_ADDRESS));
    }
    None
  }

  fn obligation_lock_key(source: &serde_json::Value, tick_key: &str) -> Option<String> {
    Some(format!(
      "{}/{}",
      Self::obligation_source_key(source)?,
      tick_key
    ))
  }

  fn tap_get_obligation_locked_bigint(
    &mut self,
    source: &serde_json::Value,
    tick_key: &str,
  ) -> Option<BigInt> {
    let key = Self::obligation_lock_key(source, tick_key)?;
    Some(
      self
        .tap_get::<String>(&format!("oll/{}", key))
        .ok()
        .flatten()
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_else(|| BigInt::from(0)),
    )
  }

  fn tap_add_obligation_locked_bigint(
    &mut self,
    source: &serde_json::Value,
    tick_key: &str,
    delta: &BigInt,
  ) -> bool {
    let Some(key) = Self::obligation_lock_key(source, tick_key) else {
      return false;
    };
    let Some(current) = self.tap_get_obligation_locked_bigint(source, tick_key) else {
      return false;
    };
    let next = current + delta;
    if next < BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("oll/{}", key), &next.to_string());
    true
  }

  fn tap_authority_active(&mut self, auth: &str) -> bool {
    self
      .tap_get::<String>(&format!("tains/{}", auth))
      .ok()
      .flatten()
      .is_some()
      && self
        .tap_get::<String>(&format!("tac/{}", auth))
        .ok()
        .flatten()
        .is_none()
  }

  fn amm_external_asset_matches_snapshot(
    asset: &serde_json::Value,
    snapshot: &serde_json::Value,
  ) -> bool {
    if asset.get("ty").and_then(|v| v.as_str()) != Some("ext")
      || asset.get("ns").and_then(|v| v.as_str()) != snapshot.get("ns").and_then(|v| v.as_str())
      || asset.get("cid").and_then(|v| v.as_str()) != snapshot.get("cid").and_then(|v| v.as_str())
      || asset.get("aid").and_then(|v| v.as_str()) != snapshot.get("aid").and_then(|v| v.as_str())
    {
      return false;
    }
    asset
      .get("pool")
      .and_then(|v| v.as_str())
      .map(|pool| Some(pool) == snapshot.get("pool").and_then(|v| v.as_str()))
      .unwrap_or(true)
  }

  fn validate_amm_obligation_context(
    &mut self,
    ctx: Option<&serde_json::Value>,
    pool: &AuthorityConfigRecord,
    side: usize,
    condition: &serde_json::Value,
    block: u32,
  ) -> bool {
    if Self::amm_pool_assets_are_tap(pool) {
      return true;
    }
    if !Self::amm_pool_asset_is_tap(pool, side) {
      return false;
    }
    let other_side = if side == 0 { 1 } else { 0 };
    let Some(ext_asset) = pool.a.get(other_side) else {
      return false;
    };
    let Some(ctx) = ctx else {
      return false;
    };
    let Some(amm) = ctx.get("amm") else {
      return false;
    };
    let Some(sid) = amm.get("sid").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(settlement) = amm.get("set").and_then(|v| v.as_str()) else {
      return false;
    };
    if amm.get("pid").and_then(|v| v.as_str()) != Some(pool.id.as_str())
      || Self::parse_amm_side(amm.get("i").unwrap_or(&serde_json::Value::Null)) != Some(side)
      || !Self::is_amm_ref(sid)
      || !Self::is_amm_ref(settlement)
      || amm.get("h").and_then(|v| v.as_str()) != condition.get("h").and_then(|v| v.as_str())
    {
      return false;
    }
    let Some(snapshot) = self
      .tap_get::<serde_json::Value>(&Self::amm_snapshot_key(&pool.id, sid))
      .ok()
      .flatten()
    else {
      return false;
    };
    let Some(snapshot_exp) = snapshot
      .get("exp")
      .and_then(|v| v.as_u64())
      .and_then(|v| u32::try_from(v).ok())
      .or_else(|| Self::parse_amm_height(snapshot.get("exp").unwrap_or(&serde_json::Value::Null)))
    else {
      return false;
    };
    if snapshot.get("pid").and_then(|v| v.as_str()) != Some(pool.id.as_str())
      || snapshot.get("sid").and_then(|v| v.as_str()) != Some(sid)
      || snapshot
        .get("ai")
        .and_then(|v| v.as_u64())
        .and_then(|v| usize::try_from(v).ok())
        != Some(other_side)
      || block > snapshot_exp
      || !Self::amm_external_asset_matches_snapshot(ext_asset, &snapshot)
    {
      return false;
    }
    for key in ["ns", "cid", "pool", "aid"] {
      if amm.get(key).is_some()
        && amm.get(key).and_then(|v| v.as_str()) != snapshot.get(key).and_then(|v| v.as_str())
      {
        return false;
      }
    }
    true
  }

  fn normalize_obligation_source(
    &mut self,
    source: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    amount: &BigInt,
    tick_key: &str,
    block: u32,
    ctx: Option<&serde_json::Value>,
    condition: &serde_json::Value,
  ) -> Option<(serde_json::Value, String, Option<serde_json::Value>)> {
    let tt = source.get("tt")?.as_str()?.to_lowercase();
    if tt == "a" {
      let link = link?;
      let source_tick = Self::js_to_lowercase(source.get("tick")?.as_str()?);
      let source_tick_key = Self::json_stringify_lower(&source_tick);
      let to = Self::normalize_address(source.get("to")?.as_str()?);
      if to != link.addr || source_tick_key != tick_key || !self.is_valid_bitcoin_address(&to) {
        return None;
      }
      let source_value = serde_json::json!({ "tt": "a", "to": to, "tick": source_tick });
      let locked = self.tap_get_obligation_locked_bigint(&source_value, tick_key)?;
      let available = self.tap_get_address_balance_bigint(&to, tick_key)
        - self.tap_get_transferable_bigint(&to, tick_key)
        - self.tap_get_locked_bigint(&to, tick_key)
        - locked;
      if available < amount.clone() {
        return None;
      }
      return Some((source_value, source_tick, None));
    }
    if tt == "h" {
      let link = link?;
      let to = source.get("to")?.as_str()?;
      let source_tick = Self::js_to_lowercase(source.get("tick")?.as_str()?);
      let source_tick_key = Self::json_stringify_lower(&source_tick);
      if to != link.ins || source_tick_key != tick_key || !self.tap_authority_active(to) {
        return None;
      }
      let source_value = serde_json::json!({ "tt": "h", "to": to, "tick": source_tick });
      let locked = self.tap_get_obligation_locked_bigint(&source_value, tick_key)?;
      if self.tap_get_authority_balance_bigint(to, tick_key) - locked < *amount {
        return None;
      }
      return Some((
        source_value,
        source_tick,
        Some(serde_json::json!({ "auth": to })),
      ));
    }
    if tt == "amm" {
      let link = link?;
      let pid = source.get("pid")?.as_str()?;
      let side = Self::parse_amm_side(source.get("i")?)?;
      let pool = self.get_amm_pool(pid)?;
      if pool.k != "amm"
        || pool.p
        || !Self::amm_pool_asset_is_tap(&pool, side)
        || pool.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
        || !self.tap_authority_active(link.ins.as_str())
        || Self::amm_pool_tick_key(&pool, side).as_deref() != Some(tick_key)
        || !self.validate_amm_obligation_context(ctx, &pool, side, condition, block)
      {
        return None;
      }
      let source_value = serde_json::json!({ "tt": "amm", "pid": pool.id.clone(), "i": side });
      let locked = self.tap_get_obligation_locked_bigint(&source_value, tick_key)?;
      let reserves = Self::amm_pool_reserves(&pool)?;
      let authority_balance = self.tap_get_authority_balance_bigint(&pool.id, tick_key);
      if reserves[side].clone() - locked.clone() < *amount || authority_balance - locked < *amount {
        return None;
      }
      let tick = Self::js_to_lowercase(pool.a.get(side)?.get("tick")?.as_str()?);
      return Some((
        source_value,
        tick,
        Some(serde_json::json!({ "auth": link.ins })),
      ));
    }
    None
  }

  fn normalize_obligation_condition(condition: &serde_json::Value) -> Option<serde_json::Value> {
    let ty = condition
      .get("ty")
      .or_else(|| condition.get("type"))?
      .as_str()?
      .to_lowercase();
    let hash = condition
      .get("h")
      .or_else(|| condition.get("hash"))?
      .as_str()?;
    if !matches!(ty.as_str(), "hash" | "hashlock") || !Self::tap_is_valid_sha256_hex(hash) {
      return None;
    }
    Some(serde_json::json!({ "ty": "hash", "h": hash.to_lowercase() }))
  }

  fn normalize_obligation_target(
    &mut self,
    target: &serde_json::Value,
    tick_key: &str,
    ctx: Option<&serde_json::Value>,
    condition: &serde_json::Value,
    block: u32,
  ) -> Option<serde_json::Value> {
    let tt = target.get("tt")?.as_str()?.to_lowercase();
    if tt == "a" {
      let to = Self::normalize_address(target.get("to")?.as_str()?);
      if !self.is_valid_bitcoin_address(&to) {
        return None;
      }
      return Some(serde_json::json!({ "tt": "a", "to": to }));
    }
    if tt == "h" {
      let to = target.get("to")?.as_str()?;
      self.tap_get_authority_config(to)?;
      return Some(serde_json::json!({ "tt": "h", "to": to }));
    }
    if tt == "b" {
      if target.get("to").is_some()
        && target.get("to").and_then(|v| v.as_str()) != Some(BURN_ADDRESS)
      {
        return None;
      }
      return Some(serde_json::json!({ "tt": "b", "to": BURN_ADDRESS }));
    }
    if tt == "amm" {
      let pid = target.get("pid")?.as_str()?;
      let side = Self::parse_amm_side(target.get("i")?)?;
      let pool = self.get_amm_pool(pid)?;
      if !Self::amm_pool_asset_is_tap(&pool, side)
        || Self::amm_pool_tick_key(&pool, side).as_deref() != Some(tick_key)
        || !self.validate_amm_obligation_context(ctx, &pool, side, condition, block)
      {
        return None;
      }
      return Some(serde_json::json!({ "tt": "amm", "pid": pool.id, "i": side }));
    }
    None
  }

  fn obligation_targets_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    Self::obligation_target_key(left) == Self::obligation_target_key(right)
  }

  fn validate_obligation_open_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    action_index: usize,
    block: u32,
  ) -> Option<TokenObligationOpenValidation> {
    if !action.is_object()
      || !action.get("src")?.is_object()
      || action.get("amt").is_none()
      || !action.get("cl")?.is_object()
      || !action.get("rf")?.is_object()
    {
      return None;
    }
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation)
      && action.get("amt")?.is_number()
    {
      return None;
    }

    let mut source_tick = action
      .get("src")
      .and_then(|src| src.get("tick"))
      .and_then(|v| v.as_str())
      .map(Self::js_to_lowercase);
    if source_tick.is_none() && action.get("src")?.get("tt").and_then(|v| v.as_str()) == Some("amm")
    {
      let pool = self.get_amm_pool(action.get("src")?.get("pid")?.as_str()?)?;
      let side = Self::parse_amm_side(action.get("src")?.get("i")?)?;
      if Self::amm_pool_asset_is_tap(&pool, side) {
        source_tick = pool
          .a
          .get(side)?
          .get("tick")?
          .as_str()
          .map(Self::js_to_lowercase);
      }
    }
    let token = self.token_proof_get_deploy(&source_tick?)?;
    let amount =
      self.token_proof_resolve_protocol_amount_bigint(action.get("amt")?, &token.record)?;
    let exp = Self::token_proof_storage_height(action.get("exp"))?;
    let refund_after = Self::token_proof_storage_height(action.get("ra"))?;
    let condition = Self::normalize_obligation_condition(action.get("cond")?)?;
    if block > exp {
      return None;
    }

    let ctx = match action.get("ctx") {
      Some(value) => {
        let normalized = Self::normalize_token_proof_metadata_object(value, 0)?;
        if normalized
          .get("ref")
          .and_then(|v| v.as_str())
          .map(|reference| !Self::token_proof_safe_id(reference, 128))
          .unwrap_or(false)
        {
          return None;
        }
        Some(normalized)
      }
      None => None,
    };

    let (source, tick, authz) = self.normalize_obligation_source(
      action.get("src")?,
      link,
      &amount,
      &token.tick_key,
      block,
      ctx.as_ref(),
      &condition,
    )?;
    let claim = self.normalize_obligation_target(
      action.get("cl")?,
      &token.tick_key,
      ctx.as_ref(),
      &condition,
      block,
    )?;
    let refund = self.normalize_obligation_target(
      action.get("rf")?,
      &token.tick_key,
      ctx.as_ref(),
      &condition,
      block,
    )?;
    let source_refund = if source.get("tt").and_then(|v| v.as_str()) == Some("amm") {
      serde_json::json!({
        "tt": "amm",
        "pid": source.get("pid")?.as_str()?,
        "i": source.get("i")?.as_u64()?
      })
    } else {
      serde_json::json!({
        "tt": source.get("tt")?.as_str()?,
        "to": source.get("to")?.as_str()?
      })
    };
    if !Self::obligation_targets_equal(&source_refund, &refund) {
      return None;
    }

    let id = Self::tap_token_proof_lock_id(inscription, action_index);
    if self
      .tap_get::<serde_json::Value>(&format!("ob/{}", id))
      .ok()
      .flatten()
      .is_some()
      || self
        .tap_get::<serde_json::Value>(&format!("obc/{}", id))
        .ok()
        .flatten()
        .is_some()
    {
      return None;
    }

    Some(TokenObligationOpenValidation {
      id,
      source,
      tick,
      tick_key: token.tick_key,
      amount,
      claim,
      refund,
      condition,
      refund_after,
      exp,
      ctx,
      authz,
    })
  }

  fn get_obligation_record(&mut self, obligation_id: &str) -> Option<serde_json::Value> {
    self
      .tap_get::<serde_json::Value>(&format!("ob/{}", obligation_id))
      .ok()
      .flatten()
  }

  fn validate_obligation_settle_action(
    &mut self,
    action: &serde_json::Value,
    block: u32,
  ) -> Option<TokenObligationSettleValidation> {
    if !action.is_object()
      || action.get("ob").and_then(|v| v.as_str()).is_none()
      || action.get("fee").is_some()
    {
      return None;
    }
    let obligation_id = action.get("ob")?.as_str()?;
    if self
      .tap_get::<serde_json::Value>(&format!("obc/{}", obligation_id))
      .ok()
      .flatten()
      .is_some()
    {
      return None;
    }
    let obligation = self.get_obligation_record(obligation_id)?;
    if obligation.get("st").and_then(|v| v.as_str()) != Some("open") {
      return None;
    }
    let action_name = action.get("op")?.as_str()?.to_lowercase();
    let target = if action_name == "ob-claim" || action_name == "ob-final" {
      if block >= Self::token_proof_storage_height(obligation.get("ra"))?
        || obligation.get("cond")?.get("ty").and_then(|v| v.as_str()) != Some("hash")
        || action.get("preimage").is_none()
        || Self::tap_hash_proof_preimage(action.get("preimage")?).to_lowercase()
          != obligation.get("cond")?.get("h")?.as_str()?.to_lowercase()
      {
        return None;
      }
      obligation.get("cl")?.clone()
    } else if action_name == "ob-refund" {
      if block < Self::token_proof_storage_height(obligation.get("ra"))? {
        return None;
      }
      obligation.get("rf")?.clone()
    } else {
      return None;
    };
    let tick = Self::js_to_lowercase(obligation.get("tick")?.as_str()?);
    let tick_key = Self::json_stringify_lower(&tick);
    let amount = obligation.get("amt")?.as_str()?.parse::<BigInt>().ok()?;
    if amount <= BigInt::from(0) {
      return None;
    }
    Some(TokenObligationSettleValidation {
      obligation,
      action_name,
      target,
      tick_key,
      amount,
    })
  }

  fn process_obligation_open_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) =
      self.validate_obligation_open_action(action, link, inscription, action_index, block)
    else {
      return false;
    };
    if !self.tap_add_obligation_locked_bigint(
      &normalized.source,
      &normalized.tick_key,
      &normalized.amount,
    ) {
      return false;
    }

    let mut record = serde_json::json!({
      "id": normalized.id,
      "op": "ob-open",
      "src": normalized.source,
      "tick": normalized.tick,
      "amt": normalized.amount.to_string(),
      "cl": normalized.claim,
      "rf": normalized.refund,
      "cond": normalized.condition,
      "ra": normalized.refund_after,
      "exp": normalized.exp,
      "st": "open",
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    if let Some(ctx) = normalized.ctx {
      record["ctx"] = ctx;
    }
    if let Some(authz) = normalized.authz {
      record["az"] = authz;
    }

    let _ = self.tap_put(&format!("ob/{}", normalized.id), &record);
    if let Ok(list_len) = self.tap_set_list_record("obl", "obli", &record) {
      if let Some(source_key) = Self::obligation_source_key(&record["src"]) {
        let _ = self.tap_set_list_record(
          &format!("obsrc/{}", source_key),
          &format!("obsrci/{}", source_key),
          &record,
        );
      }
      if let Some(claim_key) = Self::obligation_target_key(&record["cl"]) {
        let _ = self.tap_set_list_record(
          &format!("oba/{}", claim_key),
          &format!("obai/{}", claim_key),
          &record,
        );
      }
      if let Some(refund_key) = Self::obligation_target_key(&record["rf"]) {
        let _ = self.tap_set_list_record(
          &format!("oba/{}", refund_key),
          &format!("obai/{}", refund_key),
          &record,
        );
      }
      if let Some(ctx_ref) = record
        .get("ctx")
        .and_then(|ctx| ctx.get("ref"))
        .and_then(|v| v.as_str())
      {
        let _ = self.tap_set_list_record(
          &format!("obctx/{}", ctx_ref),
          &format!("obctxi/{}", ctx_ref),
          &record,
        );
      }
      let ptr = format!("obli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/ob/{}", transaction),
        &format!("txi/ob/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/ob/{}", block),
        &format!("blcki/ob/{}", block),
        &ptr,
      );
    }
    true
  }

  fn debit_obligation_source(
    &mut self,
    obligation: &serde_json::Value,
    tick_key: &str,
    amount: &BigInt,
  ) -> bool {
    let Some(source) = obligation.get("src") else {
      return false;
    };
    if !self.tap_add_obligation_locked_bigint(source, tick_key, &(-amount.clone())) {
      return false;
    }
    let Some(tt) = source.get("tt").and_then(|v| v.as_str()) else {
      return false;
    };
    if tt == "a" {
      let Some(to) = source.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      let current = self.tap_get_address_balance_bigint(to, tick_key);
      if current < *amount {
        return false;
      }
      let _ = self.tap_put(
        &format!("b/{}/{}", to, tick_key),
        &(current - amount).to_string(),
      );
      return true;
    }
    if tt == "h" {
      let Some(to) = source.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      return self.tap_add_authority_balance_bigint(to, tick_key, &(-amount.clone()));
    }
    if tt == "amm" {
      let Some(pid) = source.get("pid").and_then(|v| v.as_str()) else {
        return false;
      };
      let Some(side) = source
        .get("i")
        .and_then(|v| v.as_u64())
        .and_then(|v| usize::try_from(v).ok())
      else {
        return false;
      };
      let Some(mut pool) = self.get_amm_pool(pid) else {
        return false;
      };
      if !Self::amm_pool_asset_is_tap(&pool, side)
        || Self::amm_pool_tick_key(&pool, side).as_deref() != Some(tick_key)
      {
        return false;
      }
      let Some(mut reserves) = Self::amm_pool_reserves(&pool) else {
        return false;
      };
      if reserves[side] < *amount
        || !self.tap_add_authority_balance_bigint(&pool.id, tick_key, &(-amount.clone()))
      {
        return false;
      }
      reserves[side] = reserves[side].clone() - amount;
      let Some(shares) = Self::amm_pool_shares(&pool) else {
        return false;
      };
      Self::set_amm_pool_state(&mut pool, &reserves, &shares);
      self.put_amm_pool(&pool);
      return true;
    }
    false
  }

  fn credit_obligation_target(
    &mut self,
    target: &serde_json::Value,
    tick_key: &str,
    tick: &str,
    amount: &BigInt,
  ) -> bool {
    if amount <= &BigInt::from(0) {
      return true;
    }
    let Some(tt) = target.get("tt").and_then(|v| v.as_str()) else {
      return false;
    };
    if tt == "a" {
      let Some(to) = target.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      let next = self.tap_get_address_balance_bigint(to, tick_key) + amount;
      return self.tap_put_address_balance_bigint(to, tick_key, tick, &next);
    }
    if tt == "h" {
      let Some(to) = target.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      return self.tap_add_authority_balance_bigint(to, tick_key, amount);
    }
    if tt == "amm" {
      let Some(pid) = target.get("pid").and_then(|v| v.as_str()) else {
        return false;
      };
      let Some(side) = target
        .get("i")
        .and_then(|v| v.as_u64())
        .and_then(|v| usize::try_from(v).ok())
      else {
        return false;
      };
      let Some(mut pool) = self.get_amm_pool(pid) else {
        return false;
      };
      if !Self::amm_pool_asset_is_tap(&pool, side)
        || Self::amm_pool_tick_key(&pool, side).as_deref() != Some(tick_key)
        || !self.tap_add_authority_balance_bigint(&pool.id, tick_key, amount)
      {
        return false;
      }
      let Some(mut reserves) = Self::amm_pool_reserves(&pool) else {
        return false;
      };
      reserves[side] = reserves[side].clone() + amount;
      let Some(shares) = Self::amm_pool_shares(&pool) else {
        return false;
      };
      Self::set_amm_pool_state(&mut pool, &reserves, &shares);
      self.put_amm_pool(&pool);
      return true;
    }
    tt == "b"
  }

  fn can_debit_obligation_source(
    &mut self,
    obligation: &serde_json::Value,
    tick_key: &str,
    amount: &BigInt,
  ) -> bool {
    let Some(source) = obligation.get("src") else {
      return false;
    };
    let Some(locked) = self.tap_get_obligation_locked_bigint(source, tick_key) else {
      return false;
    };
    if locked < *amount {
      return false;
    }
    let Some(tt) = source.get("tt").and_then(|v| v.as_str()) else {
      return false;
    };
    if tt == "a" {
      let Some(to) = source.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      return self.tap_get_address_balance_bigint(to, tick_key) >= *amount;
    }
    if tt == "h" {
      let Some(to) = source.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      return self.tap_get_authority_balance_bigint(to, tick_key) >= *amount;
    }
    if tt == "amm" {
      let Some(pid) = source.get("pid").and_then(|v| v.as_str()) else {
        return false;
      };
      let Some(side) = source
        .get("i")
        .and_then(|v| v.as_u64())
        .and_then(|v| usize::try_from(v).ok())
      else {
        return false;
      };
      let Some(pool) = self.get_amm_pool(pid) else {
        return false;
      };
      let Some(reserves) = Self::amm_pool_reserves(&pool) else {
        return false;
      };
      return Self::amm_pool_asset_is_tap(&pool, side)
        && Self::amm_pool_tick_key(&pool, side).as_deref() == Some(tick_key)
        && reserves[side] >= *amount
        && self.tap_get_authority_balance_bigint(&pool.id, tick_key) >= *amount;
    }
    false
  }

  fn can_credit_obligation_target(
    &mut self,
    target: &serde_json::Value,
    tick_key: &str,
    amount: &BigInt,
  ) -> bool {
    if amount <= &BigInt::from(0) {
      return true;
    }
    let Some(tt) = target.get("tt").and_then(|v| v.as_str()) else {
      return false;
    };
    if tt == "a" || tt == "b" {
      return true;
    }
    if tt == "h" {
      let Some(to) = target.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      return self.tap_get_authority_config(to).is_some();
    }
    if tt == "amm" {
      let Some(pid) = target.get("pid").and_then(|v| v.as_str()) else {
        return false;
      };
      let Some(side) = target
        .get("i")
        .and_then(|v| v.as_u64())
        .and_then(|v| usize::try_from(v).ok())
      else {
        return false;
      };
      let Some(pool) = self.get_amm_pool(pid) else {
        return false;
      };
      return Self::amm_pool_asset_is_tap(&pool, side)
        && Self::amm_pool_tick_key(&pool, side).as_deref() == Some(tick_key);
    }
    false
  }

  fn process_obligation_settle_action(
    &mut self,
    action: &serde_json::Value,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_obligation_settle_action(action, block) else {
      return false;
    };
    let tick = normalized
      .obligation
      .get("tick")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    if !self.can_debit_obligation_source(
      &normalized.obligation,
      &normalized.tick_key,
      &normalized.amount,
    ) || !self.can_credit_obligation_target(
      &normalized.target,
      &normalized.tick_key,
      &normalized.amount,
    ) || !self.debit_obligation_source(
      &normalized.obligation,
      &normalized.tick_key,
      &normalized.amount,
    ) || !self.credit_obligation_target(
      &normalized.target,
      &normalized.tick_key,
      &tick,
      &normalized.amount,
    ) {
      return false;
    }

    let mut obligation = normalized.obligation.clone();
    obligation["st"] = serde_json::Value::String(if normalized.action_name == "ob-refund" {
      "refunded".to_string()
    } else if normalized.action_name == "ob-final" {
      "finalized".to_string()
    } else {
      "claimed".to_string()
    });
    if let Some(obligation_id) = action.get("ob").and_then(|v| v.as_str()) {
      let _ = self.tap_put(&format!("ob/{}", obligation_id), &obligation);
    }

    let consume = serde_json::json!({
      "ob": action.get("ob").and_then(|v| v.as_str()).unwrap_or(""),
      "action": normalized.action_name,
      "src": normalized.obligation.get("src").cloned().unwrap_or(serde_json::Value::Null),
      "target": normalized.target,
      "tick": tick,
      "amt": normalized.amount.to_string(),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    if let Some(obligation_id) = action.get("ob").and_then(|v| v.as_str()) {
      let _ = self.tap_put(&format!("obc/{}", obligation_id), &consume);
    }
    if let Ok(list_len) = self.tap_set_list_record("obcl", "obcli", &consume) {
      let ptr = format!("obcli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/obc/{}", transaction),
        &format!("txi/obc/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/obc/{}", block),
        &format!("blcki/obc/{}", block),
        &ptr,
      );
    }
    true
  }

  fn debit_amm_account(&mut self, address: &str, tick_key: &str, amount: &BigInt) -> bool {
    let balance = self.tap_get_address_balance_bigint(address, tick_key);
    let available = balance.clone()
      - self.tap_get_transferable_bigint(address, tick_key)
      - self.tap_get_locked_bigint(address, tick_key)
      - BigInt::from(self.tap_get_account_obligation_locked_amount(address, tick_key));
    if available < *amount {
      return false;
    }
    let next = balance - amount;
    if next < BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("b/{}/{}", address, tick_key), &next.to_string());
    true
  }

  fn record_amm_event(&mut self, event: &serde_json::Value, transaction: &str, block: u32) {
    if let Some(pid) = event.get("pid").and_then(|v| v.as_str()) {
      if let Ok(list_len) =
        self.tap_set_list_record(&format!("amme/{}", pid), &format!("ammei/{}", pid), event)
      {
        let _ = self.tap_set_list_record(
          &format!("ammbe/{}", block),
          &format!("ammbei/{}", block),
          event,
        );
        let ptr = format!("ammei/{}/{}", pid, list_len - 1);
        let _ = self.tap_set_list_record(
          &format!("tx/amm/{}", transaction),
          &format!("txi/amm/{}", transaction),
          &ptr,
        );
      }
    }
  }

  fn mark_amm_ref(
    &mut self,
    pool_id: &str,
    target: &AmmTarget,
    reference: &Option<String>,
    event_id: &str,
  ) {
    if let Some(reference) = reference {
      let _ = self.tap_put(
        &Self::amm_ref_key(pool_id, target, reference),
        &event_id.to_string(),
      );
    }
  }

  fn process_amm_add_liquidity_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) = self.normalize_amm_add_liquidity_action(action, link, block, None)
    else {
      return false;
    };
    let Some(link) = link else {
      return false;
    };
    let mut pool = normalized.pool;
    let Some(tick0) = Self::amm_pool_tick_key(&pool, 0) else {
      return false;
    };
    let Some(tick1) = Self::amm_pool_tick_key(&pool, 1) else {
      return false;
    };
    if !self.debit_amm_account(&link.addr, &tick0, &normalized.amounts[0])
      || !self.debit_amm_account(&link.addr, &tick1, &normalized.amounts[1])
      || !self.tap_add_authority_balance_bigint(&pool.id, &tick0, &normalized.amounts[0])
      || !self.tap_add_authority_balance_bigint(&pool.id, &tick1, &normalized.amounts[1])
      || !self.add_amm_lp_shares(&pool.id, &normalized.to, &normalized.minted)
    {
      return false;
    }
    Self::set_amm_pool_state(
      &mut pool,
      &normalized.reserves_after,
      &normalized.shares_after,
    );
    self.put_amm_pool(&pool);
    let event_id = format!("ammadd:{}:{}", inscription, action_index);
    self.mark_amm_ref(
      &pool.id,
      &normalized.auth_target,
      &normalized.reference,
      &event_id,
    );
    let event = serde_json::json!({
      "id": event_id,
      "pid": pool.id,
      "op": "add-liq",
      "addr": link.addr,
      "tt": normalized.to.tt,
      "to": normalized.to.to,
      "amts": [normalized.amounts[0].to_string(), normalized.amounts[1].to_string()],
      "sh": normalized.minted.to_string(),
      "r": pool.r,
      "tsh": pool.sh,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp,
      "fail": false
    });
    self.record_amm_event(&event, transaction, block);
    true
  }

  fn process_amm_remove_liquidity_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) =
      self.normalize_amm_remove_liquidity_action(action, link, block, None, None)
    else {
      return false;
    };
    let mut pool = normalized.pool;
    let Some(tick0) = Self::amm_pool_tick_key(&pool, 0) else {
      return false;
    };
    let Some(tick1) = Self::amm_pool_tick_key(&pool, 1) else {
      return false;
    };
    let Some(label0) = Self::amm_pool_tick(&pool, 0) else {
      return false;
    };
    let Some(label1) = Self::amm_pool_tick(&pool, 1) else {
      return false;
    };
    if !self.add_amm_lp_shares(&pool.id, &normalized.owner, &(-normalized.shares.clone()))
      || !self.tap_add_authority_balance_bigint(&pool.id, &tick0, &(-normalized.outputs[0].clone()))
      || !self.tap_add_authority_balance_bigint(&pool.id, &tick1, &(-normalized.outputs[1].clone()))
      || !self.credit_amm_target(&normalized.to, &tick0, &label0, &normalized.outputs[0])
      || !self.credit_amm_target(&normalized.to, &tick1, &label1, &normalized.outputs[1])
    {
      return false;
    }
    Self::set_amm_pool_state(
      &mut pool,
      &normalized.reserves_after,
      &normalized.shares_after,
    );
    self.put_amm_pool(&pool);
    let event_id = format!("ammrm:{}:{}", inscription, action_index);
    self.mark_amm_ref(
      &pool.id,
      &normalized.auth_target,
      &normalized.reference,
      &event_id,
    );
    let event = serde_json::json!({
      "id": event_id,
      "pid": pool.id,
      "op": "rm-liq",
      "tt": normalized.owner.tt,
      "to": normalized.owner.to,
      "rtt": normalized.to.tt,
      "rto": normalized.to.to,
      "out": [normalized.outputs[0].to_string(), normalized.outputs[1].to_string()],
      "sh": normalized.shares.to_string(),
      "r": pool.r,
      "tsh": pool.sh,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp,
      "fail": false
    });
    self.record_amm_event(&event, transaction, block);
    true
  }

  fn process_amm_swap_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) = self.normalize_amm_swap_action(action, link, block, None) else {
      return false;
    };
    let Some(link) = link else {
      return false;
    };
    let mut pool = normalized.pool;
    let Some(in_tick) = Self::amm_pool_tick_key(&pool, normalized.side) else {
      return false;
    };
    let Some(out_tick) = Self::amm_pool_tick_key(&pool, normalized.out_side) else {
      return false;
    };
    let Some(in_label) = Self::amm_pool_tick(&pool, normalized.side) else {
      return false;
    };
    let Some(out_label) = Self::amm_pool_tick(&pool, normalized.out_side) else {
      return false;
    };
    if !self.debit_amm_account(&link.addr, &in_tick, &normalized.amount_in)
      || !self.tap_add_authority_balance_bigint(
        &pool.id,
        &in_tick,
        &(normalized.amount_in.clone() - normalized.protocol_fee.clone()),
      )
      || !self.tap_add_authority_balance_bigint(
        &pool.id,
        &out_tick,
        &(-normalized.amount_out.clone()),
      )
      || !self.credit_amm_target(
        &normalized.to,
        &out_tick,
        &out_label,
        &normalized.amount_out,
      )
    {
      return false;
    }
    if normalized.protocol_fee > BigInt::from(0) {
      let Some(pp) = pool.pp.clone() else {
        return false;
      };
      let Some(target) = self.validate_amm_target(&pp, true) else {
        return false;
      };
      if !self.credit_amm_target(&target, &in_tick, &in_label, &normalized.protocol_fee) {
        return false;
      }
    }
    Self::set_amm_pool_state(
      &mut pool,
      &normalized.reserves_after,
      &normalized.shares_after,
    );
    self.put_amm_pool(&pool);
    let event_id = format!("ammswp:{}:{}", inscription, action_index);
    self.mark_amm_ref(
      &pool.id,
      &normalized.auth_target,
      &normalized.reference,
      &event_id,
    );
    let event = serde_json::json!({
      "id": event_id,
      "pid": pool.id,
      "op": "swap",
      "m": normalized.mode,
      "addr": link.addr,
      "i": normalized.side,
      "tt": normalized.to.tt,
      "to": normalized.to.to,
      "ain": normalized.amount_in.to_string(),
      "out": normalized.amount_out.to_string(),
      "fee": normalized.gross_fee.to_string(),
      "pf": normalized.protocol_fee.to_string(),
      "r": pool.r,
      "sh": pool.sh,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp,
      "fail": false
    });
    self.record_amm_event(&event, transaction, block);
    true
  }

  fn find_amm_external_asset(
    pool: &AuthorityConfigRecord,
    ext: &serde_json::Value,
  ) -> Option<usize> {
    for (i, asset) in pool.a.iter().enumerate() {
      if asset.get("ty").and_then(|v| v.as_str()) == Some("ext")
        && asset.get("ns").and_then(|v| v.as_str()) == ext.get("ns").and_then(|v| v.as_str())
        && asset.get("cid").and_then(|v| v.as_str()) == ext.get("cid").and_then(|v| v.as_str())
        && asset.get("aid").and_then(|v| v.as_str()) == ext.get("aid").and_then(|v| v.as_str())
      {
        if let Some(pool_id) = asset.get("pool").and_then(|v| v.as_str()) {
          if Some(pool_id) != ext.get("pool").and_then(|v| v.as_str()) {
            continue;
          }
        }
        return Some(i);
      }
    }
    None
  }

  fn normalize_amm_external_snapshot_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
    timestamp: u32,
  ) -> Option<AmmSnapshotValidation> {
    let link = link?;
    let auth = action.get("auth")?.as_str()?;
    let sid = action.get("sid")?.as_str()?;
    if !Self::is_amm_ref(sid)
      || action.get("ext")?.as_object().is_none()
      || action.get("sigs")?.as_array()?.is_empty()
      || action.get("salt")?.as_str()?.len() > 128
      || Self::amm_action_expired(action, block)
    {
      return None;
    }
    let pool = self.get_amm_pool(auth)?;
    let att = pool.att.clone()?;
    if pool.k != "amm"
      || pool.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
      || self
        .tap_get::<serde_json::Value>(&Self::amm_snapshot_key(&pool.id, sid))
        .ok()
        .flatten()
        .is_some()
    {
      return None;
    }
    let ext_raw = action.get("ext")?;
    let ns_raw = ext_raw.get("ns")?.as_str()?;
    let cid_raw = ext_raw.get("cid")?.as_str()?;
    let pool_raw = ext_raw.get("pool")?.as_str()?;
    let aid_raw = ext_raw.get("aid")?.as_str()?;
    if !Self::token_proof_safe_id(ns_raw, 128)
      || !Self::token_proof_safe_id(cid_raw, 128)
      || !Self::token_proof_safe_id(pool_raw, 128)
      || !Self::token_proof_safe_id(aid_raw, 128)
    {
      return None;
    }
    let ext = serde_json::json!({
      "ns": ns_raw.to_lowercase(),
      "cid": cid_raw.to_lowercase(),
      "pool": pool_raw.to_lowercase(),
      "aid": aid_raw.to_lowercase(),
      "res": ext_raw.get("res")?.as_str()?,
      "h": ext_raw.get("h")?.as_str()?,
      "ts": ext_raw.get("ts")?.as_str()?
    });
    let _reserve = Self::parse_amm_uint_value(ext.get("res")?, true)?;
    let _height = Self::parse_amm_uint_value(ext.get("h")?, true)?;
    let ext_ts = Self::parse_amm_uint_value(ext.get("ts")?, true)?;
    let exp = Self::parse_amm_height(action.get("exp")?)?;
    if BigInt::from(timestamp) < ext_ts {
      return None;
    }
    let max_age = att.get("max_age")?.as_u64()?;
    if BigInt::from(timestamp) - ext_ts > BigInt::from(max_age) * BigInt::from(600) {
      return None;
    }
    let asset_index = Self::find_amm_external_asset(&pool, &ext)?;
    let signer_values = att.get("signers")?.as_array()?;
    let signer_set: std::collections::HashSet<String> = signer_values
      .iter()
      .filter_map(|v| v.as_str().map(|s| s.to_string()))
      .collect();
    let threshold = att.get("thr")?.as_u64()? as usize;
    let salt = action.get("salt")?.as_str()?;
    let message = serde_json::Value::Array(vec![
      serde_json::Value::String("tap-amm-external-liquidity-v1".to_string()),
      serde_json::Value::String(pool.id.clone()),
      serde_json::Value::String(sid.to_string()),
      ext.get("ns")?.clone(),
      ext.get("cid")?.clone(),
      ext.get("pool")?.clone(),
      ext.get("aid")?.clone(),
      ext.get("res")?.clone(),
      ext.get("h")?.clone(),
      ext.get("ts")?.clone(),
      action.get("exp")?.clone(),
    ]);
    let msg_hash = Self::build_sha256_json_plus_salt(&message, salt);
    let msg_hash_hex = hex::encode(msg_hash);
    let mut valid_signers = std::collections::HashSet::new();
    for entry in action.get("sigs")?.as_array()? {
      let hash_str = entry.get("hash")?.as_str()?;
      if hash_str.to_lowercase() != msg_hash_hex {
        return None;
      }
      let (ok, _, pubkey) =
        self.verify_sig_obj_against_msg_with_hash(entry.get("sig")?, hash_str, &msg_hash)?;
      let signer = Self::token_proof_compressed_delegation_pubkey(&pubkey)?;
      if ok && signer_set.contains(&signer) {
        valid_signers.insert(signer);
      }
    }
    if valid_signers.len() < threshold {
      return None;
    }
    let mut signers: Vec<String> = valid_signers.into_iter().collect();
    signers.sort();
    Some(AmmSnapshotValidation {
      pool,
      sid: sid.to_string(),
      ext,
      exp,
      asset_index,
      signers,
    })
  }

  fn process_amm_external_snapshot_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) =
      self.normalize_amm_external_snapshot_action(action, link, block, timestamp)
    else {
      return false;
    };
    let mut pool = normalized.pool;
    let record = serde_json::json!({
      "pid": pool.id,
      "sid": normalized.sid,
      "ns": normalized.ext.get("ns").cloned().unwrap_or(serde_json::Value::Null),
      "cid": normalized.ext.get("cid").cloned().unwrap_or(serde_json::Value::Null),
      "pool": normalized.ext.get("pool").cloned().unwrap_or(serde_json::Value::Null),
      "aid": normalized.ext.get("aid").cloned().unwrap_or(serde_json::Value::Null),
      "res": normalized.ext.get("res").cloned().unwrap_or(serde_json::Value::Null),
      "h": normalized.ext.get("h").cloned().unwrap_or(serde_json::Value::Null),
      "ets": normalized.ext.get("ts").cloned().unwrap_or(serde_json::Value::Null),
      "exp": normalized.exp,
      "ai": normalized.asset_index,
      "sig": normalized.signers,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_put(&Self::amm_snapshot_key(&pool.id, &normalized.sid), &record);
    let _ = self.tap_set_list_record(
      &format!("ammssl/{}", pool.id),
      &format!("ammssli/{}", pool.id),
      &record,
    );
    pool.xs = Some(record.clone());
    self.put_amm_pool(&pool);
    let event = serde_json::json!({
      "id": format!("ammsyn:{}:{}", inscription, action_index),
      "pid": pool.id,
      "op": "sync-ext",
      "sid": normalized.sid,
      "ai": normalized.asset_index,
      "res": normalized.ext.get("res").cloned().unwrap_or(serde_json::Value::Null),
      "h": normalized.ext.get("h").cloned().unwrap_or(serde_json::Value::Null),
      "ets": normalized.ext.get("ts").cloned().unwrap_or(serde_json::Value::Null),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp,
      "fail": false
    });
    self.record_amm_event(&event, transaction, block);
    true
  }

  // START TAP-DELEGATED-LOCKS
  fn token_proof_is_delegated_execute_action(action: &serde_json::Value) -> bool {
    action
      .get("op")
      .and_then(|v| v.as_str())
      .map(|op| op.eq_ignore_ascii_case("execute"))
      .unwrap_or(false)
  }

  fn token_proof_is_generic_delegated_action(action: &serde_json::Value) -> bool {
    action
      .get("op")
      .and_then(|v| v.as_str())
      .map(|op| op.eq_ignore_ascii_case("execute-action"))
      .unwrap_or(false)
  }

  fn token_proof_is_delegated_action_envelope(action: &serde_json::Value) -> bool {
    Self::token_proof_is_delegated_execute_action(action)
      || Self::token_proof_is_generic_delegated_action(action)
  }

  fn token_proof_delegated_only_redeem(redeem: &serde_json::Value) -> bool {
    if redeem.get("auth").is_some() {
      return false;
    }
    let items_len = redeem
      .get("items")
      .and_then(|v| v.as_array())
      .map(|a| a.len())
      .unwrap_or(0);
    if items_len != 0 {
      return false;
    }
    let Some(actions) = redeem.get("actions").and_then(|v| v.as_array()) else {
      return false;
    };
    !actions.is_empty()
      && actions
        .iter()
        .all(Self::token_proof_is_delegated_action_envelope)
  }

  fn token_proof_valid_delegation_nonce(nonce: &str) -> bool {
    !nonce.is_empty()
      && nonce.len() <= 128
      && nonce
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
  }

  fn token_proof_delegation_nonce_key(auth: &str, nonce: &str) -> String {
    format!("tdn/{}/{}", auth, nonce)
  }

  fn token_proof_delegation_cancel_key(auth: &str, nonce: &str) -> String {
    format!("tdc/{}/{}", auth, nonce)
  }

  fn token_proof_valid_delegation_signer(signer: &str) -> bool {
    ((signer.len() == 66 && (signer.starts_with("02") || signer.starts_with("03")))
      || (signer.len() == 130 && signer.starts_with("04")))
      && signer.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
  }

  fn tap_token_authority_id(inscription: &str, action_index: usize) -> String {
    format!("{}:{}", inscription, action_index)
  }

  fn tap_token_stake_position_id(inscription: &str, action_index: usize) -> String {
    format!("{}:{}", inscription, action_index)
  }

  fn tap_token_sale_contribution_id(inscription: &str, action_index: usize) -> String {
    format!("{}:{}", inscription, action_index)
  }

  fn tap_token_sale_record_id(prefix: &str, inscription: &str, action_index: usize) -> String {
    format!("{}:{}:{}", prefix, inscription, action_index)
  }

  fn normalize_token_allocation_role(role: &str) -> Option<String> {
    let normalized = role.to_lowercase();
    match normalized.as_str() {
      "of" | "sr" | "bn" => Some(normalized),
      _ => None,
    }
  }

  fn token_authority_inbound_subtype(role: &str) -> String {
    match role {
      "sk" => "as".to_string(),
      "si" | "sc" => role.to_string(),
      _ => "aa".to_string(),
    }
  }

  fn token_authority_outbound_subtype(role: &str) -> String {
    match role {
      "us" => "au".to_string(),
      "sz" | "sa" | "sr" | "sw" => role.to_string(),
      _ => "ac".to_string(),
    }
  }

  fn tap_get_authority_config(&mut self, auth: &str) -> Option<AuthorityConfigRecord> {
    self
      .tap_get::<AuthorityConfigRecord>(&format!("ah/{}", auth))
      .ok()
      .flatten()
  }

  fn tap_get_authority_balance(&mut self, auth: &str, tick_key: &str) -> i128 {
    self
      .tap_get::<String>(&format!("ab/{}/{}", auth, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0)
  }

  fn tap_set_authority_balance(&mut self, auth: &str, tick_key: &str, amount: i128) -> bool {
    if amount < 0 {
      return false;
    }
    let _ = self.tap_put(&format!("ab/{}/{}", auth, tick_key), &amount.to_string());
    if amount > 0
      && self
        .tap_get::<String>(&format!("abo/{}/{}", auth, tick_key))
        .ok()
        .flatten()
        .is_none()
    {
      let tick_label =
        Self::js_json_string_parse_str(tick_key).unwrap_or_else(|| tick_key.to_string());
      let _ = self.tap_set_list_record(
        &format!("abl/{}", auth),
        &format!("abli/{}", auth),
        &tick_label,
      );
      let _ = self.tap_put(&format!("abo/{}/{}", auth, tick_key), &"".to_string());
    }
    true
  }

  fn tap_add_authority_balance(&mut self, auth: &str, tick_key: &str, delta: i128) -> bool {
    let current = self.tap_get_authority_balance(auth, tick_key);
    let Some(next) = current.checked_add(delta) else {
      return false;
    };
    self.tap_set_authority_balance(auth, tick_key, next)
  }

  fn tap_get_authority_total_shares(&mut self, auth: &str) -> BigInt {
    self
      .tap_get::<String>(&format!("ahs/{}", auth))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn tap_set_authority_total_shares(&mut self, auth: &str, shares: &BigInt) -> bool {
    if shares < &BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("ahs/{}", auth), &shares.to_string());
    true
  }

  fn tap_get_authority_acc_reward(&mut self, auth: &str, tick_key: &str) -> BigInt {
    self
      .tap_get::<String>(&format!("ahrps/{}/{}", auth, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn tap_get_authority_reward_carry(&mut self, auth: &str, tick_key: &str) -> BigInt {
    self
      .tap_get::<String>(&format!("ahrc/{}/{}", auth, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn tap_set_authority_reward_carry(&mut self, auth: &str, tick_key: &str, carry: &BigInt) -> bool {
    if carry < &BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("ahrc/{}/{}", auth, tick_key), &carry.to_string());
    true
  }

  fn authority_reward_precision() -> BigInt {
    BigInt::from(1_000_000_000_000_000_000i128)
  }

  fn authority_reward_debt_string(shares: &BigInt, acc: &BigInt) -> Option<String> {
    let value = shares * acc / Self::authority_reward_precision();
    Some(value.to_string())
  }

  fn authority_reward_pending_i128(shares: &BigInt, acc: &BigInt, paid: &BigInt) -> Option<i128> {
    let value = shares * acc / Self::authority_reward_precision() - paid;
    if value <= BigInt::from(0) {
      return Some(0);
    }
    value.to_string().parse::<i128>().ok()
  }

  fn tap_set_authority_acc_reward(&mut self, auth: &str, tick_key: &str, acc: &BigInt) -> bool {
    if acc < &BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("ahrps/{}/{}", auth, tick_key), &acc.to_string());
    true
  }

  fn authority_config_reward_ticks(config: &AuthorityConfigRecord) -> Vec<String> {
    config.rt.clone()
  }

  fn tap_authority_reward_debt_ticks(
    &mut self,
    auth: &str,
    config: &AuthorityConfigRecord,
  ) -> Vec<String> {
    if !config.rt.is_empty() {
      return config
        .rt
        .iter()
        .map(|tick| Self::js_to_lowercase(tick))
        .collect();
    }
    let length = self
      .tap_get::<String>(&format!("abl/{}", auth))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<usize>().ok())
      .unwrap_or(0);
    let mut ticks = Vec::new();
    for i in 0..length {
      if let Some(tick) = self
        .tap_get::<String>(&format!("abli/{}/{}", auth, i))
        .ok()
        .flatten()
      {
        ticks.push(Self::js_to_lowercase(&tick));
      }
    }
    ticks
  }

  fn authority_config_tier(
    config: &AuthorityConfigRecord,
    tier_id: &str,
  ) -> Option<serde_json::Value> {
    config
      .r
      .get("tr")
      .and_then(|v| v.as_array())
      .and_then(|tiers| {
        tiers.iter().find(|tier| {
          tier
            .get("id")
            .and_then(|v| v.as_str())
            .map(|id| id == tier_id)
            .unwrap_or(false)
        })
      })
      .cloned()
  }

  fn tap_apply_authority_reward_allocation(
    &mut self,
    auth_id: &str,
    tick_key: &str,
    amount: i128,
  ) -> bool {
    let Some(config) = self.tap_get_authority_config(auth_id) else {
      return false;
    };
    if config.k != "stk" {
      return false;
    }
    let total_shares = self.tap_get_authority_total_shares(auth_id);
    let empty_policy = config.r.get("ep").and_then(|v| v.as_str()).unwrap_or("");
    let carry = self.tap_get_authority_reward_carry(auth_id, tick_key);
    let distributable = carry + BigInt::from(amount);
    if total_shares == BigInt::from(0) {
      if empty_policy != "hold" && empty_policy != "carry" {
        return false;
      }
      return self.tap_set_authority_reward_carry(auth_id, tick_key, &distributable);
    }
    let precision = Self::authority_reward_precision();
    let delta = &distributable * &precision / &total_shares;
    if delta <= BigInt::from(0) {
      return self.tap_set_authority_reward_carry(auth_id, tick_key, &distributable);
    }
    let distributed = &delta * &total_shares / &precision;
    if distributed <= BigInt::from(0) || distributed > distributable {
      return self.tap_set_authority_reward_carry(auth_id, tick_key, &distributable);
    }
    let acc = self.tap_get_authority_acc_reward(auth_id, tick_key);
    let next = acc + delta;
    if !self.tap_set_authority_acc_reward(auth_id, tick_key, &next) {
      return false;
    }
    let remaining = distributable - distributed;
    self.tap_set_authority_reward_carry(auth_id, tick_key, &remaining)
  }

  fn tap_apply_authority_allocation_credit(
    &mut self,
    allocation: &TokenAllocationRecord,
    tick_key: &str,
    amount: i128,
  ) -> bool {
    if !self.tap_add_authority_balance(&allocation.to, tick_key, amount) {
      return false;
    }
    if allocation.rl == "sr" {
      return self.tap_apply_authority_reward_allocation(&allocation.to, tick_key, amount);
    }
    true
  }

  fn token_proof_get_deploy(&mut self, tick: &str) -> Option<TokenDeployInfo> {
    let normalized = Self::js_to_lowercase(tick);
    let tick_key = Self::json_stringify_lower(&normalized);
    let record = self
      .tap_get::<DeployRecord>(&format!("d/{}", tick_key))
      .ok()
      .flatten()?;
    Some(TokenDeployInfo {
      tick: normalized,
      tick_key,
      record,
    })
  }

  fn token_proof_resolve_protocol_amount(
    &self,
    value: &serde_json::Value,
    deployed: &DeployRecord,
  ) -> Option<i128> {
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) && value.is_number() {
      return None;
    }
    let amount = Self::resolve_number_string(&Self::js_value_to_string(value), deployed.dec)?
      .parse::<i128>()
      .ok()?;
    let max_amount = Self::resolve_number_string(MAX_DEC_U64_STR, deployed.dec)?
      .parse::<i128>()
      .ok()?;
    if amount <= 0 || amount > max_amount {
      return None;
    }
    Some(amount)
  }

  fn token_sale_target_value(target: &SaleTarget) -> serde_json::Value {
    serde_json::json!({ "tt": target.tt, "to": target.to })
  }

  fn validate_token_sale_target(&mut self, value: &serde_json::Value) -> Option<SaleTarget> {
    if !value.is_object() || value.is_array() {
      return None;
    }
    let tt = value.get("tt")?.as_str()?.to_lowercase();
    let mut to = value.get("to")?.as_str()?.to_string();
    if tt == "a" {
      to = Self::normalize_address(&to);
      if !self.is_valid_bitcoin_address(&to) {
        return None;
      }
    } else if tt == "h" {
      self.tap_get_authority_config(&to)?;
    } else if tt == "b" {
      if to != BURN_ADDRESS {
        return None;
      }
    } else {
      return None;
    }
    Some(SaleTarget { tt, to })
  }

  fn amm_target_key(target: &AmmTarget) -> String {
    format!("{}/{}", target.tt, target.to)
  }

  fn amm_target_value(target: &AmmTarget) -> serde_json::Value {
    serde_json::json!({ "tt": target.tt, "to": target.to })
  }

  fn parse_amm_uint_value(value: &serde_json::Value, allow_zero: bool) -> Option<BigInt> {
    let s = value.as_str()?;
    Self::parse_amm_uint_str(s, allow_zero)
  }

  fn parse_amm_uint_str(s: &str, allow_zero: bool) -> Option<BigInt> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
      return None;
    }
    let parsed = s.parse::<BigInt>().ok()?;
    if (!allow_zero && parsed == BigInt::from(0)) || parsed < BigInt::from(0) {
      return None;
    }
    Some(parsed)
  }

  fn parse_amm_height(value: &serde_json::Value) -> Option<u32> {
    let parsed = Self::parse_amm_uint_value(value, true)?;
    parsed.to_string().parse::<u32>().ok()
  }

  fn integer_sqrt(value: &BigInt) -> Option<BigInt> {
    if value < &BigInt::from(0) {
      return None;
    }
    if value < &BigInt::from(2) {
      return Some(value.clone());
    }
    let two = BigInt::from(2);
    let mut x0 = value.clone();
    let mut x1 = (value / &two) + BigInt::from(1);
    while x1 < x0 {
      x0 = x1.clone();
      x1 = (&x1 + value / &x1) / &two;
    }
    Some(x0)
  }

  fn div_ceil(numerator: &BigInt, denominator: &BigInt) -> Option<BigInt> {
    if denominator <= &BigInt::from(0) {
      return None;
    }
    Some((numerator + denominator - BigInt::from(1)) / denominator)
  }

  fn validate_amm_target(
    &mut self,
    value: &serde_json::Value,
    allow_burn: bool,
  ) -> Option<AmmTarget> {
    if !value.is_object() || value.is_array() {
      return None;
    }
    let tt = value.get("tt")?.as_str()?.to_lowercase();
    if tt == "b" {
      if !allow_burn {
        return None;
      }
      if let Some(to) = value.get("to") {
        if to.as_str()? != BURN_ADDRESS {
          return None;
        }
      }
      return Some(AmmTarget {
        tt,
        to: BURN_ADDRESS.to_string(),
      });
    }
    let mut to = value.get("to")?.as_str()?.to_string();
    if tt == "a" {
      to = Self::normalize_address(&to);
      if !self.is_valid_bitcoin_address(&to) {
        return None;
      }
    } else if tt == "h" {
      self.tap_get_authority_config(&to)?;
    } else {
      return None;
    }
    Some(AmmTarget { tt, to })
  }

  fn validate_amm_lp_target(&mut self, value: &serde_json::Value) -> Option<AmmTarget> {
    self.validate_amm_target(value, false)
  }

  fn normalize_amm_asset(&mut self, value: &serde_json::Value) -> Option<AmmAsset> {
    if !value.is_object() || value.is_array() {
      return None;
    }
    let ty = value.get("ty")?.as_str()?.to_lowercase();
    if ty == "tap" {
      let token = self.token_proof_get_deploy(value.get("tick")?.as_str()?)?;
      let asset = serde_json::json!({ "ty": "tap", "tick": token.tick });
      return Some(AmmAsset {
        value: asset,
        key: format!("tap:{}", token.tick),
      });
    }
    if ty == "ext" {
      let ns = value.get("ns")?.as_str()?.to_lowercase();
      let cid = value.get("cid")?.as_str()?.to_lowercase();
      let aid = value.get("aid")?.as_str()?.to_lowercase();
      let dec_raw = value.get("dec")?.as_str()?;
      let dec = Self::parse_amm_uint_str(dec_raw, true)?;
      if !Self::token_proof_safe_id(value.get("ns")?.as_str()?, 128)
        || !Self::token_proof_safe_id(value.get("cid")?.as_str()?, 128)
        || !Self::token_proof_safe_id(value.get("aid")?.as_str()?, 128)
        || dec > BigInt::from(38)
      {
        return None;
      }
      let mut asset = serde_json::json!({
        "ty": "ext",
        "ns": ns,
        "cid": cid,
        "aid": aid,
        "dec": dec.to_string()
      });
      if let Some(pool) = value.get("pool").and_then(|v| v.as_str()) {
        if !Self::token_proof_safe_id(pool, 128) {
          return None;
        }
        asset["pool"] = serde_json::Value::String(pool.to_lowercase());
      }
      let key = format!("ext:{}:{}:{}", ns, cid, aid);
      return Some(AmmAsset { value: asset, key });
    }
    None
  }

  fn get_amm_pool(&mut self, auth: &str) -> Option<AuthorityConfigRecord> {
    self
      .tap_get::<AuthorityConfigRecord>(&format!("amm/{}", auth))
      .ok()
      .flatten()
  }

  fn put_amm_pool(&mut self, pool: &AuthorityConfigRecord) {
    let _ = self.tap_put(&format!("amm/{}", pool.id), pool);
  }

  fn amm_pool_assets_are_tap(pool: &AuthorityConfigRecord) -> bool {
    pool
      .a
      .iter()
      .all(|asset| asset.get("ty").and_then(|v| v.as_str()) == Some("tap"))
  }

  fn amm_pool_asset_is_tap(pool: &AuthorityConfigRecord, index: usize) -> bool {
    pool
      .a
      .get(index)
      .and_then(|asset| asset.get("ty"))
      .and_then(|v| v.as_str())
      == Some("tap")
  }

  fn amm_pool_tick_key(pool: &AuthorityConfigRecord, index: usize) -> Option<String> {
    let tick = pool.a.get(index)?.get("tick")?.as_str()?;
    Some(Self::json_stringify_lower(tick))
  }

  fn amm_pool_tick(pool: &AuthorityConfigRecord, index: usize) -> Option<String> {
    Some(Self::js_to_lowercase(
      pool.a.get(index)?.get("tick")?.as_str()?,
    ))
  }

  fn amm_pool_reserves(pool: &AuthorityConfigRecord) -> Option<[BigInt; 2]> {
    let arr = pool.r.as_array()?;
    if arr.len() != 2 {
      return None;
    }
    Some([
      arr[0].as_str()?.parse::<BigInt>().ok()?,
      arr[1].as_str()?.parse::<BigInt>().ok()?,
    ])
  }

  fn amm_reserves_cover_obligation_locks(
    &mut self,
    pool: &AuthorityConfigRecord,
    reserves: &[BigInt; 2],
    pending_obligations: Option<&std::collections::HashMap<String, BigInt>>,
  ) -> bool {
    for side in 0..2 {
      if !Self::amm_pool_asset_is_tap(pool, side) {
        continue;
      }
      let Some(tick_key) = Self::amm_pool_tick_key(pool, side) else {
        return false;
      };
      let source = serde_json::json!({ "tt": "amm", "pid": pool.id, "i": side });
      let Some(locked) = self.tap_get_obligation_locked_bigint(&source, &tick_key) else {
        return false;
      };
      let pending = pending_obligations
        .and_then(|map| {
          map
            .get(&format!("amm/{}/{}/{}", pool.id, side, tick_key))
            .cloned()
        })
        .unwrap_or_else(|| BigInt::from(0));
      if reserves[side] < locked + pending {
        return false;
      }
    }
    true
  }

  fn amm_pool_shares(pool: &AuthorityConfigRecord) -> Option<BigInt> {
    pool.sh.parse::<BigInt>().ok()
  }

  fn set_amm_pool_state(pool: &mut AuthorityConfigRecord, reserves: &[BigInt; 2], shares: &BigInt) {
    pool.r = serde_json::json!([reserves[0].to_string(), reserves[1].to_string()]);
    pool.sh = shares.to_string();
  }

  fn amm_action_expired(action: &serde_json::Value, block: u32) -> bool {
    Self::parse_amm_height(action.get("exp").unwrap_or(&serde_json::Value::Null))
      .map(|exp| block > exp)
      .unwrap_or(true)
  }

  fn is_amm_ref(ref_value: &str) -> bool {
    !ref_value.is_empty()
      && ref_value.len() <= 128
      && ref_value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
  }

  fn amm_ref_key(pool_id: &str, target: &AmmTarget, ref_value: &str) -> String {
    format!(
      "ammri/{}/{}/{}",
      pool_id,
      Self::amm_target_key(target),
      ref_value
    )
  }

  fn amm_snapshot_key(pool_id: &str, sid: &str) -> String {
    format!("amms/{}/{}", pool_id, sid)
  }

  fn token_proof_resolve_protocol_amount_bigint(
    &self,
    value: &serde_json::Value,
    deployed: &DeployRecord,
  ) -> Option<BigInt> {
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation) && value.is_number() {
      return None;
    }
    let amount = Self::resolve_number_string(&Self::js_value_to_string(value), deployed.dec)?
      .parse::<BigInt>()
      .ok()?;
    let max_amount = Self::resolve_number_string(MAX_DEC_U64_STR, deployed.dec)?
      .parse::<BigInt>()
      .ok()?;
    if amount <= BigInt::from(0) || amount > max_amount {
      return None;
    }
    Some(amount)
  }

  fn tap_get_address_balance_bigint(&mut self, address: &str, tick_key: &str) -> BigInt {
    self
      .tap_get::<String>(&format!("b/{}/{}", address, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn tap_get_transferable_bigint(&mut self, address: &str, tick_key: &str) -> BigInt {
    self
      .tap_get::<String>(&format!("t/{}/{}", address, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn tap_get_locked_bigint(&mut self, address: &str, tick_key: &str) -> BigInt {
    BigInt::from(self.tap_get_locked_amount(address, tick_key))
  }

  fn has_available_with_pending_bigint(
    &mut self,
    address: &str,
    tick_key: &str,
    amount: &BigInt,
    pending_locks: &std::collections::HashMap<String, i128>,
    pending_amm_credits: &std::collections::HashMap<String, BigInt>,
    pending_amm_debits: &std::collections::HashMap<String, BigInt>,
    pending_obligations: &std::collections::HashMap<String, BigInt>,
    pending_perp_debits: &std::collections::HashMap<String, BigInt>,
  ) -> bool {
    let key = format!("{}/{}", address, tick_key);
    let pending = BigInt::from(*pending_locks.get(&key).unwrap_or(&0))
      + pending_amm_debits
        .get(&key)
        .cloned()
        .unwrap_or_else(|| BigInt::from(0))
      + pending_perp_debits
        .get(&key)
        .cloned()
        .unwrap_or_else(|| BigInt::from(0));
    let pending_credit = pending_amm_credits
      .get(&key)
      .cloned()
      .unwrap_or_else(|| BigInt::from(0));
    let account_obligation_source = serde_json::json!({ "tt": "a", "to": address });
    let obligation_locked = self
      .tap_get_obligation_locked_bigint(&account_obligation_source, tick_key)
      .unwrap_or_else(|| BigInt::from(0));
    let pending_obligation = pending_obligations
      .get(&format!("a/{}/{}", address, tick_key))
      .cloned()
      .unwrap_or_else(|| BigInt::from(0));
    self.tap_get_address_balance_bigint(address, tick_key) + pending_credit
      - self.tap_get_transferable_bigint(address, tick_key)
      - self.tap_get_locked_bigint(address, tick_key)
      - pending
      - obligation_locked
      - pending_obligation
      >= amount.clone()
  }

  fn tap_put_address_balance_bigint(
    &mut self,
    address: &str,
    tick_key: &str,
    tick: &str,
    amount: &BigInt,
  ) -> bool {
    if amount < &BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("b/{}/{}", address, tick_key), &amount.to_string());
    if amount > &BigInt::from(0) {
      if self
        .tap_get::<String>(&format!("he/{}/{}", address, tick_key))
        .ok()
        .flatten()
        .is_none()
      {
        let _ = self.tap_put(&format!("he/{}/{}", address, tick_key), &"".to_string());
        let _ = self.tap_set_list_record(
          &format!("h/{}", tick_key),
          &format!("hi/{}", tick_key),
          &address.to_string(),
        );
      }
      if self
        .tap_get::<String>(&format!("ato/{}/{}", address, tick_key))
        .ok()
        .flatten()
        .is_none()
      {
        let _ = self.tap_set_list_record(
          &format!("atl/{}", address),
          &format!("atli/{}", address),
          &Self::js_to_lowercase(tick),
        );
        let _ = self.tap_put(&format!("ato/{}/{}", address, tick_key), &"".to_string());
      }
    }
    true
  }

  fn tap_get_authority_balance_bigint(&mut self, auth: &str, tick_key: &str) -> BigInt {
    self
      .tap_get::<String>(&format!("ab/{}/{}", auth, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn tap_set_authority_balance_bigint(
    &mut self,
    auth: &str,
    tick_key: &str,
    amount: &BigInt,
  ) -> bool {
    if amount < &BigInt::from(0) {
      return false;
    }
    let _ = self.tap_put(&format!("ab/{}/{}", auth, tick_key), &amount.to_string());
    if amount > &BigInt::from(0)
      && self
        .tap_get::<String>(&format!("abo/{}/{}", auth, tick_key))
        .ok()
        .flatten()
        .is_none()
    {
      let tick_label =
        Self::js_json_string_parse_str(tick_key).unwrap_or_else(|| tick_key.to_string());
      let _ = self.tap_set_list_record(
        &format!("abl/{}", auth),
        &format!("abli/{}", auth),
        &tick_label,
      );
      let _ = self.tap_put(&format!("abo/{}/{}", auth, tick_key), &"".to_string());
    }
    true
  }

  fn tap_add_authority_balance_bigint(
    &mut self,
    auth: &str,
    tick_key: &str,
    delta: &BigInt,
  ) -> bool {
    let next = self.tap_get_authority_balance_bigint(auth, tick_key) + delta;
    self.tap_set_authority_balance_bigint(auth, tick_key, &next)
  }

  fn get_amm_lp_shares(&mut self, pool_id: &str, target: &AmmTarget) -> BigInt {
    self
      .tap_get::<String>(&format!(
        "ammp/{}/{}",
        pool_id,
        Self::amm_target_key(target)
      ))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0))
  }

  fn set_amm_lp_shares(&mut self, pool_id: &str, target: &AmmTarget, shares: &BigInt) -> bool {
    if shares < &BigInt::from(0) {
      return false;
    }
    let key = format!("ammp/{}/{}", pool_id, Self::amm_target_key(target));
    let _ = self.tap_put(&key, &shares.to_string());
    let position = serde_json::json!({
      "pid": pool_id,
      "tt": target.tt,
      "to": target.to,
      "sh": shares.to_string()
    });
    let _ = self.tap_put(
      &format!("ammpr/{}/{}", pool_id, Self::amm_target_key(target)),
      &position,
    );
    if self
      .tap_get::<String>(&format!(
        "ammpos/{}/{}",
        pool_id,
        Self::amm_target_key(target)
      ))
      .ok()
      .flatten()
      .is_none()
    {
      let _ = self.tap_set_list_record(
        &format!("amma/{}", Self::amm_target_key(target)),
        &format!("ammai/{}", Self::amm_target_key(target)),
        &format!("ammpr/{}/{}", pool_id, Self::amm_target_key(target)),
      );
      let _ = self.tap_put(
        &format!("ammpos/{}/{}", pool_id, Self::amm_target_key(target)),
        &"".to_string(),
      );
    }
    true
  }

  fn add_amm_lp_shares(&mut self, pool_id: &str, target: &AmmTarget, delta: &BigInt) -> bool {
    let next = self.get_amm_lp_shares(pool_id, target) + delta;
    self.set_amm_lp_shares(pool_id, target, &next)
  }

  fn amm_protocol_fee(gross_fee: &BigInt, protocol_share_bps: &BigInt) -> BigInt {
    gross_fee * protocol_share_bps / BigInt::from(10000)
  }

  fn calculate_amm_exact_in(
    amount_in: &BigInt,
    reserve_in: &BigInt,
    reserve_out: &BigInt,
    fee_bps: &BigInt,
    protocol_share_bps: &BigInt,
  ) -> Option<AmmSwapCalc> {
    if amount_in <= &BigInt::from(0)
      || reserve_in <= &BigInt::from(0)
      || reserve_out <= &BigInt::from(0)
    {
      return None;
    }
    let gross_fee = amount_in * fee_bps / BigInt::from(10000);
    let amount_in_after_fee = amount_in - &gross_fee;
    if amount_in_after_fee <= BigInt::from(0) {
      return None;
    }
    let amount_out = &amount_in_after_fee * reserve_out / (reserve_in + &amount_in_after_fee);
    let protocol_fee = Self::amm_protocol_fee(&gross_fee, protocol_share_bps);
    Some(AmmSwapCalc {
      amount_in: amount_in.clone(),
      amount_out,
      gross_fee,
      protocol_fee,
    })
  }

  fn calculate_amm_exact_out(
    amount_out: &BigInt,
    reserve_in: &BigInt,
    reserve_out: &BigInt,
    fee_bps: &BigInt,
    protocol_share_bps: &BigInt,
  ) -> Option<AmmSwapCalc> {
    if amount_out <= &BigInt::from(0) || reserve_in <= &BigInt::from(0) || reserve_out <= amount_out
    {
      return None;
    }
    let amount_in_after_fee =
      Self::div_ceil(&(reserve_in * amount_out), &(reserve_out - amount_out))?;
    if amount_in_after_fee <= BigInt::from(0) || fee_bps >= &BigInt::from(10000) {
      return None;
    }
    let amount_in = Self::div_ceil(
      &(amount_in_after_fee * BigInt::from(10000)),
      &(BigInt::from(10000) - fee_bps),
    )?;
    let gross_fee = &amount_in * fee_bps / BigInt::from(10000);
    let protocol_fee = Self::amm_protocol_fee(&gross_fee, protocol_share_bps);
    Some(AmmSwapCalc {
      amount_in,
      amount_out: amount_out.clone(),
      gross_fee,
      protocol_fee,
    })
  }

  fn token_sale_status_default(auth: &str, config: &AuthorityConfigRecord) -> serde_json::Value {
    serde_json::json!({
      "auth": auth,
      "st": config.st.clone().unwrap_or_default(),
      "pt": config.pt.clone().unwrap_or_default(),
      "tc": "0",
      "inv": "0",
      "alc": "0",
      "clm": "0",
      "ref": "0",
      "wdr": "0",
      "fin": false,
      "can": false,
      "fail": false,
      "pp": false
    })
  }

  fn token_sale_status_str(status: &serde_json::Value, key: &str) -> String {
    status
      .get(key)
      .and_then(|v| v.as_str())
      .unwrap_or("0")
      .to_string()
  }

  fn token_sale_status_i128(status: &serde_json::Value, key: &str) -> i128 {
    Self::token_sale_status_str(status, key)
      .parse::<i128>()
      .ok()
      .unwrap_or(0)
  }

  fn token_sale_status_bool(status: &serde_json::Value, key: &str) -> bool {
    status.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
  }

  fn token_sale_status_set_string(status: &mut serde_json::Value, key: &str, value: i128) {
    if let Some(map) = status.as_object_mut() {
      map.insert(
        key.to_string(),
        serde_json::Value::String(value.to_string()),
      );
    }
  }

  fn token_sale_status_set_bool(status: &mut serde_json::Value, key: &str, value: bool) {
    if let Some(map) = status.as_object_mut() {
      map.insert(key.to_string(), serde_json::Value::Bool(value));
    }
  }

  fn tap_get_sale_status(
    &mut self,
    auth: &str,
    config: &AuthorityConfigRecord,
  ) -> serde_json::Value {
    self
      .tap_get::<serde_json::Value>(&format!("sale/{}", auth))
      .ok()
      .flatten()
      .unwrap_or_else(|| Self::token_sale_status_default(auth, config))
  }

  fn tap_put_sale_status(&mut self, status: &serde_json::Value) {
    if let Some(auth) = status.get("auth").and_then(|v| v.as_str()) {
      let _ = self.tap_put(&format!("sale/{}", auth), status);
    }
  }

  fn tap_credit_address_balance(
    &mut self,
    address: &str,
    tick_key: &str,
    tick: &str,
    amount: i128,
  ) -> Option<i128> {
    if amount < 0 {
      return None;
    }
    let before = self
      .tap_get::<String>(&format!("b/{}/{}", address, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let after = before.checked_add(amount)?;
    let _ = self.tap_put(&format!("b/{}/{}", address, tick_key), &after.to_string());
    if after > 0 {
      if self
        .tap_get::<String>(&format!("he/{}/{}", address, tick_key))
        .ok()
        .flatten()
        .is_none()
      {
        let _ = self.tap_put(&format!("he/{}/{}", address, tick_key), &"".to_string());
        let _ = self.tap_set_list_record(
          &format!("h/{}", tick_key),
          &format!("hi/{}", tick_key),
          &address.to_string(),
        );
      }
      if self
        .tap_get::<String>(&format!("ato/{}/{}", address, tick_key))
        .ok()
        .flatten()
        .is_none()
      {
        let _ = self.tap_set_list_record(
          &format!("atl/{}", address),
          &format!("atli/{}", address),
          &Self::js_to_lowercase(tick),
        );
        let _ = self.tap_put(&format!("ato/{}/{}", address, tick_key), &"".to_string());
      }
    }
    Some(after)
  }

  fn token_sale_contribution_allocation(
    &self,
    payment_amount: i128,
    config: &AuthorityConfigRecord,
  ) -> Option<i128> {
    let rate = config.s.as_ref()?.get("r")?;
    let payment_rate = rate.get("pa")?.as_str()?.parse::<i128>().ok()?;
    let sale_rate = rate.get("sa")?.as_str()?.parse::<i128>().ok()?;
    if payment_rate <= 0 || sale_rate <= 0 {
      return Some(0);
    }
    payment_amount
      .checked_mul(sale_rate)?
      .checked_div(payment_rate)
  }

  fn token_sale_hash_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
  }

  fn token_sale_merkle_parent(left: &str, right: &str) -> Option<String> {
    let mut pair = [left.to_lowercase(), right.to_lowercase()];
    pair.sort();
    let bytes = hex::decode(format!("{}{}", pair[0], pair[1])).ok()?;
    Some(Self::token_sale_hash_hex_bytes(&bytes))
  }

  fn validate_token_sale_allowlist(
    &self,
    config: &AuthorityConfigRecord,
    action: &serde_json::Value,
    claim: &str,
    payment_amount: i128,
    payment_deployed: &DeployRecord,
  ) -> bool {
    let allowlist = config.s.as_ref().and_then(|s| s.get("alw"));
    if allowlist.is_none() || allowlist == Some(&serde_json::Value::Null) {
      return action.get("alw").is_none();
    }
    let allowlist = allowlist.unwrap();
    if !allowlist.is_object()
      || allowlist.get("ty").and_then(|v| v.as_str()) != Some("sha256-merkle")
      || !matches!(
        allowlist.get("lf").and_then(|v| v.as_str()),
        Some("addr") | Some("addr-cap")
      )
      || !allowlist
        .get("root")
        .and_then(|v| v.as_str())
        .map(Self::tap_is_valid_sha256_hex)
        .unwrap_or(false)
    {
      return false;
    }
    let Some(proof_obj) = action.get("alw").and_then(|v| v.as_object()) else {
      return false;
    };
    let Some(proof) = proof_obj.get("proof").and_then(|v| v.as_array()) else {
      return false;
    };
    if proof.len() > 32 {
      return false;
    }

    let mut leaf_content = claim.to_string();
    if allowlist.get("lf").and_then(|v| v.as_str()) == Some("addr-cap") {
      let Some(cap_value) = proof_obj.get("max") else {
        return false;
      };
      let Some(cap) = self.token_proof_resolve_protocol_amount(cap_value, payment_deployed) else {
        return false;
      };
      if payment_amount > cap {
        return false;
      }
      leaf_content = format!("{}:{}", claim, cap);
    }

    let mut node = Self::token_sale_hash_hex_bytes(leaf_content.as_bytes());
    for sibling in proof {
      let Some(sibling) = sibling.as_str() else {
        return false;
      };
      if !Self::tap_is_valid_sha256_hex(sibling) {
        return false;
      }
      let Some(parent) = Self::token_sale_merkle_parent(&node, sibling) else {
        return false;
      };
      node = parent;
    }
    node.to_lowercase()
      == allowlist
        .get("root")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase()
  }

  fn normalize_token_proof_allocations(
    &mut self,
    action: &serde_json::Value,
    deployed: &DeployRecord,
    max_amount: i128,
  ) -> Option<(Vec<TokenAllocationRecord>, i128)> {
    let mut raw: Vec<serde_json::Value> = Vec::new();
    if let Some(al) = action.get("al") {
      let list = al.as_array()?;
      if list.is_empty() || list.len() > 16 || action.get("fee").is_some() {
        return None;
      }
      raw.extend(list.iter().cloned());
    } else if let Some(fee) = action.get("fee") {
      if !fee.is_object() || fee.get("amt").is_none() {
        return None;
      }
      raw.push(serde_json::json!({
        "tt": "a",
        "to": fee.get("addr")?.clone(),
        "amt": fee.get("amt")?.clone(),
        "rl": "of"
      }));
    }

    let mut out = Vec::new();
    let mut total: i128 = 0;
    let mut seen_roles = std::collections::HashSet::new();
    for entry in raw {
      if !entry.is_object()
        || entry.is_array()
        || !entry.get("tt").map(|v| v.is_string()).unwrap_or(false)
        || !entry.get("to").map(|v| v.is_string()).unwrap_or(false)
        || entry.get("amt").is_none()
      {
        return None;
      }
      let tt = entry.get("tt")?.as_str()?.to_lowercase();
      if !matches!(tt.as_str(), "a" | "h" | "b") {
        return None;
      }
      let role = Self::normalize_token_allocation_role(
        entry.get("rl").and_then(|v| v.as_str()).unwrap_or(""),
      )?;
      if !seen_roles.insert(role.clone()) {
        return None;
      }
      if self.tap_feature_enabled(TapFeature::ValueStringifyActivation)
        && entry.get("amt")?.is_number()
      {
        return None;
      }
      let amt_str = Self::js_value_to_string(entry.get("amt")?);
      let amt_norm = Self::resolve_number_string(&amt_str, deployed.dec)?;
      let amount = amt_norm.parse::<i128>().ok()?;
      if amount <= 0 || amount > max_amount {
        return None;
      }
      let mut to = entry.get("to")?.as_str()?.to_string();
      if tt == "a" {
        to = Self::normalize_address(&to);
        if !self.is_valid_bitcoin_address(&to) {
          return None;
        }
      } else if tt == "h" {
        let auth = self.tap_get_authority_config(&to)?;
        if role == "sr" {
          if auth.k != "stk" {
            return None;
          }
          let tick_key = Self::json_stringify_lower(action.get("tick")?.as_str()?);
          let reward_ticks = Self::authority_config_reward_ticks(&auth);
          if !reward_ticks.is_empty() {
            let reward_ticks: std::collections::HashSet<String> = reward_ticks
              .into_iter()
              .map(|tick| Self::json_stringify_lower(&tick))
              .collect();
            if !reward_ticks.contains(&tick_key) {
              return None;
            }
          }
          let shares = self.tap_get_authority_total_shares(&to);
          let empty_policy_accepts = auth
            .r
            .get("ep")
            .and_then(|v| v.as_str())
            .map(|ep| ep == "hold" || ep == "carry")
            .unwrap_or(false);
          if shares == BigInt::from(0) && !empty_policy_accepts {
            return None;
          }
        }
      } else if to != BURN_ADDRESS {
        return None;
      }
      total = total.checked_add(amount)?;
      if total > max_amount {
        return None;
      }
      out.push(TokenAllocationRecord {
        tt,
        to,
        amt: amount.to_string(),
        rl: role,
      });
    }

    Some((out, total))
  }

  fn token_proof_compressed_delegation_pubkey(signer: &str) -> Option<String> {
    if !Self::token_proof_valid_delegation_signer(signer) {
      return None;
    }
    let lower = signer.to_lowercase();
    if lower.len() == 66 {
      return Some(lower);
    }
    let y_last = u8::from_str_radix(&lower[128..130], 16).ok()?;
    let prefix = if y_last % 2 == 0 { "02" } else { "03" };
    Some(format!("{}{}", prefix, &lower[2..66]))
  }

  fn certified_control_canonical_json(value: &serde_json::Value) -> Option<String> {
    match value {
      serde_json::Value::Null => Some("null".to_string()),
      serde_json::Value::Bool(b) => Some(if *b { "true" } else { "false" }.to_string()),
      serde_json::Value::Number(n) => {
        if let Some(i) = n.as_i64() {
          if (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&i) {
            Some(i.to_string())
          } else {
            None
          }
        } else if let Some(u) = n.as_u64() {
          if u <= 9_007_199_254_740_991 {
            Some(u.to_string())
          } else {
            None
          }
        } else {
          None
        }
      }
      serde_json::Value::String(s) => serde_json::to_string(s).ok(),
      serde_json::Value::Array(items) => {
        let mut parts = Vec::new();
        for item in items {
          parts.push(Self::certified_control_canonical_json(item)?);
        }
        Some(format!("[{}]", parts.join(",")))
      }
      serde_json::Value::Object(map) => {
        let mut keys = map.keys().collect::<Vec<_>>();
        keys.sort();
        let mut parts = Vec::new();
        for key in keys {
          let key_json = serde_json::to_string(key).ok()?;
          let value_json = Self::certified_control_canonical_json(map.get(key)?)?;
          parts.push(format!("{}:{}", key_json, value_json));
        }
        Some(format!("{{{}}}", parts.join(",")))
      }
    }
  }

  fn certified_control_hash(value: &serde_json::Value) -> Option<String> {
    let canonical = Self::certified_control_canonical_json(value)?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    Some(hex::encode(hasher.finalize()))
  }

  fn parse_certified_control_integer(value: &serde_json::Value) -> Option<u32> {
    if let Some(n) = value.as_u64() {
      if n <= 9_007_199_254_740_991 && n <= u32::MAX as u64 {
        return Some(n as u32);
      }
      return None;
    }
    let s = value.as_str()?;
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
      return None;
    }
    if s.len() > 1 && s.starts_with('0') {
      return None;
    }
    s.parse::<u32>().ok()
  }

  fn is_certified_control_id(value: &str) -> bool {
    !value.is_empty()
      && value.len() <= 128
      && value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
  }

  fn certified_control_replay_key(policy: &str, target: &str, action: &str, nonce: &str) -> String {
    format!("ccn/{}/{}/{}/{}", policy, target, action, nonce)
  }

  fn certified_control_payload_hash(action: &serde_json::Value) -> Option<String> {
    let mut payload = action.clone();
    payload.as_object_mut()?.remove("cert");
    Self::certified_control_hash(&payload)
  }

  fn certified_control_message(
    policy: &serde_json::Value,
    action_name: &str,
    target: &str,
    payload_hash: &str,
    nonce: &str,
    valid_until: u32,
  ) -> Option<serde_json::Value> {
    Some(serde_json::Value::Array(vec![
      serde_json::Value::String("tap-certified-control-v1".to_string()),
      serde_json::Value::String("tap".to_string()),
      serde_json::Value::String(policy.get("id")?.as_str()?.to_string()),
      serde_json::Value::String(policy.get("hash")?.as_str()?.to_string()),
      serde_json::Value::String(action_name.to_string()),
      serde_json::Value::String(target.to_string()),
      serde_json::Value::String(payload_hash.to_string()),
      serde_json::Value::String(nonce.to_string()),
      serde_json::Value::Number(serde_json::Number::from(valid_until)),
    ]))
  }

  fn normalize_certified_control_policy(
    &self,
    control: Option<&serde_json::Value>,
    action: &serde_json::Value,
  ) -> Option<Option<serde_json::Value>> {
    let Some(control) = control else {
      return Some(None);
    };
    let obj = control.as_object()?;
    let allowed: std::collections::HashSet<&str> = [
      "type",
      "id",
      "threshold",
      "signers",
      "scope",
      "expires",
      "rules",
      "hash",
    ]
    .into_iter()
    .collect();
    if obj.keys().any(|k| !allowed.contains(k.as_str()))
      || control.get("type").and_then(|v| v.as_str()) != Some("cert")
    {
      return None;
    }
    let id = control.get("id")?.as_str()?;
    if !Self::is_certified_control_id(id) {
      return None;
    }

    let signer_arr = control.get("signers")?.as_array()?;
    let mut signer_set = std::collections::BTreeSet::new();
    for signer in signer_arr {
      let normalized = Self::token_proof_compressed_delegation_pubkey(signer.as_str()?)?;
      if !signer_set.insert(normalized) {
        return None;
      }
    }
    if signer_set.is_empty() || signer_set.len() > 8 {
      return None;
    }
    let signers: Vec<serde_json::Value> = signer_set
      .iter()
      .map(|s| serde_json::Value::String(s.clone()))
      .collect();

    let threshold = Self::parse_certified_control_integer(control.get("threshold")?)?;
    if threshold == 0 || threshold as usize > signer_set.len() || threshold > 8 {
      return None;
    }

    let scope_arr = control.get("scope")?.as_array()?;
    let mut scope_set = std::collections::HashSet::new();
    for scope in scope_arr {
      let scope = scope.as_str()?;
      if scope != "claim" && scope != "refund" {
        return None;
      }
      scope_set.insert(scope.to_string());
    }
    let mut scope = Vec::new();
    if scope_set.contains("claim") {
      scope.push(serde_json::Value::String("claim".to_string()));
    }
    if scope_set.contains("refund") {
      scope.push(serde_json::Value::String("refund".to_string()));
    }
    if scope.is_empty() {
      return None;
    }

    let expires = match control.get("expires") {
      Some(value) => Some(Self::parse_certified_control_integer(value)?),
      None => None,
    };

    let mut rules_obj = serde_json::Map::new();
    if let Some(rules) = control.get("rules") {
      let rules = rules.as_object()?;
      if rules.keys().any(|k| k != "terminal_refund_after") {
        return None;
      }
      if let Some(value) = rules.get("terminal_refund_after") {
        let terminal = Self::parse_certified_control_integer(value)?;
        rules_obj.insert(
          "terminal_refund_after".to_string(),
          serde_json::Value::Number(serde_json::Number::from(terminal)),
        );
      }
    }

    if scope_set.contains("refund") {
      if action.get("refund").is_none()
        || action.get("refund_after").is_none()
        || !rules_obj.contains_key("terminal_refund_after")
      {
        return None;
      }
      let refund_after = Self::parse_certified_control_integer(action.get("refund_after")?)?;
      let terminal = rules_obj.get("terminal_refund_after")?.as_u64()? as u32;
      if terminal <= refund_after {
        return None;
      }
    }

    let mut policy_obj = serde_json::Map::new();
    policy_obj.insert(
      "type".to_string(),
      serde_json::Value::String("cert".to_string()),
    );
    policy_obj.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    policy_obj.insert(
      "threshold".to_string(),
      serde_json::Value::Number(serde_json::Number::from(threshold)),
    );
    policy_obj.insert("signers".to_string(), serde_json::Value::Array(signers));
    policy_obj.insert("scope".to_string(), serde_json::Value::Array(scope));
    if let Some(expires) = expires {
      policy_obj.insert(
        "expires".to_string(),
        serde_json::Value::Number(serde_json::Number::from(expires)),
      );
    }
    if !rules_obj.is_empty() {
      policy_obj.insert("rules".to_string(), serde_json::Value::Object(rules_obj));
    }

    let mut policy = serde_json::Value::Object(policy_obj);
    let hash = Self::certified_control_hash(&policy)?;
    if let Some(input_hash) = control.get("hash") {
      if input_hash.as_str()?.to_lowercase() != hash {
        return None;
      }
    }
    policy
      .as_object_mut()?
      .insert("hash".to_string(), serde_json::Value::String(hash));
    Some(Some(policy))
  }

  fn validate_certified_control_certificate(
    &mut self,
    action: &serde_json::Value,
    lock: &TokenLockRecord,
    action_name: &str,
    block: u32,
  ) -> Option<(Option<String>, Option<serde_json::Value>)> {
    let control = lock.control.as_ref();
    let scoped = control
      .and_then(|c| c.get("scope"))
      .and_then(|v| v.as_array())
      .map(|scope| {
        scope
          .iter()
          .any(|entry| entry.as_str().map(|s| s == action_name).unwrap_or(false))
      })
      .unwrap_or(false);
    if !scoped {
      return if action.get("cert").is_none() {
        Some((None, None))
      } else {
        None
      };
    }
    let control = control?;
    if action_name == "refund"
      && action.get("cert").is_none()
      && control
        .get("rules")
        .and_then(|r| r.get("terminal_refund_after"))
        .and_then(|v| v.as_u64())
        .map(|terminal| u64::from(block) >= terminal)
        .unwrap_or(false)
    {
      return Some((None, None));
    }

    let cert = action.get("cert")?;
    let obj = cert.as_object()?;
    let allowed: std::collections::HashSet<&str> = [
      "v",
      "policy",
      "action",
      "target",
      "payload_hash",
      "nonce",
      "valid_until",
      "sigs",
    ]
    .into_iter()
    .collect();
    if obj.keys().any(|k| !allowed.contains(k.as_str())) {
      return None;
    }

    let version = match cert.get("v") {
      Some(v) => Self::parse_certified_control_integer(v)?,
      None => 1,
    };
    let valid_until = Self::parse_certified_control_integer(cert.get("valid_until")?)?;
    let nonce = cert.get("nonce")?.as_str()?;
    if version != 1
      || cert.get("policy").and_then(|v| v.as_str()) != control.get("id").and_then(|v| v.as_str())
      || cert.get("action").and_then(|v| v.as_str()) != Some(action_name)
      || cert.get("target").and_then(|v| v.as_str()) != action.get("lock").and_then(|v| v.as_str())
      || !Self::is_certified_control_id(nonce)
      || block > valid_until
      || control
        .get("expires")
        .and_then(|v| v.as_u64())
        .map(|expires| u64::from(block) > expires)
        .unwrap_or(false)
    {
      return None;
    }

    let payload_hash = Self::certified_control_payload_hash(action)?;
    if cert
      .get("payload_hash")
      .and_then(|v| v.as_str())
      .map(|h| h.to_lowercase() == payload_hash)
      != Some(true)
    {
      return None;
    }

    let target = action.get("lock")?.as_str()?;
    let policy_id = control.get("id")?.as_str()?;
    let nonce_key = Self::certified_control_replay_key(policy_id, target, action_name, nonce);
    if self
      .tap_get::<TokenLockConsumeRecord>(&nonce_key)
      .ok()
      .flatten()
      .is_some()
      || self.tap_get::<String>(&nonce_key).ok().flatten().is_some()
    {
      return None;
    }
    let msg = Self::certified_control_message(
      control,
      action_name,
      target,
      &payload_hash,
      nonce,
      valid_until,
    )?;
    let msg_hash_hex = Self::certified_control_hash(&msg)?;
    let msg_hash_bytes = hex::decode(&msg_hash_hex).ok()?;
    let msg_hash: [u8; 32] = msg_hash_bytes.try_into().ok()?;

    let signers = control.get("signers")?.as_array()?;
    let signer_set: std::collections::HashSet<String> = signers
      .iter()
      .filter_map(|s| s.as_str().map(|s| s.to_string()))
      .collect();
    let threshold = control.get("threshold")?.as_u64()? as usize;
    let mut valid_signers = std::collections::BTreeSet::new();
    for entry in cert.get("sigs")?.as_array()? {
      let declared =
        Self::token_proof_compressed_delegation_pubkey(entry.get("signer")?.as_str()?)?;
      if !signer_set.contains(&declared)
        || entry.get("hash")?.as_str()?.to_lowercase() != msg_hash_hex
      {
        return None;
      }
      let (ok, _, pubkey) = self.verify_sig_obj_against_msg_with_hash(
        entry.get("sig")?,
        entry.get("hash")?.as_str()?,
        &msg_hash,
      )?;
      let signer = Self::token_proof_compressed_delegation_pubkey(&pubkey)?;
      if ok && signer == declared {
        valid_signers.insert(signer);
      }
    }
    if valid_signers.len() < threshold {
      return None;
    }

    let cert_record = serde_json::json!({
      "v": 1,
      "policy": policy_id,
      "action": action_name,
      "target": target,
      "payload_hash": payload_hash,
      "nonce": nonce,
      "valid_until": valid_until,
      "signers": valid_signers.into_iter().collect::<Vec<_>>()
    });
    Some((Some(nonce_key), Some(cert_record)))
  }

  fn token_proof_delegation_message(delegation: &serde_json::Value) -> Option<serde_json::Value> {
    let constraints = delegation
      .get("constraints")
      .cloned()
      .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    if let Some(finalizers) = delegation.get("finalizers") {
      return Some(serde_json::Value::Array(vec![
        serde_json::Value::String("tap-delegated-lock-v2".to_string()),
        delegation.get("auth")?.clone(),
        delegation.get("nonce")?.clone(),
        delegation.get("expiry")?.clone(),
        delegation.get("threshold")?.clone(),
        delegation.get("signers")?.clone(),
        delegation.get("template")?.clone(),
        constraints,
        finalizers.clone(),
      ]));
    }

    Some(serde_json::Value::Array(vec![
      serde_json::Value::String("tap-delegated-lock-v1".to_string()),
      delegation.get("auth")?.clone(),
      delegation.get("nonce")?.clone(),
      delegation.get("expiry")?.clone(),
      delegation.get("threshold")?.clone(),
      delegation.get("signers")?.clone(),
      delegation.get("template")?.clone(),
      constraints,
    ]))
  }

  fn token_proof_action_delegation_message(
    delegation: &serde_json::Value,
  ) -> Option<serde_json::Value> {
    let constraints = delegation
      .get("constraints")
      .cloned()
      .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    let finalizers = delegation
      .get("finalizers")
      .cloned()
      .unwrap_or(serde_json::Value::Null);
    Some(serde_json::Value::Array(vec![
      serde_json::Value::String("tap-delegated-action-v1".to_string()),
      serde_json::Value::String("tap".to_string()),
      delegation.get("kind")?.clone(),
      delegation.get("v")?.clone(),
      delegation.get("auth")?.clone(),
      delegation.get("nonce")?.clone(),
      delegation.get("expiry")?.clone(),
      delegation.get("family")?.clone(),
      delegation.get("threshold")?.clone(),
      delegation.get("signers")?.clone(),
      delegation.get("template")?.clone(),
      constraints,
      finalizers,
    ]))
  }

  fn token_proof_get_path_value<'a>(
    value: &'a serde_json::Value,
    path: &str,
  ) -> Option<&'a serde_json::Value> {
    if path.is_empty() {
      return None;
    }
    let mut current = value;
    for part in path.split('.') {
      if part.is_empty() {
        return None;
      }
      current = current.as_object()?.get(part)?;
    }
    Some(current)
  }

  fn token_proof_json_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    Self::js_json_stringify(left) == Self::js_json_stringify(right)
  }

  fn token_proof_parse_delegation_block_integer(value: &serde_json::Value) -> Option<i128> {
    const JS_MAX_SAFE_INTEGER: i128 = 9_007_199_254_740_991;
    match value {
      serde_json::Value::Number(n) => {
        if let Some(u) = n.as_u64() {
          let parsed = i128::try_from(u).ok()?;
          if parsed <= JS_MAX_SAFE_INTEGER {
            Some(parsed)
          } else {
            None
          }
        } else if let Some(i) = n.as_i64() {
          if i < 0 || i128::from(i) > JS_MAX_SAFE_INTEGER {
            None
          } else {
            Some(i128::from(i))
          }
        } else {
          let f = n.as_f64()?;
          if !f.is_finite() || f < 0.0 || f.fract() != 0.0 || f > JS_MAX_SAFE_INTEGER as f64 {
            None
          } else {
            Some(f as i128)
          }
        }
      }
      serde_json::Value::String(s) => {
        if s.is_empty()
          || (s.len() > 1 && s.starts_with('0'))
          || !s.bytes().all(|b| b.is_ascii_digit())
        {
          return None;
        }
        let parsed = s.parse::<i128>().ok()?;
        if parsed <= JS_MAX_SAFE_INTEGER {
          Some(parsed)
        } else {
          None
        }
      }
      _ => None,
    }
  }

  fn token_proof_parse_delegation_block_bound(value: &serde_json::Value) -> Option<i128> {
    if !value.is_string() {
      return None;
    }
    Self::token_proof_parse_delegation_block_integer(value)
  }

  fn token_proof_validate_delegation_constraint(
    &self,
    value: &serde_json::Value,
    constraint: &serde_json::Value,
    block: u32,
  ) -> bool {
    let Some(obj) = constraint.as_object() else {
      return false;
    };

    if let Some(expected) = obj.get("equals") {
      if !Self::token_proof_json_equal(value, expected) {
        return false;
      }
    }

    if let Some(allowed) = obj.get("allowed").and_then(|v| v.as_array()) {
      if !allowed
        .iter()
        .any(|candidate| Self::token_proof_json_equal(value, candidate))
      {
        return false;
      }
    }

    if let Some(kind) = obj
      .get("type")
      .and_then(|v| v.as_str())
      .map(|s| s.to_lowercase())
    {
      if kind == "btc-address" {
        let Some(addr) = value.as_str() else {
          return false;
        };
        let normalized = Self::normalize_address(addr);
        if !self.is_valid_bitcoin_address(&normalized) {
          return false;
        }
      } else if kind == "sha256" {
        let Some(hash) = value.as_str() else {
          return false;
        };
        if !Self::tap_is_valid_sha256_hex(hash) {
          return false;
        }
      } else if kind == "string" {
        let Some(s) = value.as_str() else {
          return false;
        };
        if let Some(min) = obj.get("min").and_then(Self::js_parse_int) {
          if s.len() < usize::try_from(min).ok().unwrap_or(usize::MAX) {
            return false;
          }
        }
        if let Some(max) = obj.get("max").and_then(Self::js_parse_int) {
          if s.len() > usize::try_from(max).ok().unwrap_or(0) {
            return false;
          }
        }
      } else if kind == "number-string" {
        let Some(s) = value.as_str() else {
          return false;
        };
        let Some((head, tail)) = s.split_once('.') else {
          return !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
        };
        if head.is_empty()
          || !head.bytes().all(|b| b.is_ascii_digit())
          || !tail.bytes().all(|b| b.is_ascii_digit())
          || tail.contains('.')
        {
          return false;
        }
      } else if kind == "block-offset" {
        if block < self.feature_height(TapFeature::TokenAuthorityStakingUpgradeActivation) {
          return false;
        }
        if obj.get("base").and_then(|v| v.as_str()) != Some("current") {
          return false;
        }
        let Some(target) = Self::token_proof_parse_delegation_block_integer(value) else {
          return false;
        };
        let has_min = obj.contains_key("min");
        let has_max = obj.contains_key("max");
        if !has_min && !has_max {
          return false;
        }
        let min = if has_min {
          let Some(parsed) = obj
            .get("min")
            .and_then(Self::token_proof_parse_delegation_block_bound)
          else {
            return false;
          };
          Some(parsed)
        } else {
          None
        };
        let max = if has_max {
          let Some(parsed) = obj
            .get("max")
            .and_then(Self::token_proof_parse_delegation_block_bound)
          else {
            return false;
          };
          Some(parsed)
        } else {
          None
        };
        if let (Some(min), Some(max)) = (min, max) {
          if min > max {
            return false;
          }
        }
        let offset = target - i128::from(block);
        if let Some(min) = min {
          if offset < min {
            return false;
          }
        }
        if let Some(max) = max {
          if offset > max {
            return false;
          }
        }
      } else {
        return false;
      }
    }

    true
  }

  fn token_proof_apply_delegation_template(
    &self,
    template: &serde_json::Value,
    fill: &serde_json::Value,
    constraints: &serde_json::Value,
    block: u32,
  ) -> Option<(serde_json::Value, std::collections::HashSet<String>)> {
    if !template.is_object()
      || !fill.is_object()
      || !constraints.is_object()
      || template.is_array()
      || fill.is_array()
      || constraints.is_array()
    {
      return None;
    }

    fn substitute(
      value: &serde_json::Value,
      fill: &serde_json::Value,
      used: &mut std::collections::HashSet<String>,
    ) -> Option<serde_json::Value> {
      match value {
        serde_json::Value::String(s) => {
          if s.starts_with('$') && s.len() > 1 {
            let name = &s[1..];
            if name
              .bytes()
              .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            {
              let replacement = fill.get(name)?.clone();
              used.insert(name.to_string());
              return Some(replacement);
            }
          }
          Some(value.clone())
        }
        serde_json::Value::Array(items) => {
          let mut out = Vec::with_capacity(items.len());
          for item in items {
            out.push(substitute(item, fill, used)?);
          }
          Some(serde_json::Value::Array(out))
        }
        serde_json::Value::Object(map) => {
          let mut out = serde_json::Map::new();
          for (key, child) in map {
            out.insert(key.clone(), substitute(child, fill, used)?);
          }
          Some(serde_json::Value::Object(out))
        }
        _ => Some(value.clone()),
      }
    }

    let mut used = std::collections::HashSet::new();
    let action = substitute(template, fill, &mut used)?;

    for name in used.iter() {
      let value = fill.get(name)?;
      let constraint = constraints.get(name)?;
      if !self.token_proof_validate_delegation_constraint(value, constraint, block) {
        return None;
      }
    }

    let Some(constraint_map) = constraints.as_object() else {
      return None;
    };
    for (path, constraint) in constraint_map {
      if used.contains(path) {
        continue;
      }
      let value = Self::token_proof_get_path_value(&action, path)?;
      if !self.token_proof_validate_delegation_constraint(value, constraint, block) {
        return None;
      }
    }

    Some((action, used))
  }

  fn token_proof_delegation_constraint_is_exact(constraint: &serde_json::Value) -> bool {
    let Some(obj) = constraint.as_object() else {
      return false;
    };
    obj.contains_key("equals")
      || obj
        .get("allowed")
        .and_then(|v| v.as_array())
        .map(|allowed| !allowed.is_empty())
        .unwrap_or(false)
  }

  fn token_proof_delegation_needs_final_fill(
    used_placeholders: &std::collections::HashSet<String>,
    constraints: &serde_json::Value,
  ) -> bool {
    let Some(constraint_map) = constraints.as_object() else {
      return true;
    };
    used_placeholders.iter().any(|name| {
      constraint_map
        .get(name)
        .map(|constraint| !Self::token_proof_delegation_constraint_is_exact(constraint))
        .unwrap_or(true)
    })
  }

  fn token_proof_final_action_message(
    delegation: &serde_json::Value,
    finalizers: &serde_json::Value,
    final_action: &serde_json::Value,
  ) -> Option<serde_json::Value> {
    Some(serde_json::Value::Array(vec![
      serde_json::Value::String("tap-delegated-final-action-v1".to_string()),
      Self::token_proof_delegation_message(delegation)?,
      finalizers.get("threshold")?.clone(),
      finalizers.get("signers")?.clone(),
      final_action.clone(),
    ]))
  }

  fn token_proof_action_final_message(
    delegation: &serde_json::Value,
    finalizers: &serde_json::Value,
    final_action: &serde_json::Value,
  ) -> Option<serde_json::Value> {
    Some(serde_json::Value::Array(vec![
      serde_json::Value::String("tap-delegated-final-action-v1".to_string()),
      Self::token_proof_action_delegation_message(delegation)?,
      finalizers.get("threshold")?.clone(),
      finalizers.get("signers")?.clone(),
      final_action.clone(),
    ]))
  }

  fn token_proof_validate_final_action_signatures(
    &self,
    action: &serde_json::Value,
    delegation: &serde_json::Value,
    final_action: &serde_json::Value,
  ) -> bool {
    let Some(finalizers) = delegation.get("finalizers") else {
      return false;
    };
    let Some(final_obj) = action.get("final") else {
      return false;
    };
    if !finalizers.is_object() || !final_obj.is_object() {
      return false;
    }
    let Some(signers_arr) = finalizers.get("signers").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(sigs_arr) = final_obj.get("sigs").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(salt_val) = final_obj.get("salt") else {
      return false;
    };
    let salt = Self::js_value_to_string(salt_val);

    let mut signers = std::collections::HashSet::new();
    for signer in signers_arr {
      let Some(s) = signer.as_str() else {
        return false;
      };
      let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(s) else {
        return false;
      };
      if !signers.insert(normalized) {
        return false;
      }
    }
    let Some(threshold_i) = finalizers.get("threshold").and_then(Self::js_parse_int) else {
      return false;
    };
    let Ok(threshold) = usize::try_from(threshold_i) else {
      return false;
    };
    if signers.is_empty()
      || signers.len() > 8
      || threshold == 0
      || threshold > signers.len()
      || threshold > 8
    {
      return false;
    }

    let Some(message) =
      Self::token_proof_final_action_message(delegation, finalizers, final_action)
    else {
      return false;
    };
    let msg_hash = Self::build_sha256_json_plus_salt(&message, &salt);
    let mut valid_pubkeys = std::collections::HashSet::new();
    for entry in sigs_arr {
      let Some(sig_obj) = entry.get("sig") else {
        return false;
      };
      let Some(hash_str) = entry.get("hash").and_then(|v| v.as_str()) else {
        return false;
      };
      if let Some((ok, _, pubkey)) =
        self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
      {
        let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(&pubkey) else {
          return false;
        };
        if ok && signers.contains(&normalized) {
          valid_pubkeys.insert(normalized);
        }
      }
    }

    valid_pubkeys.len() >= threshold
  }

  fn token_proof_validate_action_final_signatures(
    &self,
    action: &serde_json::Value,
    delegation: &serde_json::Value,
    final_action: &serde_json::Value,
  ) -> bool {
    let Some(finalizers) = delegation.get("finalizers") else {
      return false;
    };
    let Some(final_obj) = action.get("final") else {
      return false;
    };
    if !finalizers.is_object() || !final_obj.is_object() {
      return false;
    }
    let Some(signers_arr) = finalizers.get("signers").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(sigs_arr) = final_obj.get("sigs").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(salt_val) = final_obj.get("salt") else {
      return false;
    };
    let salt = Self::js_value_to_string(salt_val);

    let mut signers = std::collections::HashSet::new();
    for signer in signers_arr {
      let Some(s) = signer.as_str() else {
        return false;
      };
      let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(s) else {
        return false;
      };
      if !signers.insert(normalized) {
        return false;
      }
    }
    let Some(threshold_i) = finalizers.get("threshold").and_then(Self::js_parse_int) else {
      return false;
    };
    let Ok(threshold) = usize::try_from(threshold_i) else {
      return false;
    };
    if signers.is_empty()
      || signers.len() > 8
      || threshold == 0
      || threshold > signers.len()
      || threshold > 8
    {
      return false;
    }

    let Some(message) =
      Self::token_proof_action_final_message(delegation, finalizers, final_action)
    else {
      return false;
    };
    let msg_hash = Self::build_sha256_json_plus_salt(&message, &salt);
    let mut valid_pubkeys = std::collections::HashSet::new();
    for entry in sigs_arr {
      let Some(sig_obj) = entry.get("sig") else {
        return false;
      };
      let Some(hash_str) = entry.get("hash").and_then(|v| v.as_str()) else {
        return false;
      };
      if let Some((ok, _, pubkey)) =
        self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
      {
        let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(&pubkey) else {
          return false;
        };
        if ok && signers.contains(&normalized) {
          valid_pubkeys.insert(normalized);
        }
      }
    }

    valid_pubkeys.len() >= threshold
  }

  fn token_proof_valid_auth_link(&mut self, auth: &str) -> Option<(TokenAuthCreateRecord, String)> {
    let ptr = self
      .tap_get::<String>(&format!("tains/{}", auth))
      .ok()
      .flatten()?;
    let link = self.tap_get::<TokenAuthCreateRecord>(&ptr).ok().flatten()?;
    if self
      .tap_get::<String>(&format!("tac/{}", link.ins))
      .ok()
      .flatten()
      .is_some()
    {
      return None;
    }
    let auth_msg_hash = Self::build_sha256_json_plus_salt(
      &serde_json::Value::Array(
        link
          .auth
          .iter()
          .map(|s| serde_json::Value::String(s.clone()))
          .collect(),
      ),
      &link.slt,
    );
    let (ok, _, pubkey) =
      self.verify_sig_obj_against_msg_with_hash(&link.sig, &link.hash, &auth_msg_hash)?;
    if !ok {
      return None;
    }
    Some((link, pubkey.to_lowercase()))
  }

  fn token_proof_validate_delegation_signatures(
    &self,
    delegation: &serde_json::Value,
    auth_pubkey: &str,
  ) -> bool {
    let Some(signers_arr) = delegation.get("signers").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(sigs_arr) = delegation.get("sigs").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(salt_val) = delegation.get("salt") else {
      return false;
    };
    let salt = Self::js_value_to_string(salt_val);

    let mut signers = std::collections::HashSet::new();
    for signer in signers_arr {
      let Some(s) = signer.as_str() else {
        return false;
      };
      let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(s) else {
        return false;
      };
      if !signers.insert(normalized) {
        return false;
      }
    }

    let Some(auth_signer) = Self::token_proof_compressed_delegation_pubkey(auth_pubkey) else {
      return false;
    };
    if signers.is_empty() || signers.len() > 8 || !signers.contains(&auth_signer) {
      return false;
    }
    let Some(threshold_i) = delegation.get("threshold").and_then(Self::js_parse_int) else {
      return false;
    };
    let Ok(threshold) = usize::try_from(threshold_i) else {
      return false;
    };
    if threshold == 0 || threshold > signers.len() || threshold > 8 {
      return false;
    }

    let Some(message) = Self::token_proof_delegation_message(delegation) else {
      return false;
    };
    let msg_hash = Self::build_sha256_json_plus_salt(&message, &salt);
    let mut valid_pubkeys = std::collections::HashSet::new();
    for entry in sigs_arr {
      let Some(sig_obj) = entry.get("sig") else {
        return false;
      };
      let Some(hash_str) = entry.get("hash").and_then(|v| v.as_str()) else {
        return false;
      };
      if let Some((ok, _, pubkey)) =
        self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
      {
        let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(&pubkey) else {
          return false;
        };
        if ok && signers.contains(&normalized) {
          valid_pubkeys.insert(normalized);
        }
      }
    }

    valid_pubkeys.contains(&auth_signer) && valid_pubkeys.len() >= threshold
  }

  fn token_proof_validate_action_delegation_signatures(
    &self,
    delegation: &serde_json::Value,
    auth_pubkey: &str,
  ) -> bool {
    let Some(version_val) = delegation.get("v") else {
      return false;
    };
    if delegation.get("kind").and_then(|v| v.as_str()) != Some("action")
      || Self::js_value_to_string(version_val) != "1"
      || delegation.get("family").and_then(|v| v.as_str()).is_none()
    {
      return false;
    }
    let Some(signers_arr) = delegation.get("signers").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(sigs_arr) = delegation.get("sigs").and_then(|v| v.as_array()) else {
      return false;
    };
    let Some(salt_val) = delegation.get("salt") else {
      return false;
    };
    let salt = Self::js_value_to_string(salt_val);

    let mut signers = std::collections::HashSet::new();
    for signer in signers_arr {
      let Some(s) = signer.as_str() else {
        return false;
      };
      let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(s) else {
        return false;
      };
      if !signers.insert(normalized) {
        return false;
      }
    }

    let Some(auth_signer) = Self::token_proof_compressed_delegation_pubkey(auth_pubkey) else {
      return false;
    };
    if signers.is_empty() || signers.len() > 8 || !signers.contains(&auth_signer) {
      return false;
    }
    let Some(threshold_i) = delegation.get("threshold").and_then(Self::js_parse_int) else {
      return false;
    };
    let Ok(threshold) = usize::try_from(threshold_i) else {
      return false;
    };
    if threshold == 0 || threshold > signers.len() || threshold > 8 {
      return false;
    }

    let Some(message) = Self::token_proof_action_delegation_message(delegation) else {
      return false;
    };
    let msg_hash = Self::build_sha256_json_plus_salt(&message, &salt);
    let mut valid_pubkeys = std::collections::HashSet::new();
    for entry in sigs_arr {
      let Some(sig_obj) = entry.get("sig") else {
        return false;
      };
      let Some(hash_str) = entry.get("hash").and_then(|v| v.as_str()) else {
        return false;
      };
      if let Some((ok, _, pubkey)) =
        self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
      {
        let Some(normalized) = Self::token_proof_compressed_delegation_pubkey(&pubkey) else {
          return false;
        };
        if ok && signers.contains(&normalized) {
          valid_pubkeys.insert(normalized);
        }
      }
    }

    valid_pubkeys.contains(&auth_signer) && valid_pubkeys.len() >= threshold
  }

  fn validate_token_proof_delegated_execute_action(
    &mut self,
    action: &serde_json::Value,
    inscription: &str,
    action_index: usize,
    block: u32,
  ) -> Option<TokenDelegatedLockValidation> {
    if !Self::token_proof_is_delegated_execute_action(action) {
      return None;
    }
    let delegation = action.get("delegation")?;
    if !delegation.is_object() {
      return None;
    }
    let fill = action.get("fill")?;
    if !fill.is_object() || fill.is_array() {
      return None;
    }
    let has_final_shape = delegation.get("finalizers").is_some() || action.get("final").is_some();
    if has_final_shape && !self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation)
    {
      return None;
    }
    let auth = delegation.get("auth")?.as_str()?;
    let nonce = delegation.get("nonce")?.as_str()?;
    if !Self::token_proof_valid_delegation_nonce(nonce) {
      return None;
    }
    let expiry = delegation.get("expiry").and_then(Self::js_parse_int)?;
    if i128::from(block) > expiry {
      return None;
    }

    let nonce_key = Self::token_proof_delegation_nonce_key(auth, nonce);
    let cancel_key = Self::token_proof_delegation_cancel_key(auth, nonce);
    if self.tap_get::<String>(&nonce_key).ok().flatten().is_some()
      || self.tap_get::<String>(&cancel_key).ok().flatten().is_some()
    {
      return None;
    }

    let (link, auth_pubkey) = self.token_proof_valid_auth_link(auth)?;
    if !self.token_proof_validate_delegation_signatures(delegation, &auth_pubkey) {
      return None;
    }
    let constraints = delegation
      .get("constraints")
      .cloned()
      .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    let (mut final_action, used_placeholders) = self.token_proof_apply_delegation_template(
      delegation.get("template")?,
      fill,
      &constraints,
      block,
    )?;
    if !final_action
      .get("op")
      .and_then(|v| v.as_str())
      .map(|op| op.eq_ignore_ascii_case("lock"))
      .unwrap_or(false)
    {
      return None;
    }

    if !link.auth.is_empty() {
      let tick = Self::token_proof_action_tick(&final_action)?;
      if !link.auth.iter().any(|t| t == tick) {
        return None;
      }
    }

    let normalized = self.validate_token_proof_lock_action(&mut final_action, &link)?;
    if self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation) {
      let needs_final = has_final_shape
        || Self::token_proof_delegation_needs_final_fill(&used_placeholders, &constraints);
      if needs_final
        && !self.token_proof_validate_final_action_signatures(action, delegation, &final_action)
      {
        return None;
      }
    }
    let id = Self::tap_token_proof_lock_id(inscription, action_index);
    if self
      .tap_get::<TokenLockRecord>(&format!("l/{}", id))
      .ok()
      .flatten()
      .is_some()
    {
      return None;
    }
    Some(TokenDelegatedLockValidation {
      action: final_action,
      link,
      normalized,
      nonce_key,
    })
  }

  fn validate_token_proof_generic_delegated_action(
    &mut self,
    action: &serde_json::Value,
    inscription: &str,
    action_index: usize,
    block: u32,
    pending_locks: &std::collections::HashMap<String, i128>,
    pending_amm_credits: &std::collections::HashMap<String, BigInt>,
    pending_amm_debits: &std::collections::HashMap<String, BigInt>,
    pending_obligations: &std::collections::HashMap<String, BigInt>,
    pending_perp_debits: &std::collections::HashMap<String, BigInt>,
  ) -> Option<TokenDelegatedActionValidation> {
    if !Self::token_proof_is_generic_delegated_action(action) {
      return None;
    }
    let delegation = action.get("delegation")?;
    if !delegation.is_object() {
      return None;
    }
    let fill = action.get("fill")?;
    if !fill.is_object() || fill.is_array() {
      return None;
    }
    let family = delegation.get("family")?.as_str()?.to_lowercase();
    if delegation.get("kind").and_then(|v| v.as_str()) != Some("action")
      || delegation.get("v").map(Self::js_value_to_string).as_deref() != Some("1")
      || (family != "perp-join" && family != "perp-close")
    {
      return None;
    }
    let has_final_shape = delegation.get("finalizers").is_some() || action.get("final").is_some();
    if has_final_shape && !self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation)
    {
      return None;
    }
    let auth = delegation.get("auth")?.as_str()?;
    let nonce = delegation.get("nonce")?.as_str()?;
    if !Self::token_proof_valid_delegation_nonce(nonce) {
      return None;
    }
    let expiry = delegation.get("expiry").and_then(Self::js_parse_int)?;
    if i128::from(block) > expiry {
      return None;
    }

    let nonce_key = Self::token_proof_delegation_nonce_key(auth, nonce);
    let cancel_key = Self::token_proof_delegation_cancel_key(auth, nonce);
    if self.tap_get::<String>(&nonce_key).ok().flatten().is_some()
      || self.tap_get::<String>(&cancel_key).ok().flatten().is_some()
    {
      return None;
    }

    let (link, auth_pubkey) = self.token_proof_valid_auth_link(auth)?;
    if !self.token_proof_validate_action_delegation_signatures(delegation, &auth_pubkey) {
      return None;
    }
    let constraints = delegation
      .get("constraints")
      .cloned()
      .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    let (final_action, used_placeholders) = self.token_proof_apply_delegation_template(
      delegation.get("template")?,
      fill,
      &constraints,
      block,
    )?;
    if final_action
      .get("op")
      .and_then(|v| v.as_str())
      .map(|op| op.to_lowercase() != family)
      .unwrap_or(true)
    {
      return None;
    }
    if self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation) {
      let needs_final = has_final_shape
        || Self::token_proof_delegation_needs_final_fill(&used_placeholders, &constraints);
      if needs_final
        && !self.token_proof_validate_action_final_signatures(action, delegation, &final_action)
      {
        return None;
      }
    }

    if family == "perp-join" {
      let join = self.validate_perp_join_action(
        &final_action,
        Some(&link),
        inscription,
        action_index,
        block,
        pending_locks,
        pending_amm_credits,
        pending_amm_debits,
        pending_obligations,
        pending_perp_debits,
      )?;
      if !link.auth.is_empty() && !link.auth.iter().any(|t| t == &join.tick) {
        return None;
      }
      return Some(TokenDelegatedActionValidation {
        family,
        action: final_action,
        link,
        join: Some(join),
        position: None,
        nonce_key,
      });
    }

    let position = self.validate_perp_close_action(&final_action, Some(&link), block)?;
    let tick = position.group.get("collateral")?.get("tick")?.as_str()?;
    if !link.auth.is_empty() && !link.auth.iter().any(|t| t == tick) {
      return None;
    }
    Some(TokenDelegatedActionValidation {
      family,
      action: final_action,
      link,
      join: None,
      position: Some(position),
      nonce_key,
    })
  }

  fn token_proof_primary_delegated_link(
    &mut self,
    actions: &[serde_json::Value],
  ) -> Option<TokenAuthCreateRecord> {
    let mut auth_id: Option<String> = None;
    for action in actions {
      if !Self::token_proof_is_delegated_action_envelope(action) {
        return None;
      }
      let auth = action.get("delegation")?.get("auth")?.as_str()?.to_string();
      if let Some(existing) = &auth_id {
        if existing != &auth {
          return None;
        }
      } else {
        auth_id = Some(auth);
      }
    }
    let auth = auth_id?;
    self
      .token_proof_valid_auth_link(&auth)
      .map(|(link, _)| link)
  }

  fn validate_token_proof_delegation_cancel_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
  ) -> Option<TokenDelegationCancelValidation> {
    let link = link?;
    if !action
      .get("op")
      .and_then(|v| v.as_str())
      .map(|op| op.eq_ignore_ascii_case("cancel-delegation"))
      .unwrap_or(false)
    {
      return None;
    }
    let nonce = action.get("nonce")?.as_str()?;
    if !Self::token_proof_valid_delegation_nonce(nonce) {
      return None;
    }
    let auth = action
      .get("auth")
      .map(Self::js_value_to_string)
      .unwrap_or_else(|| link.ins.clone());
    if auth != link.ins {
      return None;
    }
    let nonce_key = Self::token_proof_delegation_nonce_key(&auth, nonce);
    let cancel_key = Self::token_proof_delegation_cancel_key(&auth, nonce);
    if self.tap_get::<String>(&nonce_key).ok().flatten().is_some()
      || self.tap_get::<String>(&cancel_key).ok().flatten().is_some()
    {
      return None;
    }
    Some(TokenDelegationCancelValidation {
      cancel_key,
      auth,
      nonce: nonce.to_string(),
    })
  }
  // END TAP-DELEGATED-LOCKS

  fn token_proof_storage_height(value: Option<&serde_json::Value>) -> Option<u32> {
    let parsed = Self::js_parse_int(value?)?;
    u32::try_from(parsed).ok()
  }

  fn token_proof_safe_id(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
      && value.len() <= max_bytes
      && value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
  }

  fn token_proof_safe_uint_number(value: &serde_json::Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
      return if n <= 9_007_199_254_740_991 {
        Some(n)
      } else {
        None
      };
    }
    let n = value.as_f64()?;
    if n.is_finite() && n >= 0.0 && n.fract() == 0.0 && n <= 9_007_199_254_740_991_f64 {
      Some(n as u64)
    } else {
      None
    }
  }

  fn normalize_token_proof_metadata_value(
    value: &serde_json::Value,
    depth: usize,
  ) -> Option<serde_json::Value> {
    if depth > 4 {
      return None;
    }
    if value.is_null() || value.is_boolean() {
      return Some(value.clone());
    }
    if let Some(s) = value.as_str() {
      return if s.len() <= 512 {
        Some(serde_json::Value::String(s.to_string()))
      } else {
        None
      };
    }
    if let Some(n) = Self::token_proof_safe_uint_number(value) {
      return Some(serde_json::json!(n));
    }
    if value.is_object() && !value.is_array() {
      return Self::normalize_token_proof_metadata_object(value, depth + 1);
    }
    None
  }

  fn normalize_token_proof_metadata_object(
    value: &serde_json::Value,
    depth: usize,
  ) -> Option<serde_json::Value> {
    let object = value.as_object()?;
    let mut normalized = serde_json::Map::new();
    let mut keys: Vec<&String> = object.keys().collect();
    keys.sort();
    for key in keys {
      if !Self::token_proof_safe_id(key, 64) {
        return None;
      }
      let item = Self::normalize_token_proof_metadata_value(object.get(key)?, depth)?;
      normalized.insert(key.clone(), item);
    }
    let out = serde_json::Value::Object(normalized);
    if Self::js_json_stringify(&out).len() <= 1024 {
      Some(out)
    } else {
      None
    }
  }

  fn normalize_token_proof_optional_data(
    value: Option<&serde_json::Value>,
  ) -> Option<Option<serde_json::Value>> {
    let Some(value) = value else {
      return Some(None);
    };
    if let Some(s) = value.as_str() {
      return if s.len() <= 512 {
        Some(Some(serde_json::Value::String(s.to_string())))
      } else {
        None
      };
    }
    if value.is_boolean() {
      return Some(Some(value.clone()));
    }
    if let Some(n) = Self::token_proof_safe_uint_number(value) {
      return Some(Some(serde_json::json!(n)));
    }
    if value.is_object() && !value.is_array() {
      return Some(Some(Self::normalize_token_proof_metadata_object(value, 0)?));
    }
    None
  }

  fn token_proof_action_data<'a>(
    action: &'a serde_json::Value,
  ) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    action.get("data")?.as_object()
  }

  fn token_proof_data_string<'a>(value: Option<&'a serde_json::Value>) -> Option<&'a str> {
    let s = value?.as_str()?;
    if s.is_empty() || s.len() > 512 {
      None
    } else {
      Some(s)
    }
  }

  fn token_proof_validate_app_data(
    action: &serde_json::Value,
    required: &[&str],
  ) -> Option<serde_json::Value> {
    let data = Self::token_proof_action_data(action)?;
    let mut normalized = serde_json::Map::new();
    for field in required {
      normalized.insert(
        field.to_string(),
        serde_json::Value::String(Self::token_proof_data_string(data.get(*field))?.to_string()),
      );
    }
    let mut keys: Vec<&String> = data.keys().collect();
    keys.sort();
    for key in keys {
      if required.iter().any(|required_key| required_key == key) {
        continue;
      }
      if key == "ext" {
        let ext = Self::normalize_token_proof_metadata_object(data.get(key)?, 0)?;
        normalized.insert(key.clone(), ext);
        continue;
      }
      if !Self::token_proof_safe_id(key, 64) {
        return None;
      }
      normalized.insert(
        key.clone(),
        serde_json::Value::String(Self::token_proof_data_string(data.get(key))?.to_string()),
      );
    }
    if let Some(dom) = normalized.get("dom").and_then(|v| v.as_str()) {
      if !Self::token_proof_safe_id(dom, 128) {
        return None;
      }
    }
    if let Some(reference) = normalized.get("ref").and_then(|v| v.as_str()) {
      if !Self::token_proof_safe_id(reference, 128) {
        return None;
      }
    }
    let mut sorted = serde_json::Map::new();
    let mut sorted_keys: Vec<String> = normalized.keys().cloned().collect();
    sorted_keys.sort();
    for key in sorted_keys {
      sorted.insert(key.clone(), normalized.get(&key)?.clone());
    }
    Some(serde_json::Value::Object(sorted))
  }

  fn token_proof_normalize_data_address(
    action: &mut serde_json::Value,
    key: &str,
  ) -> Option<String> {
    let current = action.get("data")?.get(key)?.as_str()?.to_string();
    let normalized = Self::normalize_address(&current);
    if let Some(data) = action.get_mut("data").and_then(|v| v.as_object_mut()) {
      data.insert(
        key.to_string(),
        serde_json::Value::String(normalized.clone()),
      );
    }
    Some(normalized)
  }

  fn token_proof_requires_no_refund(action: &serde_json::Value) -> bool {
    action.get("refund").is_none() && action.get("refund_after").is_none()
  }

  fn token_proof_authority_condition_active(&mut self, condition: &serde_json::Value) -> bool {
    let Some(auth) = condition.get("auth").and_then(|v| v.as_str()) else {
      return false;
    };
    self
      .tap_get::<String>(&format!("tains/{}", auth))
      .ok()
      .flatten()
      .is_some()
      && self
        .tap_get::<String>(&format!("tac/{}", auth))
        .ok()
        .flatten()
        .is_none()
  }

  fn validate_token_proof_lock_kind(
    &mut self,
    action: &mut serde_json::Value,
    kind: &str,
    link: &TokenAuthCreateRecord,
  ) -> Option<(serde_json::Value, Option<serde_json::Value>)> {
    let Some(condition) = action.get("condition").cloned() else {
      return None;
    };
    let condition_type = condition
      .get("type")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    let has_refund = action.get("refund").and_then(|v| v.as_str()).is_some();
    let has_refund_after = Self::token_proof_storage_height(action.get("refund_after")).is_some();
    let default_data = Self::normalize_token_proof_optional_data(action.get("data"))?;

    match kind {
      "htlc" => {
        let hash = condition.get("hash").and_then(|v| v.as_str())?;
        if condition_type != "hashlock"
          || !Self::tap_is_valid_sha256_hex(hash)
          || !has_refund
          || !has_refund_after
        {
          return None;
        }
        Some((
          serde_json::json!({ "type": "hashlock", "hash": hash.to_lowercase() }),
          default_data,
        ))
      }
      "vesting" => {
        let min = Self::token_proof_storage_height(condition.get("min"))?;
        if condition_type != "height" || !Self::token_proof_requires_no_refund(action) {
          return None;
        }
        Some((
          serde_json::json!({ "type": "height", "min": min }),
          Some(Self::token_proof_validate_app_data(
            action,
            &["dom", "ref"],
          )?),
        ))
      }
      "cooldown" => {
        let min = Self::token_proof_storage_height(condition.get("min"))?;
        if condition_type != "height"
          || !Self::token_proof_requires_no_refund(action)
          || action.get("claim").and_then(|v| v.as_str()) != Some(link.addr.as_str())
        {
          return None;
        }
        Some((
          serde_json::json!({ "type": "height", "min": min }),
          Some(Self::token_proof_validate_app_data(
            action,
            &["dom", "ref"],
          )?),
        ))
      }
      "escrow" => {
        if Self::token_proof_validate_app_data(action, &["dom", "ref", "payer", "payee"]).is_none()
        {
          return None;
        }
        let payer = Self::token_proof_normalize_data_address(action, "payer");
        let payee = Self::token_proof_normalize_data_address(action, "payee");
        let mut data =
          Self::token_proof_validate_app_data(action, &["dom", "ref", "payer", "payee"])?;
        let auth = condition.get("auth").and_then(|v| v.as_str())?;
        if condition_type != "authority"
          || !self.token_proof_authority_condition_active(&condition)
          || !has_refund
          || !has_refund_after
          || payer.as_deref() != Some(link.addr.as_str())
          || payee.as_deref() != action.get("claim").and_then(|v| v.as_str())
        {
          return None;
        }
        if let Some(object) = data.as_object_mut() {
          object.insert("payer".to_string(), serde_json::Value::String(payer?));
          object.insert("payee".to_string(), serde_json::Value::String(payee?));
        }
        Some((
          serde_json::json!({ "type": "authority", "auth": auth }),
          Some(data),
        ))
      }
      "otc" => {
        if !has_refund
          || !has_refund_after
          || Self::token_proof_validate_app_data(action, &["dom", "ref", "cp"]).is_none()
        {
          return None;
        }
        let data = Self::token_proof_validate_app_data(action, &["dom", "ref", "cp"])?;
        if condition_type == "hashlock" {
          let hash = condition.get("hash").and_then(|v| v.as_str())?;
          if !Self::tap_is_valid_sha256_hex(hash) {
            return None;
          }
          return Some((
            serde_json::json!({ "type": "hashlock", "hash": hash.to_lowercase() }),
            Some(data),
          ));
        }
        let auth = condition.get("auth").and_then(|v| v.as_str())?;
        if condition_type == "authority" && self.token_proof_authority_condition_active(&condition)
        {
          return Some((
            serde_json::json!({ "type": "authority", "auth": auth }),
            Some(data),
          ));
        }
        None
      }
      _ => None,
    }
  }

  fn validate_token_proof_lock_action(
    &mut self,
    action: &mut serde_json::Value,
    link: &TokenAuthCreateRecord,
  ) -> Option<TokenProofLockValidation> {
    let kind = action.get("kind")?.as_str()?.to_lowercase();
    if !matches!(
      kind.as_str(),
      "htlc" | "vesting" | "escrow" | "otc" | "cooldown"
    ) {
      return None;
    }
    let tick = action.get("tick")?.as_str()?.to_string();
    let tick_key = Self::json_stringify_lower(&tick);
    let deployed = self
      .tap_get::<DeployRecord>(&format!("d/{}", tick_key))
      .ok()
      .flatten()?;
    // START TAP-DELEGATED-LOCKS
    // Match tap-writer's post-value_stringify reviver: raw numeric max/lim/amt are rejected.
    // Delegated templates can create the final amt after parsing, so the final action must be gated too.
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation)
      && action.get("amt")?.is_number()
    {
      return None;
    }
    // END TAP-DELEGATED-LOCKS
    let amt_str = Self::js_value_to_string(action.get("amt")?);
    let amt_norm = Self::resolve_number_string(&amt_str, deployed.dec)?;
    let max_norm = Self::resolve_number_string(MAX_DEC_U64_STR, deployed.dec)?;
    let amount = amt_norm.parse::<i128>().ok()?;
    let max_amount = max_norm.parse::<i128>().ok()?;
    if amount <= 0 || amount > max_amount {
      return None;
    }

    let (allocations, allocation_amount) =
      self.normalize_token_proof_allocations(action, &deployed, max_amount)?;

    let total_amount = amount.checked_add(allocation_amount)?;
    if total_amount <= 0 || total_amount > max_amount {
      return None;
    }

    let claim_norm = Self::normalize_address(action.get("claim")?.as_str()?);
    if !self.is_valid_bitcoin_address(&claim_norm) {
      return None;
    }
    if let Some(v) = action.get_mut("claim") {
      *v = serde_json::Value::String(claim_norm);
    }

    if action.get("refund").is_some() {
      let refund_norm = Self::normalize_address(action.get("refund")?.as_str()?);
      if !self.is_valid_bitcoin_address(&refund_norm) {
        return None;
      }
      if let Some(v) = action.get_mut("refund") {
        *v = serde_json::Value::String(refund_norm);
      }
    }

    let (condition, data) = self.validate_token_proof_lock_kind(action, &kind, link)?;
    if let Some(object) = action.as_object_mut() {
      object.insert("condition".to_string(), condition);
      if let Some(data) = data {
        object.insert("data".to_string(), data);
      } else {
        object.remove("data");
      }
    }

    if action.get("cert").is_some() {
      return None;
    }
    let control = self.normalize_certified_control_policy(action.get("control"), action)?;
    if let Some(object) = action.as_object_mut() {
      if let Some(control) = control.clone() {
        object.insert("control".to_string(), control);
      } else {
        object.remove("control");
      }
    }

    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", link.addr, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", link.addr, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let locked = self.tap_get_locked_amount(&link.addr, &tick_key);
    let obligation_locked = self.tap_get_account_obligation_locked_amount(&link.addr, &tick_key);
    if balance - transferable - locked - obligation_locked - total_amount < 0 {
      return None;
    }

    Some(TokenProofLockValidation {
      kind,
      tick_key,
      tick: Self::js_to_lowercase(&tick),
      amount,
      allocations,
      allocation_amount,
      total_amount,
      control,
    })
  }

  fn process_token_proof_lock_action(
    &mut self,
    action: &mut serde_json::Value,
    action_index: usize,
    link: &TokenAuthCreateRecord,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_token_proof_lock_action(action, link) else {
      return false;
    };
    let id = Self::tap_token_proof_lock_id(inscription, action_index);
    if self
      .tap_get::<TokenLockRecord>(&format!("l/{}", id))
      .ok()
      .flatten()
      .is_some()
    {
      return false;
    }
    if !self.tap_add_locked_amount(&link.addr, &normalized.tick_key, normalized.total_amount) {
      return false;
    }

    let rec = TokenLockRecord {
      id: id.clone(),
      owner: link.addr.clone(),
      auth: link.ins.clone(),
      kind: normalized.kind,
      tick: normalized.tick.clone(),
      amt: normalized.amount.to_string(),
      remaining: normalized.amount.to_string(),
      fee: None,
      al: if normalized.allocations.is_empty() {
        None
      } else {
        Some(normalized.allocations.clone())
      },
      total: if normalized.allocations.is_empty() {
        None
      } else {
        Some(normalized.total_amount.to_string())
      },
      claim: action
        .get("claim")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string(),
      refund: action
        .get("refund")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()),
      condition: action
        .get("condition")
        .cloned()
        .unwrap_or(serde_json::Value::Null),
      refund_after: action
        .get("refund_after")
        .and_then(|value| Self::token_proof_storage_height(Some(value))),
      data: action.get("data").cloned(),
      control: normalized.control.clone(),
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    };

    let _ = self.tap_put(&format!("l/{}", id), &rec);
    let _ = self.tap_set_list_record(
      &format!("la/{}", link.addr),
      &format!("lai/{}", link.addr),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("lt/{}", normalized.tick_key),
      &format!("lti/{}", normalized.tick_key),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("lk/{}", rec.kind),
      &format!("lki/{}", rec.kind),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("lak/{}/{}", link.addr, rec.kind),
      &format!("laki/{}/{}", link.addr, rec.kind),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("ltk/{}/{}", normalized.tick_key, rec.kind),
      &format!("ltki/{}/{}", normalized.tick_key, rec.kind),
      &rec,
    );
    if let Ok(list_len) = self.tap_set_list_record("sl", "sli", &rec) {
      let ptr = format!("sli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/lck/{}", transaction),
        &format!("txi/lck/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/lck/{}", block),
        &format!("blcki/lck/{}", block),
        &ptr,
      );
    }
    true
  }

  fn validate_token_proof_release_action(
    &mut self,
    action: &serde_json::Value,
    link: &TokenAuthCreateRecord,
    block: u32,
  ) -> Option<TokenProofReleaseValidation> {
    if action.get("fee").is_some() || action.get("control").is_some() {
      return None;
    }
    let lock_id = action.get("lock").and_then(|v| v.as_str())?;
    if self
      .tap_get::<TokenLockConsumeRecord>(&format!("lc/{}", lock_id))
      .ok()
      .flatten()
      .is_some()
    {
      return None;
    }
    let lock = self
      .tap_get::<TokenLockRecord>(&format!("l/{}", lock_id))
      .ok()
      .flatten()?;
    let action_name = action
      .get("op")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    let condition_type = lock
      .condition
      .get("type")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    let mut target: Option<String> = None;

    if action_name == "claim" {
      if condition_type == "hashlock" {
        let refund_after = lock.refund_after?;
        if block >= refund_after {
          return None;
        }
        let preimage = action.get("preimage")?;
        let expected = lock
          .condition
          .get("hash")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .to_lowercase();
        if Self::tap_hash_proof_preimage(preimage).to_lowercase() != expected {
          return None;
        }
        target = Some(lock.claim.clone());
      } else if condition_type == "height" {
        let min = lock
          .condition
          .get("min")
          .and_then(|value| Self::token_proof_storage_height(Some(value)))?;
        if block < min {
          return None;
        }
        target = Some(lock.claim.clone());
      } else if condition_type == "authority" {
        let auth = lock
          .condition
          .get("auth")
          .and_then(|v| v.as_str())
          .unwrap_or("");
        if auth != link.ins {
          return None;
        }
        target = Some(lock.claim.clone());
      }
    } else if action_name == "refund" {
      let refund_after = lock.refund_after?;
      if block < refund_after {
        return None;
      }
      target = lock.refund.clone();
    }

    let target = target?;
    let tick_key = Self::json_stringify_lower(&lock.tick);
    let amount = lock.remaining.parse::<i128>().ok().unwrap_or(0);
    if amount <= 0 {
      return None;
    }
    let allocations = if let Some(list) = lock.al.clone() {
      list
    } else if let Some(fee_record) = lock.fee.clone() {
      vec![TokenAllocationRecord {
        tt: "a".to_string(),
        to: fee_record.addr,
        amt: fee_record.amt,
        rl: "of".to_string(),
      }]
    } else {
      Vec::new()
    };
    let mut allocation_amount = 0i128;
    for allocation in &allocations {
      let value = allocation.amt.parse::<i128>().ok()?;
      if value <= 0 {
        return None;
      }
      allocation_amount = allocation_amount.checked_add(value)?;
    }
    let total_amount = amount.checked_add(allocation_amount)?;
    let owner_balance_key = format!("b/{}/{}", lock.owner, tick_key);
    let owner_balance = self
      .tap_get::<String>(&owner_balance_key)
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let locked = self.tap_get_locked_amount(&lock.owner, &tick_key);
    if owner_balance < total_amount || locked < total_amount {
      return None;
    }

    let (cert_nonce_key, cert) =
      self.validate_certified_control_certificate(action, &lock, &action_name, block)?;

    Some(TokenProofReleaseValidation {
      lock,
      tick_key,
      amount,
      allocations,
      allocation_amount,
      total_amount,
      target,
      action_name,
      owner_balance,
      cert_nonce_key,
      cert,
    })
  }

  fn validate_token_proof_actions(
    &mut self,
    actions: &mut [serde_json::Value],
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    block: u32,
    timestamp: u32,
  ) -> bool {
    if !self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation) || actions.is_empty() {
      return false;
    }

    let mut pending_locks: std::collections::HashMap<String, i128> =
      std::collections::HashMap::new();
    let mut pending_sale_totals: std::collections::HashMap<String, i128> =
      std::collections::HashMap::new();
    let mut pending_sale_addresses: std::collections::HashMap<String, i128> =
      std::collections::HashMap::new();
    let mut consumed_locks: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut sale_finalizes: std::collections::HashSet<String> = std::collections::HashSet::new();
	    let mut sale_claims: std::collections::HashSet<String> = std::collections::HashSet::new();
	    let mut sale_refunds: std::collections::HashSet<String> = std::collections::HashSet::new();
	    let mut sale_cancels: std::collections::HashSet<String> = std::collections::HashSet::new();
	    let mut sale_resolves: std::collections::HashSet<String> = std::collections::HashSet::new();
	    let mut pending_sale_withdrawals: std::collections::HashMap<String, i128> =
	      std::collections::HashMap::new();
    let mut pending_amm_pools: std::collections::HashMap<String, AuthorityConfigRecord> =
      std::collections::HashMap::new();
    let mut pending_amm_credits: std::collections::HashMap<String, BigInt> =
      std::collections::HashMap::new();
    let mut pending_amm_debits: std::collections::HashMap<String, BigInt> =
      std::collections::HashMap::new();
    let mut pending_amm_lps: std::collections::HashMap<String, BigInt> =
      std::collections::HashMap::new();
    let mut pending_amm_refs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut pending_amm_snapshots: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut pending_obligations: std::collections::HashMap<String, BigInt> =
      std::collections::HashMap::new();
    let mut pending_perp_debits: std::collections::HashMap<String, BigInt> =
      std::collections::HashMap::new();
    let mut consumed_obligations: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut consumed_perp_positions: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut consumed_perp_groups: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut consumed_perp_cert_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut consumed_perp_evidence_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    // START TAP-DELEGATED-LOCKS
    let mut consumed_delegation_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut cancelled_delegation_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    // END TAP-DELEGATED-LOCKS
    let mut consumed_cert_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();

    for (i, action) in actions.iter_mut().enumerate() {
      let op = action
        .get("op")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase();
      let is_perp_cert_op = matches!(
        op.as_str(),
        "perp-activate" | "perp-close" | "perp-liquidate" | "perp-settle"
      );
      if (op != "lock" && action.get("control").is_some())
        || (op != "claim" && op != "refund" && !is_perp_cert_op && action.get("cert").is_some())
      {
        return false;
      }
      if op == "perp-policy" {
        if self.validate_perp_policy_action(action, block).is_none() {
          return false;
        }
      } else if op == "perp-open-group" {
        if self
          .validate_perp_open_group_action(action, inscription, i, block)
          .is_none()
        {
          return false;
        }
      } else if op == "perp-join" {
        let Some(normalized) = self.validate_perp_join_action(
          action,
          link,
          inscription,
          i,
          block,
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) else {
          return false;
        };
        let key = format!("{}/{}", normalized.owner, normalized.tick_key);
        let entry = pending_perp_debits
          .entry(key)
          .or_insert_with(|| BigInt::from(0));
        *entry = entry.clone() + normalized.collateral;
      } else if op == "perp-external-evidence" {
        let Some(normalized) =
          self.validate_perp_external_evidence_action(action, inscription, i, block)
        else {
          return false;
        };
        if !consumed_perp_positions.insert(normalized.position_id)
          || !consumed_perp_evidence_nonces.insert(normalized.nonce_key)
        {
          return false;
        }
      } else if op == "perp-cancel" {
        let Some(group) = self.validate_perp_cancel_action(action, block) else {
          return false;
        };
        let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_groups.insert(group_id.to_string()) {
          return false;
        }
      } else if op == "perp-refund" {
        let Some(normalized) = self.validate_perp_refund_action(action, link) else {
          return false;
        };
        let Some(position_id) = normalized.position.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_positions.insert(position_id.to_string()) {
          return false;
        }
      } else if op == "perp-claim" {
        let Some(normalized) = self.validate_perp_claim_action(action, link) else {
          return false;
        };
        let Some(position_id) = normalized.position.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_positions.insert(position_id.to_string()) {
          return false;
        }
      } else if op == "perp-activate" {
        let Some(normalized) = self.validate_perp_activate_action(action, link, block) else {
          return false;
        };
        let Some(group_id) = normalized.group.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_groups.insert(group_id.to_string())
          || !consumed_perp_cert_nonces.insert(normalized.certificate.nonce_key)
        {
          return false;
        }
      } else if op == "perp-close" {
        let Some(normalized) = self.validate_perp_close_action(action, link, block) else {
          return false;
        };
        let Some(position_id) = normalized.position.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_positions.insert(position_id.to_string())
          || !consumed_perp_cert_nonces.insert(normalized.certificate.nonce_key)
        {
          return false;
        }
      } else if op == "perp-liquidate" {
        let Some(normalized) = self.validate_perp_liquidate_action(action, link, block) else {
          return false;
        };
        let Some(position_id) = normalized.position.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_positions.insert(position_id.to_string())
          || !consumed_perp_cert_nonces.insert(normalized.certificate.nonce_key)
        {
          return false;
        }
      } else if op == "perp-settle" {
        let Some(normalized) = self.validate_perp_settle_action(action, link, block) else {
          return false;
        };
        let Some(group_id) = normalized.group.get("id").and_then(|v| v.as_str()) else {
          return false;
        };
        if !consumed_perp_groups.insert(group_id.to_string())
          || !consumed_perp_cert_nonces.insert(normalized.certificate.nonce_key)
        {
          return false;
        }
      } else if op == "lock" {
        let Some(link) = link else {
          return false;
        };
        let Some(normalized) = self.validate_token_proof_lock_action(action, link) else {
          return false;
        };
        let id = Self::tap_token_proof_lock_id(inscription, i);
        if self
          .tap_get::<TokenLockRecord>(&format!("l/{}", id))
          .ok()
          .flatten()
          .is_some()
        {
          return false;
        }

        let pending_key = format!("{}/{}", link.addr, normalized.tick_key);
        let pending = *pending_locks.get(&pending_key).unwrap_or(&0);
        if !self.has_available_with_pending_bigint(
          &link.addr,
          &normalized.tick_key,
          &BigInt::from(normalized.total_amount),
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) {
          return false;
        }
        pending_locks.insert(pending_key, pending + normalized.total_amount);
      // START TAP-DELEGATED-LOCKS
      } else if op == "execute" {
        if link.is_some() {
          return false;
        }
        let Some(delegated) =
          self.validate_token_proof_delegated_execute_action(action, inscription, i, block)
        else {
          return false;
        };
        if consumed_delegation_nonces.contains(&delegated.nonce_key) {
          return false;
        }
        let pending_key = format!("{}/{}", delegated.link.addr, delegated.normalized.tick_key);
        let pending = *pending_locks.get(&pending_key).unwrap_or(&0);
        if !self.has_available_with_pending_bigint(
          &delegated.link.addr,
          &delegated.normalized.tick_key,
          &BigInt::from(delegated.normalized.total_amount),
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) {
          return false;
        }
        pending_locks.insert(pending_key, pending + delegated.normalized.total_amount);
        consumed_delegation_nonces.insert(delegated.nonce_key);
      } else if op == "execute-action" {
        if link.is_some() {
          return false;
        }
        let Some(delegated) = self.validate_token_proof_generic_delegated_action(
          action,
          inscription,
          i,
          block,
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) else {
          return false;
        };
        if consumed_delegation_nonces.contains(&delegated.nonce_key) {
          return false;
        }
        let delegated_family = delegated.family.clone();
        let delegated_nonce_key = delegated.nonce_key.clone();
        if delegated_family == "perp-join" {
          let Some(join) = delegated.join else {
            return false;
          };
          let key = format!("{}/{}", join.owner, join.tick_key);
          let entry = pending_perp_debits
            .entry(key)
            .or_insert_with(|| BigInt::from(0));
          *entry = entry.clone() + join.collateral;
        } else if delegated_family == "perp-close" {
          let Some(position) = delegated.position else {
            return false;
          };
          let Some(position_id) = position.position.get("id").and_then(|v| v.as_str()) else {
            return false;
          };
          if !consumed_perp_positions.insert(position_id.to_string())
            || !consumed_perp_cert_nonces.insert(position.certificate.nonce_key)
          {
            return false;
          }
        } else {
          return false;
        }
        consumed_delegation_nonces.insert(delegated_nonce_key);
      } else if op == "cancel-delegation" {
        let Some(cancelled) = self.validate_token_proof_delegation_cancel_action(action, link)
        else {
          return false;
        };
        if cancelled_delegation_nonces.contains(&cancelled.cancel_key) {
          return false;
        }
        cancelled_delegation_nonces.insert(cancelled.cancel_key);
      // END TAP-DELEGATED-LOCKS
      } else if op == "ob-open" {
        let Some(normalized) =
          self.validate_obligation_open_action(action, link, inscription, i, block)
        else {
          return false;
        };
        let Some(pending_key) = Self::obligation_lock_key(&normalized.source, &normalized.tick_key)
        else {
          return false;
        };
        let pending = pending_obligations
          .get(&pending_key)
          .cloned()
          .unwrap_or_else(|| BigInt::from(0));
        let Some(source_tt) = normalized.source.get("tt").and_then(|v| v.as_str()) else {
          return false;
        };
        if source_tt == "a" {
          let Some(to) = normalized.source.get("to").and_then(|v| v.as_str()) else {
            return false;
          };
          if !self.has_available_with_pending_bigint(
            to,
            &normalized.tick_key,
            &normalized.amount,
            &pending_locks,
            &pending_amm_credits,
            &pending_amm_debits,
            &pending_obligations,
            &pending_perp_debits,
          ) {
            return false;
          }
        } else if source_tt == "h" {
          let Some(to) = normalized.source.get("to").and_then(|v| v.as_str()) else {
            return false;
          };
          let Some(locked) =
            self.tap_get_obligation_locked_bigint(&normalized.source, &normalized.tick_key)
          else {
            return false;
          };
          if self.tap_get_authority_balance_bigint(to, &normalized.tick_key)
            - locked
            - pending.clone()
            < normalized.amount
          {
            return false;
          }
        } else if source_tt == "amm" {
          let Some(pid) = normalized.source.get("pid").and_then(|v| v.as_str()) else {
            return false;
          };
          let Some(side) = normalized
            .source
            .get("i")
            .and_then(|v| v.as_u64())
            .and_then(|v| usize::try_from(v).ok())
          else {
            return false;
          };
          let Some(pool) = pending_amm_pools
            .get(pid)
            .cloned()
            .or_else(|| self.get_amm_pool(pid))
          else {
            return false;
          };
          if Self::amm_pool_tick_key(&pool, side).as_deref() != Some(normalized.tick_key.as_str()) {
            return false;
          }
          let Some(locked) =
            self.tap_get_obligation_locked_bigint(&normalized.source, &normalized.tick_key)
          else {
            return false;
          };
          let Some(reserves) = Self::amm_pool_reserves(&pool) else {
            return false;
          };
          if reserves[side].clone() - locked - pending.clone() < normalized.amount {
            return false;
          }
        } else {
          return false;
        }
        pending_obligations.insert(pending_key, pending + normalized.amount);
      } else if op == "ob-claim" || op == "ob-refund" || op == "ob-final" {
        let Some(obligation_id) = action
          .get("ob")
          .and_then(|v| v.as_str())
          .map(|s| s.to_string())
        else {
          return false;
        };
        if consumed_obligations.contains(&obligation_id)
          || self
            .validate_obligation_settle_action(action, block)
            .is_none()
        {
          return false;
        }
        consumed_obligations.insert(obligation_id);
      } else if op == "auth-cfg" {
        let Some(config) = self.validate_authority_config_action(
          action,
          link,
          inscription,
          i,
          "",
          0,
          0,
          0,
          block,
          0,
        ) else {
          return false;
        };
        if config.k == "amm" {
          pending_amm_pools.insert(config.id.clone(), config);
        }
      } else if op == "sync-ext" {
        let Some(_link) = link else {
          return false;
        };
        let Some(normalized) =
          self.normalize_amm_external_snapshot_action(action, link, block, timestamp)
        else {
          return false;
        };
        let snapshot_key = Self::amm_snapshot_key(&normalized.pool.id, &normalized.sid);
        if pending_amm_snapshots.contains(&snapshot_key) {
          return false;
        }
        pending_amm_snapshots.insert(snapshot_key);
      } else if op == "add-liq" {
        let Some(link) = link else {
          return false;
        };
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let Some(pool_probe) = pending_amm_pools
          .get(auth)
          .cloned()
          .or_else(|| self.get_amm_pool(auth))
        else {
          return false;
        };
        let Some(normalized) =
          self.normalize_amm_add_liquidity_action(action, Some(link), block, Some(pool_probe))
        else {
          return false;
        };
        if let Some(reference) = &normalized.reference {
          let ref_key = Self::amm_ref_key(&normalized.pool.id, &normalized.auth_target, reference);
          if pending_amm_refs.contains(&ref_key)
            || self.tap_get::<String>(&ref_key).ok().flatten().is_some()
          {
            return false;
          }
          pending_amm_refs.insert(ref_key);
        }
        for side in 0..2 {
          let Some(tick_key) = Self::amm_pool_tick_key(&normalized.pool, side) else {
            return false;
          };
          if !self.has_available_with_pending_bigint(
            &link.addr,
            &tick_key,
            &normalized.amounts[side],
            &pending_locks,
            &pending_amm_credits,
            &pending_amm_debits,
            &pending_obligations,
            &pending_perp_debits,
          ) {
            return false;
          }
          let key = format!("{}/{}", link.addr, tick_key);
          let entry = pending_amm_debits
            .entry(key)
            .or_insert_with(|| BigInt::from(0));
          *entry = entry.clone() + normalized.amounts[side].clone();
        }
        let mut next_pool = normalized.pool.clone();
        Self::set_amm_pool_state(
          &mut next_pool,
          &normalized.reserves_after,
          &normalized.shares_after,
        );
        pending_amm_pools.insert(next_pool.id.clone(), next_pool);
        let lp_key = format!(
          "{}/{}",
          normalized.pool.id,
          Self::amm_target_key(&normalized.to)
        );
        let entry = pending_amm_lps
          .entry(lp_key)
          .or_insert_with(|| BigInt::from(0));
        *entry = entry.clone() + normalized.minted;
      } else if op == "rm-liq" {
        let Some(link) = link else {
          return false;
        };
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let Some(pool_probe) = pending_amm_pools
          .get(auth)
          .cloned()
          .or_else(|| self.get_amm_pool(auth))
        else {
          return false;
        };
        let owner_probe = match action.get("own") {
          Some(value) => self.validate_amm_lp_target(value),
          None => Some(AmmTarget {
            tt: "a".to_string(),
            to: link.addr.clone(),
          }),
        };
        let Some(owner_probe) = owner_probe else {
          return false;
        };
        let lp_key = format!("{}/{}", auth, Self::amm_target_key(&owner_probe));
        let owner_shares = self.get_amm_lp_shares(auth, &owner_probe)
          + pending_amm_lps
            .get(&lp_key)
            .cloned()
            .unwrap_or_else(|| BigInt::from(0));
        let Some(normalized) = self.normalize_amm_remove_liquidity_action(
          action,
          Some(link),
          block,
          Some(pool_probe),
          Some(owner_shares),
        ) else {
          return false;
        };
        if !self.amm_reserves_cover_obligation_locks(
          &normalized.pool,
          &normalized.reserves_after,
          Some(&pending_obligations),
        ) {
          return false;
        }
        if let Some(reference) = &normalized.reference {
          let ref_key = Self::amm_ref_key(&normalized.pool.id, &normalized.auth_target, reference);
          if pending_amm_refs.contains(&ref_key)
            || self.tap_get::<String>(&ref_key).ok().flatten().is_some()
          {
            return false;
          }
          pending_amm_refs.insert(ref_key);
        }
        let mut next_pool = normalized.pool.clone();
        Self::set_amm_pool_state(
          &mut next_pool,
          &normalized.reserves_after,
          &normalized.shares_after,
        );
        pending_amm_pools.insert(next_pool.id.clone(), next_pool);
        let lp_key = format!(
          "{}/{}",
          normalized.pool.id,
          Self::amm_target_key(&normalized.owner)
        );
        let entry = pending_amm_lps
          .entry(lp_key)
          .or_insert_with(|| BigInt::from(0));
        *entry = entry.clone() - normalized.shares;
        if normalized.to.tt == "a" {
          for side in 0..2 {
            let Some(tick_key) = Self::amm_pool_tick_key(&normalized.pool, side) else {
              return false;
            };
            let key = format!("{}/{}", normalized.to.to, tick_key);
            let entry = pending_amm_credits
              .entry(key)
              .or_insert_with(|| BigInt::from(0));
            *entry = entry.clone() + normalized.outputs[side].clone();
          }
        }
      } else if op == "swap" {
        let Some(link) = link else {
          return false;
        };
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let Some(pool_probe) = pending_amm_pools
          .get(auth)
          .cloned()
          .or_else(|| self.get_amm_pool(auth))
        else {
          return false;
        };
        let Some(normalized) =
          self.normalize_amm_swap_action(action, Some(link), block, Some(pool_probe))
        else {
          return false;
        };
        if !self.amm_reserves_cover_obligation_locks(
          &normalized.pool,
          &normalized.reserves_after,
          Some(&pending_obligations),
        ) {
          return false;
        }
        if let Some(reference) = &normalized.reference {
          let ref_key = Self::amm_ref_key(&normalized.pool.id, &normalized.auth_target, reference);
          if pending_amm_refs.contains(&ref_key)
            || self.tap_get::<String>(&ref_key).ok().flatten().is_some()
          {
            return false;
          }
          pending_amm_refs.insert(ref_key);
        }
        let Some(tick_key) = Self::amm_pool_tick_key(&normalized.pool, normalized.side) else {
          return false;
        };
        if !self.has_available_with_pending_bigint(
          &link.addr,
          &tick_key,
          &normalized.amount_in,
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) {
          return false;
        }
        let key = format!("{}/{}", link.addr, tick_key);
        let entry = pending_amm_debits
          .entry(key)
          .or_insert_with(|| BigInt::from(0));
        *entry = entry.clone() + normalized.amount_in.clone();
        let mut next_pool = normalized.pool.clone();
        Self::set_amm_pool_state(
          &mut next_pool,
          &normalized.reserves_after,
          &normalized.shares_after,
        );
        pending_amm_pools.insert(next_pool.id.clone(), next_pool);
        let Some(out_tick_key) = Self::amm_pool_tick_key(&normalized.pool, normalized.out_side)
        else {
          return false;
        };
        if normalized.to.tt == "a" {
          let key = format!("{}/{}", normalized.to.to, out_tick_key);
          let entry = pending_amm_credits
            .entry(key)
            .or_insert_with(|| BigInt::from(0));
          *entry = entry.clone() + normalized.amount_out.clone();
        }
        if normalized.protocol_fee > BigInt::from(0) {
          if let Some(target) = normalized
            .pool
            .pp
            .as_ref()
            .and_then(|target| self.validate_amm_target(target, false))
          {
            if target.tt == "a" {
              let key = format!("{}/{}", target.to, tick_key);
              let entry = pending_amm_credits
                .entry(key)
                .or_insert_with(|| BigInt::from(0));
              *entry = entry.clone() + normalized.protocol_fee.clone();
            }
          }
        }
      } else if op == "stake" {
        let Some(normalized) = self.validate_stake_action(action, link, inscription, i, block)
        else {
          return false;
        };
        let pending_key = format!("{}/{}", normalized.addr, normalized.tick_key);
        let pending = *pending_locks.get(&pending_key).unwrap_or(&0);
        if !self.has_available_with_pending_bigint(
          &normalized.addr,
          &normalized.tick_key,
          &BigInt::from(normalized.amt),
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) {
          return false;
        }
        pending_locks.insert(pending_key, pending + normalized.amt);
      } else if op == "claim-rwd" {
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let pos_id = action.get("pos").and_then(|v| v.as_str()).unwrap_or("");
        let reward_tick = action
          .get("rt")
          .and_then(|v| v.as_str())
          .map(Self::js_to_lowercase)
          .unwrap_or_default();
        let Some(position) = self.get_stake_position(pos_id) else {
          return false;
        };
        if position.auth != auth || position.status != "open" {
          return false;
        }
        if let Some(link) = link {
          if link.addr != position.claim {
            return false;
          }
        }
        let pending = self.pending_stake_reward(&position, &reward_tick);
        let reward_key = Self::json_stringify_lower(&reward_tick);
        if pending <= 0 || self.tap_get_authority_balance(auth, &reward_key) < pending {
          return false;
        }
      } else if op == "unstake" {
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let pos_id = action.get("pos").and_then(|v| v.as_str()).unwrap_or("");
        let Some(position) = self.get_stake_position(pos_id) else {
          return false;
        };
        if position.auth != auth || position.status != "open" || block < position.uh {
          return false;
        }
        if let Some(link) = link {
          if link.addr != position.claim {
            return false;
          }
        }
        let tick_key = Self::json_stringify_lower(&position.tick);
        let amount = position.amt.parse::<i128>().ok().unwrap_or(0);
        if amount <= 0 || self.tap_get_authority_balance(auth, &tick_key) < amount {
          return false;
        }
      } else if op == "fund-sale" {
        let Some(link) = link else {
          return false;
        };
        let Some(normalized) = self.validate_fund_sale_action(action, Some(link)) else {
          return false;
        };
        let pending_key = format!("{}/{}", link.addr, normalized.tick_key);
        let pending = *pending_locks.get(&pending_key).unwrap_or(&0);
        if !self.has_available_with_pending_bigint(
          &link.addr,
          &normalized.tick_key,
          &BigInt::from(normalized.amount),
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) {
          return false;
        }
        pending_locks.insert(pending_key, pending + normalized.amount);
      } else if op == "contribute" {
        let Some(link) = link else {
          return false;
        };
        let Some(normalized) =
          self.validate_sale_contribution_action(action, Some(link), inscription, i, block)
        else {
          return false;
        };
        let pending_key = format!("{}/{}", link.addr, normalized.tick_key);
        let pending = *pending_locks.get(&pending_key).unwrap_or(&0);
        if !self.has_available_with_pending_bigint(
          &link.addr,
          &normalized.tick_key,
          &BigInt::from(normalized.amount),
          &pending_locks,
          &pending_amm_credits,
          &pending_amm_debits,
          &pending_obligations,
          &pending_perp_debits,
        ) {
          return false;
        }
        pending_locks.insert(pending_key, pending + normalized.amount);
        let auth = action
          .get("auth")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .to_string();
        let sale_pending = *pending_sale_totals.get(&auth).unwrap_or(&0);
        let hard_cap = normalized
          .config
          .s
          .as_ref()
          .and_then(|s| s.get("hc"))
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        if Self::token_sale_status_i128(&normalized.status, "tc") + sale_pending + normalized.amount
          > hard_cap
        {
          return false;
        }
        pending_sale_totals.insert(auth.clone(), sale_pending + normalized.amount);
        let addr_key = format!("{}/{}", auth, normalized.claim);
        let addr_pending = *pending_sale_addresses.get(&addr_key).unwrap_or(&0);
        let max = normalized
          .config
          .s
          .as_ref()
          .and_then(|s| s.get("mx"))
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<i128>().ok());
        if max
          .map(|limit| normalized.existing_amount + addr_pending + normalized.amount > limit)
          .unwrap_or(false)
        {
          return false;
        }
        pending_sale_addresses.insert(addr_key, addr_pending + normalized.amount);
      } else if op == "finalize-sale" {
        let auth = action
          .get("auth")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .to_string();
	        if auth.is_empty()
	          || sale_finalizes.contains(&auth)
	          || sale_resolves.contains(&auth)
	          || self
	            .validate_finalize_sale_action(action, link, block)
	            .is_none()
        {
          return false;
	        }
	        sale_finalizes.insert(auth);
	      } else if op == "resolve-sale" {
	        let auth = action
	          .get("auth")
	          .and_then(|v| v.as_str())
	          .unwrap_or("")
	          .to_string();
	        if auth.is_empty()
	          || sale_finalizes.contains(&auth)
	          || sale_cancels.contains(&auth)
	          || sale_resolves.contains(&auth)
	          || self.validate_resolve_sale_action(action, block).is_none()
	        {
	          return false;
	        }
	        sale_resolves.insert(auth);
	      } else if op == "claim-sale" {
        let Some(link) = link else {
          return false;
        };
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let cid = action.get("cid").and_then(|v| v.as_str()).unwrap_or("");
        let action_key = format!("{}/{}", auth, cid);
        if sale_claims.contains(&action_key) || sale_refunds.contains(&action_key) {
          return false;
        }
        let Some(config) = self.tap_get_authority_config(&auth) else {
          return false;
        };
        let Some(contribution) = self.get_sale_contribution(cid) else {
          return false;
        };
        let status = self.tap_get_sale_status(auth, &config);
        let tick_key = Self::json_stringify_lower(config.st.as_deref().unwrap_or(""));
        let amount = contribution
          .get("sa")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        if config.k != "sale"
          || contribution.get("auth").and_then(|v| v.as_str()) != Some(auth)
          || contribution.get("status").and_then(|v| v.as_str()) != Some("open")
          || !Self::token_sale_status_bool(&status, "fin")
          || contribution.get("claim").and_then(|v| v.as_str()) != Some(link.addr.as_str())
          || amount <= 0
          || self.tap_get_authority_balance(auth, &tick_key) < amount
        {
          return false;
        }
        sale_claims.insert(action_key);
      } else if op == "refund-sale" {
        let Some(link) = link else {
          return false;
        };
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let cid = action.get("cid").and_then(|v| v.as_str()).unwrap_or("");
        let action_key = format!("{}/{}", auth, cid);
        if sale_claims.contains(&action_key) || sale_refunds.contains(&action_key) {
          return false;
        }
        let Some(config) = self.tap_get_authority_config(&auth) else {
          return false;
        };
        let Some(contribution) = self.get_sale_contribution(cid) else {
          return false;
        };
        let status = self.tap_get_sale_status(auth, &config);
        let soft_cap = config
          .s
          .as_ref()
          .and_then(|s| s.get("sc"))
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let end_height = config
          .s
          .as_ref()
          .and_then(|s| s.get("eh"))
          .and_then(Self::js_parse_int)
          .unwrap_or(i128::MAX);
        let tick_key = Self::json_stringify_lower(config.pt.as_deref().unwrap_or(""));
        let amount = contribution
          .get("amt")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        if config.k != "sale"
          || contribution.get("auth").and_then(|v| v.as_str()) != Some(auth)
          || contribution.get("status").and_then(|v| v.as_str()) != Some("open")
          || Self::token_sale_status_bool(&status, "fin")
          || contribution.get("claim").and_then(|v| v.as_str()) != Some(link.addr.as_str())
          || (!Self::token_sale_status_bool(&status, "can") && i128::from(block) <= end_height)
          || (!Self::token_sale_status_bool(&status, "can")
            && Self::token_sale_status_i128(&status, "tc") >= soft_cap)
          || amount <= 0
          || self.tap_get_authority_balance(auth, &tick_key) < amount
        {
          return false;
        }
        sale_refunds.insert(action_key);
      } else if op == "cancel-sale" {
        let Some(link) = link else {
          return false;
        };
        let auth = action
          .get("auth")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .to_string();
	        if auth.is_empty() || sale_cancels.contains(&auth) || sale_resolves.contains(&auth) {
	          return false;
	        }
        let Some(config) = self.tap_get_authority_config(&auth) else {
          return false;
        };
        let status = self.tap_get_sale_status(&auth, &config);
        let cancel_enabled = config
          .s
          .as_ref()
          .and_then(|s| s.get("cx"))
          .and_then(|v| v.as_bool())
          .unwrap_or(false);
        if config.k != "sale"
          || config.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
          || !cancel_enabled
          || Self::token_sale_status_bool(&status, "fin")
          || Self::token_sale_status_bool(&status, "can")
        {
          return false;
        }
        sale_cancels.insert(auth);
      } else if op == "withdraw-sale" {
        let Some(link) = link else {
          return false;
        };
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let Some(config) = self.tap_get_authority_config(auth) else {
          return false;
        };
        let Some(token) = action
          .get("tick")
          .and_then(|v| v.as_str())
          .and_then(|tick| self.token_proof_get_deploy(tick))
        else {
          return false;
        };
        let status = self.tap_get_sale_status(auth, &config);
        if config.k != "sale"
          || config.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
          || config.st.as_deref() != Some(token.tick.as_str())
          || self.validate_token_sale_target(action).is_none()
          || !Self::sale_withdraw_allowed(&config, &status, block)
        {
          return false;
        }
        let Some(amount) = self.token_proof_resolve_protocol_amount(
          action.get("amt").unwrap_or(&serde_json::Value::Null),
          &token.record,
        ) else {
          return false;
        };
        let withdrawal_key = format!("{}/{}", auth, token.tick_key);
        let pending_withdrawal = *pending_sale_withdrawals.get(&withdrawal_key).unwrap_or(&0);
        let reserve = Self::sale_withdrawal_reserve(&config, &status, &token.tick);
        let Some(available) = self
          .tap_get_authority_balance(auth, &token.tick_key)
          .checked_sub(reserve)
        else {
          return false;
        };
        let Some(required) = pending_withdrawal.checked_add(amount) else {
          return false;
        };
        if amount <= 0 || available < required {
          return false;
        }
        pending_sale_withdrawals.insert(withdrawal_key, required);
      } else if op == "claim" || op == "refund" {
        let Some(link) = link else {
          return false;
        };
        let Some(lock_id) = action
          .get("lock")
          .and_then(|v| v.as_str())
          .map(|s| s.to_string())
        else {
          return false;
        };
        let Some(normalized) = self.validate_token_proof_release_action(action, link, block) else {
          return false;
        };
        if consumed_locks.contains(&lock_id)
          || normalized
            .cert_nonce_key
            .as_ref()
            .map(|key| consumed_cert_nonces.contains(key))
            .unwrap_or(false)
        {
          return false;
        }
        consumed_locks.insert(lock_id);
        if let Some(key) = normalized.cert_nonce_key {
          consumed_cert_nonces.insert(key);
        }
      } else {
        return false;
      }
    }

    true
  }

  fn tap_apply_proof_transfer_logs(
    &mut self,
    lock: &TokenLockRecord,
    tick_key: &str,
    receiver: &str,
    amount: i128,
    sender_balance_after: i128,
    receiver_balance_after: i128,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) {
    if lock.owner == receiver || amount <= 0 {
      return;
    }
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", lock.owner, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let amt = amount.to_string();
    let srec = TransferSendSenderRecord {
      addr: lock.owner.clone(),
      taddr: receiver.to_string(),
      at: Some("a".to_string()),
      tt: Some("a".to_string()),
      st: Some("x".to_string()),
      rl: None,
      rf: None,
      blck: block,
      amt: amt.clone(),
      trf: transferable.to_string(),
      bal: sender_balance_after.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("strl/{}/{}", lock.owner, tick_key),
      &format!("strli/{}/{}", lock.owner, tick_key),
      &srec,
    );
    let rrec = TransferSendReceiverRecord {
      faddr: lock.owner.clone(),
      addr: receiver.to_string(),
      at: Some("a".to_string()),
      tt: Some("a".to_string()),
      st: Some("x".to_string()),
      rl: None,
      rf: None,
      blck: block,
      amt: amt.clone(),
      bal: receiver_balance_after.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("rstrl/{}/{}", receiver, tick_key),
      &format!("rstrli/{}/{}", receiver, tick_key),
      &rrec,
    );
    let frec = TransferSendFlatRecord {
      tick: None,
      addr: lock.owner.clone(),
      taddr: receiver.to_string(),
      at: Some("a".to_string()),
      tt: Some("a".to_string()),
      st: Some("x".to_string()),
      rl: None,
      rf: None,
      blck: block,
      amt: amt.clone(),
      trf: transferable.to_string(),
      bal: sender_balance_after.to_string(),
      tbal: receiver_balance_after.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("fstrl/{}", tick_key),
      &format!("fstrli/{}", tick_key),
      &frec,
    );
    let sfrec = TransferSendSuperflatRecord {
      tick: lock.tick.clone(),
      addr: lock.owner.clone(),
      taddr: receiver.to_string(),
      at: Some("a".to_string()),
      tt: Some("a".to_string()),
      st: Some("x".to_string()),
      rl: None,
      rf: None,
      blck: block,
      amt,
      trf: transferable.to_string(),
      bal: sender_balance_after.to_string(),
      tbal: receiver_balance_after.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &sfrec) {
      let ptr = format!("sfstrli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/snd/{}", transaction),
        &format!("txi/snd/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("txt/snd/{}/{}", tick_key, transaction),
        &format!("txti/snd/{}/{}", tick_key, transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/snd/{}", block),
        &format!("blcki/snd/{}", block),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckt/snd/{}/{}", tick_key, block),
        &format!("blckti/snd/{}/{}", tick_key, block),
        &ptr,
      );
    }
  }

  fn tap_apply_authority_transfer_logs(
    &mut self,
    tick: &str,
    tick_key: &str,
    from_address: &str,
    authority_id: &str,
    transferable: i128,
    balance: i128,
    authority_balance: i128,
    amount: i128,
    block: u32,
    inscription: &str,
    number: i32,
    timestamp: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    role: &str,
    reference: &str,
  ) {
    if amount <= 0 {
      return;
    }
    let subtype = Self::token_authority_inbound_subtype(role);
    let sender = TransferSendSenderRecord {
      addr: from_address.to_string(),
      taddr: authority_id.to_string(),
      at: Some("a".to_string()),
      tt: Some("h".to_string()),
      st: Some(subtype.clone()),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: transferable.to_string(),
      bal: balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("strl/{}/{}", from_address, tick_key),
      &format!("strli/{}/{}", from_address, tick_key),
      &sender,
    );

    let flat = TransferSendFlatRecord {
      tick: None,
      addr: from_address.to_string(),
      taddr: authority_id.to_string(),
      at: Some("a".to_string()),
      tt: Some("h".to_string()),
      st: Some(subtype.clone()),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: transferable.to_string(),
      bal: balance.to_string(),
      tbal: authority_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("fstrl/{}", tick_key),
      &format!("fstrli/{}", tick_key),
      &flat,
    );

    let superflat = TransferSendSuperflatRecord {
      tick: tick.to_string(),
      addr: from_address.to_string(),
      taddr: authority_id.to_string(),
      at: Some("a".to_string()),
      tt: Some("h".to_string()),
      st: Some(subtype),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: transferable.to_string(),
      bal: balance.to_string(),
      tbal: authority_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &superflat) {
      let ptr = format!("sfstrli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/snd/{}", transaction),
        &format!("txi/snd/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("txt/snd/{}/{}", tick_key, transaction),
        &format!("txti/snd/{}/{}", tick_key, transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/snd/{}", block),
        &format!("blcki/snd/{}", block),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckt/snd/{}/{}", tick_key, block),
        &format!("blckti/snd/{}/{}", tick_key, block),
        &ptr,
      );
    }
  }

  fn tap_apply_authority_claim_transfer_logs(
    &mut self,
    tick: &str,
    tick_key: &str,
    authority_id: &str,
    to_address: &str,
    authority_balance: i128,
    receiver_balance: i128,
    amount: i128,
    block: u32,
    inscription: &str,
    number: i32,
    timestamp: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    role: &str,
    reference: &str,
  ) {
    if amount <= 0 {
      return;
    }
    let subtype = Self::token_authority_outbound_subtype(role);
    let receiver = TransferSendReceiverRecord {
      faddr: authority_id.to_string(),
      addr: to_address.to_string(),
      at: Some("h".to_string()),
      tt: Some("a".to_string()),
      st: Some(subtype.clone()),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      bal: receiver_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("rstrl/{}/{}", to_address, tick_key),
      &format!("rstrli/{}/{}", to_address, tick_key),
      &receiver,
    );

    let flat = TransferSendFlatRecord {
      tick: Some(tick.to_string()),
      addr: authority_id.to_string(),
      taddr: to_address.to_string(),
      at: Some("h".to_string()),
      tt: Some("a".to_string()),
      st: Some(subtype.clone()),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: "0".to_string(),
      bal: authority_balance.to_string(),
      tbal: receiver_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("fstrl/{}", tick_key),
      &format!("fstrli/{}", tick_key),
      &flat,
    );

    let superflat = TransferSendSuperflatRecord {
      tick: tick.to_string(),
      addr: authority_id.to_string(),
      taddr: to_address.to_string(),
      at: Some("h".to_string()),
      tt: Some("a".to_string()),
      st: Some(subtype),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: "0".to_string(),
      bal: authority_balance.to_string(),
      tbal: receiver_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &superflat) {
      let ptr = format!("sfstrli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/snd/{}", transaction),
        &format!("txi/snd/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("txt/snd/{}/{}", tick_key, transaction),
        &format!("txti/snd/{}/{}", tick_key, transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/snd/{}", block),
        &format!("blcki/snd/{}", block),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckt/snd/{}/{}", tick_key, block),
        &format!("blckti/snd/{}/{}", tick_key, block),
        &ptr,
      );
    }
  }

  fn tap_apply_authority_target_transfer_logs(
    &mut self,
    tick: &str,
    tick_key: &str,
    authority_id: &str,
    target: &SaleTarget,
    authority_balance: i128,
    target_balance: i128,
    amount: i128,
    block: u32,
    inscription: &str,
    number: i32,
    timestamp: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    role: &str,
    reference: &str,
  ) {
    if amount <= 0 {
      return;
    }
    let subtype = Self::token_authority_outbound_subtype(role);
    let flat = TransferSendFlatRecord {
      tick: Some(tick.to_string()),
      addr: authority_id.to_string(),
      taddr: target.to.clone(),
      at: Some("h".to_string()),
      tt: Some(target.tt.clone()),
      st: Some(subtype.clone()),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: "0".to_string(),
      bal: authority_balance.to_string(),
      tbal: target_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    let _ = self.tap_set_list_record(
      &format!("fstrl/{}", tick_key),
      &format!("fstrli/{}", tick_key),
      &flat,
    );

    if target.tt == "a" {
      let receiver = TransferSendReceiverRecord {
        faddr: authority_id.to_string(),
        addr: target.to.clone(),
        at: Some("h".to_string()),
        tt: Some("a".to_string()),
        st: Some(subtype.clone()),
        rl: Some(role.to_string()),
        rf: Some(reference.to_string()),
        blck: block,
        amt: amount.to_string(),
        bal: target_balance.to_string(),
        tx: transaction.to_string(),
        vo: vout,
        val: value.to_string(),
        ins: inscription.to_string(),
        num: number,
        ts: timestamp,
        fail: false,
        int: true,
        dta: None,
      };
      let _ = self.tap_set_list_record(
        &format!("rstrl/{}/{}", target.to, tick_key),
        &format!("rstrli/{}/{}", target.to, tick_key),
        &receiver,
      );
    }

    let superflat = TransferSendSuperflatRecord {
      tick: tick.to_string(),
      addr: authority_id.to_string(),
      taddr: target.to.clone(),
      at: Some("h".to_string()),
      tt: Some(target.tt.clone()),
      st: Some(subtype),
      rl: Some(role.to_string()),
      rf: Some(reference.to_string()),
      blck: block,
      amt: amount.to_string(),
      trf: "0".to_string(),
      bal: authority_balance.to_string(),
      tbal: target_balance.to_string(),
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      fail: false,
      int: true,
      dta: None,
    };
    if let Ok(list_len) = self.tap_set_list_record("sfstrl", "sfstrli", &superflat) {
      let ptr = format!("sfstrli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/snd/{}", transaction),
        &format!("txi/snd/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("txt/snd/{}/{}", tick_key, transaction),
        &format!("txti/snd/{}/{}", tick_key, transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/snd/{}", block),
        &format!("blcki/snd/{}", block),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blckt/snd/{}/{}", tick_key, block),
        &format!("blckti/snd/{}/{}", tick_key, block),
        &ptr,
      );
    }
  }

  fn process_token_proof_release_action(
    &mut self,
    action: &serde_json::Value,
    link: &TokenAuthCreateRecord,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_token_proof_release_action(action, link, block) else {
      return false;
    };
    let lock_id = action.get("lock").and_then(|v| v.as_str()).unwrap_or("");
    let lock = normalized.lock;
    let tick_key = normalized.tick_key;
    let amount = normalized.amount;
    let total_amount = normalized.total_amount;
    let target = normalized.target;
    let action_name = normalized.action_name.clone();
    let owner_balance = normalized.owner_balance;

    if !self.tap_add_locked_amount(&lock.owner, &tick_key, -total_amount) {
      return false;
    }

    let mut deltas: std::collections::HashMap<String, i128> = std::collections::HashMap::new();
    let mut add_delta = |address: String, delta: i128| {
      *deltas.entry(address).or_insert(0) += delta;
    };

    if action_name == "claim" {
      add_delta(lock.owner.clone(), -total_amount);
      add_delta(target.clone(), amount);
      for allocation in &normalized.allocations {
        if allocation.tt == "a" {
          let Some(amt) = allocation.amt.parse::<i128>().ok() else {
            return false;
          };
          add_delta(allocation.to.clone(), amt);
        }
      }
    } else {
      add_delta(lock.owner.clone(), -total_amount);
      add_delta(target.clone(), total_amount);
    }

    let mut balance_after: std::collections::HashMap<String, i128> =
      std::collections::HashMap::new();
    for (address, delta) in deltas.iter() {
      let before = if address == &lock.owner {
        owner_balance
      } else {
        self
          .tap_get::<String>(&format!("b/{}/{}", address, tick_key))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0)
      };
      let Some(after) = before.checked_add(*delta) else {
        return false;
      };
      if after < 0 {
        return false;
      }
      balance_after.insert(address.clone(), after);
    }

    if action_name == "claim" {
      for allocation in &normalized.allocations {
        if allocation.tt == "h" {
          let Some(amt) = allocation.amt.parse::<i128>().ok() else {
            return false;
          };
          if !self.tap_apply_authority_allocation_credit(allocation, &tick_key, amt) {
            return false;
          }
        }
      }
    }

    for (address, after) in balance_after.iter() {
      let _ = self.tap_put(&format!("b/{}/{}", address, tick_key), &after.to_string());
      if *after > 0 {
        if self
          .tap_get::<String>(&format!("he/{}/{}", address, tick_key))
          .ok()
          .flatten()
          .is_none()
        {
          let _ = self.tap_put(&format!("he/{}/{}", address, tick_key), &"".to_string());
          let _ = self.tap_set_list_record(
            &format!("h/{}", tick_key),
            &format!("hi/{}", tick_key),
            address,
          );
        }
        if self
          .tap_get::<String>(&format!("ato/{}/{}", address, tick_key))
          .ok()
          .flatten()
          .is_none()
        {
          let _ = self.tap_set_list_record(
            &format!("atl/{}", address),
            &format!("atli/{}", address),
            &Self::js_to_lowercase(&lock.tick),
          );
          let _ = self.tap_put(&format!("ato/{}/{}", address, tick_key), &"".to_string());
        }
      }
    }

    let consume = TokenLockConsumeRecord {
      lock: lock_id.to_string(),
      action: action_name.clone(),
      kind: lock.kind.clone(),
      owner: lock.owner.clone(),
      target: target.clone(),
      tick: lock.tick.clone(),
      amt: amount.to_string(),
      fee: None,
      al: if normalized.allocations.is_empty() {
        None
      } else {
        Some(normalized.allocations.clone())
      },
      total: if normalized.allocations.is_empty() {
        None
      } else {
        Some(total_amount.to_string())
      },
      cert: normalized.cert.clone(),
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    };
    if let Some(key) = normalized.cert_nonce_key.clone() {
      let _ = self.tap_put(&key, &consume);
    }
    let _ = self.tap_put(&format!("lc/{}", lock_id), &consume);
    if let Ok(list_len) = self.tap_set_list_record("slc", "slci", &consume) {
      let _ = self.tap_set_list_record(
        &format!("lca/{}", lock.owner),
        &format!("lcai/{}", lock.owner),
        &consume,
      );
      let _ = self.tap_set_list_record(
        &format!("lct/{}", tick_key),
        &format!("lcti/{}", tick_key),
        &consume,
      );
      let _ = self.tap_set_list_record(
        &format!("lck/{}", lock.kind),
        &format!("lcki/{}", lock.kind),
        &consume,
      );
      let _ = self.tap_set_list_record(
        &format!("lcak/{}/{}", lock.owner, lock.kind),
        &format!("lcaki/{}/{}", lock.owner, lock.kind),
        &consume,
      );
      let _ = self.tap_set_list_record(
        &format!("lctk/{}/{}", tick_key, lock.kind),
        &format!("lctki/{}/{}", tick_key, lock.kind),
        &consume,
      );
      let ptr = format!("slci/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/lckc/{}", transaction),
        &format!("txi/lckc/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/lckc/{}", block),
        &format!("blcki/lckc/{}", block),
        &ptr,
      );
    }

    let owner_balance_after = *balance_after.get(&lock.owner).unwrap_or(&owner_balance);
    if action_name == "claim" {
      let target_balance_after = *balance_after.get(&target).unwrap_or(&owner_balance_after);
      self.tap_apply_proof_transfer_logs(
        &lock,
        &tick_key,
        &target,
        amount,
        owner_balance_after,
        target_balance_after,
        transaction,
        vout,
        value,
        inscription,
        number,
        block,
        timestamp,
      );
      for allocation in &normalized.allocations {
        let Some(allocation_amount) = allocation.amt.parse::<i128>().ok() else {
          return false;
        };
        if allocation.tt == "a" {
          let allocation_balance_after = *balance_after
            .get(&allocation.to)
            .unwrap_or(&owner_balance_after);
          self.tap_apply_proof_transfer_logs(
            &lock,
            &tick_key,
            &allocation.to,
            allocation_amount,
            owner_balance_after,
            allocation_balance_after,
            transaction,
            vout,
            value,
            inscription,
            number,
            block,
            timestamp,
          );
        } else if allocation.tt == "h" {
          let transferable = self
            .tap_get::<String>(&format!("t/{}/{}", lock.owner, tick_key))
            .ok()
            .flatten()
            .and_then(|s| s.parse::<i128>().ok())
            .unwrap_or(0);
          let authority_balance = self.tap_get_authority_balance(&allocation.to, &tick_key);
          self.tap_apply_authority_transfer_logs(
            &lock.tick,
            &tick_key,
            &lock.owner,
            &allocation.to,
            transferable,
            owner_balance_after,
            authority_balance,
            allocation_amount,
            block,
            inscription,
            number,
            timestamp,
            transaction,
            vout,
            value,
            if allocation.rl == "sr" { "sr" } else { "of" },
            lock_id,
          );
        }
      }
    } else {
      let target_balance_after = *balance_after.get(&target).unwrap_or(&owner_balance_after);
      self.tap_apply_proof_transfer_logs(
        &lock,
        &tick_key,
        &target,
        total_amount,
        owner_balance_after,
        target_balance_after,
        transaction,
        vout,
        value,
        inscription,
        number,
        block,
        timestamp,
      );
    }

    true
  }

  fn validate_amm_authority_config_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    action_index: usize,
    transaction: &str,
    vout: u32,
    value: u64,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> Option<AuthorityConfigRecord> {
    let link = link?;
    if action.get("op")?.as_str()?.to_lowercase() != "auth-cfg"
      || action.get("k")?.as_str()? != "amm"
      || action.get("c")?.get("ty")?.as_str()? != "cpmm"
      || action.get("ctl")?.get("ty")?.as_str()? != "ta"
      || action.get("ctl")?.get("auth")?.as_str()? != link.ins
      || action.get("c")?.get("pause")?.as_bool().is_none()
    {
      return None;
    }
    let assets_raw = action.get("a")?.as_array()?;
    if assets_raw.len() != 2 {
      return None;
    }
    let id = Self::tap_token_authority_id(inscription, action_index);
    if self.tap_get_authority_config(&id).is_some() || self.get_amm_pool(&id).is_some() {
      return None;
    }
    let mut assets = Vec::new();
    let mut asset_keys = std::collections::HashSet::new();
    for asset_raw in assets_raw {
      let asset = self.normalize_amm_asset(asset_raw)?;
      if !asset_keys.insert(asset.key.clone()) {
        return None;
      }
      assets.push(asset);
    }
    let fee = Self::parse_amm_uint_value(action.get("c")?.get("fee")?, true)?;
    let protocol_fee_share = match action.get("c")?.get("pf") {
      Some(value) => Self::parse_amm_uint_value(value, true)?,
      None => BigInt::from(0),
    };
    let min_liquidity = Self::parse_amm_uint_value(action.get("c")?.get("min")?, false)?;
    if fee > BigInt::from(1000) || protocol_fee_share > BigInt::from(10000) {
      return None;
    }
    let protocol_fee_target = if protocol_fee_share > BigInt::from(0) {
      Some(self.validate_amm_target(action.get("c")?.get("pp")?, true)?)
    } else {
      if action.get("c")?.get("pp").is_some() {
        return None;
      }
      None
    };
    let has_external = assets.iter().any(|asset| {
      asset
        .value
        .get("ty")
        .and_then(|v| v.as_str())
        .map(|ty| ty == "ext")
        .unwrap_or(false)
    });
    let att = if has_external {
      let att_raw = action.get("att")?;
      let signer_arr = att_raw.get("signers")?.as_array()?;
      if signer_arr.len() < 2 || signer_arr.len() > 8 {
        return None;
      }
      let threshold_i = att_raw.get("thr").and_then(Self::js_parse_int)?;
      let threshold = usize::try_from(threshold_i).ok()?;
      if threshold < 2 || threshold > signer_arr.len() || threshold > 8 {
        return None;
      }
      let max_age = Self::parse_amm_height(att_raw.get("max_age")?)?;
      let reorg = Self::parse_amm_height(att_raw.get("reorg")?)?;
      let mut signers = Vec::new();
      let mut seen = std::collections::HashSet::new();
      for signer_raw in signer_arr {
        let signer = Self::token_proof_compressed_delegation_pubkey(signer_raw.as_str()?)?;
        if !seen.insert(signer.clone()) {
          return None;
        }
        signers.push(signer);
      }
      Some(serde_json::json!({
        "thr": threshold,
        "signers": signers,
        "max_age": max_age,
        "reorg": reorg
      }))
    } else {
      if action.get("att").is_some() {
        return None;
      }
      None
    };
    let asset_values: Vec<serde_json::Value> =
      assets.iter().map(|asset| asset.value.clone()).collect();
    let asset_key_values: Vec<String> = assets.iter().map(|asset| asset.key.clone()).collect();
    Some(AuthorityConfigRecord {
      id,
      k: "amm".to_string(),
      n: action
        .get("n")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()),
      stk: String::new(),
      rt: Vec::new(),
      st: None,
      pt: None,
      ctl: serde_json::json!({ "ty": "ta", "auth": link.ins }),
      tre: None,
      seq: 0,
      r: serde_json::json!(["0", "0"]),
      a: asset_values,
      ak: asset_key_values,
      sh: "0".to_string(),
      fee: fee.to_string(),
      pf: protocol_fee_share.to_string(),
      min: min_liquidity.to_string(),
      p: action.get("c")?.get("pause")?.as_bool()?,
      pp: protocol_fee_target.map(|target| Self::amm_target_value(&target)),
      att,
      xs: None,
      s: None,
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    })
  }

  fn validate_authority_config_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    action_index: usize,
    transaction: &str,
    vout: u32,
    value: u64,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> Option<AuthorityConfigRecord> {
    let link = link?;
    if action.get("k").and_then(|v| v.as_str()) == Some("amm") {
      return self.validate_amm_authority_config_action(
        action,
        Some(link),
        inscription,
        action_index,
        transaction,
        vout,
        value,
        number,
        block,
        timestamp,
      );
    }
    if action.get("k").and_then(|v| v.as_str()) == Some("sale") {
      if action.get("op")?.as_str()?.to_lowercase() != "auth-cfg"
        || action.get("st")?.as_str().is_none()
        || action.get("pt")?.as_str().is_none()
        || action.get("ctl")?.get("ty")?.as_str()? != "ta"
        || action.get("ctl")?.get("auth")?.as_str()? != link.ins
        || !action.get("s")?.is_object()
        || !action.get("s")?.get("r")?.is_object()
      {
        return None;
      }
      let id = Self::tap_token_authority_id(inscription, action_index);
      if self.tap_get_authority_config(&id).is_some() {
        return None;
      }
      let sale_token = self.token_proof_get_deploy(action.get("st")?.as_str()?)?;
      let payment_token = self.token_proof_get_deploy(action.get("pt")?.as_str()?)?;
      let treasury = self.validate_token_sale_target(action.get("tre")?)?;
      let start_height = Self::token_proof_storage_height(action.get("s")?.get("sh"))?;
      let end_height = Self::token_proof_storage_height(action.get("s")?.get("eh"))?;
      if end_height <= start_height {
        return None;
      }
      let hard_cap = self
        .token_proof_resolve_protocol_amount(action.get("s")?.get("hc")?, &payment_token.record)?;
      let soft_cap = match action.get("s")?.get("sc") {
        Some(value) => {
          Some(self.token_proof_resolve_protocol_amount(value, &payment_token.record)?)
        }
        None => None,
      };
      let min_contribution = match action.get("s")?.get("mn") {
        Some(value) => {
          Some(self.token_proof_resolve_protocol_amount(value, &payment_token.record)?)
        }
        None => None,
      };
      let max_contribution = match action.get("s")?.get("mx") {
        Some(value) => {
          Some(self.token_proof_resolve_protocol_amount(value, &payment_token.record)?)
        }
        None => None,
      };
      let payment_rate = self.token_proof_resolve_protocol_amount(
        action.get("s")?.get("r")?.get("pa")?,
        &payment_token.record,
      )?;
      let sale_rate = self.token_proof_resolve_protocol_amount(
        action.get("s")?.get("r")?.get("sa")?,
        &sale_token.record,
      )?;
      if soft_cap.map(|v| v > hard_cap).unwrap_or(false)
        || match (min_contribution, max_contribution) {
          (Some(min), Some(max)) => min > max,
          _ => false,
        }
        || action.get("s")?.get("r")?.get("cm")?.as_str()? != "fix"
        || action.get("s")?.get("r")?.get("rnd")?.as_str()? != "flr"
        || action.get("s")?.get("ov")?.as_str()? != "reject"
      {
        return None;
      }
      let allowlist = match action.get("s")?.get("alw") {
        Some(serde_json::Value::Null) | None => serde_json::Value::Null,
        Some(value) => {
          if !value.is_object()
            || value.get("ty").and_then(|v| v.as_str()) != Some("sha256-merkle")
            || !matches!(
              value.get("lf").and_then(|v| v.as_str()),
              Some("addr") | Some("addr-cap")
            )
            || !value
              .get("root")
              .and_then(|v| v.as_str())
              .map(Self::tap_is_valid_sha256_hex)
              .unwrap_or(false)
          {
            return None;
          }
          serde_json::json!({
            "ty": "sha256-merkle",
            "lf": value.get("lf")?.as_str()?,
            "root": value.get("root")?.as_str()?.to_lowercase()
          })
        }
      };
      return Some(AuthorityConfigRecord {
        id,
        k: "sale".to_string(),
        n: action
          .get("n")
          .and_then(|v| v.as_str())
          .map(|s| s.to_string()),
        stk: String::new(),
        rt: Vec::new(),
        st: Some(sale_token.tick),
        pt: Some(payment_token.tick),
        ctl: serde_json::json!({ "ty": "ta", "auth": link.ins }),
        tre: Some(Self::token_sale_target_value(&treasury)),
        seq: 0,
        r: serde_json::Value::Null,
        a: Vec::new(),
        ak: Vec::new(),
        sh: String::new(),
        fee: String::new(),
        pf: String::new(),
        min: String::new(),
        p: false,
        pp: None,
        att: None,
        xs: None,
        s: Some(serde_json::json!({
          "sh": start_height,
          "eh": end_height,
          "hc": hard_cap.to_string(),
          "sc": soft_cap.map(|v| v.to_string()),
          "mn": min_contribution.map(|v| v.to_string()),
          "mx": max_contribution.map(|v| v.to_string()),
          "r": { "cm": "fix", "pa": payment_rate.to_string(), "sa": sale_rate.to_string(), "rnd": "flr" },
          "ov": "reject",
          "cx": action.get("s").and_then(|s| s.get("cx")).and_then(|v| v.as_bool()).unwrap_or(false),
          "alw": allowlist
        })),
        blck: block,
        tx: transaction.to_string(),
        vo: vout,
        val: value.to_string(),
        ins: inscription.to_string(),
        num: number,
        ts: timestamp,
      });
    }
    if action.get("op")?.as_str()?.to_lowercase() != "auth-cfg"
      || action.get("k")?.as_str()? != "stk"
      || !action.get("stk")?.is_string()
      || !action.get("rt")?.is_array()
      || action.get("ctl")?.get("ty")?.as_str()? != "ta"
      || action.get("ctl")?.get("auth")?.as_str()? != link.ins
      || action.get("r")?.get("cm")?.as_str()? != "arps"
      || action.get("r")?.get("rnd")?.as_str()? != "flr"
      || action.get("r")?.get("aw")?.as_bool()? != false
      || !action.get("r")?.get("tr")?.is_array()
    {
      return None;
    }
    let empty_policy = action.get("r")?.get("ep")?.as_str()?;
    if !matches!(empty_policy, "reject" | "hold" | "carry") {
      return None;
    }
    let id = Self::tap_token_authority_id(inscription, action_index);
    if self.tap_get_authority_config(&id).is_some() {
      return None;
    }
    let stake_tick = Self::js_to_lowercase(action.get("stk")?.as_str()?);
    if self
      .tap_get::<DeployRecord>(&format!("d/{}", Self::json_stringify_lower(&stake_tick)))
      .ok()
      .flatten()
      .is_none()
    {
      return None;
    }
    let mut reward_ticks = Vec::new();
    for rt in action.get("rt")?.as_array()? {
      let tick = Self::js_to_lowercase(rt.as_str()?);
      if self
        .tap_get::<DeployRecord>(&format!("d/{}", Self::json_stringify_lower(&tick)))
        .ok()
        .flatten()
        .is_none()
      {
        return None;
      }
      reward_ticks.push(tick);
    }
    let mut tier_ids = std::collections::HashSet::new();
    let mut tiers = Vec::new();
    for tier in action.get("r")?.get("tr")?.as_array()? {
      let id_value = tier.get("id")?.as_str()?.to_string();
      if !tier_ids.insert(id_value.clone()) {
        return None;
      }
      let dur_i = tier.get("dur").and_then(Self::js_parse_int)?;
      if dur_i <= 0 {
        return None;
      }
      let weight = Self::js_value_to_string(tier.get("w")?)
        .parse::<i128>()
        .ok()?;
      if weight <= 0 {
        return None;
      }
      tiers.push(serde_json::json!({
        "id": id_value,
        "dur": dur_i,
        "w": weight.to_string()
      }));
    }
    let update_delay = match action.get("r").and_then(|r| r.get("ud")) {
      Some(value) => Self::js_parse_int(value)?,
      None => 0,
    };
    if update_delay < 0 {
      return None;
    }
    Some(AuthorityConfigRecord {
      id,
      k: "stk".to_string(),
      n: action
        .get("n")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()),
      stk: stake_tick,
      rt: reward_ticks,
      st: None,
      pt: None,
      ctl: serde_json::json!({ "ty": "ta", "auth": link.ins }),
      tre: None,
      seq: 0,
      r: serde_json::json!({
        "cm": "arps",
        "rnd": "flr",
        "aw": false,
        "ud": update_delay,
        "ep": empty_policy,
        "tr": tiers
      }),
      a: Vec::new(),
      ak: Vec::new(),
      sh: String::new(),
      fee: String::new(),
      pf: String::new(),
      min: String::new(),
      p: false,
      pp: None,
      att: None,
      xs: None,
      s: None,
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    })
  }

  fn process_authority_config_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(config) = self.validate_authority_config_action(
      action,
      link,
      inscription,
      action_index,
      transaction,
      vout,
      value,
      number,
      block,
      timestamp,
    ) else {
      return false;
    };
    if config.k == "amm" {
      self.put_amm_pool(&config);
      let _ = self.tap_set_list_record("amml", "ammli", &format!("amm/{}", config.id));
      let _ = self.tap_put(&format!("ammi/{}", config.id), &"".to_string());
      for key in &config.ak {
        let _ = self.tap_set_list_record(
          &format!("ammat/{}", key),
          &format!("ammati/{}", key),
          &format!("amm/{}", config.id),
        );
      }
    }
    let _ = self.tap_put(&format!("ah/{}", config.id), &config);
    let _ = self.tap_set_list_record("ahl", "ahli", &config);
    let _ = self.tap_set_list_record(
      &format!("ahk/{}", config.k),
      &format!("ahki/{}", config.k),
      &config,
    );
    true
  }

  fn normalize_amm_add_liquidity_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
    pool_override: Option<AuthorityConfigRecord>,
  ) -> Option<AmmAddValidation> {
    let link = link?;
    if action.get("auth")?.as_str().is_none()
      || action.get("amts")?.as_array()?.len() != 2
      || action.get("min")?.as_str().is_none()
      || action
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|r| !Self::is_amm_ref(r))
        .unwrap_or(false)
      || Self::amm_action_expired(action, block)
    {
      return None;
    }
    let pool = match pool_override {
      Some(pool) => pool,
      None => self.get_amm_pool(action.get("auth")?.as_str()?)?,
    };
    if pool.k != "amm" || pool.p || !Self::amm_pool_assets_are_tap(&pool) {
      return None;
    }
    let to = self.validate_amm_lp_target(action.get("to")?)?;
    let token0 = self.token_proof_get_deploy(Self::amm_pool_tick(&pool, 0)?.as_str())?;
    let token1 = self.token_proof_get_deploy(Self::amm_pool_tick(&pool, 1)?.as_str())?;
    let amts = action.get("amts")?.as_array()?;
    let amount0 = self.token_proof_resolve_protocol_amount_bigint(&amts[0], &token0.record)?;
    let amount1 = self.token_proof_resolve_protocol_amount_bigint(&amts[1], &token1.record)?;
    let min_shares = Self::parse_amm_uint_value(action.get("min")?, false)?;
    let reserves = Self::amm_pool_reserves(&pool)?;
    let total_shares = Self::amm_pool_shares(&pool)?;
    let min_locked = pool.min.parse::<BigInt>().ok()?;
    let (minted, burn_minted) = if total_shares == BigInt::from(0) {
      let root = Self::integer_sqrt(&(amount0.clone() * amount1.clone()))?;
      if root <= min_locked {
        return None;
      }
      (root - &min_locked, min_locked)
    } else {
      if reserves[0] <= BigInt::from(0) || reserves[1] <= BigInt::from(0) {
        return None;
      }
      let shares0 = &amount0 * &total_shares / &reserves[0];
      let shares1 = &amount1 * &total_shares / &reserves[1];
      (
        if shares0 < shares1 { shares0 } else { shares1 },
        BigInt::from(0),
      )
    };
    if minted <= BigInt::from(0) || minted < min_shares {
      return None;
    }
    Some(AmmAddValidation {
      pool,
      to,
      auth_target: AmmTarget {
        tt: "a".to_string(),
        to: link.addr.clone(),
      },
      reference: action
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()),
      amounts: [amount0.clone(), amount1.clone()],
      minted: minted.clone(),
      reserves_after: [reserves[0].clone() + amount0, reserves[1].clone() + amount1],
      shares_after: total_shares + minted + burn_minted,
    })
  }

  fn normalize_amm_remove_liquidity_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
    pool_override: Option<AuthorityConfigRecord>,
    owner_shares_override: Option<BigInt>,
  ) -> Option<AmmRemoveValidation> {
    let link = link?;
    if action.get("auth")?.as_str().is_none()
      || action.get("sh")?.as_str().is_none()
      || action.get("min")?.as_array()?.len() != 2
      || action
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|r| !Self::is_amm_ref(r))
        .unwrap_or(false)
      || Self::amm_action_expired(action, block)
    {
      return None;
    }
    let pool = match pool_override {
      Some(pool) => pool,
      None => self.get_amm_pool(action.get("auth")?.as_str()?)?,
    };
    if pool.k != "amm" || !Self::amm_pool_assets_are_tap(&pool) {
      return None;
    }
    let owner = match action.get("own") {
      Some(value) => self.validate_amm_lp_target(value)?,
      None => AmmTarget {
        tt: "a".to_string(),
        to: link.addr.clone(),
      },
    };
    if (owner.tt == "a" && owner.to != link.addr) || (owner.tt == "h" && owner.to != link.ins) {
      return None;
    }
    let to = self.validate_amm_target(action.get("to")?, true)?;
    let token0 = self.token_proof_get_deploy(Self::amm_pool_tick(&pool, 0)?.as_str())?;
    let token1 = self.token_proof_get_deploy(Self::amm_pool_tick(&pool, 1)?.as_str())?;
    let shares = Self::parse_amm_uint_value(action.get("sh")?, false)?;
    let min_arr = action.get("min")?.as_array()?;
    let min0 = self.token_proof_resolve_protocol_amount_bigint(&min_arr[0], &token0.record)?;
    let min1 = self.token_proof_resolve_protocol_amount_bigint(&min_arr[1], &token1.record)?;
    let reserves = Self::amm_pool_reserves(&pool)?;
    let total_shares = Self::amm_pool_shares(&pool)?;
    if total_shares <= BigInt::from(0)
      || reserves[0] <= BigInt::from(0)
      || reserves[1] <= BigInt::from(0)
    {
      return None;
    }
    let owner_shares =
      owner_shares_override.unwrap_or_else(|| self.get_amm_lp_shares(&pool.id, &owner));
    if owner_shares < shares {
      return None;
    }
    let out0 = &shares * &reserves[0] / &total_shares;
    let out1 = &shares * &reserves[1] / &total_shares;
    if out0 <= BigInt::from(0) || out1 <= BigInt::from(0) || out0 < min0 || out1 < min1 {
      return None;
    }
    let reserves_after = [reserves[0].clone() - &out0, reserves[1].clone() - &out1];
    if !self.amm_reserves_cover_obligation_locks(&pool, &reserves_after, None) {
      return None;
    }
    Some(AmmRemoveValidation {
      pool,
      owner: owner.clone(),
      to,
      auth_target: owner,
      reference: action
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()),
      shares: shares.clone(),
      outputs: [out0.clone(), out1.clone()],
      reserves_after,
      shares_after: total_shares - shares,
    })
  }

  fn parse_amm_side(value: &serde_json::Value) -> Option<usize> {
    if value.as_u64() == Some(0) || value.as_str() == Some("0") {
      return Some(0);
    }
    if value.as_u64() == Some(1) || value.as_str() == Some("1") {
      return Some(1);
    }
    None
  }

  fn normalize_amm_swap_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
    pool_override: Option<AuthorityConfigRecord>,
  ) -> Option<AmmSwapValidation> {
    let link = link?;
    if action.get("auth")?.as_str().is_none()
      || action.get("m")?.as_str().is_none()
      || action
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|r| !Self::is_amm_ref(r))
        .unwrap_or(false)
      || Self::amm_action_expired(action, block)
    {
      return None;
    }
    let pool = match pool_override {
      Some(pool) => pool,
      None => self.get_amm_pool(action.get("auth")?.as_str()?)?,
    };
    if pool.k != "amm" || pool.p || !Self::amm_pool_assets_are_tap(&pool) {
      return None;
    }
    let side = Self::parse_amm_side(action.get("i")?)?;
    let out_side = if side == 0 { 1 } else { 0 };
    let to = self.validate_amm_target(action.get("to")?, true)?;
    let token_in = self.token_proof_get_deploy(Self::amm_pool_tick(&pool, side)?.as_str())?;
    let token_out = self.token_proof_get_deploy(Self::amm_pool_tick(&pool, out_side)?.as_str())?;
    let reserves = Self::amm_pool_reserves(&pool)?;
    let fee_bps = pool.fee.parse::<BigInt>().ok()?;
    let protocol_share_bps = pool.pf.parse::<BigInt>().ok()?;
    let mode = action.get("m")?.as_str()?.to_string();
    let calc = if mode == "xin" {
      let amount_in =
        self.token_proof_resolve_protocol_amount_bigint(action.get("amt")?, &token_in.record)?;
      let limit =
        self.token_proof_resolve_protocol_amount_bigint(action.get("min")?, &token_out.record)?;
      let calc = Self::calculate_amm_exact_in(
        &amount_in,
        &reserves[side],
        &reserves[out_side],
        &fee_bps,
        &protocol_share_bps,
      )?;
      if calc.amount_out <= BigInt::from(0) || calc.amount_out < limit {
        return None;
      }
      calc
    } else if mode == "xout" {
      let amount_out =
        self.token_proof_resolve_protocol_amount_bigint(action.get("out")?, &token_out.record)?;
      let limit =
        self.token_proof_resolve_protocol_amount_bigint(action.get("max")?, &token_in.record)?;
      let calc = Self::calculate_amm_exact_out(
        &amount_out,
        &reserves[side],
        &reserves[out_side],
        &fee_bps,
        &protocol_share_bps,
      )?;
      if calc.amount_in > limit {
        return None;
      }
      calc
    } else {
      return None;
    };
    let mut reserves_after = reserves.clone();
    reserves_after[side] = reserves_after[side].clone() + &calc.amount_in - &calc.protocol_fee;
    reserves_after[out_side] = reserves_after[out_side].clone() - &calc.amount_out;
    if reserves_after[0] < BigInt::from(0) || reserves_after[1] < BigInt::from(0) {
      return None;
    }
    if !self.amm_reserves_cover_obligation_locks(&pool, &reserves_after, None) {
      return None;
    }
    Some(AmmSwapValidation {
      pool: pool.clone(),
      side,
      out_side,
      to,
      auth_target: AmmTarget {
        tt: "a".to_string(),
        to: link.addr.clone(),
      },
      reference: action
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()),
      amount_in: calc.amount_in,
      amount_out: calc.amount_out,
      gross_fee: calc.gross_fee,
      protocol_fee: calc.protocol_fee,
      reserves_after,
      shares_after: Self::amm_pool_shares(&pool)?,
      mode,
    })
  }

  fn validate_fund_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
  ) -> Option<SaleFundValidation> {
    let link = link?;
    let auth = action.get("auth")?.as_str()?;
    let tick = action.get("tick")?.as_str()?;
    let config = self.tap_get_authority_config(auth)?;
    let token = self.token_proof_get_deploy(tick)?;
    if config.k != "sale"
      || config.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
      || config.st.as_deref() != Some(token.tick.as_str())
    {
      return None;
    }
    let amount = self.token_proof_resolve_protocol_amount(action.get("amt")?, &token.record)?;
    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", link.addr, token.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", link.addr, token.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let locked = self.tap_get_locked_amount(&link.addr, &token.tick_key);
    let obligation_locked =
      self.tap_get_account_obligation_locked_amount(&link.addr, &token.tick_key);
    if balance - transferable - locked - obligation_locked - amount < 0 {
      return None;
    }
    self
      .tap_get_authority_balance(&auth, &token.tick_key)
      .checked_add(amount)?;
    Some(SaleFundValidation {
      config,
      tick: token.tick,
      tick_key: token.tick_key,
      amount,
    })
  }

  fn process_fund_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(normalized) = self.validate_fund_sale_action(action, Some(link)) else {
      return false;
    };
    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", link.addr, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let after = balance - normalized.amount;
    if after < 0 {
      return false;
    }
    let _ = self.tap_put(
      &format!("b/{}/{}", link.addr, normalized.tick_key),
      &after.to_string(),
    );
    let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
    if !self.tap_add_authority_balance(auth, &normalized.tick_key, normalized.amount) {
      return false;
    }
    let mut status = self.tap_get_sale_status(auth, &normalized.config);
    let inv = Self::token_sale_status_i128(&status, "inv") + normalized.amount;
    Self::token_sale_status_set_string(&mut status, "inv", inv);
    self.tap_put_sale_status(&status);
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", link.addr, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let auth_balance = self.tap_get_authority_balance(auth, &normalized.tick_key);
    self.tap_apply_authority_transfer_logs(
      &normalized.tick,
      &normalized.tick_key,
      &link.addr,
      auth,
      transferable,
      after,
      auth_balance,
      normalized.amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "si",
      auth,
    );
    true
  }

  fn validate_sale_contribution_action(
    &mut self,
    action: &mut serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    action_index: usize,
    block: u32,
  ) -> Option<SaleContributionValidation> {
    let link = link?;
    let auth = action.get("auth")?.as_str()?.to_string();
    let tick = action.get("tick")?.as_str()?.to_string();
    let config = self.tap_get_authority_config(&auth)?;
    let token = self.token_proof_get_deploy(&tick)?;
    if config.k != "sale" || config.pt.as_deref() != Some(token.tick.as_str()) {
      return None;
    }
    let status = self.tap_get_sale_status(&auth, &config);
    let s = config.s.as_ref()?;
    let sh = s.get("sh").and_then(Self::js_parse_int)?;
    let eh = s.get("eh").and_then(Self::js_parse_int)?;
    if Self::token_sale_status_bool(&status, "fin")
      || Self::token_sale_status_bool(&status, "can")
      || i128::from(block) < sh
      || i128::from(block) > eh
    {
      return None;
    }
    let claim = Self::normalize_address(action.get("claim")?.as_str()?);
    if !self.is_valid_bitcoin_address(&claim) || claim != link.addr {
      return None;
    }
    if let Some(v) = action.get_mut("claim") {
      *v = serde_json::Value::String(claim.clone());
    }
    let amount = self.token_proof_resolve_protocol_amount(action.get("amt")?, &token.record)?;
    let min = s
      .get("mn")
      .and_then(|v| v.as_str())
      .and_then(|v| v.parse::<i128>().ok());
    let max = s
      .get("mx")
      .and_then(|v| v.as_str())
      .and_then(|v| v.parse::<i128>().ok());
    let existing_amount = self
      .tap_get::<String>(&format!("scab/{}/{}", auth, claim))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let next_existing = existing_amount.checked_add(amount)?;
    let next_total = Self::token_sale_status_i128(&status, "tc").checked_add(amount)?;
    if min.map(|v| amount < v).unwrap_or(false)
      || max.map(|v| next_existing > v).unwrap_or(false)
      || next_total > s.get("hc")?.as_str()?.parse::<i128>().ok()?
      || !self.validate_token_sale_allowlist(&config, action, &claim, amount, &token.record)
    {
      return None;
    }
    let allocation = self.token_sale_contribution_allocation(amount, &config)?;
    if allocation <= 0 {
      return None;
    }
    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", link.addr, token.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", link.addr, token.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let locked = self.tap_get_locked_amount(&link.addr, &token.tick_key);
    let obligation_locked =
      self.tap_get_account_obligation_locked_amount(&link.addr, &token.tick_key);
    if balance - transferable - locked - obligation_locked - amount < 0 {
      return None;
    }
    self
      .tap_get_authority_balance(&auth, &token.tick_key)
      .checked_add(amount)?;
    Some(SaleContributionValidation {
      id: Self::tap_token_sale_contribution_id(inscription, action_index),
      config,
      status,
      claim,
      tick: token.tick,
      tick_key: token.tick_key,
      amount,
      allocation,
      existing_amount,
    })
  }

  fn process_sale_contribution_action(
    &mut self,
    action: &mut serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(normalized) =
      self.validate_sale_contribution_action(action, Some(link), inscription, action_index, block)
    else {
      return false;
    };
    if self
      .tap_get::<serde_json::Value>(&format!("scon/{}", normalized.id))
      .ok()
      .flatten()
      .is_some()
    {
      return false;
    }
    let auth = action
      .get("auth")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", link.addr, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let after = balance - normalized.amount;
    if after < 0 {
      return false;
    }
    let _ = self.tap_put(
      &format!("b/{}/{}", link.addr, normalized.tick_key),
      &after.to_string(),
    );
    if !self.tap_add_authority_balance(&auth, &normalized.tick_key, normalized.amount) {
      return false;
    }
    let mut status = self.tap_get_sale_status(&auth, &normalized.config);
    let tc_before = Self::token_sale_status_i128(&status, "tc");
    let alc_before = Self::token_sale_status_i128(&status, "alc");
    Self::token_sale_status_set_string(&mut status, "tc", tc_before + normalized.amount);
    Self::token_sale_status_set_string(&mut status, "alc", alc_before + normalized.allocation);
    self.tap_put_sale_status(&status);
    let _ = self.tap_put(
      &format!("scab/{}/{}", auth, normalized.claim),
      &(normalized.existing_amount + normalized.amount).to_string(),
    );
    let rec = serde_json::json!({
      "id": normalized.id,
      "auth": auth,
      "addr": link.addr,
      "claim": normalized.claim,
      "pt": normalized.config.pt.clone().unwrap_or_default(),
      "amt": normalized.amount.to_string(),
      "sa": normalized.allocation.to_string(),
      "st": normalized.config.st.clone().unwrap_or_default(),
      "status": "open",
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let cid = rec
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let _ = self.tap_put(&format!("scon/{}", cid), &rec);
    let _ = self.tap_set_list_record("sconl", "sconli", &rec);
    let _ = self.tap_set_list_record(
      &format!("scona/{}", auth),
      &format!("sconai/{}", auth),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("sconaddr/{}", link.addr),
      &format!("sconaddri/{}", link.addr),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!(
        "sconcl/{}",
        rec.get("claim").and_then(|v| v.as_str()).unwrap_or("")
      ),
      &format!(
        "sconcli/{}",
        rec.get("claim").and_then(|v| v.as_str()).unwrap_or("")
      ),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("tx/scon/{}", transaction),
      &format!("txi/scon/{}", transaction),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("blck/scon/{}", block),
      &format!("blcki/scon/{}", block),
      &rec,
    );
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", link.addr, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let auth_balance = self.tap_get_authority_balance(&auth, &normalized.tick_key);
    self.tap_apply_authority_transfer_logs(
      &normalized.tick,
      &normalized.tick_key,
      &link.addr,
      &auth,
      transferable,
      after,
      auth_balance,
      normalized.amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sc",
      &cid,
    );
    true
  }

  fn validate_finalize_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
  ) -> Option<SaleFinalizeValidation> {
    let link = link?;
    let auth = action.get("auth")?.as_str()?;
    let config = self.tap_get_authority_config(auth)?;
    if config.k != "sale"
      || config.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
    {
      return None;
    }
    let status = self.tap_get_sale_status(auth, &config);
    let total = Self::token_sale_status_i128(&status, "tc");
    let s = config.s.as_ref()?;
    let soft_cap = s
      .get("sc")
      .and_then(|v| v.as_str())
      .and_then(|v| v.parse::<i128>().ok())
      .unwrap_or(0);
    let hard_cap = s.get("hc")?.as_str()?.parse::<i128>().ok()?;
    let end_height = s.get("eh").and_then(Self::js_parse_int)?;
    if Self::token_sale_status_bool(&status, "fin")
      || Self::token_sale_status_bool(&status, "can")
      || Self::token_sale_status_bool(&status, "pp")
      || (i128::from(block) <= end_height && total < hard_cap)
      || total < soft_cap
    {
      return None;
    }
    let sale_key = Self::json_stringify_lower(config.st.as_deref()?);
    let payment_key = Self::json_stringify_lower(config.pt.as_deref()?);
    if self.tap_get_authority_balance(auth, &sale_key)
      < Self::token_sale_status_i128(&status, "alc")
      || self.tap_get_authority_balance(auth, &payment_key) < total
    {
      return None;
    }
	    Some(SaleFinalizeValidation {
	      config,
	      payment_key,
	      amount: total,
	    })
	  }

	  fn can_credit_sale_target(
	    &mut self,
	    config: &AuthorityConfigRecord,
	    target: &SaleTarget,
	  ) -> bool {
	    if target.tt == "a" || target.tt == "b" {
	      return true;
	    }
	    if target.tt == "h" {
	      return self.tap_get_authority_config(&target.to).is_some();
	    }
	    let _ = config;
	    false
	  }

	  fn credit_sale_target(
	    &mut self,
	    config: &AuthorityConfigRecord,
	    tick_key: &str,
	    tick: &str,
    amount: i128,
  ) -> Option<(SaleTarget, i128)> {
    let target = self.validate_token_sale_target(config.tre.as_ref()?)?;
    let balance = if target.tt == "a" {
      self.tap_credit_address_balance(&target.to, tick_key, tick, amount)?
    } else if target.tt == "h" {
      if !self.tap_add_authority_balance(&target.to, tick_key, amount) {
        return None;
      }
      self.tap_get_authority_balance(&target.to, tick_key)
    } else {
      0
    };
    Some((target, balance))
  }

	  fn process_finalize_sale_action(
	    &mut self,
	    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_finalize_sale_action(action, link, block) else {
      return false;
    };
    let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
    let before = self.tap_get_authority_balance(auth, &normalized.payment_key);
    if before < normalized.amount
      || !self.tap_set_authority_balance(auth, &normalized.payment_key, before - normalized.amount)
    {
      return false;
    }
    let payment_tick = normalized.config.pt.clone().unwrap_or_default();
    let Some((target, target_balance)) = self.credit_sale_target(
      &normalized.config,
      &normalized.payment_key,
      &payment_tick,
      normalized.amount,
    ) else {
      return false;
    };
    let mut status = self.tap_get_sale_status(auth, &normalized.config);
    Self::token_sale_status_set_bool(&mut status, "fin", true);
    Self::token_sale_status_set_bool(&mut status, "pp", true);
    self.tap_put_sale_status(&status);
    self.tap_apply_authority_target_transfer_logs(
      &payment_tick,
      &normalized.payment_key,
      auth,
      &target,
      before - normalized.amount,
      target_balance,
      normalized.amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sz",
      auth,
    );
	    true
	  }

	  fn validate_resolve_sale_action(
	    &mut self,
	    action: &serde_json::Value,
	    block: u32,
	  ) -> Option<SaleResolveValidation> {
	    let auth = action.get("auth")?.as_str()?;
	    let config = self.tap_get_authority_config(auth)?;
	    if config.k != "sale" {
	      return None;
	    }
	    let status = self.tap_get_sale_status(auth, &config);
	    if Self::token_sale_status_bool(&status, "fin")
	      || Self::token_sale_status_bool(&status, "can")
	      || Self::token_sale_status_bool(&status, "pp")
	    {
	      return None;
	    }
	    let total = Self::token_sale_status_i128(&status, "tc");
	    let s = config.s.as_ref()?;
	    let soft_cap = s
	      .get("sc")
	      .and_then(|v| v.as_str())
	      .and_then(|v| v.parse::<i128>().ok())
	      .unwrap_or(0);
	    let hard_cap = s.get("hc")?.as_str()?.parse::<i128>().ok()?;
	    let end_height = s.get("eh").and_then(Self::js_parse_int)?;
	    let after_end = i128::from(block) > end_height;
	    if !after_end && total < hard_cap {
	      return None;
	    }
	    let sale_key = Self::json_stringify_lower(config.st.as_deref()?);
	    let payment_key = Self::json_stringify_lower(config.pt.as_deref()?);
	    let sale_balance = self.tap_get_authority_balance(auth, &sale_key);
	    let payment_balance = self.tap_get_authority_balance(auth, &payment_key);
	    let target = self.validate_token_sale_target(config.tre.as_ref()?)?;
	    let can_finalize = total >= soft_cap
	      && sale_balance >= Self::token_sale_status_i128(&status, "alc")
	      && payment_balance >= total
	      && self.can_credit_sale_target(&config, &target);
	    if can_finalize {
	      return Some(SaleResolveValidation {
	        config,
	        payment_key,
	        amount: total,
	        outcome: "finalized".to_string(),
	        reason: None,
	      });
	    }
	    if !after_end {
	      return None;
	    }
	    let reason = if total < soft_cap {
	      "soft-cap-missed"
	    } else if sale_balance < Self::token_sale_status_i128(&status, "alc") {
	      "inventory-underfunded"
	    } else if payment_balance < total {
	      "payment-underfunded"
	    } else {
	      "treasury-unavailable"
	    };
	    Some(SaleResolveValidation {
	      config,
	      payment_key,
	      amount: total,
	      outcome: "failed".to_string(),
	      reason: Some(reason.to_string()),
	    })
	  }

	  fn process_resolve_sale_action(
	    &mut self,
	    action: &serde_json::Value,
	    _link: Option<&TokenAuthCreateRecord>,
	    transaction: &str,
	    vout: u32,
	    value: u64,
	    inscription: &str,
	    number: i32,
	    block: u32,
	    timestamp: u32,
	    action_index: usize,
	  ) -> bool {
	    let Some(normalized) = self.validate_resolve_sale_action(action, block) else {
	      return false;
	    };
	    let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
	    let mut status = self.tap_get_sale_status(auth, &normalized.config);
	    if normalized.outcome == "finalized" {
	      let before = self.tap_get_authority_balance(auth, &normalized.payment_key);
	      if before < normalized.amount
	        || !self.tap_set_authority_balance(
	          auth,
	          &normalized.payment_key,
	          before - normalized.amount,
	        )
	      {
	        return false;
	      }
	      let payment_tick = normalized.config.pt.clone().unwrap_or_default();
	      let Some((target, target_balance)) = self.credit_sale_target(
	        &normalized.config,
	        &normalized.payment_key,
	        &payment_tick,
	        normalized.amount,
	      ) else {
	        return false;
	      };
	      Self::token_sale_status_set_bool(&mut status, "fin", true);
	      Self::token_sale_status_set_bool(&mut status, "pp", true);
	      if let Some(map) = status.as_object_mut() {
	        map.insert("res".to_string(), serde_json::Value::String("finalized".to_string()));
	        map.insert("rblck".to_string(), serde_json::json!(block));
	        map.insert("rtx".to_string(), serde_json::Value::String(transaction.to_string()));
	      }
	      self.tap_put_sale_status(&status);
	      self.tap_apply_authority_target_transfer_logs(
	        &payment_tick,
	        &normalized.payment_key,
	        auth,
	        &target,
	        before - normalized.amount,
	        target_balance,
	        normalized.amount,
	        block,
	        inscription,
	        number,
	        timestamp,
	        transaction,
	        vout,
	        value,
	        "sz",
	        auth,
	      );
	    } else {
	      Self::token_sale_status_set_bool(&mut status, "can", true);
	      Self::token_sale_status_set_bool(&mut status, "fail", true);
	      if let Some(map) = status.as_object_mut() {
	        map.insert(
	          "res".to_string(),
	          serde_json::Value::String(normalized.reason.clone().unwrap_or_default()),
	        );
	        map.insert("rblck".to_string(), serde_json::json!(block));
	        map.insert("rtx".to_string(), serde_json::Value::String(transaction.to_string()));
	      }
	      self.tap_put_sale_status(&status);
	    }
	    let rec = serde_json::json!({
	      "id": Self::tap_token_sale_record_id("sres", inscription, action_index),
	      "auth": auth,
	      "out": normalized.outcome,
	      "reason": normalized.reason,
	      "blck": block,
	      "tx": transaction,
	      "vo": vout,
	      "val": value.to_string(),
	      "ins": inscription,
	      "num": number,
	      "ts": timestamp
	    });
	    let _ = self.tap_set_list_record("sresl", "sresli", &rec);
	    let _ = self.tap_set_list_record(
	      &format!("sresa/{}", auth),
	      &format!("sresai/{}", auth),
	      &rec,
	    );
	    let _ = self.tap_set_list_record(
	      &format!("tx/sres/{}", transaction),
	      &format!("txi/sres/{}", transaction),
	      &rec,
	    );
	    let _ = self.tap_set_list_record(
	      &format!("blck/sres/{}", block),
	      &format!("blcki/sres/{}", block),
	      &rec,
	    );
	    true
	  }

	  fn validate_stake_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    action_index: usize,
    block: u32,
  ) -> Option<TokenStakeValidation> {
    let link = link?;
    let auth = action.get("auth")?.as_str()?.to_string();
    let tick = Self::js_to_lowercase(action.get("tick")?.as_str()?);
    if self.tap_feature_enabled(TapFeature::ValueStringifyActivation)
      && action.get("amt")?.is_number()
    {
      return None;
    }
    let tier_id = action.get("tier")?.as_str()?.to_string();
    let claim = Self::normalize_address(action.get("claim")?.as_str()?);
    if !self.is_valid_bitcoin_address(&claim) {
      return None;
    }
    let config = self.tap_get_authority_config(&auth)?;
    if config.k != "stk" || config.stk != tick {
      return None;
    }
    let tier = Self::authority_config_tier(&config, &tier_id)?;
    let dur = tier.get("dur").and_then(Self::js_parse_int)?;
    let weight = Self::js_value_to_string(tier.get("w")?)
      .parse::<BigInt>()
      .ok()?;
    if dur <= 0 || weight <= BigInt::from(0) {
      return None;
    }
    let tick_key = Self::json_stringify_lower(&tick);
    let deployed = self
      .tap_get::<DeployRecord>(&format!("d/{}", tick_key))
      .ok()
      .flatten()?;
    let amount =
      Self::resolve_number_string(&Self::js_value_to_string(action.get("amt")?), deployed.dec)?
        .parse::<i128>()
        .ok()?;
    let shares = BigInt::from(amount) * weight;
    if amount <= 0 || shares <= BigInt::from(0) {
      return None;
    }
    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", link.addr, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", link.addr, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let locked = self.tap_get_locked_amount(&link.addr, &tick_key);
    let obligation_locked = self.tap_get_account_obligation_locked_amount(&link.addr, &tick_key);
    if balance - transferable - locked - obligation_locked - amount < 0 {
      return None;
    }
    self
      .tap_get_authority_balance(&auth, &tick_key)
      .checked_add(amount)?;
    let mut debt = serde_json::Map::new();
    for reward_tick in self.tap_authority_reward_debt_ticks(&auth, &config) {
      let reward_key = Self::json_stringify_lower(&reward_tick);
      let acc = self.tap_get_authority_acc_reward(&auth, &reward_key);
      debt.insert(
        reward_tick,
        serde_json::Value::String(Self::authority_reward_debt_string(&shares, &acc)?),
      );
    }
    Some(TokenStakeValidation {
      id: Self::tap_token_stake_position_id(inscription, action_index),
      auth,
      addr: link.addr.clone(),
      claim,
      tick,
      tick_key,
      amt: amount,
      tier: tier_id,
      shares: shares.to_string(),
      uh: u32::try_from(i128::from(block) + dur).ok()?,
      debt: serde_json::Value::Object(debt),
    })
  }

  fn process_stake_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) =
      self.validate_stake_action(action, link, inscription, action_index, block)
    else {
      return false;
    };
    if self
      .tap_get::<StakePositionRecord>(&format!("sp/{}", normalized.id))
      .ok()
      .flatten()
      .is_some()
    {
      return false;
    }
    let balance = self
      .tap_get::<String>(&format!("b/{}/{}", normalized.addr, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let after = balance - normalized.amt;
    if after < 0 {
      return false;
    }
    let _ = self.tap_put(
      &format!("b/{}/{}", normalized.addr, normalized.tick_key),
      &after.to_string(),
    );
    if !self.tap_add_authority_balance(&normalized.auth, &normalized.tick_key, normalized.amt) {
      return false;
    }
    let old_shares = self.tap_get_authority_total_shares(&normalized.auth);
    let Some(normalized_shares) = normalized.shares.parse::<BigInt>().ok() else {
      return false;
    };
    let next_shares = old_shares + normalized_shares;
    if !self.tap_set_authority_total_shares(&normalized.auth, &next_shares) {
      return false;
    }
    let pos = StakePositionRecord {
      id: normalized.id.clone(),
      auth: normalized.auth.clone(),
      addr: normalized.addr.clone(),
      claim: normalized.claim.clone(),
      tick: normalized.tick.clone(),
      amt: normalized.amt.to_string(),
      tier: normalized.tier.clone(),
      shares: normalized.shares.clone(),
      uh: normalized.uh,
      debt: normalized.debt.clone(),
      status: "open".to_string(),
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
      closed_blck: None,
      closed_tx: None,
    };
    let _ = self.tap_put(&format!("sp/{}", normalized.id), &pos);
    let _ = self.tap_set_list_record("spl", "spli", &pos);
    let _ = self.tap_set_list_record(
      &format!("spa/{}", normalized.claim),
      &format!("spai/{}", normalized.claim),
      &pos,
    );
    let _ = self.tap_set_list_record(
      &format!("sph/{}", normalized.auth),
      &format!("sphi/{}", normalized.auth),
      &pos,
    );
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", normalized.addr, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let authority_balance = self.tap_get_authority_balance(&normalized.auth, &normalized.tick_key);
    self.tap_apply_authority_transfer_logs(
      &normalized.tick,
      &normalized.tick_key,
      &normalized.addr,
      &normalized.auth,
      transferable,
      after,
      authority_balance,
      normalized.amt,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sk",
      &normalized.id,
    );
    true
  }

  fn get_stake_position(&mut self, position_id: &str) -> Option<StakePositionRecord> {
    self
      .tap_get::<StakePositionRecord>(&format!("sp/{}", position_id))
      .ok()
      .flatten()
  }

  fn pending_stake_reward(&mut self, position: &StakePositionRecord, reward_tick: &str) -> i128 {
    let reward_key = Self::json_stringify_lower(reward_tick);
    let acc = self.tap_get_authority_acc_reward(&position.auth, &reward_key);
    let shares = position
      .shares
      .parse::<BigInt>()
      .unwrap_or_else(|_| BigInt::from(0));
    let paid = position
      .debt
      .get(reward_tick)
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    Self::authority_reward_pending_i128(&shares, &acc, &paid).unwrap_or(0)
  }

  fn process_claim_reward_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let auth = match action.get("auth").and_then(|v| v.as_str()) {
      Some(s) => s.to_string(),
      None => return false,
    };
    let pos_id = match action.get("pos").and_then(|v| v.as_str()) {
      Some(s) => s.to_string(),
      None => return false,
    };
    let reward_tick = match action.get("rt").and_then(|v| v.as_str()) {
      Some(s) => Self::js_to_lowercase(s),
      None => return false,
    };
    let Some(mut position) = self.get_stake_position(&pos_id) else {
      return false;
    };
    if position.auth != auth || position.status != "open" {
      return false;
    }
    if let Some(link) = link {
      if link.addr != position.claim {
        return false;
      }
    }
    let reward_key = Self::json_stringify_lower(&reward_tick);
    let pending = self.pending_stake_reward(&position, &reward_tick);
    if pending <= 0 {
      return false;
    }
    let auth_balance = self.tap_get_authority_balance(&auth, &reward_key);
    if auth_balance < pending {
      return false;
    }
    let Some(receiver_after) =
      self.tap_credit_address_balance(&position.claim, &reward_key, &reward_tick, pending)
    else {
      return false;
    };
    if !self.tap_set_authority_balance(&auth, &reward_key, auth_balance - pending) {
      return false;
    }
    let acc = self.tap_get_authority_acc_reward(&auth, &reward_key);
    let shares = position
      .shares
      .parse::<BigInt>()
      .unwrap_or_else(|_| BigInt::from(0));
    let mut debt = position.debt.as_object().cloned().unwrap_or_default();
    debt.insert(
      reward_tick.clone(),
      serde_json::Value::String(
        Self::authority_reward_debt_string(&shares, &acc).unwrap_or_else(|| "0".to_string()),
      ),
    );
    position.debt = serde_json::Value::Object(debt);
    let _ = self.tap_put(&format!("sp/{}", pos_id), &position);
    let claim = RewardClaimRecord {
      auth: auth.clone(),
      pos: pos_id.clone(),
      rt: reward_tick.clone(),
      claim: position.claim.clone(),
      amt: pending.to_string(),
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    };
    let _ = self.tap_set_list_record("rcl", "rcli", &claim);
    let _ = self.tap_set_list_record(
      &format!("rca/{}", position.claim),
      &format!("rcai/{}", position.claim),
      &claim,
    );
    let _ = self.tap_set_list_record(&format!("rch/{}", auth), &format!("rchi/{}", auth), &claim);
    self.tap_apply_authority_claim_transfer_logs(
      &reward_tick,
      &reward_key,
      &auth,
      &position.claim,
      auth_balance - pending,
      receiver_after,
      pending,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "rw",
      &pos_id,
    );
    true
  }

  fn process_unstake_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let auth = match action.get("auth").and_then(|v| v.as_str()) {
      Some(s) => s.to_string(),
      None => return false,
    };
    let pos_id = match action.get("pos").and_then(|v| v.as_str()) {
      Some(s) => s.to_string(),
      None => return false,
    };
    let Some(mut position) = self.get_stake_position(&pos_id) else {
      return false;
    };
    if position.auth != auth || position.status != "open" || block < position.uh {
      return false;
    }
    if let Some(link) = link {
      if link.addr != position.claim {
        return false;
      }
    }
    if let Some(reward_tick_raw) = action.get("rt").and_then(|v| v.as_str()) {
      let reward_tick = Self::js_to_lowercase(reward_tick_raw);
      let pending = self.pending_stake_reward(&position, &reward_tick);
      if pending > 0 {
        let reward_key = Self::json_stringify_lower(&reward_tick);
        let stake_key = Self::json_stringify_lower(&position.tick);
        let principal = position.amt.parse::<i128>().ok().unwrap_or(0);
        let auth_reward_balance = self.tap_get_authority_balance(&auth, &reward_key);
        if auth_reward_balance < pending
          || (reward_key == stake_key && auth_reward_balance < pending + principal)
        {
          return false;
        }
        if !self.process_claim_reward_action(
          &serde_json::json!({
            "op": "claim-rwd",
            "auth": auth.clone(),
            "pos": pos_id.clone(),
            "rt": reward_tick_raw
          }),
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ) {
          return false;
        }
        position = match self.get_stake_position(&pos_id) {
          Some(p) => p,
          None => return false,
        };
      }
    }
    let tick_key = Self::json_stringify_lower(&position.tick);
    let amount = position.amt.parse::<i128>().ok().unwrap_or(0);
    let auth_balance = self.tap_get_authority_balance(&position.auth, &tick_key);
    if amount <= 0 || auth_balance < amount {
      return false;
    }
    let Some(receiver_after) =
      self.tap_credit_address_balance(&position.claim, &tick_key, &position.tick, amount)
    else {
      return false;
    };
    if !self.tap_set_authority_balance(&position.auth, &tick_key, auth_balance - amount) {
      return false;
    }
    let old_shares = self.tap_get_authority_total_shares(&position.auth);
    let shares = position
      .shares
      .parse::<BigInt>()
      .unwrap_or_else(|_| BigInt::from(0));
    let next_shares = old_shares - shares;
    if !self.tap_set_authority_total_shares(&position.auth, &next_shares) {
      return false;
    }
    position.status = "closed".to_string();
    position.closed_blck = Some(block);
    position.closed_tx = Some(transaction.to_string());
    let _ = self.tap_put(&format!("sp/{}", pos_id), &position);
    self.tap_apply_authority_claim_transfer_logs(
      &position.tick,
      &tick_key,
      &position.auth,
      &position.claim,
      auth_balance - amount,
      receiver_after,
      amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "us",
      action.get("pos").and_then(|v| v.as_str()).unwrap_or(""),
    );
    true
  }

  fn get_sale_contribution(&mut self, cid: &str) -> Option<serde_json::Value> {
    self
      .tap_get::<serde_json::Value>(&format!("scon/{}", cid))
      .ok()
      .flatten()
  }

  fn process_claim_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(auth) = action.get("auth").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(cid) = action.get("cid").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(config) = self.tap_get_authority_config(auth) else {
      return false;
    };
    let Some(mut contribution) = self.get_sale_contribution(cid) else {
      return false;
    };
    let status = self.tap_get_sale_status(auth, &config);
    let claim = contribution
      .get("claim")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if config.k != "sale"
      || contribution.get("auth").and_then(|v| v.as_str()) != Some(auth)
      || contribution.get("status").and_then(|v| v.as_str()) != Some("open")
      || !Self::token_sale_status_bool(&status, "fin")
      || link.addr != claim
    {
      return false;
    }
    let tick = config.st.clone().unwrap_or_default();
    let tick_key = Self::json_stringify_lower(&tick);
    let amount = contribution
      .get("sa")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let auth_balance = self.tap_get_authority_balance(auth, &tick_key);
    if amount <= 0 || auth_balance < amount {
      return false;
    }
    let Some(receiver_after) = self.tap_credit_address_balance(&claim, &tick_key, &tick, amount)
    else {
      return false;
    };
    if !self.tap_set_authority_balance(auth, &tick_key, auth_balance - amount) {
      return false;
    }
    if let Some(map) = contribution.as_object_mut() {
      map.insert(
        "status".to_string(),
        serde_json::Value::String("claimed".to_string()),
      );
      map.insert("claim_blck".to_string(), serde_json::json!(block));
      map.insert(
        "claim_tx".to_string(),
        serde_json::Value::String(transaction.to_string()),
      );
    }
    let _ = self.tap_put(&format!("scon/{}", cid), &contribution);
    let mut sale_status = self.tap_get_sale_status(auth, &config);
    let claimed_before = Self::token_sale_status_i128(&sale_status, "clm");
    Self::token_sale_status_set_string(&mut sale_status, "clm", claimed_before + amount);
    self.tap_put_sale_status(&sale_status);
    let rec = serde_json::json!({
      "id": Self::tap_token_sale_record_id("sclaim", inscription, action_index),
      "auth": auth,
      "cid": cid,
      "claim": claim,
      "st": tick,
      "amt": amount.to_string(),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_set_list_record("sclaiml", "sclaimli", &rec);
    let _ = self.tap_set_list_record(&format!("scla/{}", auth), &format!("sclai/{}", auth), &rec);
    let _ = self.tap_set_list_record(
      &format!("scladdr/{}", claim),
      &format!("scladdri/{}", claim),
      &rec,
    );
    self.tap_apply_authority_claim_transfer_logs(
      &tick,
      &tick_key,
      auth,
      &claim,
      auth_balance - amount,
      receiver_after,
      amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sa",
      cid,
    );
    true
  }

  fn process_refund_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(auth) = action.get("auth").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(cid) = action.get("cid").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(config) = self.tap_get_authority_config(auth) else {
      return false;
    };
    let Some(mut contribution) = self.get_sale_contribution(cid) else {
      return false;
    };
    let status = self.tap_get_sale_status(auth, &config);
    let claim = contribution
      .get("claim")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let s = match config.s.as_ref() {
      Some(s) => s,
      None => return false,
    };
    let end_height = match s.get("eh").and_then(Self::js_parse_int) {
      Some(v) => v,
      None => return false,
    };
    if config.k != "sale"
      || contribution.get("auth").and_then(|v| v.as_str()) != Some(auth)
      || contribution.get("status").and_then(|v| v.as_str()) != Some("open")
      || Self::token_sale_status_bool(&status, "fin")
      || link.addr != claim
      || (!Self::token_sale_status_bool(&status, "can") && i128::from(block) <= end_height)
    {
      return false;
    }
    let soft_cap = s
      .get("sc")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    if !Self::token_sale_status_bool(&status, "can")
      && Self::token_sale_status_i128(&status, "tc") >= soft_cap
    {
      return false;
    }
    let tick = config.pt.clone().unwrap_or_default();
    let tick_key = Self::json_stringify_lower(&tick);
    let amount = contribution
      .get("amt")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let auth_balance = self.tap_get_authority_balance(auth, &tick_key);
    if amount <= 0 || auth_balance < amount {
      return false;
    }
    let Some(receiver_after) = self.tap_credit_address_balance(&claim, &tick_key, &tick, amount)
    else {
      return false;
    };
    if !self.tap_set_authority_balance(auth, &tick_key, auth_balance - amount) {
      return false;
    }
    if let Some(map) = contribution.as_object_mut() {
      map.insert(
        "status".to_string(),
        serde_json::Value::String("refunded".to_string()),
      );
      map.insert("refund_blck".to_string(), serde_json::json!(block));
      map.insert(
        "refund_tx".to_string(),
        serde_json::Value::String(transaction.to_string()),
      );
    }
    let _ = self.tap_put(&format!("scon/{}", cid), &contribution);
    let mut sale_status = self.tap_get_sale_status(auth, &config);
    let refunded_before = Self::token_sale_status_i128(&sale_status, "ref");
    Self::token_sale_status_set_string(&mut sale_status, "ref", refunded_before + amount);
    self.tap_put_sale_status(&sale_status);
    let rec = serde_json::json!({
      "id": Self::tap_token_sale_record_id("sref", inscription, action_index),
      "auth": auth,
      "cid": cid,
      "claim": claim,
      "pt": tick,
      "amt": amount.to_string(),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_set_list_record("srefl", "srefli", &rec);
    let _ = self.tap_set_list_record(
      &format!("srefa/{}", auth),
      &format!("srefai/{}", auth),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("srefaddr/{}", claim),
      &format!("srefaddri/{}", claim),
      &rec,
    );
    self.tap_apply_authority_claim_transfer_logs(
      &tick,
      &tick_key,
      auth,
      &claim,
      auth_balance - amount,
      receiver_after,
      amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sr",
      cid,
    );
    true
  }

  fn process_cancel_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(auth) = action.get("auth").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(config) = self.tap_get_authority_config(auth) else {
      return false;
    };
    let mut status = self.tap_get_sale_status(auth, &config);
    let cancel_enabled = config
      .s
      .as_ref()
      .and_then(|s| s.get("cx"))
      .and_then(|v| v.as_bool())
      .unwrap_or(false);
    if config.k != "sale"
      || config.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
      || !cancel_enabled
      || Self::token_sale_status_bool(&status, "fin")
      || Self::token_sale_status_bool(&status, "can")
    {
      return false;
    }
    Self::token_sale_status_set_bool(&mut status, "can", true);
    self.tap_put_sale_status(&status);
    let rec = serde_json::json!({
      "id": Self::tap_token_sale_record_id("scancel", inscription, action_index),
      "auth": auth,
      "addr": link.addr,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_set_list_record("scanl", "scanli", &rec);
    let _ = self.tap_set_list_record(
      &format!("scana/{}", auth),
      &format!("scanai/{}", auth),
      &rec,
    );
    true
  }

  fn sale_withdraw_allowed(
    config: &AuthorityConfigRecord,
    status: &serde_json::Value,
    block: u32,
  ) -> bool {
    if Self::token_sale_status_bool(status, "fin") || Self::token_sale_status_bool(status, "can") {
      return true;
    }
    let Some(s) = config.s.as_ref() else {
      return false;
    };
    let end_height = s
      .get("eh")
      .and_then(Self::js_parse_int)
      .unwrap_or(i128::MAX);
    let soft_cap = s
      .get("sc")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    i128::from(block) > end_height && Self::token_sale_status_i128(status, "tc") < soft_cap
  }

  fn sale_outstanding_allocation(status: &serde_json::Value) -> i128 {
    let allocated = Self::token_sale_status_i128(status, "alc");
    let claimed = Self::token_sale_status_i128(status, "clm");
    if allocated > claimed {
      allocated - claimed
    } else {
      0
    }
  }

  fn sale_withdrawal_reserve(
    config: &AuthorityConfigRecord,
    status: &serde_json::Value,
    tick: &str,
  ) -> i128 {
    if !Self::token_sale_status_bool(status, "fin") || config.st.as_deref() != Some(tick) {
      return 0;
    }
    Self::sale_outstanding_allocation(status)
  }

  fn process_withdraw_sale_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(auth) = action.get("auth").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(config) = self.tap_get_authority_config(auth) else {
      return false;
    };
    let Some(token) = action
      .get("tick")
      .and_then(|v| v.as_str())
      .and_then(|tick| self.token_proof_get_deploy(tick))
    else {
      return false;
    };
    let Some(target) = self.validate_token_sale_target(action) else {
      return false;
    };
    let status = self.tap_get_sale_status(auth, &config);
    if config.k != "sale"
      || config.ctl.get("auth").and_then(|v| v.as_str()) != Some(link.ins.as_str())
      || config.st.as_deref() != Some(token.tick.as_str())
      || !Self::sale_withdraw_allowed(&config, &status, block)
    {
      return false;
    }
    let Some(amount) = action
      .get("amt")
      .and_then(|v| self.token_proof_resolve_protocol_amount(v, &token.record))
    else {
      return false;
    };
    let auth_balance = self.tap_get_authority_balance(auth, &token.tick_key);
    let reserve = Self::sale_withdrawal_reserve(&config, &status, &token.tick);
    let Some(available) = auth_balance.checked_sub(reserve) else {
      return false;
    };
    if amount <= 0 || available < amount {
      return false;
    }
    if !self.tap_set_authority_balance(auth, &token.tick_key, auth_balance - amount) {
      return false;
    }
    let target_balance = if target.tt == "a" {
      match self.tap_credit_address_balance(&target.to, &token.tick_key, &token.tick, amount) {
        Some(v) => v,
        None => return false,
      }
    } else if target.tt == "h" {
      if !self.tap_add_authority_balance(&target.to, &token.tick_key, amount) {
        return false;
      }
      self.tap_get_authority_balance(&target.to, &token.tick_key)
    } else {
      0
    };
    let mut sale_status = self.tap_get_sale_status(auth, &config);
    let withdrawn_before = Self::token_sale_status_i128(&sale_status, "wdr");
    Self::token_sale_status_set_string(&mut sale_status, "wdr", withdrawn_before + amount);
    self.tap_put_sale_status(&sale_status);
    let rec = serde_json::json!({
      "id": Self::tap_token_sale_record_id("swdr", inscription, action_index),
      "auth": auth,
      "tick": token.tick,
      "amt": amount.to_string(),
      "tt": target.tt,
      "to": target.to,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let rec_id = rec
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let _ = self.tap_set_list_record("swdrl", "swdrli", &rec);
    let _ = self.tap_set_list_record(
      &format!("swdra/{}", auth),
      &format!("swdrai/{}", auth),
      &rec,
    );
    let log_target = SaleTarget {
      tt: rec
        .get("tt")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string(),
      to: rec
        .get("to")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string(),
    };
    self.tap_apply_authority_target_transfer_logs(
      rec.get("tick").and_then(|v| v.as_str()).unwrap_or(""),
      &token.tick_key,
      auth,
      &log_target,
      auth_balance - amount,
      target_balance,
      amount,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sw",
      &rec_id,
    );
    true
  }

  fn perp_groups_enabled(&self) -> bool {
    self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation)
  }

  fn tap_token_perp_group_id(inscription: &str, action_index: usize) -> String {
    format!("{}:{}", inscription, action_index)
  }

  fn tap_token_perp_position_id(inscription: &str, action_index: usize) -> String {
    format!("{}:{}", inscription, action_index)
  }

  fn parse_perp_uint(value: &serde_json::Value, allow_zero: bool) -> Option<BigInt> {
    Self::parse_amm_uint_value(value, allow_zero)
  }

  fn parse_perp_height(value: &serde_json::Value) -> Option<u32> {
    Self::parse_amm_height(value)
  }

  fn parse_perp_ratio(value: &serde_json::Value, allow_zero: bool) -> Option<(BigInt, BigInt)> {
    if let Some(raw) = value.as_str() {
      let parts = raw.split('/').collect::<Vec<_>>();
      if parts.len() == 1 {
        return Some((
          Self::parse_amm_uint_str(parts[0], allow_zero)?,
          BigInt::from(1),
        ));
      }
      if parts.len() == 2 {
        return Some((
          Self::parse_amm_uint_str(parts[0], allow_zero)?,
          Self::parse_amm_uint_str(parts[1], false)?,
        ));
      }
      return None;
    }
    let obj = value.as_object()?;
    Some((
      Self::parse_perp_uint(obj.get("n")?, allow_zero)?,
      Self::parse_perp_uint(obj.get("d")?, false)?,
    ))
  }

  fn serialize_perp_ratio(ratio: &(BigInt, BigInt)) -> String {
    if ratio.1 == BigInt::from(1) {
      ratio.0.to_string()
    } else {
      format!("{}/{}", ratio.0, ratio.1)
    }
  }

  fn compare_perp_ratio(left: &(BigInt, BigInt), right: &(BigInt, BigInt)) -> i8 {
    let lhs = &left.0 * &right.1;
    let rhs = &right.0 * &left.1;
    if lhs < rhs {
      -1
    } else if lhs > rhs {
      1
    } else {
      0
    }
  }

  fn token_perp_payload_hash(action: &serde_json::Value, omitted: &[&str]) -> Option<String> {
    let mut payload = action.clone();
    let obj = payload.as_object_mut()?;
    for key in omitted {
      obj.remove(*key);
    }
    Self::certified_control_hash(&payload)
  }

  fn token_perp_policy_message(
    policy_id: &str,
    seq: &str,
    payload_hash: &str,
  ) -> serde_json::Value {
    serde_json::json!(["tap-perp-policy-v1", "tap", policy_id, seq, payload_hash])
  }

  fn token_perp_certificate_message(
    policy: &serde_json::Value,
    purpose: &str,
    group: &str,
    payload_hash: &str,
    nonce: &str,
    valid_until: u32,
  ) -> Option<serde_json::Value> {
    Some(serde_json::json!([
      "tap-perp-price-v1",
      "tap",
      policy.get("id")?.as_str()?,
      policy.get("hash")?.as_str()?,
      purpose,
      group,
      payload_hash,
      nonce,
      valid_until
    ]))
  }

  fn token_perp_external_evidence_message(
    policy: &serde_json::Value,
    group: &serde_json::Value,
    purpose: &str,
    payload_hash: &str,
    nonce: &str,
    valid_until: u32,
  ) -> Option<serde_json::Value> {
    Some(serde_json::json!([
      "tap-perp-external-evidence-v1",
      "tap",
      policy.get("id")?.as_str()?,
      policy.get("hash")?.as_str()?,
      group.get("id")?.as_str()?,
      group.get("gh")?.as_str()?,
      purpose,
      payload_hash,
      nonce,
      valid_until
    ]))
  }

  fn normalize_perp_signers(signers: &serde_json::Value) -> Option<Vec<String>> {
    let arr = signers.as_array()?;
    let mut set = std::collections::BTreeSet::new();
    for signer in arr {
      let normalized = Self::token_proof_compressed_delegation_pubkey(signer.as_str()?)?;
      if !set.insert(normalized) {
        return None;
      }
    }
    if set.is_empty() || set.len() > 16 {
      return None;
    }
    Some(set.into_iter().collect())
  }

  fn valid_perp_signature_count(
    &self,
    sigs: &serde_json::Value,
    signers: &[String],
    msg_hash_hex: &str,
  ) -> usize {
    let Some(arr) = sigs.as_array() else {
      return 0;
    };
    let Ok(msg_hash_bytes) = hex::decode(msg_hash_hex) else {
      return 0;
    };
    let Ok(msg_hash) = <[u8; 32]>::try_from(msg_hash_bytes) else {
      return 0;
    };
    let signer_set = signers
      .iter()
      .cloned()
      .collect::<std::collections::HashSet<_>>();
    let mut valid = std::collections::BTreeSet::new();
    for entry in arr {
      let Some(entry_obj) = entry.as_object() else {
        return 0;
      };
      if entry_obj
        .get("hash")
        .and_then(|v| v.as_str())
        .map(|hash| hash.to_lowercase() != msg_hash_hex)
        .unwrap_or(true)
      {
        return 0;
      }
      let Some(declared) = entry_obj
        .get("signer")
        .and_then(|v| v.as_str())
        .and_then(Self::token_proof_compressed_delegation_pubkey)
      else {
        return 0;
      };
      if !signer_set.contains(&declared) {
        return 0;
      }
      let Some((ok, _, pubkey)) = self.verify_sig_obj_against_msg_with_hash(
        entry_obj.get("sig").unwrap_or(&serde_json::Value::Null),
        msg_hash_hex,
        &msg_hash,
      ) else {
        continue;
      };
      let Some(recovered) = Self::token_proof_compressed_delegation_pubkey(&pubkey) else {
        continue;
      };
      if ok && recovered == declared {
        valid.insert(declared);
      }
    }
    valid.len()
  }

  fn get_perp_policy(&mut self, policy_id: &str) -> Option<serde_json::Value> {
    if !Self::token_proof_safe_id(policy_id, 128) {
      return None;
    }
    self
      .tap_get::<serde_json::Value>(&format!("perp/p/{}", policy_id))
      .ok()
      .flatten()
  }

  fn get_perp_group(&mut self, group_id: &str) -> Option<serde_json::Value> {
    if group_id.is_empty() || group_id.len() > 180 {
      return None;
    }
    self
      .tap_get::<serde_json::Value>(&format!("perp/g/{}", group_id))
      .ok()
      .flatten()
  }

  fn get_perp_position(&mut self, position_id: &str) -> Option<serde_json::Value> {
    if position_id.is_empty() || position_id.len() > 180 {
      return None;
    }
    self
      .tap_get::<serde_json::Value>(&format!("perp/pos/{}", position_id))
      .ok()
      .flatten()
  }

  fn normalize_perp_external_asset(asset: &serde_json::Value) -> Option<PerpAsset> {
    let ns_raw = asset.get("ns")?.as_str()?;
    let cid_raw = asset.get("cid")?.as_str()?;
    let ak_raw = asset.get("ak")?.as_str()?;
    let aid_raw = asset.get("aid")?.as_str()?;
    let dec = Self::parse_perp_uint(asset.get("dec")?, true)?;
    if dec > BigInt::from(38)
      || !Self::token_proof_safe_id(ns_raw, 128)
      || !Self::token_proof_safe_id(cid_raw, 128)
      || !Self::token_proof_safe_id(ak_raw, 128)
      || !Self::token_proof_safe_id(aid_raw, 128)
    {
      return None;
    }
    let ns = ns_raw.to_lowercase();
    let cid = cid_raw.to_lowercase();
    let ak = ak_raw.to_lowercase();
    let aid = aid_raw.to_lowercase();
    let mut value = serde_json::json!({ "ty": "ext", "ns": ns, "cid": cid, "ak": ak, "aid": aid, "dec": dec.to_string() });
    if let Some(sym) = asset.get("sym").and_then(|v| v.as_str()) {
      if !Self::token_proof_safe_id(sym, 32) {
        return None;
      }
      value.as_object_mut()?.insert(
        "sym".to_string(),
        serde_json::Value::String(sym.to_string()),
      );
    }
    Some(PerpAsset {
      key: Self::perp_asset_key(&value)?,
      value,
    })
  }

  fn normalize_perp_asset(&mut self, asset: &serde_json::Value) -> Option<PerpAsset> {
    let ns = asset.get("ns")?.as_str()?.to_lowercase();
    if ns == "tap" {
      let token = self.token_proof_get_deploy(asset.get("tick")?.as_str()?)?;
      if let Some(dec) = asset.get("dec").and_then(|v| v.as_str()) {
        if dec != token.record.dec.to_string() {
          return None;
        }
      }
      let value = serde_json::json!({ "ty": "tap", "ns": "tap", "tick": token.tick, "tick_key": token.tick_key, "dec": token.record.dec.to_string() });
      return Some(PerpAsset {
        key: Self::perp_asset_key(&value)?,
        value,
      });
    }
    Self::normalize_perp_external_asset(asset)
  }

  fn perp_key_part(value: &str) -> String {
    hex::encode(value.as_bytes())
  }

  fn perp_asset_key(asset: &serde_json::Value) -> Option<String> {
    match asset.get("ty")?.as_str()? {
      "tap" => Some(format!(
        "tap:{}",
        Self::perp_key_part(asset.get("tick")?.as_str()?)
      )),
      "ext" => Some(format!(
        "ext:{}:{}:{}:{}",
        Self::perp_key_part(asset.get("ns")?.as_str()?),
        Self::perp_key_part(asset.get("cid")?.as_str()?),
        Self::perp_key_part(asset.get("ak")?.as_str()?),
        Self::perp_key_part(asset.get("aid")?.as_str()?)
      )),
      _ => None,
    }
  }

  fn normalize_perp_settlement_surface(surface: &serde_json::Value) -> Option<serde_json::Value> {
    let kind = surface.get("kind")?.as_str()?;
    let id = surface.get("id")?.as_str()?;
    if !Self::token_proof_safe_id(kind, 64) || !Self::token_proof_safe_id(id, 128) {
      return None;
    }
    Some(serde_json::json!({
      "kind": kind.to_lowercase(),
      "id": id.to_lowercase()
    }))
  }

  fn perp_settlement_surface_key(surface: &serde_json::Value) -> Option<String> {
    Some(format!(
      "{}:{}",
      Self::perp_key_part(surface.get("kind")?.as_str()?),
      Self::perp_key_part(surface.get("id")?.as_str()?)
    ))
  }

  fn normalize_perp_target(&self, value: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(raw) = value.as_str() {
      let address = Self::normalize_address(raw);
      if !self.is_valid_bitcoin_address(&address) {
        return None;
      }
      return Some(serde_json::json!({ "tt": "a", "to": address }));
    }
    let tt = value.get("tt")?.as_str()?.to_lowercase();
    if tt == "a" {
      let address = Self::normalize_address(value.get("to")?.as_str()?);
      if !self.is_valid_bitcoin_address(&address) {
        return None;
      }
      return Some(serde_json::json!({ "tt": "a", "to": address }));
    }
    if tt == "h" {
      let to = value.get("to")?.as_str()?;
      if !Self::token_proof_safe_id(to, 160) {
        return None;
      }
      return Some(serde_json::json!({ "tt": "h", "to": to }));
    }
    None
  }

  fn normalize_perp_fee_role(role: &str) -> Option<String> {
    let normalized = role.to_lowercase();
    if normalized.is_empty()
      || normalized.len() > 16
      || !normalized
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
    {
      return None;
    }
    Some(normalized)
  }

  fn perp_fee_receiver_key(receiver: &serde_json::Value) -> Option<String> {
    Some(format!(
      "{}:{}:{}",
      Self::perp_key_part(receiver.get("tt")?.as_str()?),
      Self::perp_key_part(receiver.get("to")?.as_str()?),
      Self::perp_key_part(receiver.get("rl")?.as_str()?)
    ))
  }

  fn validate_perp_fee_receiver(
    &mut self,
    entry: &serde_json::Value,
  ) -> Option<serde_json::Value> {
    if !entry.is_object() {
      return None;
    }
    let target = self.normalize_perp_target(entry)?;
    let share = Self::parse_perp_uint(entry.get("share")?, false)?;
    if share > BigInt::from(10_000) {
      return None;
    }
    let tt = target.get("tt")?.as_str()?;
    let role_raw = entry
      .get("rl")
      .and_then(|v| v.as_str())
      .unwrap_or(if tt == "a" { "pf" } else { "" });
    let role = Self::normalize_perp_fee_role(role_raw)?;
    if tt == "a" && role != "pf" && role != "of" {
      return None;
    }
    if tt == "h" {
      if role != "sr" {
        return None;
      }
      let auth_id = target.get("to")?.as_str()?;
      let auth = self.tap_get_authority_config(auth_id)?;
      if auth.k != "stk" {
        return None;
      }
    }
    Some(serde_json::json!({
      "tt": tt,
      "to": target.get("to")?.as_str()?,
      "share": share.to_string(),
      "rl": role
    }))
  }

  fn validate_perp_fee_receivers(
    &mut self,
    receivers: &serde_json::Value,
  ) -> Option<Vec<serde_json::Value>> {
    let list = receivers.as_array()?;
    if list.is_empty() || list.len() > 16 {
      return None;
    }
    let mut out = Vec::new();
    let mut seen_roles = std::collections::HashSet::new();
    let mut seen_receivers = std::collections::HashSet::new();
    let mut total = BigInt::from(0);
    for entry in list {
      let receiver = self.validate_perp_fee_receiver(entry)?;
      let role = receiver.get("rl")?.as_str()?.to_string();
      if !seen_roles.insert(role) {
        return None;
      }
      let receiver_key = Self::perp_fee_receiver_key(&receiver)?;
      if !seen_receivers.insert(receiver_key) {
        return None;
      }
      let share = receiver.get("share")?.as_str()?.parse::<BigInt>().ok()?;
      total += share;
      if total > BigInt::from(10_000) {
        return None;
      }
      out.push(receiver);
    }
    if total == BigInt::from(10_000) {
      Some(out)
    } else {
      None
    }
  }

  fn perp_fee_receivers_equal(
    left: &[serde_json::Value],
    right: &serde_json::Value,
  ) -> bool {
    let Some(right_list) = right.as_array() else {
      return false;
    };
    if left.len() != right_list.len() {
      return false;
    }
    for (a, b) in left.iter().zip(right_list.iter()) {
      for field in ["tt", "to", "rl", "share"] {
        if a.get(field).and_then(|v| v.as_str()) != b.get(field).and_then(|v| v.as_str()) {
          return false;
        }
      }
    }
    true
  }

  fn validate_perp_group_fee_receivers(&mut self, group: &serde_json::Value) -> bool {
    let Some(receivers) = group
      .get("fee")
      .and_then(|v| v.get("receivers"))
      .and_then(|v| v.as_array())
    else {
      return false;
    };
    for receiver in receivers {
      if receiver.get("tt").and_then(|v| v.as_str()) != Some("h") {
        continue;
      }
      if receiver.get("rl").and_then(|v| v.as_str()) != Some("sr")
        || group
          .get("collateral")
          .and_then(|v| v.get("ty"))
          .and_then(|v| v.as_str())
          != Some("tap")
      {
        return false;
      }
      let Some(auth_id) = receiver.get("to").and_then(|v| v.as_str()) else {
        return false;
      };
      let Some(auth) = self.tap_get_authority_config(auth_id) else {
        return false;
      };
      if auth.k != "stk" {
        return false;
      }
      let Some(tick_key) = group
        .get("collateral")
        .and_then(|v| v.get("tick_key"))
        .and_then(|v| v.as_str())
      else {
        return false;
      };
      let reward_ticks = Self::authority_config_reward_ticks(&auth);
      if !reward_ticks.is_empty() {
        let reward_ticks: std::collections::HashSet<String> = reward_ticks
          .into_iter()
          .map(|tick| Self::json_stringify_lower(&tick))
          .collect();
        if !reward_ticks.contains(tick_key) {
          return false;
        }
      }
      let shares = self.tap_get_authority_total_shares(auth_id);
      let empty_policy_accepts = auth
        .r
        .get("ep")
        .and_then(|v| v.as_str())
        .map(|ep| ep == "hold" || ep == "carry")
        .unwrap_or(false);
      if shares == BigInt::from(0) && !empty_policy_accepts {
        return false;
      }
    }
    true
  }

  fn split_perp_fee_receivers(
    amount: &BigInt,
    receivers: &[serde_json::Value],
  ) -> Vec<(serde_json::Value, BigInt)> {
    let mut rows: Vec<(serde_json::Value, BigInt, BigInt, usize)> = receivers
      .iter()
      .enumerate()
      .map(|(index, receiver)| {
        let share = receiver
          .get("share")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_else(|| BigInt::from(0));
        let numerator = amount * &share;
        (
          receiver.clone(),
          &numerator / BigInt::from(10_000),
          numerator % BigInt::from(10_000),
          index,
        )
      })
      .collect();
    let assigned = rows
      .iter()
      .fold(BigInt::from(0), |sum, (_, fee, _, _)| sum + fee);
    let mut dust = amount - assigned;
    if dust > BigInt::from(0) {
      let mut order: Vec<usize> = (0..rows.len()).collect();
      order.sort_by(|left, right| {
        rows[*right]
          .2
          .cmp(&rows[*left].2)
          .then_with(|| rows[*left].3.cmp(&rows[*right].3))
      });
      for index in order {
        if dust <= BigInt::from(0) {
          break;
        }
        rows[index].1 += BigInt::from(1);
        dust -= BigInt::from(1);
      }
    }
    rows.into_iter()
      .map(|(receiver, fee, _, _)| (receiver, fee))
      .collect()
  }

  fn normalize_perp_price(price: &serde_json::Value) -> Option<serde_json::Value> {
    let p = Self::parse_perp_uint(price.get("p")?, false)?;
    let q = Self::parse_perp_uint(price.get("q")?, false)?;
    let seq = match price.get("seq") {
      Some(value) => Self::parse_perp_uint(value, false)?,
      None => BigInt::from(1),
    };
    let mut out = serde_json::Map::new();
    out.insert("p".to_string(), serde_json::Value::String(p.to_string()));
    out.insert("q".to_string(), serde_json::Value::String(q.to_string()));
    out.insert(
      "seq".to_string(),
      serde_json::Value::String(seq.to_string()),
    );
    if let Some(ts) = price.get("ts") {
      out.insert(
        "ts".to_string(),
        serde_json::Value::String(Self::parse_perp_uint(ts, true)?.to_string()),
      );
    }
    Some(serde_json::Value::Object(out))
  }

  fn normalize_perp_price_ratio(value: &serde_json::Value) -> Option<serde_json::Value> {
    let p = Self::parse_perp_uint(value.get("p")?, false)?;
    let q = Self::parse_perp_uint(value.get("q")?, false)?;
    Some(serde_json::json!({ "p": p.to_string(), "q": q.to_string() }))
  }

  fn compare_perp_price_ratio(left: &serde_json::Value, right: &serde_json::Value) -> Option<i8> {
    let left_p = left.get("p")?.as_str()?.parse::<BigInt>().ok()?;
    let left_q = left.get("q")?.as_str()?.parse::<BigInt>().ok()?;
    let right_p = right.get("p")?.as_str()?.parse::<BigInt>().ok()?;
    let right_q = right.get("q")?.as_str()?.parse::<BigInt>().ok()?;
    let lhs = left_p * right_q;
    let rhs = right_p * left_q;
    Some(if lhs < rhs {
      -1
    } else if lhs > rhs {
      1
    } else {
      0
    })
  }

  fn normalize_perp_entry_policy(entry: &serde_json::Value) -> Option<serde_json::Value> {
    let mode = entry.get("mode")?.as_str()?;
    if mode != "one-sided-v1" && mode != "two-sided-v1" {
      return None;
    }
    let required = entry.get("required")?.as_bool()?;
    let allow_unbounded = entry.get("allow_unbounded")?.as_bool()?;
    let max_slippage_bps = match entry.get("max_slippage_bps") {
      Some(value) => Self::parse_perp_uint(value, true)?,
      None => BigInt::from(0),
    };
    if max_slippage_bps > BigInt::from(10_000) || (required && allow_unbounded) {
      return None;
    }
    Some(serde_json::json!({
      "mode": mode,
      "required": required,
      "allow_unbounded": allow_unbounded,
      "max_slippage_bps": max_slippage_bps.to_string()
    }))
  }

  fn perp_entry_policies_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    left.get("mode").and_then(|v| v.as_str()) == right.get("mode").and_then(|v| v.as_str())
      && left.get("required").and_then(|v| v.as_bool()) == right.get("required").and_then(|v| v.as_bool())
      && left.get("allow_unbounded").and_then(|v| v.as_bool()) == right.get("allow_unbounded").and_then(|v| v.as_bool())
      && left.get("max_slippage_bps").and_then(|v| v.as_str()) == right.get("max_slippage_bps").and_then(|v| v.as_str())
  }

  fn normalize_perp_entry_bound(
    entry: &serde_json::Value,
    side: &str,
    policy: &serde_json::Value,
  ) -> Option<serde_json::Value> {
    let min = match entry.get("min") {
      Some(value) => Some(Self::normalize_perp_price_ratio(value)?),
      None => None,
    };
    let max = match entry.get("max") {
      Some(value) => Some(Self::normalize_perp_price_ratio(value)?),
      None => None,
    };
    if min.is_none() && max.is_none() {
      return None;
    }
    if let (Some(min), Some(max)) = (&min, &max) {
      if Self::compare_perp_price_ratio(min, max)? > 0 {
        return None;
      }
    }
    if policy.get("mode").and_then(|v| v.as_str()) == Some("one-sided-v1") {
      if side == "long" && (max.is_none() || min.is_some()) {
        return None;
      }
      if side == "short" && (min.is_none() || max.is_some()) {
        return None;
      }
    }
    let mut out = serde_json::Map::new();
    if let Some(min) = min {
      out.insert("min".to_string(), min);
    }
    if let Some(max) = max {
      out.insert("max".to_string(), max);
    }
    Some(serde_json::Value::Object(out))
  }

  fn resolve_perp_entry_bound(
    entry: Option<&serde_json::Value>,
    side: &str,
    policy: &serde_json::Value,
  ) -> Option<Option<serde_json::Value>> {
    match entry {
      Some(value) if !value.is_null() => Some(Some(Self::normalize_perp_entry_bound(value, side, policy)?)),
      _ => {
        let required = policy.get("required").and_then(|v| v.as_bool()).unwrap_or(false);
        let allow_unbounded = policy.get("allow_unbounded").and_then(|v| v.as_bool()).unwrap_or(false);
        if required || !allow_unbounded {
          None
        } else {
          Some(None)
        }
      }
    }
  }

  fn update_perp_group_entry_bounds(group: &mut serde_json::Value, entry: Option<&serde_json::Value>) -> Option<()> {
    let entry = entry?;
    let obj = group.as_object_mut()?;
    if !obj.get("entry_bounds").map(|v| v.is_object()).unwrap_or(false) {
      obj.insert("entry_bounds".to_string(), serde_json::json!({}));
    }
    let bounds = obj.get_mut("entry_bounds")?.as_object_mut()?;
    if let Some(max) = entry.get("max") {
      let replace = match bounds.get("long_max") {
        Some(current) => Self::compare_perp_price_ratio(max, current)? < 0,
        None => true,
      };
      if replace {
        bounds.insert("long_max".to_string(), max.clone());
      }
    }
    if let Some(min) = entry.get("min") {
      let replace = match bounds.get("short_min") {
        Some(current) => Self::compare_perp_price_ratio(min, current)? > 0,
        None => true,
      };
      if replace {
        bounds.insert("short_min".to_string(), min.clone());
      }
    }
    Some(())
  }

  fn perp_entry_bounds_allow_price(group: &serde_json::Value, price: &serde_json::Value) -> bool {
    let Some(bounds) = group.get("entry_bounds").and_then(|v| v.as_object()) else {
      return true;
    };
    if let Some(max) = bounds.get("long_max") {
      if Self::compare_perp_price_ratio(price, max).map(|cmp| cmp > 0).unwrap_or(true) {
        return false;
      }
    }
    if let Some(min) = bounds.get("short_min") {
      if Self::compare_perp_price_ratio(price, min).map(|cmp| cmp < 0).unwrap_or(true) {
        return false;
      }
    }
    true
  }

  fn normalize_perp_policy_asset_set(assets: &serde_json::Value) -> Option<serde_json::Value> {
    let obj = assets.as_object()?;
    let mut out = serde_json::Map::new();
    let tap = obj.get("tap")?.as_object()?;
    match tap.get("mode")?.as_str()? {
      "wildcard" => {
        out.insert(
          "tap".to_string(),
          serde_json::Value::String("*".to_string()),
        );
      }
      "list" | "wildcard-or-list" => {
        let arr = tap.get("ticks")?.as_array()?;
        let mut set = std::collections::BTreeSet::new();
        for item in arr {
          set.insert(Self::js_to_lowercase(item.as_str()?));
        }
        if set.len() != arr.len() {
          return None;
        }
        if tap.get("mode")?.as_str()? == "wildcard-or-list" && set.is_empty() {
          out.insert(
            "tap".to_string(),
            serde_json::Value::String("*".to_string()),
          );
        } else {
          out.insert(
            "tap".to_string(),
            serde_json::Value::Array(set.into_iter().map(serde_json::Value::String).collect()),
          );
        }
      }
      _ => return None,
    }
    if let Some(ext) = obj.get("external") {
      let ext_obj = ext.as_object()?;
      match ext_obj.get("mode")?.as_str()? {
        "wildcard" => {
          out.insert(
            "ext".to_string(),
            serde_json::Value::String("*".to_string()),
          );
        }
        "list" | "wildcard-or-list" => {
          let arr = ext_obj.get("refs")?.as_array()?;
          let mut seen = std::collections::BTreeSet::new();
          let mut refs = Vec::new();
          for item in arr {
            let normalized = Self::normalize_perp_external_asset(item)?;
            if !seen.insert(normalized.key.clone()) {
              return None;
            }
            refs.push((normalized.key, normalized.value));
          }
          refs.sort_by(|a, b| a.0.cmp(&b.0));
          if ext_obj.get("mode")?.as_str()? == "wildcard-or-list" && refs.is_empty() {
            out.insert(
              "ext".to_string(),
              serde_json::Value::String("*".to_string()),
            );
          } else {
            out.insert(
              "ext".to_string(),
              serde_json::Value::Array(refs.into_iter().map(|(_, value)| value).collect()),
            );
          }
        }
        _ => return None,
      }
    }
    if let Some(pairs) = obj.get("pairs") {
      out.insert("pairs".to_string(), pairs.clone());
    }
    if out.is_empty() {
      None
    } else {
      Some(serde_json::Value::Object(out))
    }
  }

  fn perp_policy_allows_asset(policy: &serde_json::Value, asset: &serde_json::Value) -> bool {
    let Some(ty) = asset.get("ty").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(assets) = policy.get("assets") else {
      return false;
    };
    if ty == "tap" {
      if assets.get("tap").and_then(|v| v.as_str()) == Some("*") {
        return true;
      }
      let Some(tick) = asset.get("tick").and_then(|v| v.as_str()) else {
        return false;
      };
      return assets
        .get("tap")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().any(|item| item.as_str() == Some(tick)))
        .unwrap_or(false);
    }
    if ty == "ext" {
      if assets.get("ext").and_then(|v| v.as_str()) == Some("*") {
        return true;
      }
      let Some(key) = Self::perp_asset_key(asset) else {
        return false;
      };
      return assets
        .get("ext")
        .and_then(|v| v.as_array())
        .map(|arr| {
          arr
            .iter()
            .filter_map(Self::perp_asset_key)
            .any(|allowed| allowed == key)
        })
        .unwrap_or(false);
    }
    false
  }

  fn validate_perp_policy_action(
    &mut self,
    action: &serde_json::Value,
    block: u32,
  ) -> Option<serde_json::Value> {
    if !self.perp_groups_enabled()
      || action.get("op")?.as_str()?.to_lowercase() != "perp-policy"
      || !Self::token_proof_safe_id(action.get("id")?.as_str()?, 128)
      || action.get("dom")?.as_str()? != "tap-perp-policy-v1"
      || !Self::token_proof_safe_id(action.get("net")?.as_str()?, 64)
      || !action.get("limits")?.is_object()
      || !action.get("oracle")?.is_object()
      || !action.get("liq")?.is_object()
      || !action.get("def")?.is_object()
      || !action.get("entry")?.is_object()
      || !action.get("fee")?.is_object()
      || !action.get("bounty")?.is_object()
      || !action.get("sigs")?.is_array()
    {
      return None;
    }
    let version = Self::parse_perp_uint(action.get("v")?, false)?;
    let seq = Self::parse_perp_uint(action.get("seq")?, false)?;
    let signers = Self::normalize_perp_signers(action.get("signers")?)?;
    let threshold_value = Self::parse_perp_uint(action.get("thr")?, false)?;
    let threshold = threshold_value.to_string().parse::<usize>().ok()?;
    let assets = Self::normalize_perp_policy_asset_set(action.get("assets")?)?;
    if version != BigInt::from(1) || threshold == 0 || threshold > signers.len() || threshold > 16 {
      return None;
    }
    let limits = action.get("limits")?;
    let max_leverage = Self::parse_perp_ratio(limits.get("max_lev")?, false)?;
    let entry_policy = Self::normalize_perp_entry_policy(action.get("entry")?)?;
    let min_ratio = Self::parse_perp_ratio(limits.get("min_ratio")?, true)?;
    let max_ratio = Self::parse_perp_ratio(limits.get("max_ratio")?, false)?;
    let min_collateral = Self::parse_perp_uint(limits.get("min_coll")?, false)?;
    let max_notional = Self::parse_perp_uint(limits.get("max_not")?, false)?;
    let min_duration = Self::parse_perp_uint(limits.get("min_dur")?, false)?;
    let max_duration = Self::parse_perp_uint(limits.get("max_dur")?, false)?;
    let min_formation = Self::parse_perp_uint(limits.get("min_form")?, false)?;
    let max_formation = Self::parse_perp_uint(limits.get("max_form")?, false)?;
    let maintenance_ratio = Self::parse_perp_ratio(action.get("liq")?.get("min_mmr")?, true)?;
    let maintenance_bps = &maintenance_ratio.0 * BigInt::from(10_000) / &maintenance_ratio.1;
    if Self::compare_perp_ratio(&min_ratio, &max_ratio) > 0
      || min_duration > max_duration
      || min_formation > max_formation
      || maintenance_bps > BigInt::from(10_000)
    {
      return None;
    }
    let max_age = Self::parse_perp_uint(action.get("oracle")?.get("max_age")?, false)?;
    if action.get("oracle")?.get("rules")?.as_array()?.is_empty() {
      return None;
    }
    let fee = action.get("fee")?;
    let fee_bps = Self::parse_perp_uint(fee.get("max_bps")?, true)?;
    let fee_receivers = self.validate_perp_fee_receivers(fee.get("receivers")?)?;
    if !fee
      .get("rules")?
      .as_array()?
      .iter()
      .any(|rule| rule.as_str() == Some("settlement-positive-payout-bps-v1"))
      || fee_bps > BigInt::from(10_000)
    {
      return None;
    }
    let bounty_rules = action.get("bounty")?.get("rules")?;
    let bounty_activate = Self::parse_perp_uint(bounty_rules.get("activate")?.get("cap")?, true)?;
    let bounty_liquidate = Self::parse_perp_uint(bounty_rules.get("liquidate")?.get("cap")?, true)?;
    let bounty_settle = Self::parse_perp_uint(bounty_rules.get("settle")?.get("cap")?, true)?;
    if !action
      .get("def")?
      .get("rules")?
      .as_array()?
      .iter()
      .any(|rule| rule.as_str() == Some("pro-rata-positive-equity-v1"))
      || action.get("def")?.get("dust")?.as_str()? != "largest-remainder-v1"
      || !action
        .get("oracle")?
        .get("fallbacks")?
        .as_array()?
        .iter()
        .any(|fallback| fallback.as_str() == Some("last-valid-at-expiry-v1"))
    {
      return None;
    }
    let expires = Self::parse_perp_height(action.get("exp")?)?;
    if block > expires {
      return None;
    }
    let payload_hash = Self::token_perp_payload_hash(action, &["hash", "sigs"])?;
    if action
      .get("hash")
      .and_then(|v| v.as_str())
      .map(|hash| hash.to_lowercase() != payload_hash)
      .unwrap_or(false)
    {
      return None;
    }
    let msg =
      Self::token_perp_policy_message(action.get("id")?.as_str()?, &seq.to_string(), &payload_hash);
    let msg_hash = Self::certified_control_hash(&msg)?;
    if self.valid_perp_signature_count(action.get("sigs")?, &signers, &msg_hash) < threshold {
      return None;
    }
    if let Some(previous) = self.get_perp_policy(action.get("id")?.as_str()?) {
      let previous_seq = previous.get("seq")?.as_str()?.parse::<BigInt>().ok()?;
      let previous_signers = previous
        .get("signers")?
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>()?;
      let previous_threshold = previous.get("threshold")?.as_u64()? as usize;
      if seq <= previous_seq
        || self.valid_perp_signature_count(action.get("sigs")?, &previous_signers, &msg_hash)
          < previous_threshold
      {
        return None;
      }
    }
    Some(serde_json::json!({
      "id": action.get("id")?.as_str()?,
      "v": 1,
      "dom": action.get("dom")?.as_str()?,
      "net": action.get("net")?.as_str()?,
      "seq": seq.to_string(),
      "signers": signers,
      "threshold": threshold,
      "assets": assets,
      "limits": {
        "max_leverage": Self::serialize_perp_ratio(&max_leverage),
        "min_collateral": min_collateral.to_string(),
        "max_notional": max_notional.to_string(),
        "min_duration": min_duration.to_string(),
        "max_duration": max_duration.to_string(),
        "min_formation": min_formation.to_string(),
        "max_formation": max_formation.to_string(),
        "min_ratio": Self::serialize_perp_ratio(&min_ratio),
        "max_ratio": Self::serialize_perp_ratio(&max_ratio),
        "maintenance_bps": maintenance_bps.to_string()
      },
      "oracle": { "rules": action.get("oracle")?.get("rules")?.clone(), "signers": signers, "threshold": threshold, "max_age": max_age.to_string() },
      "entry": entry_policy,
      "fee": {
        "mode": "settlement-positive-payout-bps-v1",
        "bps": fee_bps.to_string(),
        "receiver": fee_receivers.iter().find(|receiver| receiver.get("tt").and_then(|v| v.as_str()) == Some("a")).unwrap_or(&fee_receivers[0]).clone(),
        "receivers": fee_receivers
      },
      "bounty": { "activate": bounty_activate.to_string(), "liquidate": bounty_liquidate.to_string(), "settle": bounty_settle.to_string() },
      "fallback": { "type": "last-valid-at-expiry-v1" },
      "expires": expires,
      "hash": payload_hash
    }))
  }

  fn process_perp_policy_action(
    &mut self,
    action: &serde_json::Value,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(policy) = self.validate_perp_policy_action(action, block) else {
      return false;
    };
    let id = policy
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let mut record = policy.clone();
    if let Some(map) = record.as_object_mut() {
      map.insert("blck".to_string(), serde_json::json!(block));
      map.insert("tx".to_string(), serde_json::json!(transaction));
      map.insert("vo".to_string(), serde_json::json!(vout));
      map.insert("val".to_string(), serde_json::json!(value.to_string()));
      map.insert("ins".to_string(), serde_json::json!(inscription));
      map.insert("num".to_string(), serde_json::json!(number));
      map.insert("ts".to_string(), serde_json::json!(timestamp));
    }
    let _ = self.tap_put(&format!("perp/p/{}", id), &policy);
    if let Ok(list_len) = self.tap_set_list_record("perp/pl", "perp/pli", &record) {
      let ptr = format!("perp/pli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/perp/policy/{}", transaction),
        &format!("txi/perp/policy/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/perp/policy/{}", block),
        &format!("blcki/perp/policy/{}", block),
        &ptr,
      );
    }
    self.record_perp_event(
      "policy",
      &id,
      &policy,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn validate_perp_open_group_action(
    &mut self,
    action: &serde_json::Value,
    inscription: &str,
    action_index: usize,
    block: u32,
  ) -> Option<serde_json::Value> {
    if !self.perp_groups_enabled()
      || action.get("op")?.as_str()?.to_lowercase() != "perp-open-group"
      || !action.get("pair")?.is_object()
      || !action.get("coll")?.is_object()
      || !action.get("form")?.is_object()
      || !action.get("ready")?.is_object()
      || !action.get("lev")?.is_object()
      || !action.get("liq")?.is_object()
      || !action.get("entry")?.is_object()
      || !action.get("settle")?.is_object()
      || !action.get("fee")?.is_object()
    {
      return None;
    }
    let policy = self.get_perp_policy(action.get("pid")?.as_str()?)?;
    let base = self.normalize_perp_asset(action.get("pair")?.get("base")?)?;
    let quote = self.normalize_perp_asset(action.get("pair")?.get("quote")?)?;
    let collateral = self.normalize_perp_asset(action.get("coll")?.get("asset")?)?;
    let collateral_mode = action
      .get("coll")?
      .get("mode")
      .and_then(|v| v.as_str())
      .map(|s| s.to_lowercase());
    let settlement_surface = match action.get("coll")?.get("surface") {
      Some(surface) => Some(Self::normalize_perp_settlement_surface(surface)?),
      None => None,
    };
    let tap_collateral = collateral.value.get("ty").and_then(|v| v.as_str()) == Some("tap")
      && collateral_mode.as_deref() == Some("tap-account")
      && settlement_surface.is_none();
    let external_collateral = collateral.value.get("ty").and_then(|v| v.as_str()) == Some("ext")
      && matches!(
        collateral_mode.as_deref(),
        Some("evm-perp-escrow" | "bsc-perp-escrow" | "solana-perp-program")
      )
      && settlement_surface.is_some();
    if policy.get("hash")?.as_str()? != action.get("ph")?.as_str()?
      || (!tap_collateral && !external_collateral)
      || !Self::perp_policy_allows_asset(&policy, &base.value)
      || !Self::perp_policy_allows_asset(&policy, &quote.value)
      || !Self::perp_policy_allows_asset(&policy, &collateral.value)
    {
      return None;
    }
    let start = Self::parse_perp_height(action.get("form")?.get("start")?)?;
    let deadline = Self::parse_perp_height(action.get("form")?.get("deadline")?)?;
    let expiry = Self::parse_perp_height(action.get("settle")?.get("expiry")?)?;
    if start > deadline || deadline >= expiry || block > deadline {
      return None;
    }
    let formation = BigInt::from(deadline - start);
    let duration = BigInt::from(expiry - deadline);
    let limits = policy.get("limits")?;
    if formation
      < limits
        .get("min_formation")?
        .as_str()?
        .parse::<BigInt>()
        .ok()?
      || formation
        > limits
          .get("max_formation")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
      || duration
        < limits
          .get("min_duration")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
      || duration
        > limits
          .get("max_duration")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
    {
      return None;
    }
    let readiness = action.get("ready")?;
    let min_long = Self::parse_perp_uint(readiness.get("min_long_coll")?, true)?;
    let min_short = Self::parse_perp_uint(readiness.get("min_short_coll")?, true)?;
    let min_total = Self::parse_perp_uint(readiness.get("min_total_coll")?, false)?;
    let min_long_notional = Self::parse_perp_uint(readiness.get("min_long_not")?, true)?;
    let min_short_notional = Self::parse_perp_uint(readiness.get("min_short_not")?, true)?;
    let ratio_min = Self::parse_perp_ratio(readiness.get("ratio_min")?, true)?;
    let ratio_max = Self::parse_perp_ratio(readiness.get("ratio_max")?, false)?;
    let max_imbalance_notional = Self::parse_perp_uint(readiness.get("max_imbalance_not")?, true)?;
    let min_leverage = Self::parse_perp_ratio(action.get("lev")?.get("min")?, false)?;
    let max_leverage = Self::parse_perp_ratio(action.get("lev")?.get("max")?, false)?;
    let policy_max_leverage = Self::parse_perp_ratio(limits.get("max_leverage")?, false)?;
    let policy_min_ratio = Self::parse_perp_ratio(limits.get("min_ratio")?, true)?;
    let policy_max_ratio = Self::parse_perp_ratio(limits.get("max_ratio")?, false)?;
    let maintenance_ratio = Self::parse_perp_ratio(action.get("liq")?.get("mmr")?, true)?;
    let maintenance_bps = &maintenance_ratio.0 * BigInt::from(10_000) / &maintenance_ratio.1;
    let entry_policy = Self::normalize_perp_entry_policy(action.get("entry")?)?;
    if Self::compare_perp_ratio(&min_leverage, &max_leverage) > 0
      || Self::compare_perp_ratio(&min_leverage, &max_leverage) != 0
      || Self::compare_perp_ratio(&ratio_min, &ratio_max) > 0
      || Self::compare_perp_ratio(&ratio_min, &policy_min_ratio) < 0
      || Self::compare_perp_ratio(&ratio_max, &policy_max_ratio) > 0
      || Self::compare_perp_ratio(&max_leverage, &policy_max_leverage) > 0
      || max_imbalance_notional
        > limits
          .get("max_notional")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
      || maintenance_bps
        > limits
          .get("maintenance_bps")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
      || !Self::perp_entry_policies_equal(&entry_policy, policy.get("entry")?)
    {
      return None;
    }
    let fee = action.get("fee")?;
    let fee_bps = Self::parse_perp_uint(fee.get("bps")?, true)?;
    let fee_receivers = self.validate_perp_fee_receivers(fee.get("recv")?)?;
    let bounty = Self::resolve_perp_group_bounty(&policy, action.get("bounty"))?;
    if fee.get("rule")?.as_str()? != policy.get("fee")?.get("mode")?.as_str()?
      || fee_bps
        != policy
          .get("fee")?
          .get("bps")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
      || !Self::perp_fee_receivers_equal(
        &fee_receivers,
        policy.get("fee")?.get("receivers")?
      )
    {
      return None;
    }
    let id = Self::tap_token_perp_group_id(inscription, action_index);
    if self.get_perp_group(&id).is_some() {
      return None;
    }
    let group_payload_hash = Self::token_perp_payload_hash(action, &["hash"])?;
    if action
      .get("hash")
      .and_then(|v| v.as_str())
      .map(|hash| hash.to_lowercase() != group_payload_hash)
      .unwrap_or(false)
    {
      return None;
    }
    let group = serde_json::json!({
      "id": id,
      "policy": policy.get("id")?.as_str()?,
      "ph": policy.get("hash")?.as_str()?,
      "gh": group_payload_hash,
      "state": "formation",
      "pair": { "base": base.value, "quote": quote.value },
      "collateral": collateral.value,
      "collateral_mode": collateral_mode?,
      "settlement_surface": settlement_surface,
      "start": start,
      "deadline": deadline,
      "expiry": expiry,
      "readiness": {
        "min_long": min_long.to_string(),
        "min_short": min_short.to_string(),
        "min_total": min_total.to_string(),
        "min_long_notional": min_long_notional.to_string(),
        "min_short_notional": min_short_notional.to_string(),
        "ratio_min": Self::serialize_perp_ratio(&ratio_min),
        "ratio_max": Self::serialize_perp_ratio(&ratio_max),
        "max_imbalance_notional": max_imbalance_notional.to_string()
      },
      "leverage": { "min": Self::serialize_perp_ratio(&min_leverage), "max": Self::serialize_perp_ratio(&max_leverage), "value": Self::serialize_perp_ratio(&min_leverage) },
      "entry_policy": entry_policy,
      "entry_bounds": {},
      "maintenance_bps": maintenance_bps.to_string(),
      "fee": {
        "mode": policy.get("fee")?.get("mode")?.as_str()?,
        "bps": fee_bps.to_string(),
        "receiver": fee_receivers.iter().find(|receiver| receiver.get("tt").and_then(|v| v.as_str()) == Some("a")).unwrap_or(&fee_receivers[0]).clone(),
        "receivers": fee_receivers
      },
      "bounty": bounty,
      "fallback": policy.get("fallback")?.clone(),
      "long_collateral": "0",
      "short_collateral": "0",
      "total_collateral": "0",
      "long_open_collateral": "0",
      "short_open_collateral": "0",
      "total_open_collateral": "0",
      "closed_equity_total": "0",
      "liquidated_equity_total": "0",
      "claimed_total": "0",
      "long_notional": "0",
      "short_notional": "0",
      "total_notional": "0",
      "bounty_reserve": "0",
      "bounty_paid": { "activate": "0", "liquidate": "0", "settle": "0" },
      "positions": "0",
      "entry_price": serde_json::Value::Null,
      "final_price": serde_json::Value::Null
    });
    if self.validate_perp_group_fee_receivers(&group) {
      Some(group)
    } else {
      None
    }
  }

  fn process_perp_open_group_action(
    &mut self,
    action: &serde_json::Value,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(group) =
      self.validate_perp_open_group_action(action, inscription, action_index, block)
    else {
      return false;
    };
    let id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let state = group
      .get("state")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let policy = group
      .get("policy")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let mut record = group.clone();
    if let Some(map) = record.as_object_mut() {
      map.insert("blck".to_string(), serde_json::json!(block));
      map.insert("tx".to_string(), serde_json::json!(transaction));
      map.insert("vo".to_string(), serde_json::json!(vout));
      map.insert("val".to_string(), serde_json::json!(value.to_string()));
      map.insert("ins".to_string(), serde_json::json!(inscription));
      map.insert("num".to_string(), serde_json::json!(number));
      map.insert("ts".to_string(), serde_json::json!(timestamp));
    }
    let _ = self.tap_put(&format!("perp/g/{}", id), &group);
    if let Ok(list_len) = self.tap_set_list_record("perp/gl", "perp/gli", &record) {
      let ptr = format!("perp/gli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("perp/gs/{}", state),
        &format!("perp/gsi/{}", state),
        &id,
      );
      let _ = self.tap_set_list_record(
        &format!("perp/gpol/{}", policy),
        &format!("perp/gpoli/{}", policy),
        &id,
      );
      if let Some(pair_key) = Self::perp_group_pair_key(&group) {
        let _ = self.tap_set_list_record(
          &format!("perp/gpair/{}", pair_key),
          &format!("perp/gpairi/{}", pair_key),
          &id,
        );
      }
      let _ = self.tap_set_list_record(
        &format!("tx/perp/group/{}", transaction),
        &format!("txi/perp/group/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/perp/group/{}", block),
        &format!("blcki/perp/group/{}", block),
        &ptr,
      );
    }
    self.record_perp_event(
      "group",
      &id,
      &group,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn perp_group_pair_key(group: &serde_json::Value) -> Option<String> {
    Some(format!(
      "{}|{}",
      Self::perp_asset_key(group.get("pair")?.get("base")?)?,
      Self::perp_asset_key(group.get("pair")?.get("quote")?)?
    ))
  }

  fn record_perp_event(
    &mut self,
    kind: &str,
    id: &str,
    data: &serde_json::Value,
    block: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    timestamp: u32,
  ) {
    let record = serde_json::json!({
      "kind": kind,
      "id": id,
      "data": data,
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_set_list_record(
      &format!("blck/perp/event/{}", block),
      &format!("blcki/perp/event/{}", block),
      &record,
    );
  }

  fn record_perp_certificate(
    &mut self,
    normalized: &PerpCertificateValidation,
    block: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    timestamp: u32,
  ) {
    let Some(policy) = normalized.cert.get("policy").and_then(|v| v.as_str()) else {
      return;
    };
    let Some(group) = normalized.cert.get("group").and_then(|v| v.as_str()) else {
      return;
    };
    let Some(purpose) = normalized.cert.get("purpose").and_then(|v| v.as_str()) else {
      return;
    };
    let Some(seq) = normalized.cert.get("seq").and_then(|v| v.as_str()) else {
      return;
    };
    let id = format!("{}:{}:{}:{}", policy, group, purpose, seq);
    let mut record = normalized.cert.clone();
    if let Some(map) = record.as_object_mut() {
      map.insert("id".to_string(), serde_json::json!(id.clone()));
      map.insert("price".to_string(), normalized.price.clone());
      map.insert("blck".to_string(), serde_json::json!(block));
      map.insert("tx".to_string(), serde_json::json!(transaction));
      map.insert("vo".to_string(), serde_json::json!(vout));
      map.insert("val".to_string(), serde_json::json!(value.to_string()));
      map.insert("ins".to_string(), serde_json::json!(inscription));
      map.insert("num".to_string(), serde_json::json!(number));
      map.insert("ts".to_string(), serde_json::json!(timestamp));
    }
    let _ = self.tap_put(&format!("perp/c/{}", id), &record);
    if let Some(sequence_key) = &normalized.sequence_key {
      let _ = self.tap_put(sequence_key, &seq.to_string());
    }
    let _ = self.tap_set_list_record("perp/certl", "perp/certi", &record);
    let _ = self.tap_set_list_record(
      &format!("perp/cg/{}", group),
      &format!("perp/cgi/{}", group),
      &id,
    );
    let _ = self.tap_set_list_record(
      &format!("perp/cp/{}", policy),
      &format!("perp/cpi/{}", policy),
      &id,
    );
    let _ = self.tap_set_list_record(
      &format!("blck/perp/cert/{}", block),
      &format!("blcki/perp/cert/{}", block),
      &id,
    );
    let _ = self.tap_set_list_record(
      &format!("tx/perp/cert/{}", transaction),
      &format!("txi/perp/cert/{}", transaction),
      &id,
    );
    self.record_perp_event(
      "certificate",
      &id,
      &record,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
  }

  fn record_perp_bounty(
    &mut self,
    group: &serde_json::Value,
    receiver: &str,
    amount: &BigInt,
    kind: &str,
    reference: &str,
    block: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    timestamp: u32,
  ) {
    if amount <= &BigInt::from(0) || receiver.is_empty() {
      return;
    }
    let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
      return;
    };
    let record = serde_json::json!({
      "group": group_id,
      "receiver": receiver,
      "amount": amount.to_string(),
      "kind": kind,
      "ref": reference,
      "tick": group.get("collateral").and_then(|v| v.get("tick")).and_then(|v| v.as_str()).unwrap_or(""),
      "tick_key": group.get("collateral").and_then(|v| v.get("tick_key")).and_then(|v| v.as_str()).unwrap_or(""),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_set_list_record(
      &format!("perp/bg/{}", group_id),
      &format!("perp/bgi/{}", group_id),
      &record,
    );
    let _ = self.tap_set_list_record(
      &format!("perp/ba/{}", receiver),
      &format!("perp/bai/{}", receiver),
      &record,
    );
    let _ = self.tap_set_list_record(
      &format!("blck/perp/bounty/{}", block),
      &format!("blcki/perp/bounty/{}", block),
      &record,
    );
    self.record_perp_event(
      "bounty",
      reference,
      &record,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
  }

  fn record_perp_claim_or_refund(
    &mut self,
    kind: &str,
    group: &serde_json::Value,
    position: &serde_json::Value,
    amount: &BigInt,
    block: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    timestamp: u32,
  ) {
    let target_key = if kind == "claim" { "claim" } else { "refund" };
    let Some(position_id) = position.get("id").and_then(|v| v.as_str()) else {
      return;
    };
    let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
      return;
    };
    let target = position
      .get(target_key)
      .and_then(|v| v.get("to"))
      .and_then(|v| v.as_str())
      .unwrap_or("");
    let record = serde_json::json!({
      "position": position_id,
      "group": group_id,
      "owner": position.get("owner").and_then(|v| v.as_str()).unwrap_or(""),
      "target": target,
      "amount": amount.to_string(),
      "tick": group.get("collateral").and_then(|v| v.get("tick")).and_then(|v| v.as_str()).unwrap_or(""),
      "tick_key": group.get("collateral").and_then(|v| v.get("tick_key")).and_then(|v| v.as_str()).unwrap_or(""),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let prefix = if kind == "claim" {
      "perp/claim"
    } else {
      "perp/refund"
    };
    let direct_prefix = if kind == "claim" {
      "perp/cl"
    } else {
      "perp/rf"
    };
    let _ = self.tap_put(&format!("{}/{}", direct_prefix, position_id), &record);
    let _ = self.tap_set_list_record(
      &format!("{}g/{}", prefix, group_id),
      &format!("{}gi/{}", prefix, group_id),
      &record,
    );
    let _ = self.tap_set_list_record(
      &format!("{}a/{}", prefix, target),
      &format!("{}ai/{}", prefix, target),
      &record,
    );
    let _ = self.tap_set_list_record(
      &format!("blck/{}/{}", prefix, block),
      &format!("blcki/{}/{}", prefix, block),
      &record,
    );
    self.record_perp_event(
      kind,
      position_id,
      &record,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
  }

  fn record_perp_settlement(
    &mut self,
    group: &serde_json::Value,
    block: u32,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    timestamp: u32,
  ) {
    let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
      return;
    };
    let record = serde_json::json!({
      "group": group_id,
      "state": group.get("state").cloned().unwrap_or(serde_json::Value::Null),
      "final_price": group.get("final_price").cloned().unwrap_or(serde_json::Value::Null),
      "settlement": group.get("settlement").cloned().unwrap_or(serde_json::Value::Null),
      "collateral": group.get("collateral").cloned().unwrap_or(serde_json::Value::Null),
      "blck": block,
      "tx": transaction,
      "vo": vout,
      "val": value.to_string(),
      "ins": inscription,
      "num": number,
      "ts": timestamp
    });
    let _ = self.tap_put(&format!("perp/st/{}", group_id), &record);
    let _ = self.tap_set_list_record("perp/stl", "perp/stli", &record);
    self.record_perp_event(
      "settlement",
      group_id,
      &record,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
  }

  fn perp_group_ready(group: &serde_json::Value) -> bool {
    let parse = |path: &[&str]| -> Option<BigInt> {
      let mut v = group;
      for key in path {
        v = v.get(*key)?;
      }
      v.as_str()?.parse::<BigInt>().ok()
    };
    let long_collateral = parse(&["long_collateral"]).unwrap_or_default();
    let short_collateral = parse(&["short_collateral"]).unwrap_or_default();
    let total_collateral = parse(&["total_collateral"]).unwrap_or_default();
    let long_notional = parse(&["long_notional"]).unwrap_or_default();
    let short_notional = parse(&["short_notional"]).unwrap_or_default();
    if long_collateral < parse(&["readiness", "min_long"]).unwrap_or_default()
      || short_collateral < parse(&["readiness", "min_short"]).unwrap_or_default()
      || total_collateral < parse(&["readiness", "min_total"]).unwrap_or_default()
      || long_notional < parse(&["readiness", "min_long_notional"]).unwrap_or_default()
      || short_notional < parse(&["readiness", "min_short_notional"]).unwrap_or_default()
    {
      return false;
    }
    if short_notional == BigInt::from(0) {
      return long_notional == BigInt::from(0);
    }
    let Some(ratio_min) = group
      .get("readiness")
      .and_then(|v| v.get("ratio_min"))
      .and_then(|v| Self::parse_perp_ratio(v, true))
    else {
      return false;
    };
    let Some(ratio_max) = group
      .get("readiness")
      .and_then(|v| v.get("ratio_max"))
      .and_then(|v| Self::parse_perp_ratio(v, false))
    else {
      return false;
    };
    if &long_notional * &ratio_min.1 < &short_notional * &ratio_min.0
      || &long_notional * &ratio_max.1 > &short_notional * &ratio_max.0
    {
      return false;
    }
    let imbalance = if long_notional > short_notional {
      long_notional - short_notional
    } else {
      short_notional - long_notional
    };
    imbalance <= parse(&["readiness", "max_imbalance_notional"]).unwrap_or_default()
  }

  fn resolve_perp_group_bounty(
    policy: &serde_json::Value,
    bounty: Option<&serde_json::Value>,
  ) -> Option<serde_json::Value> {
    let Some(bounty_value) = bounty else {
      return Some(policy.get("bounty")?.clone());
    };
    if bounty_value.get("rule")?.as_str()? != "operator-policy-bounty-v1" {
      return None;
    }
    let policy_bounty = policy.get("bounty")?;
    let mut resolved = serde_json::Map::new();
    for key in ["activate", "liquidate", "settle"] {
      let value = bounty_value.get(key)?;
      let policy_cap = policy_bounty.get(key)?.as_str()?.parse::<BigInt>().ok()?;
      let amount = if value.as_str() == Some("policy-default") {
        policy_cap.clone()
      } else {
        Self::parse_perp_uint(value, true)?
      };
      if amount > policy_cap {
        return None;
      }
      resolved.insert(key.to_string(), serde_json::json!(amount.to_string()));
    }
    Some(serde_json::Value::Object(resolved))
  }

  fn perp_leverage_in_bounds(group: &serde_json::Value, leverage: &(BigInt, BigInt)) -> bool {
    let Some(min) = group
      .get("leverage")
      .and_then(|v| v.get("min"))
      .and_then(|v| Self::parse_perp_ratio(v, false))
    else {
      return false;
    };
    let Some(max) = group
      .get("leverage")
      .and_then(|v| v.get("max"))
      .and_then(|v| Self::parse_perp_ratio(v, false))
    else {
      return false;
    };
    Self::compare_perp_ratio(leverage, &min) >= 0 && Self::compare_perp_ratio(leverage, &max) <= 0
  }

  fn perp_group_leverage(group: &serde_json::Value) -> Option<String> {
    group
      .get("leverage")
      .and_then(|v| v.get("value"))
      .or_else(|| group.get("leverage").and_then(|v| v.get("min")))
      .and_then(|v| v.as_str())
      .map(|s| s.to_string())
  }

  fn perp_position_active_state(position: &serde_json::Value, group: &serde_json::Value) -> bool {
    position.get("state").and_then(|v| v.as_str()) == Some("active")
      || (position.get("state").and_then(|v| v.as_str()) == Some("formation")
        && group.get("state").and_then(|v| v.as_str()) == Some("active"))
  }

  fn perp_position_claimable_state(position: &serde_json::Value) -> bool {
    matches!(
      position.get("state").and_then(|v| v.as_str()),
      Some("formation") | Some("active") | Some("closed") | Some("liquidated")
    )
  }

  fn add_perp_group_amount(group: &mut serde_json::Value, key: &str, amount: &BigInt) {
    let current = group
      .get(key)
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    if let Some(map) = group.as_object_mut() {
      map.insert(
        key.to_string(),
        serde_json::Value::String((current + amount).to_string()),
      );
    }
  }

  fn sub_perp_group_amount(
    group: &mut serde_json::Value,
    key: &str,
    amount: &BigInt,
  ) -> bool {
    let current = group
      .get(key)
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    let next = current - amount;
    if next < BigInt::from(0) {
      return false;
    }
    if let Some(map) = group.as_object_mut() {
      map.insert(key.to_string(), serde_json::Value::String(next.to_string()));
    }
    true
  }

  fn payable_perp_bounty(&mut self, group: &serde_json::Value, kind: &str, link_present: bool) -> BigInt {
    if !link_present
      || group.get("collateral").and_then(|v| v.get("ty")).and_then(|v| v.as_str()) != Some("tap")
    {
      return BigInt::from(0);
    }
    let configured = group
      .get("bounty")
      .and_then(|v| v.get(kind))
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    let reserve = group
      .get("bounty_reserve")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    let mut bounty = if configured < reserve { configured } else { reserve };
    let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
      return BigInt::from(0);
    };
    let Some(tick_key) = group
      .get("collateral")
      .and_then(|v| v.get("tick_key"))
      .and_then(|v| v.as_str())
    else {
      return BigInt::from(0);
    };
    let authority_balance = self.tap_get_authority_balance_bigint(group_id, tick_key);
    if bounty > authority_balance {
      bounty = authority_balance;
    }
    bounty
  }

  fn perp_notional(collateral: &BigInt, leverage: &(BigInt, BigInt)) -> BigInt {
    collateral * &leverage.0 / &leverage.1
  }

  fn compute_perp_open_side_equity(
    group: &serde_json::Value,
    side: &str,
    price: &serde_json::Value,
  ) -> Option<BigInt> {
    let key = format!("{}_open_collateral", side);
    let fallback_key = format!("{}_collateral", side);
    let collateral = group
      .get(&key)
      .or_else(|| group.get(&fallback_key))
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    if collateral <= BigInt::from(0) {
      return Some(BigInt::from(0));
    }
    let leverage = Self::perp_group_leverage(group)?;
    Self::compute_perp_equity(&collateral, &leverage, side, group.get("entry_price")?, price)
  }

  fn compute_perp_settlement_aggregate(
    group: &serde_json::Value,
    certificate: &PerpCertificateValidation,
  ) -> Option<PerpSettlementAggregate> {
    let external_fallback = group
      .get("collateral")
      .and_then(|v| v.get("ty"))
      .and_then(|v| v.as_str())
      != Some("tap")
      && !certificate.signed;
    let long_open_collateral = group
      .get("long_open_collateral")
      .or_else(|| group.get("long_collateral"))
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    let short_open_collateral = group
      .get("short_open_collateral")
      .or_else(|| group.get("short_collateral"))
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    let long_open_equity = if external_fallback {
      long_open_collateral.clone()
    } else {
      Self::compute_perp_open_side_equity(group, "long", &certificate.price)?
    };
    let short_open_equity = if external_fallback {
      short_open_collateral.clone()
    } else {
      Self::compute_perp_open_side_equity(group, "short", &certificate.price)?
    };
    let closed_equity = if external_fallback {
      BigInt::from(0)
    } else {
      group
        .get("closed_equity_total")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_else(|| BigInt::from(0))
    };
    let total_equity = &long_open_equity + &short_open_equity + &closed_equity;
    Some(PerpSettlementAggregate {
      external_fallback,
      long_open_collateral,
      short_open_collateral,
      long_open_equity,
      short_open_equity,
      closed_equity,
      total_equity,
    })
  }

  fn validate_perp_join_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    action_index: usize,
    block: u32,
    pending_locks: &std::collections::HashMap<String, i128>,
    pending_amm_credits: &std::collections::HashMap<String, BigInt>,
    pending_amm_debits: &std::collections::HashMap<String, BigInt>,
    pending_obligations: &std::collections::HashMap<String, BigInt>,
    pending_perp_debits: &std::collections::HashMap<String, BigInt>,
  ) -> Option<PerpJoinValidation> {
    let link = link?;
    if !self.perp_groups_enabled()
      || action.get("op")?.as_str()?.to_lowercase() != "perp-join"
      || !action.get("src")?.is_object()
      || action
        .get("side")?
        .as_str()
        .map(|s| s != "long" && s != "short")
        .unwrap_or(true)
    {
      return None;
    }
    let source = self.normalize_perp_target(action.get("src")?)?;
    let group = self.get_perp_group(action.get("gid")?.as_str()?)?;
    let policy = self.get_perp_policy(group.get("policy")?.as_str()?)?;
    if source.get("to")?.as_str()? != link.addr
      || group.get("state").and_then(|v| v.as_str()) != Some("formation")
      || group.get("collateral")?.get("ty").and_then(|v| v.as_str()) != Some("tap")
      || block < group.get("start")?.as_u64()? as u32
      || block > group.get("deadline")?.as_u64()? as u32
    {
      return None;
    }
    let token = self.token_proof_get_deploy(group.get("collateral")?.get("tick")?.as_str()?)?;
    let amount =
      self.token_proof_resolve_protocol_amount_bigint(action.get("coll")?, &token.record)?;
    let leverage = Self::parse_perp_ratio(action.get("lev")?, false)?;
    let claim = self.normalize_perp_target(action.get("claim")?)?;
    let refund = self.normalize_perp_target(action.get("refund")?)?;
    let entry_policy = group.get("entry_policy").or_else(|| policy.get("entry"))?;
    let entry =
      Self::resolve_perp_entry_bound(action.get("entry"), action.get("side")?.as_str()?, entry_policy)?;
    let min_collateral = policy
      .get("limits")?
      .get("min_collateral")?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    if amount < min_collateral
      || !Self::perp_leverage_in_bounds(&group, &leverage)
      || !self.has_available_with_pending_bigint(
        &link.addr,
        group.get("collateral")?.get("tick_key")?.as_str()?,
        &amount,
        pending_locks,
        pending_amm_credits,
        pending_amm_debits,
        pending_obligations,
        pending_perp_debits,
      )
    {
      return None;
    }
    let notional = Self::perp_notional(&amount, &leverage);
    if notional <= BigInt::from(0)
      || notional
        > policy
          .get("limits")?
          .get("max_notional")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
    {
      return None;
    }
    let group_leverage = Self::parse_perp_ratio(
      &serde_json::Value::String(Self::perp_group_leverage(&group)?),
      false,
    )?;
    if Self::compare_perp_ratio(&leverage, &group_leverage) != 0 {
      return None;
    }
    let id = Self::tap_token_perp_position_id(inscription, action_index);
    if self.get_perp_position(&id).is_some() {
      return None;
    }
    let mut value = serde_json::json!({
      "id": id,
      "group": group.get("id")?.as_str()?,
      "owner": link.addr,
      "side": action.get("side")?.as_str()?,
      "tick": group.get("collateral")?.get("tick")?.as_str()?,
      "tick_key": group.get("collateral")?.get("tick_key")?.as_str()?,
      "collateral": amount.to_string(),
      "open_collateral": amount.to_string(),
      "closed_equity": "0",
      "leverage": Self::serialize_perp_ratio(&leverage),
      "notional": notional.to_string(),
      "claim": claim,
      "refund": refund,
      "state": "formation",
      "payout": "0",
      "claimed": false,
      "refunded": false
    });
    if let Some(entry) = entry {
      if let Some(map) = value.as_object_mut() {
        map.insert("entry".to_string(), entry);
      }
    }
    Some(PerpJoinValidation {
      owner: link.addr.clone(),
      tick: group.get("collateral")?.get("tick")?.as_str()?.to_string(),
      tick_key: group
        .get("collateral")?
        .get("tick_key")?
        .as_str()?
        .to_string(),
      collateral: amount,
      notional,
      value,
    })
  }

  fn process_perp_join_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let empty_i128 = std::collections::HashMap::new();
    let empty_bigint = std::collections::HashMap::new();
    let Some(normalized) = self.validate_perp_join_action(
      action,
      link,
      inscription,
      action_index,
      block,
      &empty_i128,
      &empty_bigint,
      &empty_bigint,
      &empty_bigint,
      &empty_bigint,
    ) else {
      return false;
    };
    let Some(mut group) = self.get_perp_group(
      normalized
        .value
        .get("group")
        .and_then(|v| v.as_str())
        .unwrap_or(""),
    ) else {
      return false;
    };
    let before = self.tap_get_address_balance_bigint(&normalized.owner, &normalized.tick_key);
    let after = before - normalized.collateral.clone();
    if after < BigInt::from(0) {
      return false;
    }
    if !self.tap_put_address_balance_bigint(
      &normalized.owner,
      &normalized.tick_key,
      &normalized.tick,
      &after,
    ) || !self.tap_add_authority_balance_bigint(
      group.get("id").and_then(|v| v.as_str()).unwrap_or(""),
      &normalized.tick_key,
      &normalized.collateral,
    ) {
      return false;
    }
    let group_id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let add_str = |group: &mut serde_json::Value, key: &str, amount: &BigInt| {
      let current = group
        .get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_else(|| BigInt::from(0));
      if let Some(map) = group.as_object_mut() {
        map.insert(
          key.to_string(),
          serde_json::Value::String((current + amount).to_string()),
        );
      }
    };
    add_str(&mut group, "total_collateral", &normalized.collateral);
    add_str(&mut group, "total_notional", &normalized.notional);
    if normalized.value.get("side").and_then(|v| v.as_str()) == Some("long") {
      add_str(&mut group, "long_collateral", &normalized.collateral);
      add_str(&mut group, "long_open_collateral", &normalized.collateral);
      add_str(&mut group, "long_notional", &normalized.notional);
    } else {
      add_str(&mut group, "short_collateral", &normalized.collateral);
      add_str(&mut group, "short_open_collateral", &normalized.collateral);
      add_str(&mut group, "short_notional", &normalized.notional);
    }
    add_str(&mut group, "total_open_collateral", &normalized.collateral);
    add_str(&mut group, "positions", &BigInt::from(1));
    if let Some(entry) = normalized.value.get("entry") {
      let _ = Self::update_perp_group_entry_bounds(&mut group, Some(entry));
    }
    let mut record = normalized.value.clone();
    if let Some(map) = record.as_object_mut() {
      map.insert("blck".to_string(), serde_json::json!(block));
      map.insert("tx".to_string(), serde_json::json!(transaction));
      map.insert("vo".to_string(), serde_json::json!(vout));
      map.insert("val".to_string(), serde_json::json!(value.to_string()));
      map.insert("ins".to_string(), serde_json::json!(inscription));
      map.insert("num".to_string(), serde_json::json!(number));
      map.insert("ts".to_string(), serde_json::json!(timestamp));
    }
    let position_id = normalized
      .value
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    let _ = self.tap_put(&format!("perp/pos/{}", position_id), &normalized.value);
    let _ = self.tap_set_list_record(
      &format!("perp/pgl/{}", group_id),
      &format!("perp/pgli/{}", group_id),
      &position_id,
    );
    let _ = self.tap_set_list_record(
      &format!("perp/pa/{}", normalized.owner),
      &format!("perp/pai/{}", normalized.owner),
      &position_id,
    );
    if self
      .tap_get::<String>(&format!("perp/gae/{}/{}", normalized.owner, group_id))
      .ok()
      .flatten()
      .is_none()
    {
      let _ = self.tap_put(
        &format!("perp/gae/{}/{}", normalized.owner, group_id),
        &"".to_string(),
      );
      let _ = self.tap_set_list_record(
        &format!("perp/ga/{}", normalized.owner),
        &format!("perp/gai/{}", normalized.owner),
        &group_id,
      );
    }
    if let Ok(list_len) = self.tap_set_list_record("perp/posl", "perp/posli", &record) {
      let ptr = format!("perp/posli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/perp/join/{}", transaction),
        &format!("txi/perp/join/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/perp/join/{}", block),
        &format!("blcki/perp/join/{}", block),
        &ptr,
      );
    }
    self.record_perp_event(
      "join",
      &position_id,
      &normalized.value,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    let transferable = self
      .tap_get::<String>(&format!("t/{}/{}", normalized.owner, normalized.tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let after_i128 = after.to_string().parse::<i128>().ok().unwrap_or(0);
    let auth_balance = self
      .tap_get_authority_balance_bigint(&group_id, &normalized.tick_key)
      .to_string()
      .parse::<i128>()
      .ok()
      .unwrap_or(0);
    let amount_i128 = normalized
      .collateral
      .to_string()
      .parse::<i128>()
      .ok()
      .unwrap_or(0);
    self.tap_apply_authority_transfer_logs(
      &normalized.tick,
      &normalized.tick_key,
      &normalized.owner,
      &group_id,
      transferable,
      after_i128,
      auth_balance,
      amount_i128,
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "pj",
      &position_id,
    );
    true
  }

  fn validate_perp_cancel_action(
    &mut self,
    action: &serde_json::Value,
    block: u32,
  ) -> Option<serde_json::Value> {
    if !self.perp_groups_enabled() || action.get("op")?.as_str()?.to_lowercase() != "perp-cancel" {
      return None;
    }
    let group = self.get_perp_group(action.get("gid")?.as_str()?)?;
    if group.get("state").and_then(|v| v.as_str()) != Some("formation")
      || block <= group.get("deadline")?.as_u64()? as u32
    {
      return None;
    }
    Some(group)
  }

  fn process_perp_cancel_action(
    &mut self,
    action: &serde_json::Value,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(mut group) = self.validate_perp_cancel_action(action, block) else {
      return false;
    };
    let id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if let Some(map) = group.as_object_mut() {
      map.insert("state".to_string(), serde_json::json!("cancelled"));
      map.insert(
        "cancelled".to_string(),
        serde_json::json!({ "blck": block, "tx": transaction, "vo": vout, "val": value.to_string(), "ins": inscription, "num": number, "ts": timestamp }),
      );
    }
    let _ = self.tap_put(&format!("perp/g/{}", id), &group);
    let _ = self.tap_set_list_record("perp/gs/cancelled", "perp/gsi/cancelled", &id);
    let _ = self.tap_set_list_record(
      &format!("blck/perp/cancel/{}", block),
      &format!("blcki/perp/cancel/{}", block),
      &id,
    );
    self.record_perp_event(
      "cancel",
      &id,
      &group,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn validate_perp_price_certificate(
    &mut self,
    action: &serde_json::Value,
    group: &serde_json::Value,
    purpose: &str,
    block: u32,
  ) -> Option<PerpCertificateValidation> {
    let policy = self.get_perp_policy(group.get("policy")?.as_str()?)?;
    let cert = action.get("cert")?;
    let price = Self::normalize_perp_price(cert.get("price")?)?;
    if cert.get("dom")?.as_str()? != "tap-perp-price-v1"
      || cert.get("net")?.as_str()? != policy.get("net")?.as_str()?
      || cert.get("pid")?.as_str()? != policy.get("id")?.as_str()?
      || cert.get("ph")?.as_str()? != policy.get("hash")?.as_str()?
      || cert.get("gid")?.as_str()? != group.get("id")?.as_str()?
      || cert.get("gh")?.as_str()? != group.get("gh")?.as_str()?
      || cert.get("purpose")?.as_str()? != purpose
      || !cert.get("sigs")?.is_array()
    {
      return None;
    }
    let cert_seq = Self::parse_perp_uint(cert.get("seq")?, false)?;
    let valid_from = Self::parse_perp_height(cert.get("valid_from")?)?;
    let valid_until = Self::parse_perp_height(cert.get("valid_until")?)?;
    if block < valid_from || block > valid_until {
      return None;
    }
    let cert_pair = if let Some(pair) = cert.get("pair") {
      let cert_base = self.normalize_perp_asset(pair.get("base")?)?;
      let cert_quote = self.normalize_perp_asset(pair.get("quote")?)?;
      if cert_base.key != Self::perp_asset_key(group.get("pair")?.get("base")?)?
        || cert_quote.key != Self::perp_asset_key(group.get("pair")?.get("quote")?)?
        || pair
          .get("price_dir")
          .map(|v| v.as_str() != Some("quote-per-base"))
          .unwrap_or(false)
      {
        return None;
      }
      pair.clone()
    } else {
      group.get("pair")?.clone()
    };
    let payload_hash = Self::token_perp_payload_hash(action, &["cert"])?;
    let cert_payload_hash = Self::token_perp_payload_hash(cert, &["sigs"])?;
    if cert
      .get("state_hash")
      .and_then(|v| v.as_str())
      .map(|hash| hash.to_lowercase() != payload_hash)
      .unwrap_or(true)
    {
      return None;
    }
    let nonce_key = format!(
      "perp/cn/{}/{}/{}/{}",
      policy.get("id")?.as_str()?,
      group.get("id")?.as_str()?,
      purpose,
      cert.get("seq")?.as_str()?
    );
    if self.tap_get::<String>(&nonce_key).ok().flatten().is_some() {
      return None;
    }
    let sequence_key = format!(
      "perp/cseq/{}/{}/{}",
      policy.get("id")?.as_str()?,
      group.get("id")?.as_str()?,
      purpose,
    );
    if let Some(last_sequence) = self.tap_get::<String>(&sequence_key).ok().flatten() {
      if let Ok(last_sequence) = last_sequence.parse::<BigInt>() {
        if cert_seq <= last_sequence {
          return None;
        }
      } else {
        return None;
      }
    }
    let msg = Self::token_perp_certificate_message(
      &policy,
      purpose,
      group.get("id")?.as_str()?,
      &cert_payload_hash,
      cert.get("seq")?.as_str()?,
      valid_until,
    )?;
    let msg_hash = Self::certified_control_hash(&msg)?;
    let signers = policy
      .get("oracle")?
      .get("signers")?
      .as_array()?
      .iter()
      .map(|v| v.as_str().map(|s| s.to_string()))
      .collect::<Option<Vec<_>>>()?;
    let threshold = policy.get("oracle")?.get("threshold")?.as_u64()? as usize;
    if self.valid_perp_signature_count(cert.get("sigs")?, &signers, &msg_hash) < threshold {
      return None;
    }
    Some(PerpCertificateValidation {
      price,
      cert: serde_json::json!({
        "v": "1",
        "dom": cert.get("dom")?.as_str()?,
        "net": cert.get("net")?.as_str()?,
        "policy": policy.get("id")?.as_str()?,
        "pid": policy.get("id")?.as_str()?,
        "ph": policy.get("hash")?.as_str()?,
        "group": group.get("id")?.as_str()?,
        "gid": group.get("id")?.as_str()?,
        "gh": group.get("gh")?.as_str()?,
        "purpose": purpose,
        "payload_hash": cert_payload_hash,
        "state_hash": payload_hash,
        "seq": cert.get("seq")?.as_str()?,
        "valid_from": valid_from,
        "valid_until": valid_until,
        "source": cert.get("source").cloned().unwrap_or_else(|| serde_json::json!({})),
        "pair": cert_pair
      }),
      nonce_key,
      sequence_key: Some(sequence_key),
      signed: true,
    })
  }

  fn validate_perp_settlement_fallback(
    &mut self,
    action: &serde_json::Value,
    group: &serde_json::Value,
    block: u32,
  ) -> Option<PerpCertificateValidation> {
    let policy = self.get_perp_policy(group.get("policy")?.as_str()?)?;
    if action.get("fallback")?.as_str()? != "last-valid-at-expiry-v1"
      || action.get("cert").is_some()
      || group.get("fallback")?.get("type")?.as_str()? != "last-valid-at-expiry-v1"
    {
      return None;
    }
    let max_age = policy
      .get("oracle")?
      .get("max_age")?
      .as_str()?
      .parse::<u64>()
      .ok()?;
    let expiry = group.get("expiry")?.as_u64()?;
    if u64::from(block) <= expiry.checked_add(max_age)? {
      return None;
    }
    let price = Self::normalize_perp_price(group.get("mark_price")?)?;
    let payload_hash = Self::token_perp_payload_hash(action, &["cert"])?;
    let policy_id = policy.get("id")?.as_str()?;
    let group_id = group.get("id")?.as_str()?;
    Some(PerpCertificateValidation {
      price,
      cert: serde_json::json!({
        "v": "1",
        "dom": "tap-perp-fallback-v1",
        "net": policy.get("net")?.as_str()?,
        "policy": policy_id,
        "pid": policy_id,
        "ph": policy.get("hash")?.as_str()?,
        "group": group_id,
        "gid": group_id,
        "gh": group.get("gh")?.as_str()?,
        "purpose": "settlement",
        "state_hash": payload_hash,
        "seq": format!("fallback-{}", block),
        "fallback": "last-valid-at-expiry-v1",
        "source": "mark-price",
        "price_source": group.get("mark_cert").cloned().unwrap_or_else(|| serde_json::json!({}))
      }),
      nonce_key: format!("perp/fn/{}/{}/settlement/{}", policy_id, group_id, block),
      sequence_key: None,
      signed: false,
    })
  }

  fn normalize_perp_external_evidence_body(ext: &serde_json::Value) -> Option<serde_json::Value> {
    let group = ext.get("group")?.as_str()?;
    let position = ext.get("position")?.as_str()?;
    let tx = ext.get("tx")?.as_str()?;
    let finality = ext.get("finality")?;
    let finality_rule = finality.get("rule")?.as_str()?;
    let owner = ext.get("owner")?.as_str()?;
    let claim = ext.get("claim")?.as_str()?;
    let refund = ext.get("refund")?.as_str()?;
    let side = ext.get("side")?.as_str()?;
    if !Self::token_proof_safe_id(group, 128)
      || !Self::token_proof_safe_id(position, 128)
      || !Self::token_proof_safe_id(tx, 128)
      || !Self::token_proof_safe_id(finality_rule, 64)
      || !Self::token_proof_safe_id(owner, 128)
      || !Self::token_proof_safe_id(claim, 128)
      || !Self::token_proof_safe_id(refund, 128)
      || (side != "long" && side != "short")
    {
      return None;
    }
    let amount = Self::parse_perp_uint(ext.get("amount")?, false)?;
    let height = Self::parse_perp_uint(ext.get("height")?, true)?;
    let confirmations = Self::parse_perp_uint(finality.get("count")?, false)?;
    let leverage = Self::parse_perp_ratio(ext.get("lev")?, false)?;
    let mut normalized = serde_json::json!({
      "group": group.to_lowercase(),
      "position": position.to_lowercase(),
      "tx": tx.to_lowercase(),
      "height": height.to_string(),
      "finality": {
        "rule": finality_rule.to_lowercase(),
        "count": confirmations.to_string()
      },
      "owner": owner.to_lowercase(),
      "side": side,
      "amount": amount.to_string(),
      "lev": Self::serialize_perp_ratio(&leverage),
      "claim": claim.to_lowercase(),
      "refund": refund.to_lowercase()
    });
    if let Some(index_value) = ext.get("index") {
      let index = Self::parse_perp_uint(index_value, true)?;
      normalized
        .as_object_mut()?
        .insert("index".to_string(), serde_json::json!(index.to_string()));
    }
    if let Some(entry) = ext.get("entry") {
      normalized
        .as_object_mut()?
        .insert("entry".to_string(), entry.clone());
    }
    Some(normalized)
  }

  fn normalize_perp_external_terminal_evidence_body(
    ext: &serde_json::Value,
    purpose: &str,
  ) -> Option<serde_json::Value> {
    let expected_action = match purpose {
      "external-close" => "close",
      "external-liquidation" => "liquidation",
      _ => return None,
    };
    let group = ext.get("group")?.as_str()?;
    let position = ext.get("position")?.as_str()?;
    let tx = ext.get("tx")?.as_str()?;
    let finality = ext.get("finality")?;
    let finality_rule = finality.get("rule")?.as_str()?;
    let owner = ext.get("owner")?.as_str()?;
    let recipient = ext.get("recipient")?.as_str()?;
    let state_hash = ext.get("state_hash")?.as_str()?;
    if !Self::token_proof_safe_id(group, 128)
      || !Self::token_proof_safe_id(position, 128)
      || !Self::token_proof_safe_id(tx, 128)
      || !Self::token_proof_safe_id(finality_rule, 64)
      || !Self::token_proof_safe_id(owner, 128)
      || !Self::token_proof_safe_id(recipient, 128)
      || !Self::token_proof_safe_id(state_hash, 128)
      || ext.get("action")?.as_str()? != expected_action
    {
      return None;
    }
    let height = Self::parse_perp_uint(ext.get("height")?, true)?;
    let confirmations = Self::parse_perp_uint(finality.get("count")?, false)?;
    let price = Self::normalize_perp_price(ext.get("price")?)?;
    let open_before = Self::parse_perp_uint(ext.get("open_before")?, false)?;
    let equity = Self::parse_perp_uint(ext.get("equity")?, true)?;
    let bounty = Self::parse_perp_uint(ext.get("bounty")?, true)?;
    let maintenance = if purpose == "external-liquidation" {
      Self::parse_perp_uint(ext.get("maintenance")?, true)?
    } else {
      BigInt::from(0)
    };
    Some(serde_json::json!({
      "group": group.to_lowercase(),
      "position": position.to_lowercase(),
      "tx": tx.to_lowercase(),
      "height": height.to_string(),
      "finality": {
        "rule": finality_rule.to_lowercase(),
        "count": confirmations.to_string()
      },
      "owner": owner.to_lowercase(),
      "action": expected_action,
      "price": price,
      "open_before": open_before.to_string(),
      "equity": equity.to_string(),
      "maintenance": maintenance.to_string(),
      "bounty": bounty.to_string(),
      "recipient": recipient.to_lowercase(),
      "state_hash": state_hash.to_lowercase()
    }))
  }

  fn perp_external_evidence_id(
    policy: &serde_json::Value,
    group: &serde_json::Value,
    purpose: &str,
    collateral: &serde_json::Value,
    surface: &serde_json::Value,
    ext: &serde_json::Value,
    seq: &str,
  ) -> Option<String> {
    Some(format!(
      "{}:{}:{}:{}:{}:{}:{}",
      policy.get("id")?.as_str()?,
      group.get("id")?.as_str()?,
      purpose,
      Self::perp_asset_key(collateral)?,
      Self::perp_settlement_surface_key(surface)?,
      Self::perp_key_part(ext.get("position")?.as_str()?),
      seq
    ))
  }

  fn validate_perp_external_evidence_action(
    &mut self,
    action: &serde_json::Value,
    _inscription: &str,
    _action_index: usize,
    block: u32,
  ) -> Option<PerpExternalEvidenceValidation> {
    if !self.perp_groups_enabled()
      || action.get("op")?.as_str()?.to_lowercase() != "perp-external-evidence"
      || !action.get("evidence")?.is_object()
    {
      return None;
    }
    let purpose = action.get("purpose")?.as_str()?.to_lowercase();
    if !matches!(
      purpose.as_str(),
      "external-lock" | "external-close" | "external-liquidation"
    ) {
      return None;
    }
    let group = self.get_perp_group(action.get("gid")?.as_str()?)?;
    let policy = self.get_perp_policy(group.get("policy")?.as_str()?)?;
    let evidence = action.get("evidence")?;
    if group.get("collateral")?.get("ty").and_then(|v| v.as_str()) != Some("ext")
      || !group
        .get("settlement_surface")
        .map(|v| v.is_object())
        .unwrap_or(false)
      || evidence.get("dom")?.as_str()? != "tap-perp-external-evidence-v1"
      || evidence.get("net")?.as_str()? != policy.get("net")?.as_str()?
      || evidence.get("pid")?.as_str()? != policy.get("id")?.as_str()?
      || evidence.get("ph")?.as_str()? != policy.get("hash")?.as_str()?
      || evidence.get("gid")?.as_str()? != group.get("id")?.as_str()?
      || evidence.get("gh")?.as_str()? != group.get("gh")?.as_str()?
      || evidence.get("purpose")?.as_str()? != purpose
      || !evidence.get("sigs")?.is_array()
    {
      return None;
    }
    let seq = Self::parse_perp_uint(evidence.get("seq")?, false)?;
    let valid_from = Self::parse_perp_height(evidence.get("valid_from")?)?;
    let valid_until = Self::parse_perp_height(evidence.get("valid_until")?)?;
    let collateral = self.normalize_perp_asset(evidence.get("coll")?)?;
    let surface = Self::normalize_perp_settlement_surface(evidence.get("surface")?)?;
    let mut ext = if purpose == "external-lock" {
      Self::normalize_perp_external_evidence_body(evidence.get("ext")?)?
    } else {
      Self::normalize_perp_external_terminal_evidence_body(evidence.get("ext")?, &purpose)?
    };
    if block < valid_from
      || block > valid_until
      || collateral.value.get("ty").and_then(|v| v.as_str()) != Some("ext")
      || collateral.key != Self::perp_asset_key(group.get("collateral")?)?
      || evidence.get("mode")?.as_str()? != group.get("collateral_mode")?.as_str()?
      || Self::perp_settlement_surface_key(&surface)?
        != Self::perp_settlement_surface_key(group.get("settlement_surface")?)?
    {
      return None;
    }
    let payload_hash = Self::token_perp_payload_hash(action, &["evidence"])?;
    let evidence_payload_hash = Self::token_perp_payload_hash(evidence, &["sigs"])?;
    if evidence
      .get("state_hash")
      .and_then(|v| v.as_str())
      .map(|hash| hash.to_lowercase() != payload_hash)
      .unwrap_or(true)
    {
      return None;
    }
    if purpose != "external-lock" {
      if group.get("state").and_then(|v| v.as_str()) != Some("active") {
        return None;
      }
      let position_id = format!(
        "{}:ext:{}",
        group.get("id")?.as_str()?,
        Self::perp_key_part(ext.get("position")?.as_str()?)
      );
      let position = self.get_perp_position(&position_id)?;
      if position.get("group")?.as_str()? != group.get("id")?.as_str()?
        || !Self::perp_position_active_state(&position, &group)
        || position.get("owner")?.as_str()? != ext.get("owner")?.as_str()?
        || position
          .get("external")?
          .get("group")?
          .as_str()? != ext.get("group")?.as_str()?
        || position
          .get("external")?
          .get("position")?
          .as_str()? != ext.get("position")?.as_str()?
        || !position
          .get("settlement_surface")
          .map(|v| v.is_object())
          .unwrap_or(false)
        || Self::perp_settlement_surface_key(position.get("settlement_surface")?)?
          != Self::perp_settlement_surface_key(&surface)?
        || Self::perp_asset_key(position.get("collateral_asset")?)?
          != Self::perp_asset_key(&collateral.value)?
        || position
          .get("open_collateral")?
          .as_str()?
          .parse::<BigInt>()
          .ok()? != ext.get("open_before")?.as_str()?.parse::<BigInt>().ok()?
      {
        return None;
      }
      let open_before = ext.get("open_before")?.as_str()?.parse::<BigInt>().ok()?;
      let computed_equity = Self::compute_perp_equity(
        &open_before,
        position.get("leverage")?.as_str()?,
        position.get("side")?.as_str()?,
        group.get("entry_price")?,
        ext.get("price")?,
      )?;
      let evidence_equity = ext.get("equity")?.as_str()?.parse::<BigInt>().ok()?;
      if computed_equity != evidence_equity {
        return None;
      }
      let bounty = ext.get("bounty")?.as_str()?.parse::<BigInt>().ok()?;
      if purpose == "external-liquidation" {
        let maintenance_bps = group.get("maintenance_bps")?.as_str()?.parse::<BigInt>().ok()?;
        let expected_maintenance = &open_before * &maintenance_bps / BigInt::from(10_000);
        if ext.get("maintenance")?.as_str()?.parse::<BigInt>().ok()? != expected_maintenance
          || computed_equity.clone() * BigInt::from(10_000) > open_before.clone() * maintenance_bps
        {
          return None;
        }
      } else if bounty != BigInt::from(0) {
        return None;
      }
      let id = Self::perp_external_evidence_id(
        &policy,
        &group,
        &purpose,
        &collateral.value,
        &surface,
        &ext,
        &seq.to_string(),
      )?;
      if self
        .tap_get::<serde_json::Value>(&format!("perp/e/{}", id))
        .ok()
        .flatten()
        .is_some()
      {
        return None;
      }
      let nonce_key = format!("perp/en/{}", id);
      if self.tap_get::<String>(&nonce_key).ok().flatten().is_some() {
        return None;
      }
      let sequence_key = format!(
        "perp/eseq/{}/{}/{}/{}/{}/{}",
        policy.get("id")?.as_str()?,
        group.get("id")?.as_str()?,
        purpose,
        Self::perp_asset_key(&collateral.value)?,
        Self::perp_settlement_surface_key(&surface)?,
        Self::perp_key_part(ext.get("position")?.as_str()?)
      );
      if let Some(last_sequence) = self.tap_get::<String>(&sequence_key).ok().flatten() {
        if seq <= last_sequence.parse::<BigInt>().ok()? {
          return None;
        }
      }
      let msg = Self::token_perp_external_evidence_message(
        &policy,
        &group,
        &purpose,
        &evidence_payload_hash,
        evidence.get("seq")?.as_str()?,
        valid_until,
      )?;
      let msg_hash = Self::certified_control_hash(&msg)?;
      let signers = policy
        .get("signers")?
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>()?;
      let threshold = policy.get("threshold")?.as_u64()? as usize;
      if self.valid_perp_signature_count(evidence.get("sigs")?, &signers, &msg_hash) < threshold {
        return None;
      }
      return Some(PerpExternalEvidenceValidation {
        kind: purpose.clone(),
        id,
        position_id,
        position: Some(position),
        group: group.clone(),
        collateral: collateral.value.clone(),
        mode: evidence.get("mode")?.as_str()?.to_string(),
        surface: surface.clone(),
        ext: ext.clone(),
        amount: open_before,
        notional: BigInt::from(0),
        leverage: (BigInt::from(0), BigInt::from(1)),
        equity: evidence_equity,
        bounty,
        nonce_key,
        sequence_key,
        evidence: serde_json::json!({
          "v": "1",
          "dom": evidence.get("dom")?.as_str()?,
          "net": evidence.get("net")?.as_str()?,
          "policy": policy.get("id")?.as_str()?,
          "pid": policy.get("id")?.as_str()?,
          "ph": policy.get("hash")?.as_str()?,
          "group": group.get("id")?.as_str()?,
          "gid": group.get("id")?.as_str()?,
          "gh": group.get("gh")?.as_str()?,
          "purpose": purpose,
          "payload_hash": evidence_payload_hash,
          "state_hash": payload_hash,
          "seq": evidence.get("seq")?.as_str()?,
          "valid_from": valid_from,
          "valid_until": valid_until,
          "collateral": collateral.value,
          "mode": evidence.get("mode")?.as_str()?,
          "surface": surface,
          "ext": ext
        }),
      });
    }
    if group.get("state").and_then(|v| v.as_str()) != Some("formation") {
      return None;
    }
    let amount = ext.get("amount")?.as_str()?.parse::<BigInt>().ok()?;
    let min_collateral = policy
      .get("limits")?
      .get("min_collateral")?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    if amount < min_collateral {
      return None;
    }
    let leverage = Self::parse_perp_ratio(
      &serde_json::Value::String(ext.get("lev")?.as_str()?.to_string()),
      false,
    )?;
    let entry_policy = group.get("entry_policy").or_else(|| policy.get("entry"))?;
    let entry =
      Self::resolve_perp_entry_bound(ext.get("entry"), ext.get("side")?.as_str()?, entry_policy)?;
    if let Some(entry) = entry {
      ext.as_object_mut()?.insert("entry".to_string(), entry);
    } else if let Some(map) = ext.as_object_mut() {
      map.remove("entry");
    }
    if !Self::perp_leverage_in_bounds(&group, &leverage) {
      return None;
    }
    let group_leverage = Self::parse_perp_ratio(
      &serde_json::Value::String(Self::perp_group_leverage(&group)?),
      false,
    )?;
    if Self::compare_perp_ratio(&leverage, &group_leverage) != 0 {
      return None;
    }
    let notional = Self::perp_notional(&amount, &leverage);
    if notional <= BigInt::from(0)
      || notional
        > policy
          .get("limits")?
          .get("max_notional")?
          .as_str()?
          .parse::<BigInt>()
          .ok()?
    {
      return None;
    }
    let position_id = format!(
      "{}:ext:{}",
      group.get("id")?.as_str()?,
      Self::perp_key_part(ext.get("position")?.as_str()?)
    );
    if self.get_perp_position(&position_id).is_some() {
      return None;
    }
    let id = Self::perp_external_evidence_id(
      &policy,
      &group,
      &purpose,
      &collateral.value,
      &surface,
      &ext,
      &seq.to_string(),
    )?;
    if self
      .tap_get::<serde_json::Value>(&format!("perp/e/{}", id))
      .ok()
      .flatten()
      .is_some()
    {
      return None;
    }
    let nonce_key = format!("perp/en/{}", id);
    if self.tap_get::<String>(&nonce_key).ok().flatten().is_some() {
      return None;
    }
    let sequence_key = format!(
      "perp/eseq/{}/{}/{}/{}/{}/{}",
      policy.get("id")?.as_str()?,
      group.get("id")?.as_str()?,
      purpose,
      Self::perp_asset_key(&collateral.value)?,
      Self::perp_settlement_surface_key(&surface)?,
      Self::perp_key_part(ext.get("position")?.as_str()?)
    );
    if let Some(last_sequence) = self.tap_get::<String>(&sequence_key).ok().flatten() {
      if seq <= last_sequence.parse::<BigInt>().ok()? {
        return None;
      }
    }
    let msg = Self::token_perp_external_evidence_message(
      &policy,
      &group,
      &purpose,
      &evidence_payload_hash,
      evidence.get("seq")?.as_str()?,
      valid_until,
    )?;
    let msg_hash = Self::certified_control_hash(&msg)?;
    let signers = policy
      .get("signers")?
      .as_array()?
      .iter()
      .map(|v| v.as_str().map(|s| s.to_string()))
      .collect::<Option<Vec<_>>>()?;
    let threshold = policy.get("threshold")?.as_u64()? as usize;
    if self.valid_perp_signature_count(evidence.get("sigs")?, &signers, &msg_hash) < threshold {
      return None;
    }
    Some(PerpExternalEvidenceValidation {
      kind: "external-lock".to_string(),
      id,
      position_id,
      position: None,
      group: group.clone(),
      collateral: collateral.value.clone(),
      mode: evidence.get("mode")?.as_str()?.to_string(),
      surface: surface.clone(),
      ext: ext.clone(),
      amount,
      notional,
      leverage,
      equity: BigInt::from(0),
      bounty: BigInt::from(0),
      nonce_key,
      sequence_key,
      evidence: serde_json::json!({
        "v": "1",
        "dom": evidence.get("dom")?.as_str()?,
        "net": evidence.get("net")?.as_str()?,
        "policy": policy.get("id")?.as_str()?,
        "pid": policy.get("id")?.as_str()?,
        "ph": policy.get("hash")?.as_str()?,
        "group": group.get("id")?.as_str()?,
        "gid": group.get("id")?.as_str()?,
        "gh": group.get("gh")?.as_str()?,
        "purpose": purpose,
        "payload_hash": evidence_payload_hash,
        "state_hash": payload_hash,
        "seq": evidence.get("seq")?.as_str()?,
        "valid_from": valid_from,
        "valid_until": valid_until,
        "collateral": collateral.value,
        "mode": evidence.get("mode")?.as_str()?,
        "surface": surface,
        "ext": ext
      }),
    })
  }

  fn process_perp_external_evidence_action(
    &mut self,
    action: &serde_json::Value,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    action_index: usize,
  ) -> bool {
    let Some(normalized) =
      self.validate_perp_external_evidence_action(action, inscription, action_index, block)
    else {
      return false;
    };
    let mut group = normalized.group;
    let group_id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if normalized.kind == "external-close" || normalized.kind == "external-liquidation" {
      let Some(mut position) = normalized.position.clone() else {
        return false;
      };
      let pos_id = position
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
      let open_collateral = position
        .get("open_collateral")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_default();
      let closed_after = position
        .get("closed_equity")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_default()
        + normalized.equity.clone();
      let event_key = if normalized.kind == "external-close" {
        "close"
      } else {
        "liquidate"
      };
      let mut external = position
        .get("external")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
      if let Some(external_map) = external.as_object_mut() {
        external_map.insert(event_key.to_string(), normalized.ext.clone());
      }
      if let Some(map) = position.as_object_mut() {
        map.insert(
          "closed_equity".to_string(),
          serde_json::json!(closed_after.to_string()),
        );
        map.insert("open_collateral".to_string(), serde_json::json!("0"));
        map.insert(
          "state".to_string(),
          serde_json::json!(if normalized.kind == "external-close" {
            "closed"
          } else {
            "liquidated"
          }),
        );
        map.insert("external".to_string(), external);
        map.insert(
          if event_key == "close" { "last_close" } else { "liquidated" }.to_string(),
          serde_json::json!({
            "amt": open_collateral.to_string(),
            "equity": normalized.equity.to_string(),
            "price": normalized.ext.get("price").cloned().unwrap_or_else(|| serde_json::json!({})),
            "blck": block,
            "tx": transaction,
            "vo": vout,
            "val": value.to_string(),
            "ins": inscription,
            "num": number,
            "ts": timestamp,
            "evidence": normalized.id
          }),
        );
      }
      let side_key = format!(
        "{}_open_collateral",
        position.get("side").and_then(|v| v.as_str()).unwrap_or("")
      );
      if !Self::sub_perp_group_amount(&mut group, &side_key, &open_collateral)
        || !Self::sub_perp_group_amount(&mut group, "total_open_collateral", &open_collateral)
      {
        return false;
      }
      Self::add_perp_group_amount(&mut group, "closed_equity_total", &normalized.equity);
      if normalized.kind == "external-liquidation" {
        Self::add_perp_group_amount(&mut group, "liquidated_equity_total", &normalized.equity);
      }
      if normalized.bounty > BigInt::from(0) {
        if !Self::sub_perp_group_amount(&mut group, "total_collateral", &normalized.bounty) {
          return false;
        }
        let mut paid = group
          .get("bounty_paid")
          .cloned()
          .unwrap_or_else(|| serde_json::json!({ "activate": "0", "liquidate": "0", "settle": "0" }));
        let next = paid
          .get("liquidate")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_default()
          + normalized.bounty.clone();
        if let Some(paid_map) = paid.as_object_mut() {
          paid_map.insert("liquidate".to_string(), serde_json::json!(next.to_string()));
        }
        if let Some(map) = group.as_object_mut() {
          map.insert("bounty_paid".to_string(), paid);
        }
      }
      if let Some(map) = group.as_object_mut() {
        map.insert(
          "mark_price".to_string(),
          normalized.ext.get("price").cloned().unwrap_or_else(|| serde_json::json!({})),
        );
      }
      let mut evidence_record = normalized.evidence.clone();
      if let Some(map) = evidence_record.as_object_mut() {
        map.insert("id".to_string(), serde_json::json!(normalized.id.clone()));
        map.insert("position".to_string(), serde_json::json!(pos_id.clone()));
        map.insert("blck".to_string(), serde_json::json!(block));
        map.insert("tx".to_string(), serde_json::json!(transaction));
        map.insert("vo".to_string(), serde_json::json!(vout));
        map.insert("val".to_string(), serde_json::json!(value.to_string()));
        map.insert("ins".to_string(), serde_json::json!(inscription));
        map.insert("num".to_string(), serde_json::json!(number));
        map.insert("ts".to_string(), serde_json::json!(timestamp));
      }
      let _ = self.tap_put(&normalized.nonce_key, &"".to_string());
      let _ = self.tap_put(
        &normalized.sequence_key,
        &normalized
          .evidence
          .get("seq")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .to_string(),
      );
      let _ = self.tap_put(&format!("perp/e/{}", normalized.id), &evidence_record);
      let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
      let _ = self.tap_put(&format!("perp/pos/{}", pos_id), &position);
      let _ = self.tap_set_list_record("perp/el", "perp/eli", &evidence_record);
      let _ = self.tap_set_list_record(
        &format!("perp/eg/{}", group_id),
        &format!("perp/egi/{}", group_id),
        &normalized.id,
      );
      let _ = self.tap_set_list_record(
        &format!("perp/ep/{}", pos_id),
        &format!("perp/epi/{}", pos_id),
        &normalized.id,
      );
      if let Some(cid) = normalized.collateral.get("cid").and_then(|v| v.as_str()) {
        let _ = self.tap_set_list_record(
          &format!("perp/ec/{}", cid),
          &format!("perp/eci/{}", cid),
          &normalized.id,
        );
      }
      let _ = self.tap_set_list_record(
        &format!("tx/perp/evidence/{}", transaction),
        &format!("txi/perp/evidence/{}", transaction),
        &normalized.id,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/perp/evidence/{}", block),
        &format!("blcki/perp/evidence/{}", block),
        &normalized.id,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/perp/{}/{}", event_key, block),
        &format!("blcki/perp/{}/{}", event_key, block),
        &pos_id,
      );
      if normalized.kind == "external-liquidation" {
        let _ = self.tap_set_list_record("perp/ll", "perp/lli", &position);
      }
      self.record_perp_event(
        "external-evidence",
        &normalized.id,
        &evidence_record,
        block,
        transaction,
        vout,
        value,
        inscription,
        number,
        timestamp,
      );
      self.record_perp_event(
        event_key,
        &pos_id,
        &position,
        block,
        transaction,
        vout,
        value,
        inscription,
        number,
        timestamp,
      );
      return true;
    }
    let mut position = serde_json::json!({
      "id": normalized.position_id,
      "group": group_id,
      "owner": normalized.ext.get("owner").and_then(|v| v.as_str()).unwrap_or(""),
      "side": normalized.ext.get("side").and_then(|v| v.as_str()).unwrap_or(""),
      "collateral_asset": normalized.collateral,
      "collateral_mode": normalized.mode,
      "settlement_surface": normalized.surface,
      "external": normalized.ext,
      "collateral": normalized.amount.to_string(),
      "open_collateral": normalized.amount.to_string(),
      "closed_equity": "0",
      "leverage": Self::serialize_perp_ratio(&normalized.leverage),
      "notional": normalized.notional.to_string(),
      "claim": { "tt": "x", "to": normalized.ext.get("claim").and_then(|v| v.as_str()).unwrap_or("") },
      "refund": { "tt": "x", "to": normalized.ext.get("refund").and_then(|v| v.as_str()).unwrap_or("") },
      "state": "formation",
      "payout": "0",
      "claimed": false,
      "refunded": false
    });
    if let Some(entry) = normalized.ext.get("entry") {
      if let Some(map) = position.as_object_mut() {
        map.insert("entry".to_string(), entry.clone());
      }
    }
    let add_str = |group: &mut serde_json::Value, key: &str, amount: &BigInt| {
      let current = group
        .get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_else(|| BigInt::from(0));
      if let Some(map) = group.as_object_mut() {
        map.insert(
          key.to_string(),
          serde_json::Value::String((current + amount).to_string()),
        );
      }
    };
    add_str(&mut group, "total_collateral", &normalized.amount);
    add_str(&mut group, "total_notional", &normalized.notional);
    if position.get("side").and_then(|v| v.as_str()) == Some("long") {
      add_str(&mut group, "long_collateral", &normalized.amount);
      add_str(&mut group, "long_open_collateral", &normalized.amount);
      add_str(&mut group, "long_notional", &normalized.notional);
    } else {
      add_str(&mut group, "short_collateral", &normalized.amount);
      add_str(&mut group, "short_open_collateral", &normalized.amount);
      add_str(&mut group, "short_notional", &normalized.notional);
    }
    add_str(&mut group, "total_open_collateral", &normalized.amount);
    add_str(&mut group, "positions", &BigInt::from(1));
    if let Some(entry) = position.get("entry") {
      let _ = Self::update_perp_group_entry_bounds(&mut group, Some(entry));
    }
    let mut evidence_record = normalized.evidence.clone();
    if let Some(map) = evidence_record.as_object_mut() {
      map.insert("id".to_string(), serde_json::json!(normalized.id.clone()));
      map.insert(
        "position".to_string(),
        serde_json::json!(position.get("id").and_then(|v| v.as_str()).unwrap_or("")),
      );
      map.insert("blck".to_string(), serde_json::json!(block));
      map.insert("tx".to_string(), serde_json::json!(transaction));
      map.insert("vo".to_string(), serde_json::json!(vout));
      map.insert("val".to_string(), serde_json::json!(value.to_string()));
      map.insert("ins".to_string(), serde_json::json!(inscription));
      map.insert("num".to_string(), serde_json::json!(number));
      map.insert("ts".to_string(), serde_json::json!(timestamp));
    }
    let mut position_record = position.clone();
    if let Some(map) = position_record.as_object_mut() {
      map.insert("blck".to_string(), serde_json::json!(block));
      map.insert("tx".to_string(), serde_json::json!(transaction));
      map.insert("vo".to_string(), serde_json::json!(vout));
      map.insert("val".to_string(), serde_json::json!(value.to_string()));
      map.insert("ins".to_string(), serde_json::json!(inscription));
      map.insert("num".to_string(), serde_json::json!(number));
      map.insert("ts".to_string(), serde_json::json!(timestamp));
    }
    let position_id = position
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let owner = position
      .get("owner")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let _ = self.tap_put(&normalized.nonce_key, &"".to_string());
    let _ = self.tap_put(
      &normalized.sequence_key,
      &normalized
        .evidence
        .get("seq")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string(),
    );
    let _ = self.tap_put(&format!("perp/e/{}", normalized.id), &evidence_record);
    let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    let _ = self.tap_put(&format!("perp/pos/{}", position_id), &position);
    let _ = self.tap_set_list_record("perp/el", "perp/eli", &evidence_record);
    let _ = self.tap_set_list_record(
      &format!("perp/eg/{}", group_id),
      &format!("perp/egi/{}", group_id),
      &normalized.id,
    );
    let _ = self.tap_set_list_record(
      &format!("perp/ep/{}", position_id),
      &format!("perp/epi/{}", position_id),
      &normalized.id,
    );
    if let Some(cid) = normalized.collateral.get("cid").and_then(|v| v.as_str()) {
      let _ = self.tap_set_list_record(
        &format!("perp/ec/{}", cid),
        &format!("perp/eci/{}", cid),
        &normalized.id,
      );
    }
    let _ = self.tap_set_list_record(
      &format!("perp/pgl/{}", group_id),
      &format!("perp/pgli/{}", group_id),
      &position_id,
    );
    let _ = self.tap_set_list_record(
      &format!("perp/pa/{}", owner),
      &format!("perp/pai/{}", owner),
      &position_id,
    );
    if self
      .tap_get::<String>(&format!("perp/gae/{}/{}", owner, group_id))
      .ok()
      .flatten()
      .is_none()
    {
      let _ = self.tap_put(&format!("perp/gae/{}/{}", owner, group_id), &"".to_string());
      let _ = self.tap_set_list_record(
        &format!("perp/ga/{}", owner),
        &format!("perp/gai/{}", owner),
        &group_id,
      );
    }
    if let Ok(list_len) = self.tap_set_list_record("perp/posl", "perp/posli", &position_record) {
      let ptr = format!("perp/posli/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/perp/join/{}", transaction),
        &format!("txi/perp/join/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/perp/join/{}", block),
        &format!("blcki/perp/join/{}", block),
        &ptr,
      );
    }
    let _ = self.tap_set_list_record(
      &format!("tx/perp/evidence/{}", transaction),
      &format!("txi/perp/evidence/{}", transaction),
      &normalized.id,
    );
    let _ = self.tap_set_list_record(
      &format!("blck/perp/evidence/{}", block),
      &format!("blcki/perp/evidence/{}", block),
      &normalized.id,
    );
    self.record_perp_event(
      "external-evidence",
      &normalized.id,
      &evidence_record,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    self.record_perp_event(
      "join",
      &position_id,
      &position,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn compute_perp_equity(
    collateral: &BigInt,
    leverage_text: &str,
    side: &str,
    entry_price: &serde_json::Value,
    price: &serde_json::Value,
  ) -> Option<BigInt> {
    if entry_price.is_null() {
      return None;
    }
    let leverage =
      Self::parse_perp_ratio(&serde_json::Value::String(leverage_text.to_string()), false)?;
    let delta = price.get("p")?.as_str()?.parse::<BigInt>().ok()?
      * entry_price.get("q")?.as_str()?.parse::<BigInt>().ok()?
      - entry_price.get("p")?.as_str()?.parse::<BigInt>().ok()?
        * price.get("q")?.as_str()?.parse::<BigInt>().ok()?;
    let signed_delta = if side == "short" { -delta } else { delta };
    let numerator = collateral * leverage.0 * signed_delta;
    let denominator = leverage.1
      * price.get("q")?.as_str()?.parse::<BigInt>().ok()?
      * entry_price.get("p")?.as_str()?.parse::<BigInt>().ok()?;
    if denominator <= BigInt::from(0) {
      return None;
    }
    let equity = collateral + numerator / denominator;
    Some(if equity < BigInt::from(0) {
      BigInt::from(0)
    } else {
      equity
    })
  }

  fn pay_perp_authority_to_account(
    &mut self,
    group: &serde_json::Value,
    to_address: &str,
    amount: &BigInt,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    role: &str,
    reference: &str,
  ) -> bool {
    if amount <= &BigInt::from(0) {
      return true;
    }
    let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(tick_key) = group
      .get("collateral")
      .and_then(|v| v.get("tick_key"))
      .and_then(|v| v.as_str())
    else {
      return false;
    };
    let Some(tick) = group
      .get("collateral")
      .and_then(|v| v.get("tick"))
      .and_then(|v| v.as_str())
    else {
      return false;
    };
    let authority_balance = self.tap_get_authority_balance_bigint(group_id, tick_key);
    if authority_balance < *amount {
      return false;
    }
    let receiver_balance = self.tap_get_address_balance_bigint(to_address, tick_key) + amount;
    if !self.tap_set_authority_balance_bigint(
      group_id,
      tick_key,
      &(authority_balance.clone() - amount),
    ) || !self.tap_put_address_balance_bigint(to_address, tick_key, tick, &receiver_balance)
    {
      return false;
    }
    self.tap_apply_authority_claim_transfer_logs(
      tick,
      tick_key,
      group_id,
      to_address,
      (authority_balance - amount)
        .to_string()
        .parse::<i128>()
        .ok()
        .unwrap_or(0),
      receiver_balance
        .to_string()
        .parse::<i128>()
        .ok()
        .unwrap_or(0),
      amount.to_string().parse::<i128>().ok().unwrap_or(0),
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      role,
      reference,
    );
    true
  }

  fn pay_perp_authority_to_staking_reward(
    &mut self,
    group: &serde_json::Value,
    auth_id: &str,
    amount: &BigInt,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
    reference: &str,
  ) -> bool {
    if amount <= &BigInt::from(0) {
      return true;
    }
    let Some(group_id) = group.get("id").and_then(|v| v.as_str()) else {
      return false;
    };
    let Some(tick_key) = group
      .get("collateral")
      .and_then(|v| v.get("tick_key"))
      .and_then(|v| v.as_str())
    else {
      return false;
    };
    let Some(tick) = group
      .get("collateral")
      .and_then(|v| v.get("tick"))
      .and_then(|v| v.as_str())
    else {
      return false;
    };
    let authority_balance = self.tap_get_authority_balance_bigint(group_id, tick_key);
    if authority_balance < *amount {
      return false;
    }
    let group_after = authority_balance.clone() - amount;
    if !self.tap_set_authority_balance_bigint(group_id, tick_key, &group_after)
      || !self.tap_add_authority_balance_bigint(auth_id, tick_key, amount)
      || !self.tap_apply_authority_reward_allocation(
        auth_id,
        tick_key,
        amount.to_string().parse::<i128>().ok().unwrap_or(0),
      )
    {
      return false;
    }
    let receiver_after = self.tap_get_authority_balance_bigint(auth_id, tick_key);
    self.tap_apply_authority_transfer_logs(
      tick,
      tick_key,
      group_id,
      auth_id,
      0,
      group_after.to_string().parse::<i128>().ok().unwrap_or(0),
      receiver_after.to_string().parse::<i128>().ok().unwrap_or(0),
      amount.to_string().parse::<i128>().ok().unwrap_or(0),
      block,
      inscription,
      number,
      timestamp,
      transaction,
      vout,
      value,
      "sr",
      reference,
    );
    true
  }

  fn validate_perp_activate_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
  ) -> Option<PerpActivateValidation> {
    if !self.perp_groups_enabled() || action.get("op")?.as_str()?.to_lowercase() != "perp-activate"
    {
      return None;
    }
    let group = self.get_perp_group(action.get("gid")?.as_str()?)?;
    if group.get("state").and_then(|v| v.as_str()) != Some("formation")
      || !Self::perp_group_ready(&group)
    {
      return None;
    }
    let certificate = self.validate_perp_price_certificate(action, &group, "entry", block)?;
    if !Self::perp_entry_bounds_allow_price(&group, &certificate.price) {
      return None;
    }
    let bounty = self.payable_perp_bounty(&group, "activate", link.is_some());
    if group.get("collateral")?.get("ty")?.as_str()? == "tap" {
      if !self.validate_perp_group_fee_receivers(&group) {
        return None;
      }
    }
    Some(PerpActivateValidation {
      group,
      certificate,
      bounty,
    })
  }

  fn process_perp_activate_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_perp_activate_action(action, link, block) else {
      return false;
    };
    let mut group = normalized.group;
    let group_id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if let Some(map) = group.as_object_mut() {
      map.insert("state".to_string(), serde_json::json!("active"));
      map.insert(
        "entry_price".to_string(),
        normalized.certificate.price.clone(),
      );
      map.insert(
        "mark_price".to_string(),
        normalized.certificate.price.clone(),
      );
      map.insert("mark_cert".to_string(), normalized.certificate.cert.clone());
      map.insert("activated".to_string(), serde_json::json!({ "blck": block, "tx": transaction, "vo": vout, "val": value.to_string(), "ins": inscription, "num": number, "ts": timestamp, "cert": normalized.certificate.cert }));
    }
    let _ = self.tap_put(&normalized.certificate.nonce_key, &"".to_string());
    self.record_perp_certificate(
      &normalized.certificate,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    let _ = self.tap_set_list_record("perp/gs/active", "perp/gsi/active", &group_id);
    let _ = self.tap_set_list_record(
      &format!("blck/perp/activate/{}", block),
      &format!("blcki/perp/activate/{}", block),
      &group_id,
    );
    if normalized.bounty > BigInt::from(0) {
      if let Some(link) = link {
        if !self.pay_perp_authority_to_account(
          &group,
          &link.addr,
          &normalized.bounty,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          "pba",
          &group_id,
        ) {
          return false;
        }
        if !Self::sub_perp_group_amount(&mut group, "bounty_reserve", &normalized.bounty) {
          return false;
        }
        let mut paid = group
          .get("bounty_paid")
          .cloned()
          .unwrap_or_else(|| serde_json::json!({ "activate": "0", "liquidate": "0", "settle": "0" }));
        let next = paid
          .get("activate")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_default()
          + normalized.bounty.clone();
        if let Some(map) = paid.as_object_mut() {
          map.insert("activate".to_string(), serde_json::json!(next.to_string()));
        }
        if let Some(map) = group.as_object_mut() {
          map.insert("bounty_paid".to_string(), paid);
        }
        let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
        self.record_perp_bounty(
          &group,
          &link.addr,
          &normalized.bounty,
          "activate",
          &group_id,
          block,
          transaction,
          vout,
          value,
          inscription,
          number,
          timestamp,
        );
      }
    }
    self.record_perp_event(
      "activate",
      &group_id,
      &group,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn validate_perp_close_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
  ) -> Option<PerpPositionValidation> {
    let link = link?;
    if !self.perp_groups_enabled()
      || action.get("op")?.as_str()?.to_lowercase() != "perp-close"
      || !action.get("qty")?.is_object()
    {
      return None;
    }
    let position = self.get_perp_position(action.get("pos")?.as_str()?)?;
    let group = self.get_perp_group(position.get("group")?.as_str()?)?;
    if group.get("id")?.as_str()? != action.get("gid")?.as_str()?
      || group.get("state").and_then(|v| v.as_str()) != Some("active")
      || !Self::perp_position_active_state(&position, &group)
      || position.get("owner").and_then(|v| v.as_str()) != Some(link.addr.as_str())
    {
      return None;
    }
    let open_collateral = position
      .get("open_collateral")?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    let qty = action.get("qty")?;
    if qty.get("mode")?.as_str()? != "fraction" {
      return None;
    }
    let qty_n = Self::parse_perp_uint(qty.get("n")?, false)?;
    let qty_d = Self::parse_perp_uint(qty.get("d")?, false)?;
    let amount = &open_collateral * qty_n / qty_d;
    if amount <= BigInt::from(0) || amount > open_collateral {
      return None;
    }
    let certificate = self.validate_perp_price_certificate(action, &group, "close", block)?;
    let equity = Self::compute_perp_equity(
      &amount,
      position.get("leverage")?.as_str()?,
      position.get("side")?.as_str()?,
      group.get("entry_price")?,
      &certificate.price,
    )?;
    Some(PerpPositionValidation {
      group,
      position,
      amount,
      equity,
      certificate,
    })
  }

  fn process_perp_close_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_perp_close_action(action, link, block) else {
      return false;
    };
    let mut group = normalized.group;
    let group_id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let mut position = normalized.position;
    let pos_id = position
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let open_after = position
      .get("open_collateral")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_default()
      - normalized.amount.clone();
    let closed_after = position
      .get("closed_equity")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_default()
      + normalized.equity.clone();
    if let Some(map) = position.as_object_mut() {
      map.insert(
        "open_collateral".to_string(),
        serde_json::json!(open_after.to_string()),
      );
      map.insert(
        "closed_equity".to_string(),
        serde_json::json!(closed_after.to_string()),
      );
      if open_after == BigInt::from(0) {
        map.insert("state".to_string(), serde_json::json!("closed"));
      } else {
        map.insert("state".to_string(), serde_json::json!("active"));
      }
      map.insert("last_close".to_string(), serde_json::json!({ "amt": normalized.amount.to_string(), "equity": normalized.equity.to_string(), "price": normalized.certificate.price, "blck": block, "tx": transaction, "vo": vout, "val": value.to_string(), "ins": inscription, "num": number, "ts": timestamp, "cert": normalized.certificate.cert }));
    }
    let side_key = format!(
      "{}_open_collateral",
      position.get("side").and_then(|v| v.as_str()).unwrap_or("")
    );
    if !Self::sub_perp_group_amount(&mut group, &side_key, &normalized.amount)
      || !Self::sub_perp_group_amount(&mut group, "total_open_collateral", &normalized.amount)
    {
      return false;
    }
    Self::add_perp_group_amount(&mut group, "closed_equity_total", &normalized.equity);
    let _ = self.tap_put(&normalized.certificate.nonce_key, &"".to_string());
    self.record_perp_certificate(
      &normalized.certificate,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    if let Some(map) = group.as_object_mut() {
      map.insert(
        "mark_price".to_string(),
        normalized.certificate.price.clone(),
      );
      map.insert("mark_cert".to_string(), normalized.certificate.cert.clone());
    }
    let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    let _ = self.tap_put(&format!("perp/pos/{}", pos_id), &position);
    let _ = self.tap_set_list_record(
      &format!("blck/perp/close/{}", block),
      &format!("blcki/perp/close/{}", block),
      &pos_id,
    );
    self.record_perp_event(
      "close",
      &pos_id,
      &position,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn validate_perp_liquidate_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
  ) -> Option<PerpLiquidateValidation> {
    if !self.perp_groups_enabled() || action.get("op")?.as_str()?.to_lowercase() != "perp-liquidate"
    {
      return None;
    }
    let position = self.get_perp_position(action.get("pos")?.as_str()?)?;
    let group = self.get_perp_group(position.get("group")?.as_str()?)?;
    if group.get("id")?.as_str()? != action.get("gid")?.as_str()?
      || group.get("state").and_then(|v| v.as_str()) != Some("active")
      || !Self::perp_position_active_state(&position, &group)
    {
      return None;
    }
    let certificate = self.validate_perp_price_certificate(action, &group, "liquidation", block)?;
    let open_collateral = position
      .get("open_collateral")?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    let equity = Self::compute_perp_equity(
      &open_collateral,
      position.get("leverage")?.as_str()?,
      position.get("side")?.as_str()?,
      group.get("entry_price")?,
      &certificate.price,
    )?;
    let maintenance_bps = group
      .get("maintenance_bps")?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    if equity.clone() * BigInt::from(10_000) > open_collateral * maintenance_bps {
      return None;
    }
    let bounty = self.payable_perp_bounty(&group, "liquidate", link.is_some());
    Some(PerpLiquidateValidation {
      group,
      position,
      equity,
      certificate,
      bounty,
    })
  }

  fn process_perp_liquidate_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_perp_liquidate_action(action, link, block) else {
      return false;
    };
    let mut group = normalized.group;
    let group_id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let mut position = normalized.position;
    let pos_id = position
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let open_collateral = position
      .get("open_collateral")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_default();
    let closed_after = position
      .get("closed_equity")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_default()
      + normalized.equity.clone();
    if let Some(map) = position.as_object_mut() {
      map.insert(
        "closed_equity".to_string(),
        serde_json::json!(closed_after.to_string()),
      );
      map.insert("open_collateral".to_string(), serde_json::json!("0"));
      map.insert("state".to_string(), serde_json::json!("liquidated"));
      map.insert("liquidated".to_string(), serde_json::json!({ "equity": normalized.equity.to_string(), "price": normalized.certificate.price, "blck": block, "tx": transaction, "vo": vout, "val": value.to_string(), "ins": inscription, "num": number, "ts": timestamp, "cert": normalized.certificate.cert }));
    }
    let side_key = format!(
      "{}_open_collateral",
      position.get("side").and_then(|v| v.as_str()).unwrap_or("")
    );
    if !Self::sub_perp_group_amount(&mut group, &side_key, &open_collateral)
      || !Self::sub_perp_group_amount(&mut group, "total_open_collateral", &open_collateral)
    {
      return false;
    }
    Self::add_perp_group_amount(&mut group, "closed_equity_total", &normalized.equity);
    Self::add_perp_group_amount(&mut group, "liquidated_equity_total", &normalized.equity);
    let _ = self.tap_put(&normalized.certificate.nonce_key, &"".to_string());
    self.record_perp_certificate(
      &normalized.certificate,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    if let Some(map) = group.as_object_mut() {
      map.insert(
        "mark_price".to_string(),
        normalized.certificate.price.clone(),
      );
      map.insert("mark_cert".to_string(), normalized.certificate.cert.clone());
    }
    let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    let _ = self.tap_put(&format!("perp/pos/{}", pos_id), &position);
    let _ = self.tap_set_list_record("perp/ll", "perp/lli", &position);
    let _ = self.tap_set_list_record(
      &format!("blck/perp/liquidate/{}", block),
      &format!("blcki/perp/liquidate/{}", block),
      &pos_id,
    );
    if normalized.bounty > BigInt::from(0) {
      if let Some(link) = link {
        if !self.pay_perp_authority_to_account(
          &group,
          &link.addr,
          &normalized.bounty,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          "pbl",
          &pos_id,
        ) {
          return false;
        }
        if !Self::sub_perp_group_amount(&mut group, "bounty_reserve", &normalized.bounty) {
          return false;
        }
        let mut paid = group
          .get("bounty_paid")
          .cloned()
          .unwrap_or_else(|| serde_json::json!({ "activate": "0", "liquidate": "0", "settle": "0" }));
        let next = paid
          .get("liquidate")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_default()
          + normalized.bounty.clone();
        if let Some(map) = paid.as_object_mut() {
          map.insert("liquidate".to_string(), serde_json::json!(next.to_string()));
        }
        if let Some(map) = group.as_object_mut() {
          map.insert("bounty_paid".to_string(), paid);
        }
        let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
        self.record_perp_bounty(
          &group,
          &link.addr,
          &normalized.bounty,
          "liquidate",
          &pos_id,
          block,
          transaction,
          vout,
          value,
          inscription,
          number,
          timestamp,
        );
      }
    }
    self.record_perp_event(
      "liquidate",
      &pos_id,
      &position,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn validate_perp_settle_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    block: u32,
  ) -> Option<PerpSettleValidation> {
    if !self.perp_groups_enabled() || action.get("op")?.as_str()?.to_lowercase() != "perp-settle" {
      return None;
    }
    let group = self.get_perp_group(action.get("gid")?.as_str()?)?;
    if group.get("state").and_then(|v| v.as_str()) != Some("active")
      || block < group.get("expiry")?.as_u64()? as u32
    {
      return None;
    }
    let certificate = self
      .validate_perp_price_certificate(action, &group, "settlement", block)
      .or_else(|| self.validate_perp_settlement_fallback(action, &group, block))?;
    let aggregate = Self::compute_perp_settlement_aggregate(&group, &certificate)?;
    let bounty = self.payable_perp_bounty(&group, "settle", link.is_some());
    let collateral_ty = group.get("collateral")?.get("ty")?.as_str()?;
    if collateral_ty == "tap" && !self.validate_perp_group_fee_receivers(&group) {
      return None;
    }
    let total_equity = aggregate.total_equity.clone();
    Some(PerpSettleValidation {
      group,
      certificate,
      aggregate,
      total_equity,
      bounty,
    })
  }

  fn process_perp_settle_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_perp_settle_action(action, link, block) else {
      return false;
    };
    let mut group = normalized.group;
    let group_id = group
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let tick_key = group
      .get("collateral")
      .and_then(|v| v.get("tick_key"))
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let collateral_ty = group
      .get("collateral")
      .and_then(|v| v.get("ty"))
      .and_then(|v| v.as_str())
      .unwrap_or("");
    let collateral_ty = collateral_ty.to_string();
    let authority_balance = if collateral_ty == "tap" {
      self.tap_get_authority_balance_bigint(&group_id, &tick_key)
    } else {
      group
        .get("total_collateral")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_else(|| BigInt::from(0))
    };
    let fee_bps = group
      .get("fee")
      .and_then(|v| v.get("bps"))
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_default();
    let bounty = normalized.bounty.clone();
    let external_fallback = normalized.aggregate.external_fallback;
    let mut fee_amount = if external_fallback {
      BigInt::from(0)
    } else {
      &normalized.total_equity * &fee_bps / BigInt::from(10_000)
    };
    let settlement_balance = &authority_balance - &bounty;
    if &normalized.total_equity + &fee_amount > settlement_balance {
      let claim_pool =
        &settlement_balance * (BigInt::from(10_000) - &fee_bps) / BigInt::from(10_000);
      fee_amount = &settlement_balance - &claim_pool;
    }
    let claim_pool = &settlement_balance - &fee_amount;
    let claim_basis_total = if normalized.total_equity > BigInt::from(0) {
      normalized.total_equity.clone()
    } else {
      group
        .get("total_collateral")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<BigInt>().ok())
        .unwrap_or_else(|| BigInt::from(0))
    };
    let fee_receivers = group
      .get("fee")
      .and_then(|v| v.get("receivers"))
      .and_then(|v| v.as_array())
      .cloned()
      .unwrap_or_default();
    let fee_splits = Self::split_perp_fee_receivers(&fee_amount, &fee_receivers);
    let defaulted = normalized.total_equity > settlement_balance;
    let pro_rata = claim_pool != claim_basis_total;
    let new_state = if defaulted { "defaulted" } else { "settled" };
    if let Some(map) = group.as_object_mut() {
      map.insert("state".to_string(), serde_json::json!(new_state));
      map.insert(
        "final_price".to_string(),
        normalized.certificate.price.clone(),
      );
      map.insert(
        "settlement".to_string(),
        serde_json::json!({
          "total_equity": normalized.total_equity.to_string(),
          "claim_pool": claim_pool.to_string(),
          "claim_basis_total": claim_basis_total.to_string(),
          "claim_basis_remaining": claim_basis_total.to_string(),
          "claim_pool_remaining": claim_pool.to_string(),
          "assigned": "0",
          "claimed": "0",
          "fee": fee_amount.to_string(),
          "fees": fee_splits.iter().map(|(receiver, amount)| serde_json::json!({
            "tt": receiver.get("tt").and_then(|v| v.as_str()).unwrap_or(""),
            "to": receiver.get("to").and_then(|v| v.as_str()).unwrap_or(""),
            "rl": receiver.get("rl").and_then(|v| v.as_str()).unwrap_or(""),
            "share": receiver.get("share").and_then(|v| v.as_str()).unwrap_or(""),
            "amt": amount.to_string()
          })).collect::<Vec<_>>(),
          "residual": "0",
          "pro_rata": pro_rata,
          "long_open_collateral": normalized.aggregate.long_open_collateral.to_string(),
          "short_open_collateral": normalized.aggregate.short_open_collateral.to_string(),
          "long_open_equity": normalized.aggregate.long_open_equity.to_string(),
          "short_open_equity": normalized.aggregate.short_open_equity.to_string(),
          "closed_equity": normalized.aggregate.closed_equity.to_string(),
          "defaulted": defaulted,
          "blck": block,
          "tx": transaction,
          "vo": vout,
          "val": value.to_string(),
          "ins": inscription,
          "num": number,
          "ts": timestamp,
          "cert": normalized.certificate.cert
        }),
      );
    }
    let _ = self.tap_put(&normalized.certificate.nonce_key, &"".to_string());
    if normalized.certificate.signed {
      self.record_perp_certificate(
        &normalized.certificate,
        block,
        transaction,
        vout,
        value,
        inscription,
        number,
        timestamp,
      );
    }
    let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    let _ = self.tap_set_list_record(
      &format!("perp/gs/{}", new_state),
      &format!("perp/gsi/{}", new_state),
      &group_id,
    );
    let _ = self.tap_set_list_record(
      &format!("blck/perp/settle/{}", block),
      &format!("blcki/perp/settle/{}", block),
      &group_id,
    );
    self.record_perp_settlement(
      &group,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    if fee_amount > BigInt::from(0) && collateral_ty == "tap" {
      for (receiver, amount) in fee_splits {
        if amount <= BigInt::from(0) {
          continue;
        }
        let Some(tt) = receiver.get("tt").and_then(|v| v.as_str()) else {
          return false;
        };
        let Some(to) = receiver.get("to").and_then(|v| v.as_str()) else {
          return false;
        };
        let role = receiver.get("rl").and_then(|v| v.as_str()).unwrap_or("pf");
        if tt == "a" {
          if !self.pay_perp_authority_to_account(
            &group,
            to,
            &amount,
            transaction,
            vout,
            value,
            inscription,
            number,
            block,
            timestamp,
            role,
            &group_id,
          ) {
            return false;
          }
        } else if tt == "h" && role == "sr" {
          if !self.pay_perp_authority_to_staking_reward(
            &group,
            to,
            &amount,
            transaction,
            vout,
            value,
            inscription,
            number,
            block,
            timestamp,
            &group_id,
          ) {
            return false;
          }
        } else {
          return false;
        }
      }
    }
    if bounty > BigInt::from(0) && collateral_ty == "tap" {
      if let Some(link) = link {
        if !self.pay_perp_authority_to_account(
          &group,
          &link.addr,
          &bounty,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          "pbs",
          &group_id,
        ) {
          return false;
        }
        if !Self::sub_perp_group_amount(&mut group, "bounty_reserve", &bounty) {
          return false;
        }
        let mut paid = group
          .get("bounty_paid")
          .cloned()
          .unwrap_or_else(|| serde_json::json!({ "activate": "0", "liquidate": "0", "settle": "0" }));
        let next = paid
          .get("settle")
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_default()
          + bounty.clone();
        if let Some(map) = paid.as_object_mut() {
          map.insert("settle".to_string(), serde_json::json!(next.to_string()));
        }
        if let Some(map) = group.as_object_mut() {
          map.insert("bounty_paid".to_string(), paid);
        }
        let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
        self.record_perp_bounty(
          &group,
          &link.addr,
          &bounty,
          "settle",
          &group_id,
          block,
          transaction,
          vout,
          value,
          inscription,
          number,
          timestamp,
        );
      }
    }
    true
  }

  fn compute_perp_position_settlement_equity(
    position: &serde_json::Value,
    group: &serde_json::Value,
  ) -> Option<BigInt> {
    let settlement = group.get("settlement")?;
    let mut equity = position
      .get("closed_equity")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    let open_collateral = position
      .get("open_collateral")
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_else(|| BigInt::from(0));
    if open_collateral > BigInt::from(0) {
      let side = position.get("side")?.as_str()?;
      let side_collateral_key = format!("{}_open_collateral", side);
      let side_equity_key = format!("{}_open_equity", side);
      let side_collateral = settlement
        .get(side_collateral_key.as_str())?
        .as_str()?
        .parse::<BigInt>()
        .ok()?;
      let side_equity = settlement
        .get(side_equity_key.as_str())?
        .as_str()?
        .parse::<BigInt>()
        .ok()?;
      if side_collateral <= BigInt::from(0) {
        return None;
      }
      equity += open_collateral * side_equity / side_collateral;
    }
    Some(equity)
  }

  fn compute_perp_position_settlement_payout(
    position: &serde_json::Value,
    group: &serde_json::Value,
  ) -> Option<(BigInt, BigInt, BigInt)> {
    let equity = Self::compute_perp_position_settlement_equity(position, group)?;
    let settlement = group.get("settlement")?;
    let total_equity = settlement.get("total_equity")?.as_str()?.parse::<BigInt>().ok()?;
    let claim_basis = if total_equity > BigInt::from(0) {
      equity.clone()
    } else {
      position
        .get("collateral")?
        .as_str()?
        .parse::<BigInt>()
        .ok()?
    };
    let claim_basis_remaining = settlement
      .get("claim_basis_remaining")
      .or_else(|| settlement.get("total_equity"))?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    let claim_pool_remaining = settlement
      .get("claim_pool_remaining")
      .or_else(|| settlement.get("claim_pool"))?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    if claim_basis < BigInt::from(0) || claim_basis > claim_basis_remaining {
      return None;
    }
    let payout = if claim_basis_remaining <= BigInt::from(0)
      || claim_pool_remaining <= BigInt::from(0)
    {
      BigInt::from(0)
    } else if claim_basis == claim_basis_remaining {
      claim_pool_remaining.clone()
    } else {
      &claim_basis * claim_pool_remaining / claim_basis_remaining
    };
    Some((equity, claim_basis, payout))
  }

  fn validate_perp_claim_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
  ) -> Option<PerpPayoutValidation> {
    let link = link?;
    if !self.perp_groups_enabled() || action.get("op")?.as_str()?.to_lowercase() != "perp-claim" {
      return None;
    }
    let position = self.get_perp_position(action.get("pos")?.as_str()?)?;
    let group = self.get_perp_group(position.get("group")?.as_str()?)?;
    let group_state = group.get("state")?.as_str()?;
    if group.get("id")?.as_str()? != action.get("gid")?.as_str()?
      || group.get("collateral")?.get("ty").and_then(|v| v.as_str()) != Some("tap")
      || (group_state != "settled" && group_state != "defaulted")
      || !Self::perp_position_claimable_state(&position)
      || position
        .get("claimed")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
      || position.get("claim")?.get("to")?.as_str()? != link.addr
    {
      return None;
    }
    if let Some(requested_to) = action.get("to") {
      let target = self.normalize_perp_target(requested_to)?;
      if target.get("tt")?.as_str()? != position.get("claim")?.get("tt")?.as_str()?
        || target.get("to")?.as_str()? != position.get("claim")?.get("to")?.as_str()?
      {
        return None;
      }
    }
    let (equity, basis, amount) = Self::compute_perp_position_settlement_payout(&position, &group)?;
    let settlement = group.get("settlement")?;
    let claim_basis_remaining = settlement
      .get("claim_basis_remaining")
      .or_else(|| settlement.get("total_equity"))?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    let claim_pool_remaining = settlement
      .get("claim_pool_remaining")
      .or_else(|| settlement.get("claim_pool"))?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    if basis > claim_basis_remaining || amount > claim_pool_remaining {
      return None;
    }
    Some(PerpPayoutValidation {
      group,
      position,
      equity,
      basis,
      amount,
    })
  }

  fn process_perp_claim_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_perp_claim_action(action, link) else {
      return false;
    };
    let mut group = normalized.group;
    let mut position = normalized.position;
    let pos_id = position
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let to = position
      .get("claim")
      .and_then(|v| v.get("to"))
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if !self.pay_perp_authority_to_account(
      &group,
      &to,
      &normalized.amount,
      transaction,
      vout,
      value,
      inscription,
      number,
      block,
      timestamp,
      "pc",
      &pos_id,
    ) {
      return false;
    }
    if let Some(map) = position.as_object_mut() {
      map.insert("claimed".to_string(), serde_json::json!(true));
      map.insert("payout".to_string(), serde_json::json!(normalized.amount.to_string()));
      map.insert("state".to_string(), serde_json::json!("claimed"));
      map.insert(
        "settled".to_string(),
        serde_json::json!({ "equity": normalized.equity.to_string(), "payout": normalized.amount.to_string() }),
      );
      map.insert("claimed_at".to_string(), serde_json::json!({ "blck": block, "tx": transaction, "vo": vout, "val": value.to_string(), "ins": inscription, "num": number, "ts": timestamp }));
    }
    let next_claimed = group
      .get("settlement")
      .and_then(|v| v.get("claimed"))
      .and_then(|v| v.as_str())
      .and_then(|s| s.parse::<BigInt>().ok())
      .unwrap_or_default()
      + normalized.amount.clone();
    if let Some(map) = group.as_object_mut() {
      if let Some(settlement) = map.get_mut("settlement").and_then(|v| v.as_object_mut()) {
        let next_basis_remaining = settlement
          .get("claim_basis_remaining")
          .or_else(|| settlement.get("total_equity"))
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_default()
          - normalized.basis.clone();
        let next_pool_remaining = settlement
          .get("claim_pool_remaining")
          .or_else(|| settlement.get("claim_pool"))
          .and_then(|v| v.as_str())
          .and_then(|s| s.parse::<BigInt>().ok())
          .unwrap_or_default()
          - normalized.amount.clone();
        settlement.insert(
          "claim_basis_remaining".to_string(),
          serde_json::json!(next_basis_remaining.to_string()),
        );
        settlement.insert(
          "claim_pool_remaining".to_string(),
          serde_json::json!(next_pool_remaining.to_string()),
        );
        settlement.insert("claimed".to_string(), serde_json::json!(next_claimed.to_string()));
        settlement.insert("assigned".to_string(), serde_json::json!(next_claimed.to_string()));
      }
      map.insert("claimed_total".to_string(), serde_json::json!(next_claimed.to_string()));
    }
    let _ = self.tap_put(&format!("perp/pos/{}", pos_id), &position);
    if let Some(group_id) = group.get("id").and_then(|v| v.as_str()) {
      let _ = self.tap_put(&format!("perp/g/{}", group_id), &group);
    }
    self.record_perp_claim_or_refund(
      "claim",
      &group,
      &position,
      &normalized.amount,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn validate_perp_refund_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
  ) -> Option<PerpPayoutValidation> {
    let link = link?;
    if !self.perp_groups_enabled() || action.get("op")?.as_str()?.to_lowercase() != "perp-refund" {
      return None;
    }
    let position = self.get_perp_position(action.get("pos")?.as_str()?)?;
    let group = self.get_perp_group(position.get("group")?.as_str()?)?;
    if group.get("id")?.as_str()? != action.get("gid")?.as_str()?
      || group.get("collateral")?.get("ty").and_then(|v| v.as_str()) != Some("tap")
      || group.get("state")?.as_str()? != "cancelled"
      || position
        .get("refunded")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
      || position.get("refund")?.get("to")?.as_str()? != link.addr
    {
      return None;
    }
    if let Some(requested_to) = action.get("to") {
      let target = self.normalize_perp_target(requested_to)?;
      if target.get("tt")?.as_str()? != position.get("refund")?.get("tt")?.as_str()?
        || target.get("to")?.as_str()? != position.get("refund")?.get("to")?.as_str()?
      {
        return None;
      }
    }
    let amount = position
      .get("collateral")?
      .as_str()?
      .parse::<BigInt>()
      .ok()?;
    Some(PerpPayoutValidation {
      group,
      position,
      equity: BigInt::from(0),
      basis: BigInt::from(0),
      amount,
    })
  }

  fn process_perp_refund_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(normalized) = self.validate_perp_refund_action(action, link) else {
      return false;
    };
    let mut position = normalized.position;
    let pos_id = position
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    let to = position
      .get("refund")
      .and_then(|v| v.get("to"))
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if !self.pay_perp_authority_to_account(
      &normalized.group,
      &to,
      &normalized.amount,
      transaction,
      vout,
      value,
      inscription,
      number,
      block,
      timestamp,
      "pr",
      &pos_id,
    ) {
      return false;
    }
    if let Some(map) = position.as_object_mut() {
      map.insert("refunded".to_string(), serde_json::json!(true));
      map.insert("state".to_string(), serde_json::json!("refunded"));
      map.insert("refunded_at".to_string(), serde_json::json!({ "blck": block, "tx": transaction, "vo": vout, "val": value.to_string(), "ins": inscription, "num": number, "ts": timestamp }));
    }
    let _ = self.tap_put(&format!("perp/pos/{}", pos_id), &position);
    self.record_perp_claim_or_refund(
      "refund",
      &normalized.group,
      &position,
      &normalized.amount,
      block,
      transaction,
      vout,
      value,
      inscription,
      number,
      timestamp,
    );
    true
  }

  fn process_token_proof_delegation_cancel_action(
    &mut self,
    action: &serde_json::Value,
    link: Option<&TokenAuthCreateRecord>,
    iaddr: &str,
    redeem: &serde_json::Value,
    sig: &serde_json::Value,
    hash: &str,
    salt: &str,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let Some(link) = link else {
      return false;
    };
    let Some(cancelled) = self.validate_token_proof_delegation_cancel_action(action, Some(link))
    else {
      return false;
    };

    let rec = TokenDelegationCancelRecord {
      auth: cancelled.auth.clone(),
      nonce: cancelled.nonce.clone(),
      addr: link.addr.clone(),
      iaddr: iaddr.to_string(),
      rdm: Some(redeem.clone()),
      sig: Some(sig.clone()),
      hash: Some(hash.to_string()),
      slt: Some(salt.to_string()),
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    };

    let _ = self.tap_put(&cancelled.cancel_key, &"".to_string());
    let _ = self.tap_put(
      &format!("tdcr/{}/{}", cancelled.auth, cancelled.nonce),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("tdca/{}", link.addr),
      &format!("tdcai/{}", link.addr),
      &rec,
    );
    let _ = self.tap_set_list_record(
      &format!("tdcath/{}", cancelled.auth),
      &format!("tdcathi/{}", cancelled.auth),
      &rec,
    );
    if let Ok(list_len) = self.tap_set_list_record("sftdc", "sftdci", &rec) {
      let ptr = format!("sftdci/{}", list_len - 1);
      let _ = self.tap_set_list_record(
        &format!("tx/tdc/{}", transaction),
        &format!("txi/tdc/{}", transaction),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/tdc/{}", block),
        &format!("blcki/tdc/{}", block),
        &ptr,
      );
    }

    true
  }

  fn process_token_proof_actions(
    &mut self,
    actions: &mut [serde_json::Value],
    link: Option<&TokenAuthCreateRecord>,
    iaddr: &str,
    redeem: &serde_json::Value,
    sig: &serde_json::Value,
    hash: &str,
    salt: &str,
    transaction: &str,
    vout: u32,
    value: u64,
    inscription: &str,
    number: i32,
    block: u32,
    timestamp: u32,
  ) -> bool {
    let started_atomic = self.tap_atomic_overlay.is_none();
    if started_atomic {
      self.tap_atomic_begin();
    }
    macro_rules! fail {
      () => {{
        if started_atomic {
          self.tap_atomic_abort();
        }
        return false;
      }};
    }
    if !self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation) {
      if started_atomic && self.tap_atomic_commit().is_err() {
        self.tap_atomic_abort();
        return false;
      }
      return true;
    }
    macro_rules! processed {
      ($expr:expr) => {
        if !$expr {
          fail!();
        }
      };
    }
    for (i, action) in actions.iter_mut().enumerate() {
      let op = action
        .get("op")
        .and_then(|v| v.as_str())
        .map(|value| value.to_lowercase());
      let Some(op) = op else {
        fail!();
      };
      if op == "perp-policy" {
        processed!(self.process_perp_policy_action(
          action,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-open-group" {
        processed!(self.process_perp_open_group_action(
          action,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "perp-join" {
        processed!(self.process_perp_join_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "perp-external-evidence" {
        processed!(self.process_perp_external_evidence_action(
          action,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "perp-cancel" {
        processed!(self.process_perp_cancel_action(
          action,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-refund" {
        processed!(self.process_perp_refund_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-activate" {
        processed!(self.process_perp_activate_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-close" {
        processed!(self.process_perp_close_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-liquidate" {
        processed!(self.process_perp_liquidate_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-settle" {
        processed!(self.process_perp_settle_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "perp-claim" {
        processed!(self.process_perp_claim_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "lock" {
        if let Some(link) = link {
          processed!(self.process_token_proof_lock_action(
            action,
            i,
            link,
            transaction,
            vout,
            value,
            inscription,
            number,
            block,
            timestamp,
          ));
        } else {
          fail!();
        }
      // START TAP-DELEGATED-LOCKS
      } else if op == "execute" {
        if link.is_none() {
          if let Some(mut delegated) =
            self.validate_token_proof_delegated_execute_action(action, inscription, i, block)
          {
            processed!(self.process_token_proof_lock_action(
              &mut delegated.action,
              i,
              &delegated.link,
              transaction,
              vout,
              value,
              inscription,
              number,
              block,
              timestamp,
            ));
            let _ = self.tap_put(&delegated.nonce_key, &"".to_string());
          } else {
            fail!();
          }
        } else {
          fail!();
        }
      } else if op == "execute-action" {
        if link.is_none() {
          let empty_i128 = std::collections::HashMap::new();
          let empty_bigint = std::collections::HashMap::new();
          if let Some(delegated) = self.validate_token_proof_generic_delegated_action(
            action,
            inscription,
            i,
            block,
            &empty_i128,
            &empty_bigint,
            &empty_bigint,
            &empty_bigint,
            &empty_bigint,
          ) {
            let processed = if delegated.family == "perp-join" {
              self.process_perp_join_action(
                &delegated.action,
                Some(&delegated.link),
                transaction,
                vout,
                value,
                inscription,
                number,
                block,
                timestamp,
                i,
              )
            } else if delegated.family == "perp-close" {
              self.process_perp_close_action(
                &delegated.action,
                Some(&delegated.link),
                transaction,
                vout,
                value,
                inscription,
                number,
                block,
                timestamp,
              )
            } else {
              false
            };
            processed!(processed);
            let _ = self.tap_put(&delegated.nonce_key, &"".to_string());
          } else {
            fail!();
          }
        } else {
          fail!();
        }
      } else if op == "cancel-delegation" {
        processed!(self.process_token_proof_delegation_cancel_action(
          action,
          link,
          iaddr,
          redeem,
          sig,
          hash,
          salt,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      // END TAP-DELEGATED-LOCKS
      } else if op == "ob-open" {
        processed!(self.process_obligation_open_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "ob-claim" || op == "ob-refund" || op == "ob-final" {
        processed!(self.process_obligation_settle_action(
          action,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "auth-cfg" {
        processed!(self.process_authority_config_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "sync-ext" {
        processed!(self.process_amm_external_snapshot_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "add-liq" {
        processed!(self.process_amm_add_liquidity_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "rm-liq" {
        processed!(self.process_amm_remove_liquidity_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "swap" {
        processed!(self.process_amm_swap_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "stake" {
        processed!(self.process_stake_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "claim-rwd" {
        processed!(self.process_claim_reward_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "unstake" {
        processed!(self.process_unstake_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "fund-sale" {
        processed!(self.process_fund_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        ));
      } else if op == "contribute" {
        processed!(self.process_sale_contribution_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
	      } else if op == "finalize-sale" {
	        processed!(self.process_finalize_sale_action(
	          action,
	          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
	          timestamp,
	        ));
	      } else if op == "resolve-sale" {
	        processed!(self.process_resolve_sale_action(
	          action,
	          link,
	          transaction,
	          vout,
	          value,
	          inscription,
	          number,
	          block,
	          timestamp,
	          i,
	        ));
	      } else if op == "claim-sale" {
        processed!(self.process_claim_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "refund-sale" {
        processed!(self.process_refund_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "cancel-sale" {
        processed!(self.process_cancel_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "withdraw-sale" {
        processed!(self.process_withdraw_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
          i,
        ));
      } else if op == "claim" || op == "refund" {
        if let Some(link) = link {
          processed!(self.process_token_proof_release_action(
            action,
            link,
            transaction,
            vout,
            value,
            inscription,
            number,
            block,
            timestamp,
          ));
        } else {
          fail!();
        }
      } else {
        fail!();
      }
    }
    if started_atomic && self.tap_atomic_commit().is_err() {
      self.tap_atomic_abort();
      return false;
    }
    true
  }
  // END TAP-PROOFS

  pub(crate) fn index_token_auth_created(
    &mut self,
    inscription_id: InscriptionId,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Only process creation-time inscriptions
    if satpoint.outpoint.txid.to_string() != inscription_id.txid.to_string() {
      return;
    }
    let Some(body) = payload.body() else {
      return;
    };
    let s = String::from_utf8_lossy(body);
    let json_val = match self.parse_tap_json_value(&s) {
      Some(v) => v,
      None => return,
    };
    let p = json_val
      .get("p")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    let op = json_val
      .get("op")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_lowercase();
    if p != "tap" || op != "token-auth" {
      return;
    }

    if json_val.get("cancel").is_some() {
      let acc = TapAccumulatorEntry {
        op: "token-auth".to_string(),
        json: json_val.clone(),
        ins: inscription_id.to_string(),
        blck: self.height,
        tx: satpoint.outpoint.txid.to_string(),
        vo: u32::from(satpoint.outpoint.vout),
        val: None,
        num: inscription_number,
        ts: self.timestamp,
        addr: owner_address.to_string(),
      };
      let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
      let _ = self.tap_set_list_record(
        &format!("al/{}", owner_address),
        &format!("ali/{}", owner_address),
        &acc,
      );
      if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
        let ptr = format!("ali/{}", list_len - 1);
        let txs = satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(
          &format!("tx/a-athc/{}", txs),
          &format!("txi/a-athc/{}", txs),
          &ptr,
        );
        let _ = self.tap_set_list_record(
          &format!("blck/a-athc/{}", self.height),
          &format!("blcki/a-athc/{}", self.height),
          &ptr,
        );
      }
      return;
    }

    let Some(sig_obj) = json_val.get("sig") else {
      return;
    };
    if !sig_obj.is_object() {
      return;
    }
    let Some(hash_val) = json_val.get("hash") else {
      return;
    };
    let Some(salt_val) = json_val.get("salt") else {
      return;
    };

    if let Some(redeem) = json_val.get("redeem") {
      let mut redeem_norm = redeem.clone();
      if redeem_norm.get("data").is_none() {
        return;
      }
      // START TAP-PROOFS
      let actions_enabled = self.tap_feature_enabled(TapFeature::TokenAuthorityStakingUpgradeActivation);
      let has_actions = actions_enabled
        && redeem_norm
          .get("actions")
          .and_then(|v| v.as_array())
          .map(|a| !a.is_empty())
          .unwrap_or(false);
      // START TAP-DELEGATED-LOCKS
      let delegated_only_redeem =
        actions_enabled && Self::token_proof_delegated_only_redeem(&redeem_norm);
      // END TAP-DELEGATED-LOCKS
      // END TAP-PROOFS
      let items_norm = {
        let mut out = Vec::new();
        if let Some(items) = redeem_norm.get_mut("items").and_then(|v| v.as_array_mut()) {
          for it in items.iter_mut() {
            let Some(tick) = it.get("tick").and_then(|v| v.as_str()) else {
              return;
            };
            let t = Self::strip_prefix_for_len_check(tick);
            if !Self::valid_tap_ticker_visible_len(
              self.feature_height(TapFeature::FullTicker),
              self.height,
              Self::visible_length(t),
            ) {
              return;
            }
            if let Some(addr) = it.get("address").and_then(|v| v.as_str()) {
              let norm = Self::normalize_address(addr);
              if !self.is_valid_bitcoin_address(&norm) {
                return;
              }
              if let Some(v) = it.get_mut("address") {
                *v = serde_json::Value::String(norm);
              }
            } else {
              return;
            }
          }
          out = items.clone();
        }
        if out.is_empty() && !has_actions {
          return;
        }
        out
      };
      let Some(hash_str) = hash_val.as_str() else {
        return;
      };
      let salt_str = Self::js_value_to_string(salt_val);
      let msg_hash = Self::build_sha256_json_plus_salt(&redeem_norm, &salt_str);
      let Some((ok, compact_sig, pubkey_hex)) =
        self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
      else {
        return;
      };
      if !ok {
        return;
      }
      if self
        .tap_get::<String>(&format!("tah/{}", compact_sig))
        .ok()
        .flatten()
        .is_some()
      {
        return;
      }
      // START TAP-DELEGATED-LOCKS
      // Delegated-only redeems are relayer-safe: the outer signer only submits
      // the envelope, while maker authority signatures inside execute actions
      // authorize the lock creation.
      if delegated_only_redeem {
        let actions_pass = if let Some(actions) = redeem_norm
          .get_mut("actions")
          .and_then(|v| v.as_array_mut())
        {
          self.validate_token_proof_actions(
            actions,
            None,
            &inscription_id.to_string(),
            self.height,
            self.timestamp,
          )
        } else {
          false
        };
        if !actions_pass {
          return;
        }
        let Some(delegated_link) = redeem_norm
          .get("actions")
          .and_then(|v| v.as_array())
          .and_then(|actions| self.token_proof_primary_delegated_link(actions))
        else {
          return;
        };
        let redeem_proof = redeem_norm.clone();
        self.tap_atomic_begin();
        if let Some(actions) = redeem_norm
          .get_mut("actions")
          .and_then(|v| v.as_array_mut())
        {
          if !self.process_token_proof_actions(
            actions,
            None,
            owner_address,
            &redeem_proof,
            sig_obj,
            hash_str,
            &salt_str,
            &satpoint.outpoint.txid.to_string(),
            u32::from(satpoint.outpoint.vout),
            output_value_sat,
            &inscription_id.to_string(),
            inscription_number,
            self.height,
            self.timestamp,
          ) {
            self.tap_atomic_abort();
            return;
          }
        }
        let rec = TokenAuthRedeemRecord {
          addr: delegated_link.addr.clone(),
          iaddr: owner_address.to_string(),
          rdm: redeem_norm.clone(),
          sig: sig_obj.clone(),
          hash: hash_str.to_string(),
          slt: salt_str,
          blck: self.height,
          tx: satpoint.outpoint.txid.to_string(),
          vo: u32::from(satpoint.outpoint.vout),
          val: output_value_sat.to_string(),
          ins: inscription_id.to_string(),
          num: inscription_number,
          ts: self.timestamp,
        };
        if let Ok(list_len) = self.tap_set_list_record(
          &format!("tr/{}", delegated_link.addr),
          &format!("tri/{}", delegated_link.addr),
          &rec,
        ) {
          let _ = self.tap_put(
            &format!("trins/{}", inscription_id),
            &format!("tri/{}/{}", delegated_link.addr, list_len.saturating_sub(1)),
          );
        }
        if let Ok(list_len) = self.tap_set_list_record("sftr", "sftri", &rec) {
          let ptr = format!("sftri/{}", list_len - 1);
          let txs = satpoint.outpoint.txid.to_string();
          let _ = self.tap_set_list_record(
            &format!("tx/ath-rdm/{}", txs),
            &format!("txi/ath-rdm/{}", txs),
            &ptr,
          );
          let _ = self.tap_set_list_record(
            &format!("blck/ath-rdm/{}", self.height),
            &format!("blcki/ath-rdm/{}", self.height),
            &ptr,
          );
        }
        let _ = self.tap_put(&format!("tah/{}", compact_sig), &"".to_string());
        if self.tap_atomic_commit().is_err() {
          self.tap_atomic_abort();
          return;
        }
        return;
      }
      // END TAP-DELEGATED-LOCKS
      let Some(auth_val) = redeem_norm.get("auth") else {
        return;
      };
      let auth_id = Self::js_value_to_string(auth_val);
      let Some(ptr) = self
        .tap_get::<String>(&format!("tains/{}", auth_id))
        .ok()
        .flatten()
      else {
        return;
      };
      let Some(link) = self.tap_get::<TokenAuthCreateRecord>(&ptr).ok().flatten() else {
        return;
      };
      let auth_msg_hash = Self::build_sha256_json_plus_salt(
        &serde_json::Value::Array(
          link
            .auth
            .iter()
            .map(|s| serde_json::Value::String(s.clone()))
            .collect(),
        ),
        &link.slt,
      );
      let Some((auth_ok, _, auth_pub)) =
        self.verify_sig_obj_against_msg_with_hash(&link.sig, &link.hash, &auth_msg_hash)
      else {
        return;
      };
      if !auth_ok {
        return;
      }
      if auth_pub.to_lowercase() != pubkey_hex.to_lowercase() {
        return;
      }
      // Enforce redeem items whitelist parity from activation height:
      // if link.auth is non-empty, every redeem item.tick must be included in link.auth
      if self.tap_feature_enabled(TapFeature::TokenAuthWhitelistFixActivation) {
        if !link.auth.is_empty() {
          for it in items_norm.iter() {
            let Some(tick) = it.get("tick").and_then(|v| v.as_str()) else {
              return;
            };
            if !link.auth.iter().any(|t| t == tick) {
              return;
            }
          }
          // START TAP-PROOFS
          // Lock creation spends the authority owner's token balance, so it uses the same ticker whitelist as item redeems.
          if has_actions {
            if let Some(actions) = redeem_norm.get("actions").and_then(|v| v.as_array()) {
              for action in actions {
                if action
                  .get("op")
                  .and_then(|v| v.as_str())
                  .map(|s| s.eq_ignore_ascii_case("lock"))
                  .unwrap_or(false)
                {
                  let Some(tick) = Self::token_proof_action_tick(action) else {
                    return;
                  };
                  if !link.auth.iter().any(|t| t == tick) {
                    return;
                  }
                }
              }
            }
          }
          // END TAP-PROOFS
        }
      }
      // START TAP-PROOFS
      // Cancellation retires an authority for new obligations, but existing
      // locks, stakes, sale positions, and delegated offer exits must remain
      // settleable through their normal validators.
      let auth_cancelled = self
        .tap_get::<String>(&format!("tac/{}", link.ins))
        .ok()
        .flatten()
        .is_some();
      let cancelled_settlement_only = actions_enabled
        && auth_cancelled
        && Self::token_proof_post_cancel_settlement_actions(&items_norm, &redeem_norm);
      if auth_cancelled && !cancelled_settlement_only {
        return;
      }
      let actions_pass = if has_actions {
        if let Some(actions) = redeem_norm
          .get_mut("actions")
          .and_then(|v| v.as_array_mut())
        {
          self.validate_token_proof_actions(
            actions,
            Some(&link),
            &inscription_id.to_string(),
            self.height,
            self.timestamp,
          )
        } else {
          false
        }
      } else {
        true
      };
      if !actions_pass {
        return;
      }
      // END TAP-PROOFS
      self.tap_atomic_begin();
      for it in items_norm.iter() {
        let tick = it.get("tick").and_then(|v| v.as_str()).unwrap_or("");
        let to_addr = it.get("address").and_then(|v| v.as_str()).unwrap_or("");
        let amt_v = it.get("amt").unwrap();
        let dta = it
          .get("dta")
          .and_then(|v| v.as_str())
          .map(|s| s.to_string());
        self.exec_internal_send_one(
          &link.addr,
          to_addr,
          tick,
          amt_v,
          dta,
          &inscription_id.to_string(),
          inscription_number,
          satpoint,
          output_value_sat,
        );
      }
      // START TAP-PROOFS
      if has_actions {
        let redeem_proof = redeem_norm.clone();
        if let Some(actions) = redeem_norm
          .get_mut("actions")
          .and_then(|v| v.as_array_mut())
        {
          if !self.process_token_proof_actions(
            actions,
            Some(&link),
            owner_address,
            &redeem_proof,
            sig_obj,
            hash_str,
            &salt_str,
            &satpoint.outpoint.txid.to_string(),
            u32::from(satpoint.outpoint.vout),
            output_value_sat,
            &inscription_id.to_string(),
            inscription_number,
            self.height,
            self.timestamp,
          ) {
            self.tap_atomic_abort();
            return;
          }
        }
      }
      // END TAP-PROOFS
      let rec = TokenAuthRedeemRecord {
        addr: link.addr.clone(),
        iaddr: owner_address.to_string(),
        rdm: redeem_norm.clone(),
        sig: sig_obj.clone(),
        hash: hash_str.to_string(),
        slt: salt_str,
        blck: self.height,
        tx: satpoint.outpoint.txid.to_string(),
        vo: u32::from(satpoint.outpoint.vout),
        val: output_value_sat.to_string(),
        ins: inscription_id.to_string(),
        num: inscription_number,
        ts: self.timestamp,
      };
      if let Ok(list_len) = self.tap_set_list_record(
        &format!("tr/{}", link.addr),
        &format!("tri/{}", link.addr),
        &rec,
      ) {
        let _ = self.tap_put(
          &format!("trins/{}", inscription_id),
          &format!("tri/{}/{}", link.addr, list_len.saturating_sub(1)),
        );
      }
      if let Ok(list_len) = self.tap_set_list_record("sftr", "sftri", &rec) {
        let ptr = format!("sftri/{}", list_len - 1);
        let txs = satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(
          &format!("tx/ath-rdm/{}", txs),
          &format!("txi/ath-rdm/{}", txs),
          &ptr,
        );
        let _ = self.tap_set_list_record(
          &format!("blck/ath-rdm/{}", self.height),
          &format!("blcki/ath-rdm/{}", self.height),
          &ptr,
        );
      }
      let _ = self.tap_put(&format!("tah/{}", compact_sig), &"".to_string());
      if self.tap_atomic_commit().is_err() {
        self.tap_atomic_abort();
        return;
      }
      return;
    }

    let acc = TapAccumulatorEntry {
      op: "token-auth".to_string(),
      json: json_val,
      ins: inscription_id.to_string(),
      blck: self.height,
      tx: satpoint.outpoint.txid.to_string(),
      vo: u32::from(satpoint.outpoint.vout),
      val: Some(output_value_sat.to_string()),
      num: inscription_number,
      ts: self.timestamp,
      addr: owner_address.to_string(),
    };
    let _ = self.tap_put(&format!("a/{}", inscription_id), &acc);
    let _ = self.tap_set_list_record(
      &format!("al/{}", owner_address),
      &format!("ali/{}", owner_address),
      &acc,
    );
    if let Ok(list_len) = self.tap_set_list_record("al", "ali", &acc) {
      let ptr = format!("ali/{}", list_len - 1);
      let txs = satpoint.outpoint.txid.to_string();
      let _ = self.tap_set_list_record(
        &format!("tx/a-ath/{}", txs),
        &format!("txi/a-ath/{}", txs),
        &ptr,
      );
      let _ = self.tap_set_list_record(
        &format!("blck/a-ath/{}", self.height),
        &format!("blcki/a-ath/{}", self.height),
        &ptr,
      );
    }
  }

  pub(crate) fn index_token_auth_executed(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    new_satpoint: SatPoint,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // Only execute on transfer (not creation tx)
    if new_satpoint.outpoint.txid.to_string() == inscription_id.txid.to_string() {
      return;
    }
    let key = format!("a/{}", inscription_id);
    let Some(acc) = self.tap_get::<TapAccumulatorEntry>(&key).ok().flatten() else {
      return;
    };
    if acc.addr != owner_address {
      return;
    }
    if acc.op.to_lowercase() != "token-auth" {
      return;
    }
    macro_rules! delete_acc_and_return {
      () => {{
        let _ = self.tap_del(&key);
        return;
      }};
    }

    if acc.json.get("cancel").is_some() {
      if let Some(cancel_val) = acc.json.get("cancel") {
        let cancel_id = Self::js_value_to_string(cancel_val);
        if let Some(ptr) = self
          .tap_get::<String>(&format!("tains/{}", cancel_id))
          .ok()
          .flatten()
        {
          if let Some(link) = self.tap_get::<TokenAuthCreateRecord>(&ptr).ok().flatten() {
            if link.addr == acc.addr {
              let _ = self.tap_put(&format!("tac/{}", link.ins), &"".to_string());
            }
          }
        }
      }
      let _ = self.tap_del(&key);
      return;
    }

    let Some(sig_obj) = acc.json.get("sig") else {
      delete_acc_and_return!();
    };
    let Some(hash_str) = acc.json.get("hash").and_then(|v| v.as_str()) else {
      delete_acc_and_return!();
    };
    let Some(salt_val) = acc.json.get("salt") else {
      delete_acc_and_return!();
    };
    let Some(auth_arr) = acc.json.get("auth").and_then(|v| v.as_array()) else {
      delete_acc_and_return!();
    };
    let salt_str = Self::js_value_to_string(salt_val);
    let msg_hash =
      Self::build_sha256_json_plus_salt(&serde_json::Value::Array(auth_arr.clone()), &salt_str);
    let Some((ok, compact_sig, _pub)) =
      self.verify_sig_obj_against_msg_with_hash(sig_obj, hash_str, &msg_hash)
    else {
      delete_acc_and_return!();
    };
    if !ok {
      delete_acc_and_return!();
    }
    if self
      .tap_get::<String>(&format!("tah/{}", compact_sig))
      .ok()
      .flatten()
      .is_some()
    {
      delete_acc_and_return!();
    }
    for t in auth_arr.iter() {
      let Some(ts) = t.as_str() else {
        delete_acc_and_return!();
      };
      if self
        .tap_get::<DeployRecord>(&format!("d/{}", Self::json_stringify_lower(ts)))
        .ok()
        .flatten()
        .is_none()
      {
        delete_acc_and_return!();
      }
    }
    let auth_vec: Vec<String> = auth_arr
      .iter()
      .filter_map(|v| v.as_str().map(|s| s.to_string()))
      .collect();
    let rec = TokenAuthCreateRecord {
      addr: acc.addr.clone(),
      auth: auth_vec,
      sig: sig_obj.clone(),
      hash: hash_str.to_string(),
      slt: salt_str,
      blck: self.height,
      tx: new_satpoint.outpoint.txid.to_string(),
      vo: u32::from(new_satpoint.outpoint.vout),
      val: output_value_sat.to_string(),
      ins: inscription_id.to_string(),
      num: acc.num,
      ts: self.timestamp,
    };
    if let Ok(list_len) = self.tap_set_list_record(
      &format!("ta/{}", acc.addr),
      &format!("tai/{}", acc.addr),
      &rec,
    ) {
      let ptr = format!("tai/{}/{}", acc.addr, list_len - 1);
      let _ = self.tap_put(&format!("tains/{}", inscription_id), &ptr);
      if let Ok(sflen) = self.tap_set_list_record("sfta", "sftai", &rec) {
        let sptr = format!("sftai/{}", sflen - 1);
        let txs = new_satpoint.outpoint.txid.to_string();
        let _ = self.tap_set_list_record(
          &format!("tx/ath/{}", txs),
          &format!("txi/ath/{}", txs),
          &sptr,
        );
        let _ = self.tap_set_list_record(
          &format!("blck/ath/{}", self.height),
          &format!("blcki/ath/{}", self.height),
          &sptr,
        );
      }
    }
    let _ = self.tap_put(&format!("tah/{}", compact_sig), &"".to_string());
    let _ = self.tap_del(&key);
  }
}

#[cfg(test)]
mod amm_tests {
  use super::*;
  use crate::index::{
    HOME_INSCRIPTIONS, INSCRIPTION_ID_TO_SEQUENCE_NUMBER, INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER,
    SAT_TO_SEQUENCE_NUMBER, SEQUENCE_NUMBER_TO_CHILDREN, SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY,
    TAP_KV, TRANSACTION_ID_TO_TRANSACTION,
  };
  use bitcoin::Network as BtcNetwork;
  use redb::Database;
  use serde_json::json;
  use std::collections::HashMap;
  use tempfile::TempDir;

  const USER_ADDRESS: &str = "tb1qjsv26lap3ffssj6hfy8mzn0lg5vte6a42j75ww";
  const RECEIVER_ADDRESS: &str = "tb1qakxxzv9n7706kc3xdcycrtfv8cqv62hnwexc0l";

  fn with_test_updater<T>(
    network: BtcNetwork,
    height: u32,
    test: impl FnOnce(&mut InscriptionUpdater<'_, '_>) -> T,
  ) -> T {
    let tempdir = TempDir::new().unwrap();
    let db = Database::create(tempdir.path().join("tap-amm.redb")).unwrap();
    let write_tx = db.begin_write().unwrap();

    let mut home_inscriptions = write_tx.open_table(HOME_INSCRIPTIONS).unwrap();
    let mut id_to_sequence_number = write_tx
      .open_table(INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let mut inscription_number_to_sequence_number = write_tx
      .open_table(INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER)
      .unwrap();
    let mut transaction_id_to_transaction =
      write_tx.open_table(TRANSACTION_ID_TO_TRANSACTION).unwrap();
    let mut sat_to_sequence_number = write_tx
      .open_multimap_table(SAT_TO_SEQUENCE_NUMBER)
      .unwrap();
    let mut sequence_number_to_children = write_tx
      .open_multimap_table(SEQUENCE_NUMBER_TO_CHILDREN)
      .unwrap();
    let mut sequence_number_to_entry = write_tx
      .open_table(SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    let mut tap_kv = write_tx.open_table(TAP_KV).unwrap();

    let mut updater = InscriptionUpdater {
      blessed_inscription_count: 0,
      cursed_inscription_count: 0,
      flotsam: Vec::new(),
      height,
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
      tap_delta_db: None,
      tap_atomic_writes: None,
      tap_atomic_overlay: None,
      tap_atomic_list_len_cache: None,
      tap_route_index: None,
      tap_route_index_verify: false,
      list_len_cache: HashMap::new(),
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

  fn auth_link(addr: &str, ins: &str) -> TokenAuthCreateRecord {
    TokenAuthCreateRecord {
      addr: addr.to_string(),
      auth: Vec::new(),
      sig: json!({}),
      hash: String::new(),
      slt: String::new(),
      blck: 1,
      tx: "00".repeat(32),
      vo: 0,
      val: "0".to_string(),
      ins: ins.to_string(),
      num: 1,
      ts: 1,
    }
  }

  fn put_deploy(updater: &mut InscriptionUpdater<'_, '_>, tick: &str, dec: u32) {
    updater
      .tap_put(
        &format!("d/{}", InscriptionUpdater::json_stringify_lower(tick)),
        &DeployRecord {
          tick: InscriptionUpdater::js_to_lowercase(tick),
          max: "1000000000000000000000000000000".to_string(),
          lim: "1000000000000000000000000000000".to_string(),
          dec,
          blck: 1,
          tx: "11".repeat(32),
          vo: 0,
          val: "0".to_string(),
          ins: format!("{}i0", "11".repeat(32)),
          num: 1,
          ts: 1,
          addr: USER_ADDRESS.to_string(),
          crsd: false,
          dmt: false,
          elem: None,
          prj: None,
          dim: None,
          dt: None,
          prv: None,
          dta: None,
        },
      )
      .unwrap();
  }

  fn put_authority_config(updater: &mut InscriptionUpdater<'_, '_>, auth: &str) {
    updater
      .tap_put(
        &format!("ah/{}", auth),
        &json!({
          "id": auth,
          "k": "test",
          "ctl": { "ty": "ta", "auth": auth },
          "seq": 0,
          "r": null,
          "blck": 10,
          "tx": "authority-tx",
          "vo": 0,
          "val": "0",
          "ins": auth,
          "num": 0,
          "ts": 1000
        }),
      )
      .unwrap();
  }

  fn put_stake_authority(updater: &mut InscriptionUpdater<'_, '_>, auth: &str) {
    updater
      .tap_put(
        &format!("ah/{}", auth),
        &json!({
          "id": auth,
          "k": "stk",
          "stk": "tap",
          "rt": ["tap"],
          "ctl": { "ty": "ta", "auth": auth },
          "seq": 0,
          "r": {
            "cm": "arps",
            "rnd": "flr",
            "aw": false,
            "ep": "hold",
            "tr": [{ "id": "base", "dur": "1", "w": "1" }]
          },
          "blck": 10,
          "tx": "stake-authority-tx",
          "vo": 0,
          "val": "0",
          "ins": auth,
          "num": 0,
          "ts": 1000
        }),
      )
      .unwrap();
  }

  fn put_balance(updater: &mut InscriptionUpdater<'_, '_>, addr: &str, tick: &str, balance: &str) {
    updater
      .tap_put(
        &format!(
          "b/{}/{}",
          addr,
          InscriptionUpdater::json_stringify_lower(tick)
        ),
        &balance.to_string(),
      )
      .unwrap();
  }

  fn get_string(updater: &mut InscriptionUpdater<'_, '_>, key: &str) -> Option<String> {
    updater.tap_get::<String>(key).unwrap()
  }

  fn cert_pubkey(byte: u8) -> String {
    let secp = secp256k1::Secp256k1::new();
    let secret = secp256k1::SecretKey::from_slice(&[byte; 32]).unwrap();
    let pubkey = secp256k1::PublicKey::from_secret_key(&secp, &secret);
    hex::encode(pubkey.serialize())
  }

  fn sign_cert_hash(byte: u8, signer: &str, hash: &str) -> serde_json::Value {
    let secp = secp256k1::Secp256k1::new();
    let secret = secp256k1::SecretKey::from_slice(&[byte; 32]).unwrap();
    let hash_bytes = hex::decode(hash).unwrap();
    let msg = secp256k1::Message::from_digest_slice(&hash_bytes).unwrap();
    let sig = secp.sign_ecdsa_recoverable(&msg, &secret);
    let (recovery_id, compact) = sig.serialize_compact();
    json!({
      "signer": signer,
      "hash": hash,
      "sig": {
        "v": recovery_id.to_i32().to_string(),
        "r": num_bigint::BigUint::from_bytes_be(&compact[..32]).to_string(),
        "s": num_bigint::BigUint::from_bytes_be(&compact[32..]).to_string()
      }
    })
  }

  fn attach_cert(
    action: &mut serde_json::Value,
    policy: &serde_json::Value,
    action_name: &str,
    nonce: &str,
    valid_until: u32,
    signers: &[(u8, String)],
  ) {
    let payload_hash = InscriptionUpdater::certified_control_payload_hash(action).unwrap();
    let msg = InscriptionUpdater::certified_control_message(
      policy,
      action_name,
      action.get("lock").unwrap().as_str().unwrap(),
      &payload_hash,
      nonce,
      valid_until,
    )
    .unwrap();
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    let sigs = signers
      .iter()
      .map(|(byte, signer)| sign_cert_hash(*byte, signer, &msg_hash))
      .collect::<Vec<_>>();
    action["cert"] = json!({
      "v": 1,
      "policy": policy.get("id").unwrap(),
      "action": action_name,
      "target": action.get("lock").unwrap(),
      "payload_hash": payload_hash,
      "nonce": nonce,
      "valid_until": valid_until,
      "sigs": sigs
    });
  }

  fn certified_lock_action(
    policy_id: &str,
    threshold: u32,
    signers: Vec<String>,
  ) -> serde_json::Value {
    json!({
      "op": "lock",
      "kind": "htlc",
      "tick": "tap",
      "amt": "1",
      "claim": RECEIVER_ADDRESS,
      "refund": USER_ADDRESS,
      "condition": {
        "type": "hashlock",
        "hash": InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"))
      },
      "refund_after": "20",
      "control": {
        "type": "cert",
        "id": policy_id,
        "threshold": threshold,
        "signers": signers,
        "scope": ["claim", "refund"],
        "expires": "30",
        "rules": { "terminal_refund_after": "40" }
      }
    })
  }

  fn amm_config(link: &TokenAuthCreateRecord, fee: &str, pf: &str) -> serde_json::Value {
    let mut c = json!({
      "ty": "cpmm",
      "fee": fee,
      "pf": pf,
      "min": "1000",
      "pause": false
    });
    if pf != "0" {
      c["pp"] = json!({ "tt": "a", "to": RECEIVER_ADDRESS });
    }
    json!({
      "op": "auth-cfg",
      "k": "amm",
      "n": "Test AMM",
      "a": [
        { "ty": "tap", "tick": "tap" },
        { "ty": "tap", "tick": "dmt" }
      ],
      "c": c,
      "ctl": { "ty": "ta", "auth": link.ins },
      "seq": 0
    })
  }

  fn apply_actions(
    updater: &mut InscriptionUpdater<'_, '_>,
    link: &TokenAuthCreateRecord,
    inscription: &str,
    actions: Vec<serde_json::Value>,
  ) -> bool {
    let mut actions = actions;
    if !updater.validate_token_proof_actions(&mut actions, Some(link), inscription, 10, 1000) {
      return false;
    }
    let redeem = json!({ "actions": actions.clone() });
    updater.process_token_proof_actions(
      &mut actions,
      Some(link),
      link.addr.as_str(),
      &redeem,
      &json!({}),
      "",
      "",
      &"22".repeat(32),
      0,
      0,
      inscription,
      1,
      10,
      1000,
    );
    true
  }

  fn obligation_open(hash: &str) -> serde_json::Value {
    json!({
      "op": "ob-open",
      "src": { "tt": "a", "to": USER_ADDRESS, "tick": "tap" },
      "amt": "10",
      "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
      "rf": { "tt": "a", "to": USER_ADDRESS },
      "cond": { "ty": "hash", "h": hash },
      "ra": "20",
      "exp": "30",
      "ctx": { "app": "test", "ref": "quote-1" }
    })
  }

  fn apply_actions_at(
    updater: &mut InscriptionUpdater<'_, '_>,
    link: &TokenAuthCreateRecord,
    inscription: &str,
    actions: Vec<serde_json::Value>,
    block: u32,
  ) -> bool {
    let mut actions = actions;
    if !updater.validate_token_proof_actions(&mut actions, Some(link), inscription, block, 1000) {
      return false;
    }
    let redeem = json!({ "actions": actions.clone() });
    updater.process_token_proof_actions(
      &mut actions,
      Some(link),
      link.addr.as_str(),
      &redeem,
      &json!({}),
      "",
      "",
      &"33".repeat(32),
      0,
      0,
      inscription,
      1,
      block,
      1000,
    );
    true
  }

  fn apply_perp_actions_at(
    updater: &mut InscriptionUpdater<'_, '_>,
    link: Option<&TokenAuthCreateRecord>,
    inscription: &str,
    actions: Vec<serde_json::Value>,
    block: u32,
  ) -> bool {
    let mut actions = actions;
    if !updater.validate_token_proof_actions(&mut actions, link, inscription, block, 1000) {
      return false;
    }
    let redeem = json!({ "actions": actions.clone() });
    updater.process_token_proof_actions(
      &mut actions,
      link,
      link.map(|l| l.addr.as_str()).unwrap_or(""),
      &redeem,
      &json!({}),
      "",
      "",
      &"44".repeat(32),
      0,
      0,
      inscription,
      1,
      block,
      1000,
    );
    true
  }

  fn perp_signer() -> String {
    cert_pubkey(1)
  }

  fn perp_signer_two() -> String {
    cert_pubkey(3)
  }

  fn sign_perp_hash(hash: &str) -> serde_json::Value {
    sign_cert_hash(1, &perp_signer(), hash)
  }

  fn sign_perp_hash_two(hash: &str) -> serde_json::Value {
    sign_cert_hash(3, &perp_signer_two(), hash)
  }

  fn delegated_signer() -> String {
    cert_pubkey(2)
  }

  fn sign_delegated_hash(hash: &[u8; 32]) -> serde_json::Value {
    sign_cert_hash(2, &delegated_signer(), &hex::encode(hash))
  }

  fn put_signed_token_auth(
    updater: &mut InscriptionUpdater<'_, '_>,
    auth: &str,
    addr: &str,
    ticks: Vec<&str>,
  ) {
    let auth_values = serde_json::Value::Array(
      ticks
        .iter()
        .map(|tick| serde_json::Value::String(tick.to_string()))
        .collect(),
    );
    let salt = format!("auth-salt-{auth}");
    let hash = InscriptionUpdater::build_sha256_json_plus_salt(&auth_values, &salt);
    let sig_entry = sign_delegated_hash(&hash);
    let rec = TokenAuthCreateRecord {
      addr: addr.to_string(),
      auth: ticks.iter().map(|tick| tick.to_string()).collect(),
      sig: sig_entry.get("sig").unwrap().clone(),
      hash: hex::encode(hash),
      slt: salt,
      blck: 10,
      tx: "22".repeat(32),
      vo: 0,
      val: "0".to_string(),
      ins: auth.to_string(),
      num: 1,
      ts: 1000,
    };
    updater.tap_put(&format!("ta/{auth}"), &rec).unwrap();
    updater
      .tap_put(&format!("tains/{auth}"), &format!("ta/{auth}"))
      .unwrap();
  }

  fn sign_action_delegation(delegation: &serde_json::Value) -> serde_json::Value {
    let message = InscriptionUpdater::token_proof_action_delegation_message(delegation).unwrap();
    let salt = InscriptionUpdater::js_value_to_string(delegation.get("salt").unwrap());
    let hash = InscriptionUpdater::build_sha256_json_plus_salt(&message, &salt);
    sign_delegated_hash(&hash)
  }

  fn sign_action_final(
    delegation: &serde_json::Value,
    final_action: &serde_json::Value,
    salt: &str,
  ) -> serde_json::Value {
    let finalizers = delegation.get("finalizers").unwrap();
    let message =
      InscriptionUpdater::token_proof_action_final_message(delegation, finalizers, final_action)
        .unwrap();
    let hash = InscriptionUpdater::build_sha256_json_plus_salt(&message, salt);
    sign_delegated_hash(&hash)
  }

  fn delegated_perp_join_action(group: &str, nonce: &str) -> serde_json::Value {
    let auth = "perp-authi0";
    let mut delegation = json!({
      "kind": "action",
      "v": "1",
      "auth": auth,
      "nonce": nonce,
      "expiry": "20",
      "family": "perp-join",
      "threshold": "1",
      "signers": [delegated_signer()],
      "template": {
        "op": "perp-join",
        "gid": group,
        "src": { "tt": "a", "to": USER_ADDRESS },
        "side": "$side",
        "coll": "$coll",
        "lev": "$lev",
        "entry": "$entry",
        "claim": { "tt": "a", "to": USER_ADDRESS },
        "refund": { "tt": "a", "to": USER_ADDRESS }
      },
      "constraints": {
        "side": { "allowed": ["long"] },
        "coll": { "allowed": ["100"] },
        "lev": { "equals": { "n": "2", "d": "1" } },
        "entry": { "equals": { "max": { "p": "1000", "q": "1" } } }
      },
      "finalizers": { "threshold": "1", "signers": [delegated_signer()] },
      "salt": format!("delegation-salt-{nonce}"),
      "sigs": []
    });
    delegation["sigs"] = json!([sign_action_delegation(&delegation)]);
    let final_action = json!({
      "op": "perp-join",
      "gid": group,
      "src": { "tt": "a", "to": USER_ADDRESS },
      "side": "long",
      "coll": "100",
      "lev": { "n": "2", "d": "1" },
      "entry": { "max": { "p": "1000", "q": "1" } },
      "claim": { "tt": "a", "to": USER_ADDRESS },
      "refund": { "tt": "a", "to": USER_ADDRESS }
    });
    let final_salt = format!("final-salt-{nonce}");
    json!({
      "op": "execute-action",
      "delegation": delegation,
      "fill": { "side": "long", "coll": "100", "lev": { "n": "2", "d": "1" }, "entry": { "max": { "p": "1000", "q": "1" } } },
      "final": {
        "salt": final_salt,
        "sigs": [sign_action_final(&delegation, &final_action, &final_salt)]
      }
    })
  }

  fn signed_perp_policy(fee_receiver: &str) -> serde_json::Value {
    signed_perp_policy_with_bounty(fee_receiver, "0")
  }

  fn sign_perp_policy_action(action: &mut serde_json::Value) {
    let policy_id = action.get("id").unwrap().as_str().unwrap().to_string();
    let seq = action.get("seq").unwrap().as_str().unwrap().to_string();
    action["sigs"] = json!([]);
    let payload_hash =
      InscriptionUpdater::token_perp_payload_hash(action, &["hash", "sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_policy_message(&policy_id, &seq, &payload_hash);
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["sigs"] = json!([sign_perp_hash(&msg_hash)]);
  }

  fn split_perp_fee_receivers() -> serde_json::Value {
    json!([
      { "tt": "a", "to": RECEIVER_ADDRESS, "share": "7500", "rl": "pf" },
      { "tt": "h", "to": "stake-authorityi0", "share": "2500", "rl": "sr" }
    ])
  }

  fn signed_perp_policy_with_receivers(receivers: serde_json::Value) -> serde_json::Value {
    let mut action = signed_perp_policy(RECEIVER_ADDRESS);
    action["fee"]["receivers"] = receivers;
    sign_perp_policy_action(&mut action);
    action
  }

  fn signed_perp_policy_with_bounty(fee_receiver: &str, settle_bounty: &str) -> serde_json::Value {
    signed_perp_policy_with_bounties(fee_receiver, "0", "0", settle_bounty)
  }

  fn signed_perp_policy_with_bounties(
    fee_receiver: &str,
    activate_bounty: &str,
    liquidate_bounty: &str,
    settle_bounty: &str,
  ) -> serde_json::Value {
    let mut action = json!({
      "op": "perp-policy",
      "id": "perp-main",
      "v": "1",
      "dom": "tap-perp-policy-v1",
      "net": "bitcoin:signet",
      "seq": "1",
      "thr": "1",
      "signers": [perp_signer()],
      "assets": { "tap": { "mode": "wildcard-or-list", "ticks": [] }, "external": { "mode": "wildcard-or-list", "refs": [] }, "pairs": { "mode": "wildcard-or-list", "items": [] } },
      "limits": {
        "max_lev": { "n": "200", "d": "1" },
        "min_coll": "1",
        "max_not": "1000000000",
        "min_dur": "1",
        "max_dur": "1000000",
        "min_form": "1",
        "max_form": "1000",
        "min_ratio": { "n": "0", "d": "1" },
        "max_ratio": { "n": "999999999", "d": "1" }
      },
      "oracle": { "rules": ["spot-vwap-v1"], "max_age": "12", "min_trades": "1", "min_volume": "1", "stale": "fallback-or-reject", "fallbacks": ["last-valid-at-expiry-v1"] },
      "liq": { "rules": ["isolated-maintenance-margin-v1"], "min_mmr": { "n": "5", "d": "1000" } },
      "entry": { "mode": "one-sided-v1", "required": true, "allow_unbounded": false, "max_slippage_bps": "500" },
      "def": { "rules": ["pro-rata-positive-equity-v1"], "dust": "largest-remainder-v1" },
      "fee": {
        "rules": ["settlement-positive-payout-bps-v1"],
        "max_bps": "200",
        "receivers": [{ "tt": "a", "to": fee_receiver, "share": "10000" }]
      },
      "bounty": { "rules": {
        "activate": { "mode": "cap", "bps": "0", "cap": activate_bounty, "public": true },
        "liquidate": { "mode": "cap", "bps": "0", "cap": liquidate_bounty, "public": true },
        "settle": { "mode": "cap", "bps": "0", "cap": settle_bounty, "public": true }
      } },
      "exp": "2000000000",
      "sigs": []
    });
    let payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action, &["hash", "sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_policy_message("perp-main", "1", &payload_hash);
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["sigs"] = json!([sign_perp_hash(&msg_hash)]);
    action
  }

  fn signed_perp_threshold_policy(fee_receiver: &str, second_signature: bool) -> serde_json::Value {
    let mut action = signed_perp_policy(fee_receiver);
    action["thr"] = json!("2");
    action["signers"] = json!([perp_signer(), perp_signer_two()]);
    action["sigs"] = json!([]);
    let payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action, &["hash", "sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_policy_message("perp-main", "1", &payload_hash);
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    let mut sigs = vec![sign_perp_hash(&msg_hash)];
    if second_signature {
      sigs.push(sign_perp_hash_two(&msg_hash));
    }
    action["sigs"] = json!(sigs);
    action
  }

  fn perp_group_action(policy: &serde_json::Value) -> serde_json::Value {
    json!({
      "op": "perp-open-group",
      "pid": "perp-main",
      "ph": policy.get("hash").unwrap(),
      "pair": {
        "base": { "ns": "tap", "tick": "tap", "dec": "0" },
        "quote": { "ns": "tap", "tick": "tap", "dec": "0" },
        "price_dir": "quote-per-base"
      },
      "coll": { "asset": { "ns": "tap", "tick": "tap", "dec": "0" }, "mode": "tap-account", "min": "1", "max": "1000000" },
      "form": { "start": "10", "deadline": "15", "early": true },
      "ready": {
        "min_long_coll": "100",
        "min_short_coll": "100",
        "min_total_coll": "200",
        "min_long_not": "100",
        "min_short_not": "100",
        "ratio_min": { "n": "0", "d": "1" },
        "ratio_max": { "n": "999999999", "d": "1" },
        "max_imbalance_not": "999999999"
      },
      "lev": { "min": { "n": "2", "d": "1" }, "max": { "n": "2", "d": "1" }, "step": { "n": "1", "d": "1" } },
      "close": { "full": true, "partial": true, "payout": "reserved-until-settlement", "min_remaining_not": "0" },
      "liq": { "rule": "isolated-maintenance-margin-v1", "mmr": { "n": "5", "d": "1000" }, "fee_bps": "0" },
      "entry": policy.get("entry").unwrap(),
      "settle": { "expiry": "30", "rule": "expiry-price-v1", "fallback": "last-valid-at-expiry-v1" },
      "def": { "rule": "pro-rata-positive-equity-v1", "dust": "largest-remainder-v1" },
      "fee": { "rule": "settlement-positive-payout-bps-v1", "bps": "200", "recv": [{ "tt": "a", "to": RECEIVER_ADDRESS, "share": "10000" }] },
      "bounty": { "rule": "operator-policy-bounty-v1", "activate": "policy-default", "liquidate": "policy-default", "settle": "policy-default" },
      "oracle": { "rule": "spot-vwap-v1", "source": "marketplace-spot", "max_age": "12" }
    })
  }

  fn perp_external_usdt_asset() -> serde_json::Value {
    json!({
      "ns": "eip155",
      "cid": "eip155:31337",
      "ak": "erc20",
      "aid": "0x0000000000000000000000000000000000000001",
      "dec": "6",
      "sym": "USDT"
    })
  }

  fn perp_external_surface() -> serde_json::Value {
    json!({
      "kind": "evm-perp-escrow",
      "id": "0x00000000000000000000000000000000000000aa"
    })
  }

  fn perp_bsc_native_asset() -> serde_json::Value {
    json!({
      "ns": "eip155",
      "cid": "eip155:31338",
      "ak": "native",
      "aid": "native",
      "dec": "18",
      "sym": "BNB"
    })
  }

  fn perp_bsc_surface() -> serde_json::Value {
    json!({
      "kind": "bsc-perp-escrow",
      "id": "eip155:31338:0x00000000000000000000000000000000000000bb"
    })
  }

  fn perp_solana_native_asset() -> serde_json::Value {
    json!({
      "ns": "solana",
      "cid": "solana:localnet",
      "ak": "native",
      "aid": "native",
      "dec": "9",
      "sym": "SOL"
    })
  }

  fn perp_solana_surface() -> serde_json::Value {
    json!({
      "kind": "solana-perp-program",
      "id": "solana:localnet:So11111111111111111111111111111111111111112"
    })
  }

  fn perp_join_action(group: &str, side: &str, owner: &str) -> serde_json::Value {
    json!({
      "op": "perp-join",
      "gid": group,
      "src": { "tt": "a", "to": owner },
      "side": side,
      "coll": "100",
      "lev": { "n": "2", "d": "1" },
      "entry": if side == "long" { json!({ "max": { "p": "1000", "q": "1" } }) } else { json!({ "min": { "p": "1", "q": "1" } }) },
      "claim": { "tt": "a", "to": owner },
      "refund": { "tt": "a", "to": owner }
    })
  }

  fn perp_external_evidence_action(
    group: &str,
    policy: &serde_json::Value,
    group_record: &serde_json::Value,
    side: &str,
    ext_position: &str,
    amount: &str,
    seq: &str,
  ) -> serde_json::Value {
    perp_external_evidence_action_with_collateral(
      group,
      policy,
      group_record,
      side,
      ext_position,
      amount,
      seq,
      perp_external_usdt_asset(),
      "evm-perp-escrow",
      perp_external_surface(),
    )
  }

  fn perp_external_evidence_action_with_collateral(
    group: &str,
    policy: &serde_json::Value,
    group_record: &serde_json::Value,
    side: &str,
    ext_position: &str,
    amount: &str,
    seq: &str,
    collateral_asset: serde_json::Value,
    collateral_mode: &str,
    settlement_surface: serde_json::Value,
  ) -> serde_json::Value {
    let mut action = json!({
      "op": "perp-external-evidence",
      "gid": group,
      "purpose": "external-lock",
      "evidence": {
        "v": "1",
        "dom": "tap-perp-external-evidence-v1",
        "net": "bitcoin:signet",
        "pid": policy.get("id").unwrap(),
        "ph": policy.get("hash").unwrap(),
        "gid": group,
        "gh": group_record.get("gh").unwrap(),
        "purpose": "external-lock",
        "seq": seq,
        "valid_from": "10",
        "valid_until": "40",
        "coll": collateral_asset,
        "mode": collateral_mode,
        "surface": settlement_surface,
        "ext": {
          "group": "0xexternalgroup",
          "position": ext_position,
          "tx": format!("0xtx{ext_position}"),
          "index": "0",
          "height": "12",
          "finality": { "rule": "confirmations", "count": "12" },
          "owner": if side == "long" { "0xlongowner" } else { "0xshortowner" },
          "side": side,
          "amount": amount,
          "lev": { "n": "2", "d": "1" },
          "entry": if side == "long" { json!({ "max": { "p": "1000", "q": "1" } }) } else { json!({ "min": { "p": "1", "q": "1" } }) },
          "claim": if side == "long" { "0xlongclaim" } else { "0xshortclaim" },
          "refund": if side == "long" { "0xlongrefund" } else { "0xshortrefund" }
        },
        "state_hash": "",
        "sigs": []
      }
    });
    let state_hash = InscriptionUpdater::token_perp_payload_hash(&action, &["evidence"]).unwrap();
    action["evidence"]["state_hash"] = json!(state_hash);
    let evidence_payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action["evidence"], &["sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_external_evidence_message(
      policy,
      group_record,
      "external-lock",
      &evidence_payload_hash,
      seq,
      40,
    )
    .unwrap();
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["evidence"]["sigs"] = json!([sign_perp_hash(&msg_hash)]);
    action
  }

  fn add_perp_evidence_signature_two(
    action: &mut serde_json::Value,
    policy: &serde_json::Value,
    group_record: &serde_json::Value,
  ) {
    let evidence_payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action["evidence"], &["sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_external_evidence_message(
      policy,
      group_record,
      action["evidence"].get("purpose").unwrap().as_str().unwrap(),
      &evidence_payload_hash,
      action["evidence"].get("seq").unwrap().as_str().unwrap(),
      action["evidence"]
        .get("valid_until")
        .unwrap()
        .as_str()
        .unwrap()
        .parse::<u32>()
        .unwrap(),
    )
    .unwrap();
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["evidence"]["sigs"]
      .as_array_mut()
      .unwrap()
      .push(sign_perp_hash_two(&msg_hash));
  }

  fn perp_price_action(op: &str, target: &str, group: &str, _price: &str) -> serde_json::Value {
    let mut action = serde_json::Map::new();
    action.insert("op".to_string(), json!(op));
    action.insert("gid".to_string(), json!(group));
    if op != "perp-settle" && op != "perp-activate" {
      action.insert("pos".to_string(), json!(target));
    }
    if op == "perp-close" {
      action.insert(
        "qty".to_string(),
        json!({ "mode": "fraction", "n": "1", "d": "1" }),
      );
    }
    serde_json::Value::Object(action)
  }

  fn attach_perp_cert(
    action: &mut serde_json::Value,
    policy: &serde_json::Value,
    group_record: &serde_json::Value,
    purpose: &str,
    nonce: &str,
    valid_until: u32,
    price: &str,
  ) {
    let cert_purpose = match purpose {
      "activate" => "entry",
      "settle" => "settlement",
      "liquidate" => "liquidation",
      other => other,
    };
    let group = group_record.get("id").unwrap().as_str().unwrap();
    let payload_hash = InscriptionUpdater::token_perp_payload_hash(action, &["cert"]).unwrap();
    let seq = nonce
      .chars()
      .filter(|c| c.is_ascii_digit())
      .collect::<String>();
    let seq = if seq.is_empty() { "1".to_string() } else { seq };
    action["cert"] = json!({
      "v": "1",
      "dom": "tap-perp-price-v1",
      "net": policy.get("net").unwrap(),
      "pid": policy.get("id").unwrap(),
      "ph": policy.get("hash").unwrap(),
      "gid": group,
      "gh": group_record.get("gh").unwrap(),
      "purpose": cert_purpose,
      "seq": seq,
      "valid_from": "10",
      "valid_until": valid_until.to_string(),
      "source": { "rule": "spot-vwap-v1", "from": "9", "to": "10", "trades": "1", "volume": "1" },
      "pair": {
        "base": group_record.get("pair").unwrap().get("base").unwrap(),
        "quote": group_record.get("pair").unwrap().get("quote").unwrap(),
        "price_dir": "quote-per-base"
      },
      "price": { "p": price, "q": "1", "seq": "1" },
      "state_hash": payload_hash,
      "salt": nonce,
      "sigs": []
    });
    let cert_payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action["cert"], &["sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_certificate_message(
      policy,
      cert_purpose,
      group,
      &cert_payload_hash,
      action["cert"].get("seq").unwrap().as_str().unwrap(),
      valid_until,
    )
    .unwrap();
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["cert"]["sigs"] = json!([sign_perp_hash(&msg_hash)]);
  }

  fn add_perp_cert_signature_two(
    action: &mut serde_json::Value,
    policy: &serde_json::Value,
    group_record: &serde_json::Value,
    purpose: &str,
    valid_until: u32,
  ) {
    let cert_purpose = match purpose {
      "activate" => "entry",
      "settle" => "settlement",
      "liquidate" => "liquidation",
      other => other,
    };
    let cert_payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action["cert"], &["sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_certificate_message(
      policy,
      cert_purpose,
      group_record.get("id").unwrap().as_str().unwrap(),
      &cert_payload_hash,
      action["cert"].get("seq").unwrap().as_str().unwrap(),
      valid_until,
    )
    .unwrap();
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["cert"]["sigs"]
      .as_array_mut()
      .unwrap()
      .push(sign_perp_hash_two(&msg_hash));
  }

  fn resign_perp_cert(
    action: &mut serde_json::Value,
    policy: &serde_json::Value,
    group_record: &serde_json::Value,
    purpose: &str,
    valid_until: u32,
  ) {
    let cert_purpose = match purpose {
      "activate" => "entry",
      "settle" => "settlement",
      "liquidate" => "liquidation",
      other => other,
    };
    action["cert"]["sigs"] = json!([]);
    let cert_payload_hash =
      InscriptionUpdater::token_perp_payload_hash(&action["cert"], &["sigs"]).unwrap();
    let msg = InscriptionUpdater::token_perp_certificate_message(
      policy,
      cert_purpose,
      group_record.get("id").unwrap().as_str().unwrap(),
      &cert_payload_hash,
      action["cert"].get("seq").unwrap().as_str().unwrap(),
      valid_until,
    )
    .unwrap();
    let msg_hash = InscriptionUpdater::certified_control_hash(&msg).unwrap();
    action["cert"]["sigs"] = json!([sign_perp_hash(&msg_hash)]);
  }

  #[test]
  fn perp_pair_index_encodes_slash_sensitive_tap_tickers() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "ta/p", 0);
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let slash_asset = json!({ "ns": "tap", "tick": "ta/p", "dec": "0" });
      let mut group_action = perp_group_action(&policy);
      group_action["pair"]["base"] = slash_asset.clone();
      group_action["pair"]["quote"] = slash_asset.clone();
      group_action["coll"]["asset"] = slash_asset;
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-slash-groupi0",
        vec![group_action],
        10
      ));
      let key = hex::encode("ta/p".as_bytes());
      assert_eq!(
        get_string(updater, &format!("perp/gpair/tap:{}|tap:{}", key, key)).as_deref(),
        Some("1")
      );
      assert_eq!(
        get_string(updater, "perp/gpair/tap:ta/p|tap:ta/p").as_deref(),
        None
      );
    });
  }

  #[test]
  fn generic_delegated_perp_join_is_domain_separated_from_delegated_locks() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let mut group_action = perp_group_action(&policy);
      group_action["ready"] = json!({
        "min_long_coll": "100",
        "min_short_coll": "0",
        "min_total_coll": "100",
        "min_long_not": "100",
        "min_short_not": "0",
        "ratio_min": { "n": "0", "d": "1" },
        "ratio_max": { "n": "999999999", "d": "1" },
        "max_imbalance_not": "999999999"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "groupi0",
        vec![group_action],
        10
      ));
      put_signed_token_auth(updater, "perp-authi0", USER_ADDRESS, vec!["tap"]);

      let action = delegated_perp_join_action("groupi0:0", "delegated-perp-join-1");
      let mut wrong_domain = vec![{
        let mut wrong = action.clone();
        wrong["op"] = json!("execute");
        wrong
      }];
      assert!(!updater.validate_token_proof_actions(
        &mut wrong_domain,
        None,
        "wrong-domaini0",
        10,
        1000
      ));

      let mut duplicate_nonce = vec![
        action.clone(),
        delegated_perp_join_action("groupi0:0", "delegated-perp-join-1"),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut duplicate_nonce,
        None,
        "duplicate-noncei0",
        10,
        1000
      ));

      let mut old_lock_delegation = json!({
        "auth": "perp-authi0",
        "nonce": "old-lock-delegation-1",
        "expiry": "20",
        "threshold": "1",
        "signers": [delegated_signer()],
        "template": { "op": "lock", "tick": "tap", "amt": "1", "claim": USER_ADDRESS, "refund": USER_ADDRESS, "refund_after": "20" },
        "constraints": {},
        "salt": "old-lock-salt",
        "sigs": []
      });
      let old_message =
        InscriptionUpdater::token_proof_delegation_message(&old_lock_delegation).unwrap();
      let old_hash = InscriptionUpdater::build_sha256_json_plus_salt(&old_message, "old-lock-salt");
      old_lock_delegation["sigs"] = json!([sign_delegated_hash(&old_hash)]);
      let mut old_as_action = vec![json!({
        "op": "execute-action",
        "delegation": old_lock_delegation,
        "fill": {}
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut old_as_action,
        None,
        "old-lock-as-actioni0",
        10,
        1000
      ));

      assert!(apply_perp_actions_at(
        updater,
        None,
        "delegated-joini0",
        vec![action],
        10
      ));
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("900")
      );
      let position = updater
        .tap_get::<serde_json::Value>("perp/pos/delegated-joini0:0")
        .unwrap()
        .unwrap();
      assert_eq!(
        position.get("owner").and_then(|v| v.as_str()),
        Some(USER_ADDRESS)
      );
      assert_eq!(
        get_string(updater, "tdn/perp-authi0/delegated-perp-join-1").as_deref(),
        Some("")
      );
    });
  }

  #[test]
  fn perp_group_activates_settles_fees_and_claims() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");

      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      assert_eq!(
        get_string(updater, "perp/gpair/tap:746170|tap:746170").as_deref(),
        Some("1")
      );
      assert_eq!(
        get_string(updater, &format!("perp/ga/{}", USER_ADDRESS)).as_deref(),
        Some("1")
      );
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("900")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        Some("900")
      );
      assert_eq!(
        get_string(updater, &format!("ab/{}/{}", group, tick_key)).as_deref(),
        Some("200")
      );

      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-activatei0",
        vec![activate],
        10
      ));

      let mut settle = perp_price_action("perp-settle", group, group, "110");
      attach_perp_cert(
        &mut settle,
        &policy,
        &group_record,
        "settle",
        "set-1",
        40,
        "110",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-settlei0",
        vec![settle],
        30
      ));

      let long_pos = "perp-longi0:0";
      let short_pos = "perp-shorti0:0";
      let long_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/pos/{}", long_pos))
        .unwrap()
        .unwrap();
      let short_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/pos/{}", short_pos))
        .unwrap()
        .unwrap();
      assert_eq!(
        long_record.get("payout").and_then(|v| v.as_str()),
        Some("0")
      );
      assert_eq!(
        short_record.get("payout").and_then(|v| v.as_str()),
        Some("0")
      );
      let settled_group = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        settled_group
          .get("settlement")
          .and_then(|v| v.get("fee"))
          .and_then(|v| v.as_str()),
        Some("4")
      );
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>(&format!("perp/st/{}", group))
          .unwrap()
          .unwrap()
          .get("settlement")
          .and_then(|v| v.get("fee"))
          .and_then(|v| v.as_str()),
        Some("4")
      );
      assert_eq!(get_string(updater, "perp/certl").as_deref(), Some("2"));
      assert_eq!(
        get_string(updater, "blck/perp/event/30").as_deref(),
        Some("2")
      );

      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-claim-longi0",
        vec![json!({ "op": "perp-claim", "gid": group, "pos": long_pos })],
        31
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-claim-shorti0",
        vec![json!({ "op": "perp-claim", "gid": group, "pos": short_pos })],
        31
      ));
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>(&format!("perp/cl/{}", long_pos))
          .unwrap()
          .unwrap()
          .get("amount")
          .and_then(|v| v.as_str()),
        Some("117")
      );
      assert_eq!(
        get_string(updater, &format!("perp/claimg/{}", group)).as_deref(),
        Some("2")
      );
      assert_eq!(
        get_string(updater, &format!("perp/claima/{}", USER_ADDRESS)).as_deref(),
        Some("1")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("1017")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        Some("982")
      );
    });
  }

  #[test]
  fn perp_settlement_bounty_is_reduced_to_zero_without_bounty_reserve() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");

      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy_with_bounty(RECEIVER_ADDRESS, "1")],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-activatei0",
        vec![activate],
        10
      ));
      let mut settle = perp_price_action("perp-settle", group, group, "100");
      attach_perp_cert(
        &mut settle,
        &policy,
        &group_record,
        "settle",
        "set-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-settlei0",
        vec![settle],
        30
      ));

      let settlement = updater
        .tap_get::<serde_json::Value>(&format!("perp/st/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        settlement
          .get("settlement")
          .and_then(|v| v.get("claim_pool"))
          .and_then(|v| v.as_str()),
        Some("196")
      );
      assert_eq!(
        settlement
          .get("settlement")
          .and_then(|v| v.get("fee"))
          .and_then(|v| v.as_str()),
        Some("4")
      );
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("900")
      );
      assert!(updater
        .tap_get::<serde_json::Value>(&format!("perp/bgi/{}/0", group))
        .unwrap()
        .is_none());
    });
  }

  #[test]
  fn perp_settlement_splits_tap_fees_into_operator_and_staking_rewards() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      put_stake_authority(updater, "stake-authorityi0");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");

      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-stakei0",
        vec![json!({ "op": "stake", "auth": "stake-authorityi0", "tick": "tap", "amt": "100", "tier": "base", "claim": USER_ADDRESS })],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy_with_receivers(split_perp_fee_receivers())],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let mut group_action = perp_group_action(&policy);
      group_action["fee"]["recv"] = split_perp_fee_receivers();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![group_action],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-activatei0",
        vec![activate],
        10
      ));
      let mut settle = perp_price_action("perp-settle", group, group, "100");
      attach_perp_cert(
        &mut settle,
        &policy,
        &group_record,
        "settle",
        "set-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-settlei0",
        vec![settle],
        30
      ));

      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      let settlement = updater
        .tap_get::<serde_json::Value>(&format!("perp/st/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        settlement
          .get("settlement")
          .and_then(|v| v.get("fee"))
          .and_then(|v| v.as_str()),
        Some("4")
      );
      assert_eq!(
        settlement
          .get("settlement")
          .and_then(|v| v.get("fees"))
          .and_then(|v| v.as_array())
          .and_then(|v| v.get(0))
          .and_then(|v| v.get("amt"))
          .and_then(|v| v.as_str()),
        Some("3")
      );
      assert_eq!(
        settlement
          .get("settlement")
          .and_then(|v| v.get("fees"))
          .and_then(|v| v.as_array())
          .and_then(|v| v.get(1))
          .and_then(|v| v.get("amt"))
          .and_then(|v| v.as_str()),
        Some("1")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        Some("903")
      );
      assert_eq!(
        get_string(updater, &format!("ab/{}/{}", "stake-authorityi0", tick_key)).as_deref(),
        Some("101")
      );
      assert_eq!(
        get_string(updater, &format!("ahrps/{}/{}", "stake-authorityi0", tick_key)).as_deref(),
        Some("10000000000000000")
      );
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-reward-claimi0",
        vec![json!({ "op": "claim-rwd", "auth": "stake-authorityi0", "pos": "perp-stakei0:0", "rt": "tap" })],
        31
      ));
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("801")
      );
    });
  }

  #[test]
  fn perp_fee_receivers_reject_spoofed_or_unsafe_updates() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      let mut missing_auth = vec![signed_perp_policy_with_receivers(split_perp_fee_receivers())];
      assert!(!updater.validate_token_proof_actions(
        &mut missing_auth,
        None,
        "perp-missing-stake-authi0",
        10,
        1000
      ));
    });

    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_stake_authority(updater, "stake-authorityi0");
      for receivers in [
        json!([{ "tt": "a", "to": RECEIVER_ADDRESS, "share": "9999", "rl": "pf" }]),
        json!([{ "tt": "a", "to": RECEIVER_ADDRESS, "share": "5000", "rl": "pf" }, { "tt": "a", "to": USER_ADDRESS, "share": "5000", "rl": "pf" }]),
        json!([{ "tt": "h", "to": "stake-authorityi0/bad", "share": "2500", "rl": "sr" }, { "tt": "a", "to": RECEIVER_ADDRESS, "share": "7500", "rl": "pf" }]),
        json!([{ "tt": "h", "to": "stake-authorityi0", "share": "2500", "rl": "pf" }, { "tt": "a", "to": RECEIVER_ADDRESS, "share": "7500", "rl": "of" }]),
      ] {
        let mut actions = vec![signed_perp_policy_with_receivers(receivers)];
        assert!(!updater.validate_token_proof_actions(
          &mut actions,
          None,
          "perp-bad-fee-policyi0",
          10,
          1000
        ));
      }
    });

    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_stake_authority(updater, "stake-authorityi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy_with_receivers(split_perp_fee_receivers())],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let mut reduced_fee = perp_group_action(&policy);
      reduced_fee["fee"]["recv"] = split_perp_fee_receivers();
      reduced_fee["fee"]["bps"] = json!("199");
      let mut actions = vec![reduced_fee];
      assert!(!updater.validate_token_proof_actions(
        &mut actions,
        None,
        "perp-reduced-feei0",
        10,
        1000
      ));

      let mut reordered = perp_group_action(&policy);
      reordered["fee"]["recv"] = json!([
        { "tt": "h", "to": "stake-authorityi0", "share": "2500", "rl": "sr" },
        { "tt": "a", "to": RECEIVER_ADDRESS, "share": "7500", "rl": "pf" }
      ]);
      let mut actions = vec![reordered];
      assert!(!updater.validate_token_proof_actions(
        &mut actions,
        None,
        "perp-reordered-feei0",
        10,
        1000
      ));

      let mut external = perp_group_action(&policy);
      external["pair"]["quote"] = perp_external_usdt_asset();
      external["coll"] = json!({ "asset": perp_external_usdt_asset(), "mode": "evm-perp-escrow", "surface": perp_external_surface(), "min": "1", "max": "1000000" });
      external["fee"]["recv"] = split_perp_fee_receivers();
      let mut actions = vec![external];
      assert!(!updater.validate_token_proof_actions(
        &mut actions,
        None,
        "perp-external-staking-feei0",
        10,
        1000
      ));
    });
  }

  #[test]
  fn perp_policy_external_wildcard_permits_canonical_external_quote_assets() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let mut group_action = perp_group_action(&policy);
      group_action["pair"]["quote"] = json!({
        "ns": "eip155",
        "cid": "eip155:1",
        "ak": "native",
        "aid": "native",
        "dec": "18",
        "sym": "ETH"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-ext-groupi0",
        vec![group_action],
        10
      ));
      let group = updater
        .tap_get::<serde_json::Value>("perp/g/perp-ext-groupi0:0")
        .unwrap()
        .unwrap();
      assert_eq!(
        group
          .get("pair")
          .and_then(|v| v.get("quote"))
          .and_then(|v| v.get("ty"))
          .and_then(|v| v.as_str()),
        Some("ext")
      );
      assert_eq!(
        get_string(
          updater,
          "perp/gpair/tap:746170|ext:656970313535:6569703135353a31:6e6174697665:6e6174697665"
        )
        .as_deref(),
        Some("1")
      );
    });
  }

  #[test]
  fn perp_external_collateral_uses_certified_evidence_without_tap_balance_debits() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let ext_pair = json!({
        "base": { "ns": "tap", "tick": "tap", "dec": "0" },
        "quote": perp_external_usdt_asset(),
        "price_dir": "quote-per-base"
      });
      let mut missing_surface = perp_group_action(&policy);
      missing_surface["pair"] = ext_pair.clone();
      missing_surface["coll"] = json!({
        "asset": perp_external_usdt_asset(),
        "mode": "evm-perp-escrow",
        "min": "1",
        "max": "1000000"
      });
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "bad-ext-groupi0",
        vec![missing_surface],
        10
      ));

      let mut group_action = perp_group_action(&policy);
      group_action["pair"] = ext_pair;
      group_action["coll"] = json!({
        "asset": perp_external_usdt_asset(),
        "mode": "evm-perp-escrow",
        "surface": perp_external_surface(),
        "min": "1",
        "max": "1000000"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-groupi0",
        vec![group_action],
        10
      ));
      let group = "ext-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        group_record
          .get("collateral")
          .and_then(|v| v.get("ty"))
          .and_then(|v| v.as_str()),
        Some("ext")
      );
      assert_eq!(
        group_record.get("collateral_mode").and_then(|v| v.as_str()),
        Some("evm-perp-escrow")
      );
      assert!(!apply_perp_actions_at(
        updater,
        Some(&long),
        "tap-join-exti0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));

      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-long-evi0",
        vec![perp_external_evidence_action(
          group,
          &policy,
          &group_record,
          "long",
          "longpos",
          "100",
          "1"
        )],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-short-evi0",
        vec![perp_external_evidence_action(
          group,
          &policy,
          &group_record,
          "short",
          "shortpos",
          "100",
          "1"
        )],
        10
      ));
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("1000")
      );
      assert_eq!(
        get_string(updater, &format!("ab/{}/{}", group, tick_key)).as_deref(),
        None
      );
      assert_eq!(get_string(updater, "perp/el").as_deref(), Some("2"));
      assert_eq!(
        get_string(updater, &format!("perp/eg/{}", group)).as_deref(),
        Some("2")
      );
      let ext_group = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        ext_group.get("total_collateral").and_then(|v| v.as_str()),
        Some("200")
      );
      assert_eq!(
        ext_group.get("long_notional").and_then(|v| v.as_str()),
        Some("200")
      );
      let long_pos = format!("{}:ext:{}", group, hex::encode("longpos".as_bytes()));
      let long_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/pos/{}", long_pos))
        .unwrap()
        .unwrap();
      assert_eq!(
        long_record
          .get("claim")
          .and_then(|v| v.get("tt"))
          .and_then(|v| v.as_str()),
        Some("x")
      );
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "dup-ext-evi0",
        vec![perp_external_evidence_action(
          group,
          &policy,
          &group_record,
          "long",
          "longpos",
          "100",
          "2"
        )],
        10
      ));

      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-activatei0",
        vec![activate],
        10
      ));

      let mut settle = perp_price_action("perp-settle", group, group, "100");
      attach_perp_cert(
        &mut settle,
        &policy,
        &group_record,
        "settle",
        "set-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-settlei0",
        vec![settle],
        30
      ));
      let settlement = updater
        .tap_get::<serde_json::Value>(&format!("perp/st/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        settlement
          .get("settlement")
          .and_then(|v| v.get("fee"))
          .and_then(|v| v.as_str()),
        Some("4")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        Some("1000")
      );
      assert!(!apply_perp_actions_at(
        updater,
        Some(&long),
        "ext-claimi0",
        vec![json!({ "op": "perp-claim", "gid": group, "pos": long_pos })],
        31
      ));

      let mut fallback_group_action = perp_group_action(&policy);
      fallback_group_action["pair"] = json!({
        "base": { "ns": "tap", "tick": "tap", "dec": "0" },
        "quote": perp_external_usdt_asset(),
        "price_dir": "quote-per-base"
      });
      fallback_group_action["coll"] = json!({
        "asset": perp_external_usdt_asset(),
        "mode": "evm-perp-escrow",
        "surface": perp_external_surface(),
        "min": "1",
        "max": "1000000"
      });
      fallback_group_action["bounty"] = json!({
        "rule": "operator-policy-bounty-v1",
        "activate": "0",
        "liquidate": "0",
        "settle": "0"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-fallback-groupi0",
        vec![fallback_group_action],
        10
      ));
      let fallback_group = "ext-fallback-groupi0:0";
      let fallback_group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", fallback_group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-fallback-long-evi0",
        vec![perp_external_evidence_action(
          fallback_group,
          &policy,
          &fallback_group_record,
          "long",
          "fallback-longpos",
          "100",
          "1"
        )],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-fallback-short-evi0",
        vec![perp_external_evidence_action(
          fallback_group,
          &policy,
          &fallback_group_record,
          "short",
          "fallback-shortpos",
          "100",
          "1"
        )],
        10
      ));
      let mut fallback_activate =
        perp_price_action("perp-activate", fallback_group, fallback_group, "100");
      attach_perp_cert(
        &mut fallback_activate,
        &policy,
        &fallback_group_record,
        "activate",
        "act-2",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-fallback-activatei0",
        vec![fallback_activate],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-fallback-settlei0",
        vec![
          json!({ "op": "perp-settle", "gid": fallback_group, "fallback": "last-valid-at-expiry-v1" })
        ],
        43
      ));
      let fallback_settlement = updater
        .tap_get::<serde_json::Value>(&format!("perp/st/{}", fallback_group))
        .unwrap()
        .unwrap();
      assert_eq!(
        fallback_settlement
          .get("settlement")
          .and_then(|v| v.get("fee"))
          .and_then(|v| v.as_str()),
        Some("0")
      );
      assert_eq!(
        fallback_settlement
          .get("settlement")
          .and_then(|v| v.get("claim_pool"))
          .and_then(|v| v.as_str()),
        Some("200")
      );
      let fallback_long_pos = format!(
        "{}:ext:{}",
        fallback_group,
        hex::encode("fallback-longpos".as_bytes())
      );
      let fallback_short_pos = format!(
        "{}:ext:{}",
        fallback_group,
        hex::encode("fallback-shortpos".as_bytes())
      );
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>(&format!("perp/pos/{}", fallback_long_pos))
          .unwrap()
          .unwrap()
          .get("payout")
          .and_then(|v| v.as_str()),
        Some("0")
      );
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>(&format!("perp/pos/{}", fallback_short_pos))
          .unwrap()
          .unwrap()
          .get("payout")
          .and_then(|v| v.as_str()),
        Some("0")
      );
    });
  }

  #[test]
  fn perp_external_collateral_accepts_supported_settlement_modes() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();

      for (suffix, asset, mode, surface) in vec![
        (
          "evm",
          perp_external_usdt_asset(),
          "evm-perp-escrow",
          perp_external_surface(),
        ),
        (
          "bsc",
          perp_bsc_native_asset(),
          "bsc-perp-escrow",
          perp_bsc_surface(),
        ),
        (
          "solana",
          perp_solana_native_asset(),
          "solana-perp-program",
          perp_solana_surface(),
        ),
      ] {
        let group_inscription = format!("{suffix}-ext-mode-groupi0");
        let group = format!("{group_inscription}:0");
        let mut group_action = perp_group_action(&policy);
        group_action["pair"] = json!({
          "base": { "ns": "tap", "tick": "tap", "dec": "0" },
          "quote": asset.clone(),
          "price_dir": "quote-per-base"
        });
        group_action["coll"] = json!({
          "asset": asset.clone(),
          "mode": mode,
          "surface": surface.clone(),
          "min": "1",
          "max": "1000000"
        });
        assert!(apply_perp_actions_at(
          updater,
          None,
          &group_inscription,
          vec![group_action],
          10
        ));
        let group_record = updater
          .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
          .unwrap()
          .unwrap();
        assert_eq!(
          group_record.get("collateral_mode").and_then(|v| v.as_str()),
          Some(mode)
        );
        assert_eq!(
          group_record
            .get("settlement_surface")
            .and_then(|v| v.get("kind"))
            .and_then(|v| v.as_str()),
          Some(surface.get("kind").unwrap().as_str().unwrap())
        );

        let wrong_mode = if mode == "evm-perp-escrow" {
          "bsc-perp-escrow"
        } else {
          "evm-perp-escrow"
        };
        assert!(!apply_perp_actions_at(
          updater,
          None,
          &format!("{suffix}-wrong-mode-evi0"),
          vec![perp_external_evidence_action_with_collateral(
            &group,
            &policy,
            &group_record,
            "long",
            &format!("{suffix}-wrong-mode-pos"),
            "100",
            "1",
            asset.clone(),
            wrong_mode,
            surface.clone()
          )],
          10
        ));

        let wrong_surface = if mode == "solana-perp-program" {
          perp_external_surface()
        } else {
          perp_solana_surface()
        };
        assert!(!apply_perp_actions_at(
          updater,
          None,
          &format!("{suffix}-wrong-surface-evi0"),
          vec![perp_external_evidence_action_with_collateral(
            &group,
            &policy,
            &group_record,
            "long",
            &format!("{suffix}-wrong-surface-pos"),
            "100",
            "1",
            asset.clone(),
            mode,
            wrong_surface
          )],
          10
        ));

        let position = format!("{suffix}-longpos");
        assert!(apply_perp_actions_at(
          updater,
          None,
          &format!("{suffix}-long-evi0"),
          vec![perp_external_evidence_action_with_collateral(
            &group,
            &policy,
            &group_record,
            "long",
            &position,
            "100",
            "1",
            asset,
            mode,
            surface
          )],
          10
        ));
        let position_key = format!("{}:ext:{}", group, hex::encode(position.as_bytes()));
        assert_eq!(
          updater
            .tap_get::<serde_json::Value>(&format!("perp/pos/{}", position_key))
            .unwrap()
            .unwrap()
            .get("collateral_mode")
            .and_then(|v| v.as_str()),
          Some(mode)
        );
      }
    });
  }

  #[test]
  fn perp_policy_price_certificate_and_external_evidence_enforce_signer_thresholds() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "one-sig-policyi0",
        vec![signed_perp_threshold_policy(RECEIVER_ADDRESS, false)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_threshold_policy(RECEIVER_ADDRESS, true)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert_eq!(policy.get("threshold").and_then(|v| v.as_u64()), Some(2));
      assert_eq!(
        policy
          .get("oracle")
          .and_then(|v| v.get("threshold"))
          .and_then(|v| v.as_u64()),
        Some(2)
      );

      let mut group_action = perp_group_action(&policy);
      group_action["pair"] = json!({
        "base": { "ns": "tap", "tick": "tap", "dec": "0" },
        "quote": perp_external_usdt_asset(),
        "price_dir": "quote-per-base"
      });
      group_action["coll"] = json!({
        "asset": perp_external_usdt_asset(),
        "mode": "evm-perp-escrow",
        "surface": perp_external_surface(),
        "min": "1",
        "max": "1000000"
      });
      group_action["bounty"] = json!({
        "rule": "operator-policy-bounty-v1",
        "activate": "0",
        "liquidate": "0",
        "settle": "0"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "ext-threshold-groupi0",
        vec![group_action],
        10
      ));
      let group = "ext-threshold-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();

      let mut one_sig_evidence = perp_external_evidence_action(
        group,
        &policy,
        &group_record,
        "long",
        "threshold-longpos",
        "100",
        "1",
      );
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "one-sig-evidencei0",
        vec![one_sig_evidence.clone()],
        10
      ));
      add_perp_evidence_signature_two(&mut one_sig_evidence, &policy, &group_record);
      assert!(apply_perp_actions_at(
        updater,
        None,
        "threshold-long-evi0",
        vec![one_sig_evidence],
        10
      ));

      let mut short_evidence = perp_external_evidence_action(
        group,
        &policy,
        &group_record,
        "short",
        "threshold-shortpos",
        "100",
        "1",
      );
      add_perp_evidence_signature_two(&mut short_evidence, &policy, &group_record);
      assert!(apply_perp_actions_at(
        updater,
        None,
        "threshold-short-evi0",
        vec![short_evidence],
        10
      ));

      let mut one_sig_activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut one_sig_activate,
        &policy,
        &group_record,
        "activate",
        "act-threshold",
        40,
        "100",
      );
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "one-sig-activatei0",
        vec![one_sig_activate.clone()],
        10
      ));
      add_perp_cert_signature_two(
        &mut one_sig_activate,
        &policy,
        &group_record,
        "activate",
        40,
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "threshold-activatei0",
        vec![one_sig_activate],
        10
      ));
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
          .unwrap()
          .unwrap()
          .get("state")
          .and_then(|v| v.as_str()),
        Some("active")
      );
    });
  }

  #[test]
  fn perp_external_collateral_can_choose_zero_bounties_under_nonzero_policy_caps() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy_with_bounties(
          RECEIVER_ADDRESS,
          "5",
          "0",
          "5"
        )],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let ext_pair = json!({
        "base": { "ns": "tap", "tick": "tap", "dec": "0" },
        "quote": perp_external_usdt_asset(),
        "price_dir": "quote-per-base"
      });
      let mut over_bounty = perp_group_action(&policy);
      over_bounty["bounty"] = json!({
        "rule": "operator-policy-bounty-v1",
        "activate": "6",
        "liquidate": "0",
        "settle": "0"
      });
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "over-bountyi0",
        vec![over_bounty],
        10
      ));

      let mut default_group_action = perp_group_action(&policy);
      default_group_action["pair"] = ext_pair.clone();
      default_group_action["coll"] = json!({
        "asset": perp_external_usdt_asset(),
        "mode": "evm-perp-escrow",
        "surface": perp_external_surface(),
        "min": "1",
        "max": "1000000"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "default-ext-groupi0",
        vec![default_group_action],
        10
      ));
      let default_group = "default-ext-groupi0:0";
      let default_group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", default_group))
        .unwrap()
        .unwrap();
      assert_eq!(
        default_group_record
          .get("bounty")
          .and_then(|v| v.get("activate"))
          .and_then(|v| v.as_str()),
        Some("5")
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "default-ext-long-evi0",
        vec![perp_external_evidence_action(
          default_group,
          &policy,
          &default_group_record,
          "long",
          "default-longpos",
          "100",
          "1"
        )],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "default-ext-short-evi0",
        vec![perp_external_evidence_action(
          default_group,
          &policy,
          &default_group_record,
          "short",
          "default-shortpos",
          "100",
          "1"
        )],
        10
      ));
      let mut default_activate =
        perp_price_action("perp-activate", default_group, default_group, "100");
      attach_perp_cert(
        &mut default_activate,
        &policy,
        &default_group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "default-ext-activatei0",
        vec![default_activate],
        10
      ));

      let mut zero_group_action = perp_group_action(&policy);
      zero_group_action["pair"] = ext_pair;
      zero_group_action["coll"] = json!({
        "asset": perp_external_usdt_asset(),
        "mode": "evm-perp-escrow",
        "surface": perp_external_surface(),
        "min": "1",
        "max": "1000000"
      });
      zero_group_action["bounty"] = json!({
        "rule": "operator-policy-bounty-v1",
        "activate": "0",
        "liquidate": "0",
        "settle": "0"
      });
      assert!(apply_perp_actions_at(
        updater,
        None,
        "zero-ext-groupi0",
        vec![zero_group_action],
        10
      ));
      let group = "zero-ext-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        group_record
          .get("bounty")
          .and_then(|v| v.get("activate"))
          .and_then(|v| v.as_str()),
        Some("0")
      );
      assert_eq!(
        group_record
          .get("bounty")
          .and_then(|v| v.get("settle"))
          .and_then(|v| v.as_str()),
        Some("0")
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "zero-ext-long-evi0",
        vec![perp_external_evidence_action(
          group,
          &policy,
          &group_record,
          "long",
          "zero-longpos",
          "100",
          "1"
        )],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "zero-ext-short-evi0",
        vec![perp_external_evidence_action(
          group,
          &policy,
          &group_record,
          "short",
          "zero-shortpos",
          "100",
          "1"
        )],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "zero-ext-activatei0",
        vec![activate],
        10
      ));
      let mut settle = perp_price_action("perp-settle", group, group, "100");
      attach_perp_cert(
        &mut settle,
        &policy,
        &group_record,
        "settle",
        "set-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "zero-ext-settlei0",
        vec![settle],
        30
      ));
    });
  }

  #[test]
  fn perp_ready_group_can_activate_or_cancel_after_formation_deadline() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-late-activatei0",
        vec![activate],
        16
      ));
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
          .unwrap()
          .unwrap()
          .get("state")
          .and_then(|v| v.as_str()),
        Some("active")
      );
    });

    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-late-canceli0",
        vec![json!({ "op": "perp-cancel", "gid": group })],
        16
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-late-refund-longi0",
        vec![json!({ "op": "perp-refund", "gid": group, "pos": "perp-longi0:0" })],
        17
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-late-refund-shorti0",
        vec![json!({ "op": "perp-refund", "gid": group, "pos": "perp-shorti0:0" })],
        17
      ));
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("1000")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        Some("1000")
      );
    });
  }

  #[test]
  fn perp_join_entry_bounds_are_enforced_at_activation_without_stranding_refunds() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let mut long_join = perp_join_action(group, "long", USER_ADDRESS);
      long_join["entry"] = json!({ "max": { "p": "99", "q": "1" } });
      assert!(apply_perp_actions_at(updater, Some(&long), "perp-longi0", vec![long_join], 10));
      let mut short_join = perp_join_action(group, "short", RECEIVER_ADDRESS);
      short_join["entry"] = json!({ "min": { "p": "101", "q": "1" } });
      assert!(apply_perp_actions_at(updater, Some(&short), "perp-shorti0", vec![short_join], 10));
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        group_record.get("entry_bounds"),
        Some(&json!({
          "long_max": { "p": "99", "q": "1" },
          "short_min": { "p": "101", "q": "1" }
        }))
      );
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(&mut activate, &policy, &group_record, "activate", "act-1", 40, "100");
      let mut actions = vec![activate];
      assert!(!updater.validate_token_proof_actions(&mut actions, None, "perp-bound-rejecti0", 16, 1000));
      assert_eq!(
        get_string(updater, &format!("perp/cn/{}/{}/entry/1", policy.get("id").unwrap().as_str().unwrap(), group)),
        None
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-bound-canceli0",
        vec![json!({ "op": "perp-cancel", "gid": group })],
        16
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-bound-refund-longi0",
        vec![json!({ "op": "perp-refund", "gid": group, "pos": "perp-longi0:0" })],
        17
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-bound-refund-shorti0",
        vec![json!({ "op": "perp-refund", "gid": group, "pos": "perp-shorti0:0" })],
        17
      ));
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(), Some("1000"));
      assert_eq!(get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(), Some("1000"));
    });
  }

  #[test]
  fn perp_join_entry_bounds_reject_malformed_and_missing_required_values() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let mut missing = perp_join_action(group, "long", USER_ADDRESS);
      missing.as_object_mut().unwrap().remove("entry");
      let mut actions = vec![missing];
      assert!(!updater.validate_token_proof_actions(&mut actions, Some(&long), "perp-missing-entryi0", 10, 1000));
      let mut zero = perp_join_action(group, "long", USER_ADDRESS);
      zero["entry"] = json!({ "max": { "p": "0", "q": "1" } });
      let mut actions = vec![zero];
      assert!(!updater.validate_token_proof_actions(&mut actions, Some(&long), "perp-zero-entryi0", 10, 1000));
      let mut wrong_side = perp_join_action(group, "long", USER_ADDRESS);
      wrong_side["entry"] = json!({ "min": { "p": "1", "q": "1" } });
      let mut actions = vec![wrong_side];
      assert!(!updater.validate_token_proof_actions(&mut actions, Some(&long), "perp-wrong-side-entryi0", 10, 1000));
    });
  }

  #[test]
  fn perp_activation_readiness_enforces_notional_ratio_and_imbalance() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let mut group_action = perp_group_action(&policy);
      group_action["ready"]["min_long_not"] = json!("300");
      group_action["ready"]["min_short_not"] = json!("300");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![group_action],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let joined_group = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        joined_group.get("long_notional").and_then(|v| v.as_str()),
        Some("200")
      );
      assert_eq!(
        joined_group.get("short_notional").and_then(|v| v.as_str()),
        Some("200")
      );
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "perp-notional-not-readyi0",
        vec![activate],
        16
      ));
    });

    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      let mut group_action = perp_group_action(&policy);
      group_action["ready"]["ratio_min"] = json!({ "n": "2", "d": "1" });
      group_action["ready"]["ratio_max"] = json!({ "n": "3", "d": "1" });
      group_action["ready"]["max_imbalance_not"] = json!("50");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![group_action],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      let mut long_join = perp_join_action(group, "long", USER_ADDRESS);
      long_join["coll"] = json!("200");
      let short_join = perp_join_action(group, "short", RECEIVER_ADDRESS);
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![long_join],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![short_join],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "perp-ratio-imbalance-not-readyi0",
        vec![activate],
        16
      ));
    });
  }

  #[test]
  fn perp_terminal_and_exposure_reducing_actions_remain_allowed_after_authority_cancellation() {
    assert!(
      InscriptionUpdater::token_proof_post_cancel_settlement_actions(
        &[],
        &json!({
          "actions": [
            { "op": "perp-cancel", "gid": "groupi0:0" },
            { "op": "perp-refund", "gid": "groupi0:0", "pos": "posi0:0" },
            { "op": "perp-activate", "gid": "groupi0:0", "cert": {} },
            { "op": "perp-close", "gid": "groupi0:0", "pos": "posi0:0", "qty": {} },
            { "op": "perp-liquidate", "gid": "groupi0:0", "pos": "posi0:0", "cert": {} },
            { "op": "perp-settle", "gid": "groupi0:0", "fallback": "last-valid-at-expiry-v1" },
            { "op": "perp-claim", "gid": "groupi0:0", "pos": "posi0:0" }
          ]
        })
      )
    );
    assert!(
      !InscriptionUpdater::token_proof_post_cancel_settlement_actions(
        &[],
        &json!({ "actions": [{ "op": "perp-join", "gid": "groupi0:0" }] })
      )
    );
    assert!(
      !InscriptionUpdater::token_proof_post_cancel_settlement_actions(
        &[],
        &json!({ "actions": [{ "op": "perp-open-group", "pid": "perp-main" }] })
      )
    );
  }

  #[test]
  fn perp_price_certificates_reject_wrong_pair_binding() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let mut wrong_pair = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut wrong_pair,
        &policy,
        &group_record,
        "activate",
        "wrong-pair-1",
        40,
        "100",
      );
      wrong_pair["cert"]["pair"] = json!({
        "base": { "ns": "tap", "tick": "tap", "dec": "0" },
        "quote": { "ns": "evm", "cid": "eip155:1", "ak": "erc20", "aid": "0x0000000000000000000000000000000000000001", "dec": "6", "sym": "USDT" },
        "price_dir": "quote-per-base"
      });
      resign_perp_cert(&mut wrong_pair, &policy, &group_record, "activate", 40);
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "perp-wrong-pairi0",
        vec![wrong_pair],
        10
      ));

      let mut wrong_dir = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut wrong_dir,
        &policy,
        &group_record,
        "activate",
        "wrong-dir-1",
        40,
        "100",
      );
      wrong_dir["cert"]["pair"]["price_dir"] = json!("base-per-quote");
      resign_perp_cert(&mut wrong_dir, &policy, &group_record, "activate", 40);
      assert!(!apply_perp_actions_at(
        updater,
        None,
        "perp-wrong-diri0",
        vec![wrong_dir],
        10
      ));
    });
  }

  #[test]
  fn perp_price_certificates_reject_lower_sequence_after_newer_certificate() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));

      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-activatei0",
        vec![activate],
        10
      ));

      let mut close_high = perp_price_action("perp-close", "perp-longi0:0", group, "100");
      close_high["qty"] = json!({ "mode": "fraction", "n": "1", "d": "2" });
      attach_perp_cert(
        &mut close_high,
        &policy,
        &group_record,
        "close",
        "close-10",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-close-highi0",
        vec![close_high],
        20
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "perp/cseq/{}/{}/close",
            policy.get("id").unwrap().as_str().unwrap(),
            group
          )
        )
        .as_deref(),
        Some("10")
      );

      let mut close_low = perp_price_action("perp-close", "perp-longi0:0", group, "101");
      attach_perp_cert(
        &mut close_low,
        &policy,
        &group_record,
        "close",
        "close-9",
        40,
        "101",
      );
      assert!(!apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-close-lowi0",
        vec![close_low],
        20
      ));
    });
  }

  #[test]
  fn perp_active_group_can_settle_through_bounded_fallback_without_signer_liveness() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-activatei0",
        vec![activate],
        10
      ));

      let mut early_fallback =
        vec![json!({ "op": "perp-settle", "gid": group, "fallback": "last-valid-at-expiry-v1" })];
      assert!(!updater.validate_token_proof_actions(
        &mut early_fallback,
        None,
        "perp-early-fallbacki0",
        42,
        1000
      ));

      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-fallback-settlei0",
        vec![json!({ "op": "perp-settle", "gid": group, "fallback": "last-valid-at-expiry-v1" })],
        43
      ));
      let settled_group = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert_eq!(
        settled_group.get("state").and_then(|v| v.as_str()),
        Some("settled")
      );
      assert_eq!(
        settled_group
          .get("settlement")
          .and_then(|v| v.get("cert"))
          .and_then(|v| v.get("dom"))
          .and_then(|v| v.as_str()),
        Some("tap-perp-fallback-v1")
      );
      assert_eq!(get_string(updater, "perp/certl").as_deref(), Some("1"));

      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-fallback-claim-longi0",
        vec![json!({ "op": "perp-claim", "gid": group, "pos": "perp-longi0:0" })],
        44
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-fallback-claim-shorti0",
        vec![json!({ "op": "perp-claim", "gid": group, "pos": "perp-shorti0:0" })],
        44
      ));
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("998")
      );
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        Some("1002")
      );
    });
  }

  #[test]
  fn perp_duplicate_claim_rejected_atomically() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      put_balance(updater, RECEIVER_ADDRESS, "tap", "1000");
      let long = auth_link(USER_ADDRESS, "long-authi0");
      let short = auth_link(RECEIVER_ADDRESS, "short-authi0");
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-policyi0",
        vec![signed_perp_policy(RECEIVER_ADDRESS)],
        10
      ));
      let policy = updater
        .tap_get::<serde_json::Value>("perp/p/perp-main")
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let group = "perp-groupi0:0";
      let group_record = updater
        .tap_get::<serde_json::Value>(&format!("perp/g/{}", group))
        .unwrap()
        .unwrap();
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-longi0",
        vec![perp_join_action(group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        Some(&short),
        "perp-shorti0",
        vec![perp_join_action(group, "short", RECEIVER_ADDRESS)],
        10
      ));
      let mut activate = perp_price_action("perp-activate", group, group, "100");
      attach_perp_cert(
        &mut activate,
        &policy,
        &group_record,
        "activate",
        "act-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-activatei0",
        vec![activate],
        10
      ));
      let mut settle = perp_price_action("perp-settle", group, group, "100");
      attach_perp_cert(
        &mut settle,
        &policy,
        &group_record,
        "settle",
        "set-1",
        40,
        "100",
      );
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-settlei0",
        vec![settle],
        30
      ));

      let mut spoof_claim = vec![json!({
        "op": "perp-claim",
        "gid": group,
        "pos": "perp-longi0:0",
        "to": RECEIVER_ADDRESS
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut spoof_claim,
        Some(&long),
        "perp-spoof-claimi0",
        31,
        1000
      ));

      let mut duplicate = vec![
        json!({ "op": "perp-claim", "gid": group, "pos": "perp-longi0:0" }),
        json!({ "op": "perp-claim", "gid": group, "pos": "perp-longi0:0" }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut duplicate,
        Some(&long),
        "perp-duplicatei0",
        31,
        1000
      ));
      assert!(updater
        .tap_get::<String>("perp/cl/perp-longi0:0")
        .unwrap()
        .is_none());

      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-refund-groupi0",
        vec![perp_group_action(&policy)],
        10
      ));
      let refund_group = "perp-refund-groupi0:0";
      assert!(apply_perp_actions_at(
        updater,
        Some(&long),
        "perp-refund-longi0",
        vec![perp_join_action(refund_group, "long", USER_ADDRESS)],
        10
      ));
      assert!(apply_perp_actions_at(
        updater,
        None,
        "perp-cancel-refundi0",
        vec![json!({ "op": "perp-cancel", "gid": refund_group })],
        16
      ));
      let mut spoof_refund = vec![json!({
        "op": "perp-refund",
        "gid": refund_group,
        "pos": "perp-refund-longi0:0",
        "to": RECEIVER_ADDRESS
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut spoof_refund,
        Some(&long),
        "perp-spoof-refundi0",
        17,
        1000
      ));
    });
  }

  #[test]
  fn certified_control_lock_requires_cert_and_stores_consume_replay() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      let signer_a = cert_pubkey(1);
      let signer_b = cert_pubkey(2);
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      let owner = auth_link(USER_ADDRESS, "owner-authi0");
      let claimant = auth_link(RECEIVER_ADDRESS, "claim-authi0");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));
      let lock = json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "control": {
          "type": "cert",
          "id": "policy-1",
          "threshold": 2,
          "signers": [signer_b.to_uppercase(), signer_a],
          "scope": ["claim", "refund"],
          "expires": "30",
          "rules": { "terminal_refund_after": "40" }
        }
      });
      assert!(apply_actions_at(
        updater,
        &owner,
        "cert-locki0",
        vec![lock],
        10
      ));
      let stored = updater
        .tap_get::<TokenLockRecord>("l/cert-locki0:0")
        .unwrap()
        .unwrap();
      let control = stored.control.clone().unwrap();
      let mut expected_signers = vec![cert_pubkey(1), cert_pubkey(2)];
      expected_signers.sort();
      assert_eq!(control.get("signers").unwrap(), &json!(expected_signers));
      assert_eq!(control.get("hash").unwrap().as_str().unwrap().len(), 64);

      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-claim-missingi0",
        vec![json!({ "op": "claim", "lock": "cert-locki0:0", "preimage": "secret" })],
        11
      ));

      let mut claim = json!({ "op": "claim", "lock": "cert-locki0:0", "preimage": "secret" });
      attach_cert(
        &mut claim,
        &control,
        "claim",
        "claim-nonce-1",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      assert!(apply_actions_at(
        updater,
        &claimant,
        "cert-claimi0",
        vec![claim],
        11
      ));
      let consume = updater
        .tap_get::<TokenLockConsumeRecord>("lc/cert-locki0:0")
        .unwrap()
        .unwrap();
      assert_eq!(
        consume.cert.as_ref().unwrap().get("signers").unwrap(),
        &json!(expected_signers)
      );
      assert!(updater
        .tap_get::<TokenLockConsumeRecord>("ccn/policy-1/cert-locki0:0/claim/claim-nonce-1")
        .unwrap()
        .is_some());
    });
  }

  #[test]
  fn certified_refund_terminal_fallback_does_not_need_cert() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      let owner = auth_link(USER_ADDRESS, "owner-authi0");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));
      let lock = json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "control": {
          "type": "cert",
          "id": "policy-terminal",
          "threshold": 1,
          "signers": [cert_pubkey(1)],
          "scope": ["refund"],
          "rules": { "terminal_refund_after": "40" }
        }
      });
      assert!(apply_actions_at(
        updater,
        &owner,
        "cert-terminali0",
        vec![lock],
        10
      ));
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-refund-missingi0",
        vec![json!({ "op": "refund", "lock": "cert-terminali0:0" })],
        20
      ));
      assert!(apply_actions_at(
        updater,
        &owner,
        "cert-refund-terminali0",
        vec![json!({ "op": "refund", "lock": "cert-terminali0:0" })],
        40
      ));
    });
  }

  #[test]
  fn certified_control_rejects_malformed_policies_and_misplaced_fields() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      let owner = auth_link(USER_ADDRESS, "owner-authi0");
      let claimant = auth_link(RECEIVER_ADDRESS, "claim-authi0");
      let signer = cert_pubkey(1);

      let mut bad = certified_lock_action("policy-extra", 1, vec![signer.clone()]);
      bad["control"]["extra"] = json!(true);
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-extrai0",
        vec![bad],
        10
      ));

      let bad = certified_lock_action("bad/id", 1, vec![signer.clone()]);
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-idi0",
        vec![bad],
        10
      ));

      let bad = certified_lock_action("policy-dup", 1, vec![signer.clone(), signer.clone()]);
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-dupi0",
        vec![bad],
        10
      ));

      let bad = certified_lock_action("policy-threshold", 2, vec![signer.clone()]);
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-thresholdi0",
        vec![bad],
        10
      ));

      let mut bad = certified_lock_action("policy-leading-zero", 1, vec![signer.clone()]);
      bad["control"]["threshold"] = json!("01");
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-leading-zeroi0",
        vec![bad],
        10
      ));

      let mut bad = certified_lock_action("policy-empty-scope", 1, vec![signer.clone()]);
      bad["control"]["scope"] = json!([]);
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-empty-scopei0",
        vec![bad],
        10
      ));

      let mut bad = certified_lock_action("policy-wrong-hash", 1, vec![signer.clone()]);
      bad["control"]["hash"] = json!("00".repeat(32));
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-hashi0",
        vec![bad],
        10
      ));

      let mut bad = certified_lock_action("policy-missing-terminal", 1, vec![signer.clone()]);
      bad["control"].as_object_mut().unwrap().remove("rules");
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-missing-terminali0",
        vec![bad],
        10
      ));

      let mut bad = certified_lock_action("policy-terminal", 1, vec![signer.clone()]);
      bad["control"]["rules"]["terminal_refund_after"] = json!("20");
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-terminali0",
        vec![bad],
        10
      ));

      let mut bad = certified_lock_action("policy-lock-cert", 1, vec![signer.clone()]);
      bad["cert"] = json!({ "nonce": "not-allowed" });
      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-bad-lock-certi0",
        vec![bad],
        10
      ));

      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-control-on-claimi0",
        vec![json!({
          "op": "claim",
          "lock": "missingi0:0",
          "preimage": "secret",
          "control": certified_lock_action("policy-misplaced", 1, vec![signer.clone()])["control"]
        })],
        10
      ));

      assert!(!apply_actions_at(
        updater,
        &owner,
        "cert-on-sendi0",
        vec![json!({
          "op": "send",
          "tick": "tap",
          "amt": "1",
          "to": RECEIVER_ADDRESS,
          "cert": { "nonce": "not-allowed" }
        })],
        10
      ));
    });
  }

  #[test]
  fn certified_control_rejects_bad_certs_and_preserves_current_lock_flow() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      let owner = auth_link(USER_ADDRESS, "owner-authi0");
      let claimant = auth_link(RECEIVER_ADDRESS, "claim-authi0");

      let lock = certified_lock_action("policy-negative", 2, vec![cert_pubkey(1), cert_pubkey(2)]);
      assert!(apply_actions_at(
        updater,
        &owner,
        "cert-negativei0",
        vec![lock],
        10
      ));
      let policy = updater
        .tap_get::<TokenLockRecord>("l/cert-negativei0:0")
        .unwrap()
        .unwrap()
        .control
        .unwrap();

      let mut one_sig = json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut one_sig,
        &policy,
        "claim",
        "one-sig",
        15,
        &[(1, cert_pubkey(1))],
      );
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-one-sigi0",
        vec![one_sig],
        11
      ));

      let mut duplicate_sig =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut duplicate_sig,
        &policy,
        "claim",
        "duplicate-sig",
        15,
        &[(1, cert_pubkey(1)), (1, cert_pubkey(1))],
      );
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-duplicate-sigi0",
        vec![duplicate_sig],
        11
      ));

      let mut unknown_signer =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut unknown_signer,
        &policy,
        "claim",
        "unknown-signer",
        15,
        &[(1, cert_pubkey(1)), (3, cert_pubkey(3))],
      );
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-unknown-signeri0",
        vec![unknown_signer],
        11
      ));

      let mut wrong_signer =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut wrong_signer,
        &policy,
        "claim",
        "wrong-signer",
        15,
        &[(1, cert_pubkey(2)), (2, cert_pubkey(2))],
      );
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-wrong-signeri0",
        vec![wrong_signer],
        11
      ));

      let mut wrong_action =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut wrong_action,
        &policy,
        "refund",
        "wrong-action",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-wrong-actioni0",
        vec![wrong_action],
        11
      ));

      let mut wrong_target =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut wrong_target,
        &policy,
        "claim",
        "wrong-target",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      wrong_target["cert"]["target"] = json!("other-locki0:0");
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-wrong-targeti0",
        vec![wrong_target],
        11
      ));

      let mut wrong_policy =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut wrong_policy,
        &policy,
        "claim",
        "wrong-policy",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      wrong_policy["cert"]["policy"] = json!("other-policy");
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-wrong-policyi0",
        vec![wrong_policy],
        11
      ));

      let mut bad_version =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut bad_version,
        &policy,
        "claim",
        "bad-version",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      bad_version["cert"]["v"] = json!(2);
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-bad-versioni0",
        vec![bad_version],
        11
      ));

      let mut bad_nonce =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut bad_nonce,
        &policy,
        "claim",
        "bad-nonce",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      bad_nonce["cert"]["nonce"] = json!("bad/nonce");
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-bad-noncei0",
        vec![bad_nonce],
        11
      ));

      let mut extra_cert_field =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut extra_cert_field,
        &policy,
        "claim",
        "extra-cert-field",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      extra_cert_field["cert"]["extra"] = json!(true);
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-extra-fieldi0",
        vec![extra_cert_field],
        11
      ));

      let mut expired = json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut expired,
        &policy,
        "claim",
        "expired",
        10,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-expiredi0",
        vec![expired],
        11
      ));

      let mut tampered =
        json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut tampered,
        &policy,
        "claim",
        "tampered",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      tampered["preimage"] = json!("wrong");
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-tamperedi0",
        vec![tampered],
        11
      ));

      let mut replay = json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut replay,
        &policy,
        "claim",
        "replay",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      let replay_marker = TokenLockConsumeRecord {
        lock: "cert-negativei0:0".to_string(),
        action: "claim".to_string(),
        kind: "htlc".to_string(),
        owner: USER_ADDRESS.to_string(),
        target: RECEIVER_ADDRESS.to_string(),
        tick: "tap".to_string(),
        amt: "1".to_string(),
        blck: 11,
        tx: "replay".to_string(),
        vo: 0,
        val: "0".to_string(),
        ins: "replayi0".to_string(),
        num: 1,
        ts: 1000,
        fee: None,
        al: None,
        total: None,
        cert: None,
      };
      updater
        .tap_put(
          "ccn/policy-negative/cert-negativei0:0/claim/replay",
          &replay_marker,
        )
        .unwrap();
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "cert-replayi0",
        vec![replay],
        11
      ));
      updater
        .tap_del("ccn/policy-negative/cert-negativei0:0/claim/replay")
        .unwrap();

      let mut good = json!({ "op": "claim", "lock": "cert-negativei0:0", "preimage": "secret" });
      attach_cert(
        &mut good,
        &policy,
        "claim",
        "good",
        15,
        &[(1, cert_pubkey(1)), (2, cert_pubkey(2))],
      );
      assert!(apply_actions_at(
        updater,
        &claimant,
        "cert-goodi0",
        vec![good],
        11
      ));

      put_balance(updater, USER_ADDRESS, "tap", "100");
      let mut plain_lock = certified_lock_action("policy-plain", 1, vec![cert_pubkey(1)]);
      plain_lock.as_object_mut().unwrap().remove("control");
      assert!(apply_actions_at(
        updater,
        &owner,
        "plain-locki0",
        vec![plain_lock],
        10
      ));
      assert!(updater
        .tap_get::<TokenLockRecord>("l/plain-locki0:0")
        .unwrap()
        .unwrap()
        .control
        .is_none());
      assert!(!apply_actions_at(
        updater,
        &claimant,
        "plain-cert-claimi0",
        vec![json!({
          "op": "claim",
          "lock": "plain-locki0:0",
          "preimage": "secret",
          "cert": { "nonce": "unexpected" }
        })],
        11
      ));
      assert!(apply_actions_at(
        updater,
        &claimant,
        "plain-claimi0",
        vec![json!({ "op": "claim", "lock": "plain-locki0:0", "preimage": "secret" })],
        11
      ));
      assert!(updater
        .tap_get::<TokenLockConsumeRecord>("lc/plain-locki0:0")
        .unwrap()
        .unwrap()
        .cert
        .is_none());
    });
  }

  #[test]
  fn redeem_actions_use_writer_ticker_keying_for_unicode_tickers() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      let stake_tick = "👨‍👩‍👧‍👦/İ";
      let reward_tick = "ΟΣ";
      let stake_lc = InscriptionUpdater::js_to_lowercase(stake_tick);
      let reward_lc = InscriptionUpdater::js_to_lowercase(reward_tick);
      let stake_key = InscriptionUpdater::json_stringify_lower(stake_tick);
      let reward_key = InscriptionUpdater::json_stringify_lower(reward_tick);
      put_deploy(updater, stake_tick, 0);
      put_deploy(updater, reward_tick, 0);
      put_balance(updater, USER_ADDRESS, stake_tick, "1000");
      let link = auth_link(USER_ADDRESS, "authority-inscription");

      assert!(apply_actions_at(
        updater,
        &link,
        "unicode-lock",
        vec![json!({
          "op": "lock",
          "kind": "htlc",
          "tick": stake_tick,
          "amt": "10",
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "refund_after": "20",
          "condition": { "type": "hashlock", "hash": "00".repeat(32) }
        })],
        10
      ));
      let lock = updater
        .tap_get::<TokenLockRecord>("l/unicode-lock:0")
        .unwrap()
        .unwrap();
      assert_eq!(lock.tick, stake_lc);
      assert_eq!(
        get_string(updater, &format!("ll/{}/{}", USER_ADDRESS, stake_key)).as_deref(),
        Some("10")
      );

      assert!(apply_actions_at(
        updater,
        &link,
        "unicode-stake-cfg",
        vec![json!({
          "op": "auth-cfg",
          "k": "stk",
          "stk": stake_tick,
          "rt": [reward_tick],
          "ctl": { "ty": "ta", "auth": link.ins },
          "r": {
            "cm": "arps",
            "rnd": "flr",
            "aw": false,
            "ep": "reject",
            "tr": [{ "id": "base", "dur": "1", "w": "1" }]
          }
        })],
        10
      ));
      let stake_cfg = updater
        .tap_get::<AuthorityConfigRecord>("ah/unicode-stake-cfg:0")
        .unwrap()
        .unwrap();
      assert_eq!(stake_cfg.stk, stake_lc);
      assert_eq!(stake_cfg.rt, vec![reward_lc.clone()]);

      assert!(apply_actions_at(
        updater,
        &link,
        "unicode-stake",
        vec![json!({
          "op": "stake",
          "auth": "unicode-stake-cfg:0",
          "tick": stake_tick,
          "amt": "5",
          "tier": "base",
          "claim": RECEIVER_ADDRESS
        })],
        10
      ));
      let position = updater
        .tap_get::<StakePositionRecord>("sp/unicode-stake:0")
        .unwrap()
        .unwrap();
      assert_eq!(position.tick, stake_lc);
      assert_eq!(
        get_string(
          updater,
          &format!("ab/{}/{}", "unicode-stake-cfg:0", stake_key)
        )
        .as_deref(),
        Some("5")
      );

      let mut amm_action = amm_config(&link, "30", "0");
      amm_action["a"] = json!([
        { "ty": "tap", "tick": stake_tick },
        { "ty": "tap", "tick": reward_tick }
      ]);
      assert!(apply_actions_at(
        updater,
        &link,
        "unicode-amm-cfg",
        vec![amm_action],
        10
      ));
      let amm_cfg = updater
        .tap_get::<AuthorityConfigRecord>("ah/unicode-amm-cfg:0")
        .unwrap()
        .unwrap();
      assert_eq!(amm_cfg.a[0]["tick"], json!(stake_lc.clone()));
      assert_eq!(amm_cfg.a[1]["tick"], json!(reward_lc.clone()));
      assert_eq!(
        amm_cfg.ak,
        vec![format!("tap:{stake_lc}"), format!("tap:{reward_lc}")]
      );

      assert!(apply_actions_at(
        updater,
        &link,
        "unicode-sale-cfg",
        vec![json!({
          "op": "auth-cfg",
          "k": "sale",
          "st": stake_tick,
          "pt": reward_tick,
          "ctl": { "ty": "ta", "auth": link.ins },
          "tre": { "tt": "a", "to": RECEIVER_ADDRESS },
          "s": {
            "sh": "10",
            "eh": "20",
            "hc": "100",
            "r": { "cm": "fix", "pa": "1", "sa": "1", "rnd": "flr" },
            "ov": "reject"
          }
        })],
        10
      ));
      let sale_cfg = updater
        .tap_get::<AuthorityConfigRecord>("ah/unicode-sale-cfg:0")
        .unwrap()
        .unwrap();
      assert_eq!(sale_cfg.st.as_deref(), Some(stake_lc.as_str()));
      assert_eq!(sale_cfg.pt.as_deref(), Some(reward_lc.as_str()));
      assert!(updater
        .tap_get::<DeployRecord>(&format!("d/{}", reward_key))
        .unwrap()
        .is_some());
    });
  }

  #[test]
  fn redeem_action_null_object_fields_reject_without_state() {
    with_test_updater(BtcNetwork::Signet, 10, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "dmt", 0);
      put_balance(updater, USER_ADDRESS, "tap", "1000");
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));
      let tap_key = InscriptionUpdater::json_stringify_lower("tap");

      let mut null_condition_lock = json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "10",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "refund_after": "20",
        "condition": { "type": "hashlock", "hash": hash }
      });
      null_condition_lock["condition"] = serde_json::Value::Null;
      assert!(!apply_actions_at(
        updater,
        &link,
        "null-lock-condition",
        vec![null_condition_lock],
        10
      ));
      assert!(updater
        .tap_get::<TokenLockRecord>("l/null-lock-condition:0")
        .unwrap()
        .is_none());
      assert!(get_string(updater, &format!("ll/{}/{}", USER_ADDRESS, tap_key)).is_none());

      for (case_id, field) in [
        ("null-ob-src", "src"),
        ("null-ob-claim", "cl"),
        ("null-ob-refund", "rf"),
        ("null-ob-condition", "cond"),
      ] {
        let mut action = obligation_open(&hash);
        action[field] = serde_json::Value::Null;
        assert!(!apply_actions_at(updater, &link, case_id, vec![action], 10));
        assert!(updater
          .tap_get::<serde_json::Value>(&format!("ob/{case_id}:0"))
          .unwrap()
          .is_none());
      }

      for (case_id, mut action) in [
        (
          "null-staking-ctl",
          json!({
            "op": "auth-cfg",
            "k": "stk",
            "stk": "tap",
            "rt": ["dmt"],
            "ctl": null,
            "r": {
              "cm": "arps",
              "rnd": "flr",
              "aw": false,
              "ep": "reject",
              "tr": [{ "id": "base", "dur": "1", "w": "1" }]
            }
          }),
        ),
        (
          "null-staking-r",
          json!({
            "op": "auth-cfg",
            "k": "stk",
            "stk": "tap",
            "rt": ["dmt"],
            "ctl": { "ty": "ta", "auth": link.ins },
            "r": null
          }),
        ),
      ] {
        assert!(!apply_actions_at(
          updater,
          &link,
          case_id,
          vec![action.take()],
          10
        ));
        assert!(updater
          .tap_get::<AuthorityConfigRecord>(&format!("ah/{case_id}:0"))
          .unwrap()
          .is_none());
      }

      let mut null_amm_ctl = amm_config(&link, "30", "0");
      null_amm_ctl["ctl"] = serde_json::Value::Null;
      assert!(!apply_actions_at(
        updater,
        &link,
        "null-amm-ctl",
        vec![null_amm_ctl],
        10
      ));
      assert!(updater
        .tap_get::<AuthorityConfigRecord>("ah/null-amm-ctl:0")
        .unwrap()
        .is_none());
      assert!(updater
        .tap_get::<AuthorityConfigRecord>("amm/null-amm-ctl:0")
        .unwrap()
        .is_none());

      let mut null_amm_c = amm_config(&link, "30", "0");
      null_amm_c["c"] = serde_json::Value::Null;
      assert!(!apply_actions_at(
        updater,
        &link,
        "null-amm-c",
        vec![null_amm_c],
        10
      ));
      assert!(updater
        .tap_get::<AuthorityConfigRecord>("ah/null-amm-c:0")
        .unwrap()
        .is_none());
      assert!(updater
        .tap_get::<AuthorityConfigRecord>("amm/null-amm-c:0")
        .unwrap()
        .is_none());

      for (case_id, field_path) in [
        ("null-sale-ctl", "ctl"),
        ("null-sale-s", "s"),
        ("null-sale-r", "s.r"),
      ] {
        let mut action = json!({
          "op": "auth-cfg",
          "k": "sale",
          "st": "tap",
          "pt": "dmt",
          "ctl": { "ty": "ta", "auth": link.ins },
          "tre": { "tt": "a", "to": RECEIVER_ADDRESS },
          "s": {
            "sh": "10",
            "eh": "20",
            "hc": "100",
            "r": { "cm": "fix", "pa": "1", "sa": "1", "rnd": "flr" },
            "ov": "reject"
          }
        });
        if field_path == "ctl" {
          action["ctl"] = serde_json::Value::Null;
        } else if field_path == "s" {
          action["s"] = serde_json::Value::Null;
        } else {
          action["s"]["r"] = serde_json::Value::Null;
        }
        assert!(!apply_actions_at(updater, &link, case_id, vec![action], 10));
        assert!(updater
          .tap_get::<AuthorityConfigRecord>(&format!("ah/{case_id}:0"))
          .unwrap()
          .is_none());
        assert!(updater
          .tap_get::<serde_json::Value>(&format!("sale/{case_id}:0"))
          .unwrap()
          .is_none());
      }

      assert!(!apply_actions_at(
        updater,
        &link,
        "null-release-lock",
        vec![json!({ "op": "claim", "lock": null, "preimage": "secret" })],
        20
      ));
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tap_key)).as_deref(),
        Some("1000")
      );
    });
  }

  #[test]
  fn obligation_open_claim_refund_and_validation_edges_match_writer() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      put_authority_config(updater, "authority-inscription");
      updater
        .tap_put("tains/authority-inscription", &"".to_string())
        .unwrap();
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));

      assert!(apply_actions_at(
        updater,
        &link,
        "ob-open",
        vec![obligation_open(&hash)],
        10,
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "oll/a/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("10")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("100")
      );

      assert!(apply_actions_at(
        updater,
        &link,
        "ob-claim",
        vec![json!({ "op": "ob-claim", "ob": "ob-open:0", "preimage": "secret" })],
        11,
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "oll/a/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("0")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("90")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            RECEIVER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("10")
      );
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>("ob/ob-open:0")
          .unwrap()
          .unwrap()
          .get("st")
          .and_then(|v| v.as_str()),
        Some("claimed")
      );

      let mut duplicate =
        vec![json!({ "op": "ob-claim", "ob": "ob-open:0", "preimage": "secret" })];
      assert!(!updater.validate_token_proof_actions(&mut duplicate, Some(&link), "dup", 12, 1000));

      for bad_amt in [
        json!(10),
        json!("abc0.9999"),
        json!("1e3"),
        json!("1,000"),
        json!("-1"),
        json!(""),
      ] {
        let mut bad = obligation_open(&hash);
        bad["amt"] = bad_amt;
        assert!(updater
          .validate_obligation_open_action(&bad, Some(&link), "bad", 0, 10)
          .is_none());
      }

      let mut bad_source = obligation_open(&hash);
      bad_source["src"] = json!({ "tt": "sale", "to": "authority-inscription", "tick": "tap" });
      assert!(updater
        .validate_obligation_open_action(&bad_source, Some(&link), "bad-source", 0, 10)
        .is_none());

      let mut bad_target = obligation_open(&hash);
      bad_target["cl"] = json!({ "tt": "sale", "to": RECEIVER_ADDRESS });
      assert!(updater
        .validate_obligation_open_action(&bad_target, Some(&link), "bad-target", 0, 10)
        .is_none());

      let mut bad_expiry = obligation_open(&hash);
      bad_expiry["exp"] = json!("9");
      assert!(updater
        .validate_obligation_open_action(&bad_expiry, Some(&link), "bad-expiry", 0, 10)
        .is_none());

      let mut bad_refund = obligation_open(&hash);
      bad_refund["rf"] = json!({ "tt": "a", "to": RECEIVER_ADDRESS });
      assert!(updater
        .validate_obligation_open_action(&bad_refund, Some(&link), "bad-refund", 0, 10)
        .is_none());

      let mut bad_ctx_ref = obligation_open(&hash);
      bad_ctx_ref["ctx"] = json!({ "app": "test", "ref": "bad/ref" });
      assert!(updater
        .validate_obligation_open_action(&bad_ctx_ref, Some(&link), "bad-ctx-ref", 0, 10)
        .is_none());

      let mut bad_ctx_array = obligation_open(&hash);
      bad_ctx_array["ctx"] = json!({ "app": "test", "ref": "ok-ref", "extra": ["array"] });
      assert!(updater
        .validate_obligation_open_action(&bad_ctx_array, Some(&link), "bad-ctx-array", 0, 10)
        .is_none());

      let mut canonical_lock = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash.to_uppercase(), "ignored": "field" },
        "refund_after": "20",
        "data": { "ref": "lock-1", "purpose": "marketplace", "ext": { "z": "ok" } }
      })];
      assert!(updater.validate_token_proof_actions(
        &mut canonical_lock,
        Some(&link),
        "canonical-lock",
        10,
        1000
      ));
      assert_eq!(
        canonical_lock[0]["condition"],
        json!({ "type": "hashlock", "hash": hash })
      );
      assert_eq!(
        canonical_lock[0]["data"],
        json!({ "ext": { "z": "ok" }, "purpose": "marketplace", "ref": "lock-1" })
      );

      let mut string_lock_data = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "data": "proof-lock-claim"
      })];
      assert!(updater.validate_token_proof_actions(
        &mut string_lock_data,
        Some(&link),
        "string-lock-data",
        10,
        1000
      ));
      assert_eq!(string_lock_data[0]["data"], json!("proof-lock-claim"));

      let mut number_lock_data = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "data": 1.0
      })];
      assert!(updater.validate_token_proof_actions(
        &mut number_lock_data,
        Some(&link),
        "number-lock-data",
        10,
        1000
      ));
      assert_eq!(number_lock_data[0]["data"], json!(1));

      let mut bad_lock_array_data = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "data": ["array"]
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut bad_lock_array_data,
        Some(&link),
        "bad-lock-array-data",
        10,
        1000
      ));

      let mut bad_lock_data = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "data": { "bad/key": "x" }
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut bad_lock_data,
        Some(&link),
        "bad-lock-data",
        10,
        1000
      ));

      let mut bad_lock_ext = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20",
        "data": { "ref": "lock-2", "ext": ["array"] }
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut bad_lock_ext,
        Some(&link),
        "bad-lock-ext",
        10,
        1000
      ));

      updater
        .tap_put(
          &format!(
            "ab/{}/{}",
            "authority-inscription",
            InscriptionUpdater::json_stringify_lower("tap")
          ),
          &"25".to_string(),
        )
        .unwrap();
      let authority_open = json!({
        "op": "ob-open",
        "src": { "tt": "h", "to": "authority-inscription", "tick": "tap" },
        "amt": "10",
        "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
        "rf": { "tt": "h", "to": "authority-inscription" },
        "cond": { "ty": "hash", "h": hash },
        "ra": "20",
        "exp": "30",
        "ctx": { "app": "test", "ref": "authority-obligation" }
      });
      assert!(apply_actions_at(
        updater,
        &link,
        "authority-open",
        vec![authority_open],
        10,
      ));
      assert!(apply_actions_at(
        updater,
        &link,
        "authority-claim",
        vec![json!({ "op": "ob-claim", "ob": "authority-open:0", "preimage": "secret" })],
        11,
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ab/{}/{}",
            "authority-inscription",
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("15")
      );
      let bad_authority_refund = json!({
        "op": "ob-open",
        "src": { "tt": "h", "to": "authority-inscription", "tick": "tap" },
        "amt": "10",
        "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
        "rf": { "tt": "a", "to": USER_ADDRESS },
        "cond": { "ty": "hash", "h": hash },
        "ra": "20",
        "exp": "30"
      });
      assert!(updater
        .validate_obligation_open_action(
          &bad_authority_refund,
          Some(&link),
          "bad-authority-refund",
          0,
          10
        )
        .is_none());
    });
  }

  #[test]
  fn obligation_settlement_target_revalidation_is_atomic() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      put_authority_config(updater, "authority-inscription");
      updater
        .tap_put(
          "ah/claim-authority",
          &json!({
            "id": "claim-authority",
            "k": "test",
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "seq": 0,
            "r": null,
            "blck": 10,
            "tx": "target-tx",
            "vo": 0,
            "val": "0",
            "ins": "claim-authority",
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      updater
        .tap_put("tains/authority-inscription", &"".to_string())
        .unwrap();
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));
      let mut open = obligation_open(&hash);
      open["cl"] = json!({ "tt": "h", "to": "claim-authority" });

      assert!(apply_actions_at(
        updater,
        &link,
        "ob-atomic",
        vec![open],
        10
      ));
      updater
        .tap_put("ah/claim-authority", &"broken".to_string())
        .unwrap();
      assert!(!updater.process_obligation_settle_action(
        &json!({ "op": "ob-claim", "ob": "ob-atomic:0", "preimage": "secret" }),
        &"44".repeat(32),
        0,
        0,
        "ob-atomic-claim",
        1,
        11,
        1000,
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "oll/a/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("10")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("100")
      );
      assert!(updater
        .tap_get::<serde_json::Value>("obc/ob-atomic:0")
        .unwrap()
        .is_none());
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>("ob/ob-atomic:0")
          .unwrap()
          .unwrap()
          .get("st")
          .and_then(|v| v.as_str()),
        Some("open")
      );
    });
  }

  #[test]
  fn obligation_refund_boundary_pending_overcommit_and_amm_source() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "dmt", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      put_balance(updater, USER_ADDRESS, "dmt", "1000");
      put_authority_config(updater, "authority-inscription");
      updater
        .tap_put("tains/authority-inscription", &"".to_string())
        .unwrap();
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));

      let mut overcommit = vec![
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
        obligation_open(&hash),
      ];
      for (idx, action) in overcommit.iter_mut().enumerate() {
        action["ctx"] = json!({ "app": "test", "ref": format!("quote-{idx}") });
      }
      assert!(!updater.validate_token_proof_actions(
        &mut overcommit,
        Some(&link),
        "overcommit",
        10,
        1000
      ));

      assert!(apply_actions_at(
        updater,
        &link,
        "ob-ref-open",
        vec![obligation_open(&hash)],
        10,
      ));
      let mut early_refund = vec![json!({ "op": "ob-refund", "ob": "ob-ref-open:0" })];
      assert!(!updater.validate_token_proof_actions(
        &mut early_refund,
        Some(&link),
        "early-refund",
        19,
        1000
      ));
      assert!(apply_actions_at(
        updater,
        &link,
        "ob-refund",
        vec![json!({ "op": "ob-refund", "ob": "ob-ref-open:0" })],
        20,
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("100")
      );
      put_balance(updater, USER_ADDRESS, "tap", "10000");
      put_balance(updater, USER_ADDRESS, "dmt", "10000");

      assert!(apply_actions(
        updater,
        &link,
        "amm-ob",
        vec![
          amm_config(&link, "30", "0"),
          json!({
            "op": "add-liq",
            "auth": "amm-ob:0",
            "amts": ["5000", "2000"],
            "min": "1",
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "init"
          })
        ],
      ));
      assert!(apply_actions_at(
        updater,
        &link,
        "amm-ob-open",
        vec![json!({
          "op": "ob-open",
          "src": { "tt": "amm", "pid": "amm-ob:0", "i": 0 },
          "amt": "10",
          "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
          "rf": { "tt": "amm", "pid": "amm-ob:0", "i": 0 },
          "cond": { "ty": "hash", "h": hash },
          "ra": "20",
          "exp": "30"
        })],
        10,
      ));
      assert!(apply_actions_at(
        updater,
        &link,
        "amm-ob-final",
        vec![json!({ "op": "ob-final", "ob": "amm-ob-open:0", "preimage": "secret" })],
        11,
      ));
      let pool = updater
        .tap_get::<AuthorityConfigRecord>("amm/amm-ob:0")
        .unwrap()
        .unwrap();
      assert_eq!(pool.r[0], "4990");
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ab/{}/{}",
            "amm-ob:0",
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("4990")
      );

      let ext_pool = "amm-ext:0";
      updater
        .tap_put(
          &format!("amm/{ext_pool}"),
          &json!({
            "id": ext_pool,
            "k": "amm",
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "seq": 0,
            "r": ["50", "0"],
            "a": [
              { "ty": "tap", "tick": "tap" },
              { "ty": "ext", "ns": "eip155", "cid": "31337", "pool": "0xpool", "aid": "native" }
            ],
            "ak": [InscriptionUpdater::json_stringify_lower("tap")],
            "sh": "100",
            "fee": "30",
            "pf": "0",
            "min": "0",
            "att": { "thr": 1, "signers": ["02aa"], "max_age": 12, "reorg": 1 },
            "blck": 10,
            "tx": "amm-ext-tx",
            "vo": 0,
            "val": "0",
            "ins": ext_pool,
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      updater
        .tap_put(
          &format!(
            "ab/{}/{}",
            ext_pool,
            InscriptionUpdater::json_stringify_lower("tap")
          ),
          &"50".to_string(),
        )
        .unwrap();
      updater
        .tap_put(
          &InscriptionUpdater::amm_snapshot_key(ext_pool, "snap-1"),
          &json!({
            "pid": ext_pool,
            "sid": "snap-1",
            "ns": "eip155",
            "cid": "31337",
            "pool": "0xpool",
            "aid": "native",
            "res": "1000000000000000000",
            "h": "123",
            "ets": "1000",
            "exp": 30,
            "ai": 1
          }),
        )
        .unwrap();
      let ext_ctx = json!({
        "app": "amm-test",
        "ref": "xswap-1",
        "amm": { "pid": ext_pool, "i": 0, "sid": "snap-1", "set": "settle-1", "h": hash }
      });
      assert!(apply_actions_at(
        updater,
        &link,
        "amm-ext-open",
        vec![json!({
          "op": "ob-open",
          "src": { "tt": "amm", "pid": ext_pool, "i": 0 },
          "amt": "10",
          "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
          "rf": { "tt": "amm", "pid": ext_pool, "i": 0 },
          "cond": { "ty": "hash", "h": hash },
          "ra": "20",
          "exp": "30",
          "ctx": ext_ctx
        })],
        10,
      ));
      assert!(apply_actions_at(
        updater,
        &link,
        "amm-ext-final",
        vec![json!({ "op": "ob-final", "ob": "amm-ext-open:0", "preimage": "secret" })],
        11,
      ));
      let ext_state = updater
        .tap_get::<AuthorityConfigRecord>(&format!("amm/{ext_pool}"))
        .unwrap()
        .unwrap();
      assert_eq!(ext_state.r[0], "40");
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ab/{}/{}",
            ext_pool,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("40")
      );

      let bad_ctx = json!({
        "app": "amm-test",
        "ref": "xswap-bad",
        "amm": { "pid": ext_pool, "i": 0, "sid": "snap-1", "set": "settle-bad", "h": "0000000000000000000000000000000000000000000000000000000000000000" }
      });
      let mut bad_open = json!({
        "op": "ob-open",
        "src": { "tt": "amm", "pid": ext_pool, "i": 0 },
        "amt": "10",
        "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
        "rf": { "tt": "amm", "pid": ext_pool, "i": 0 },
        "cond": { "ty": "hash", "h": hash },
        "ra": "20",
        "exp": "30",
        "ctx": bad_ctx
      });
      assert!(updater
        .validate_obligation_open_action(&bad_open, Some(&link), "amm-ext-bad", 0, 10)
        .is_none());
      bad_open["ctx"] = json!({ "app": "amm-test", "ref": "xswap-missing" });
      assert!(updater
        .validate_obligation_open_action(&bad_open, Some(&link), "amm-ext-missing", 0, 10)
        .is_none());
    });
  }

  #[test]
  fn amm_same_redeem_create_add_swap_remove_uses_pending_state() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "dmt", 0);
      put_balance(updater, USER_ADDRESS, "tap", "11100");
      put_balance(updater, USER_ADDRESS, "dmt", "10000");
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let pool_id = "amm-life:0";

      assert!(apply_actions(
        updater,
        &link,
        "amm-life",
        vec![
          amm_config(&link, "30", "0"),
          json!({
            "op": "add-liq",
            "auth": pool_id,
            "amts": ["10000", "10000"],
            "min": "9000",
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "init"
          }),
          json!({
            "op": "swap",
            "auth": pool_id,
            "m": "xin",
            "i": 0,
            "amt": "100",
            "min": "99",
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "swap"
          }),
          json!({
            "op": "rm-liq",
            "auth": pool_id,
            "sh": "9000",
            "min": ["9000", "8800"],
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "remove"
          })
        ],
      ));

      let pool = updater
        .tap_get::<AuthorityConfigRecord>(&format!("amm/{pool_id}"))
        .unwrap()
        .unwrap();
      assert_eq!(pool.r, json!(["1010", "991"]));
      assert_eq!(pool.sh, "1000");
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("10090")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("dmt")
          )
        )
        .as_deref(),
        Some("9009")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ab/{}/{}",
            pool_id,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("1010")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ab/{}/{}",
            pool_id,
            InscriptionUpdater::json_stringify_lower("dmt")
          )
        )
        .as_deref(),
        Some("991")
      );
      assert_eq!(
        get_string(updater, &format!("ammp/{}/a/{}", pool_id, USER_ADDRESS)).as_deref(),
        Some("0")
      );
    });
  }

  #[test]
	  fn competing_redeem_actions_reject_atomically() {
	    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      put_authority_config(updater, "authority-inscription");
      updater
        .tap_put("tains/authority-inscription", &"".to_string())
        .unwrap();
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let claim_link = auth_link(RECEIVER_ADDRESS, "claim-authority");
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));

      let mut lock_overcommit = vec![
        json!({
          "op": "lock",
          "kind": "htlc",
          "tick": "tap",
          "amt": "60",
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "condition": { "type": "hashlock", "hash": hash },
          "refund_after": "20"
        }),
        json!({
          "op": "lock",
          "kind": "htlc",
          "tick": "tap",
          "amt": "50",
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "condition": { "type": "hashlock", "hash": hash },
          "refund_after": "20"
        }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut lock_overcommit,
        Some(&link),
        "atomic-lock-overcommit",
        10,
        1000
      ));
      assert!(updater
        .tap_get::<TokenLockRecord>("l/atomic-lock-overcommit:0")
        .unwrap()
        .is_none());
      assert!(updater
        .tap_get::<TokenLockRecord>("l/atomic-lock-overcommit:1")
        .unwrap()
        .is_none());
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("100")
      );

      assert!(apply_actions_at(
        updater,
        &link,
        "atomic-claim-lock",
        vec![json!({
          "op": "lock",
          "kind": "htlc",
          "tick": "tap",
          "amt": "1",
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "condition": { "type": "hashlock", "hash": hash },
          "refund_after": "20"
        })],
        10,
      ));
      let mut double_claim = vec![
        json!({ "op": "claim", "lock": "atomic-claim-lock:0", "preimage": "secret" }),
        json!({ "op": "claim", "lock": "atomic-claim-lock:0", "preimage": "secret" }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut double_claim,
        Some(&claim_link),
        "atomic-double-claim",
        11,
        1000
      ));
      assert!(updater
        .tap_get::<TokenLockConsumeRecord>("lc/atomic-claim-lock:0")
        .unwrap()
        .is_none());
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", RECEIVER_ADDRESS, tick_key)).as_deref(),
        None
      );

      assert!(apply_actions_at(
        updater,
        &link,
        "atomic-refund-lock",
        vec![json!({
          "op": "lock",
          "kind": "htlc",
          "tick": "tap",
          "amt": "1",
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "condition": { "type": "hashlock", "hash": hash },
          "refund_after": "20"
        })],
        10,
      ));
      let mut double_refund = vec![
        json!({ "op": "refund", "lock": "atomic-refund-lock:0" }),
        json!({ "op": "refund", "lock": "atomic-refund-lock:0" }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut double_refund,
        Some(&link),
        "atomic-double-refund",
        20,
        1000
      ));
      assert!(updater
        .tap_get::<TokenLockConsumeRecord>("lc/atomic-refund-lock:0")
        .unwrap()
        .is_none());
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("100")
      );

      assert!(apply_actions_at(
        updater,
        &link,
        "atomic-ob-open",
        vec![obligation_open(&hash)],
        10,
      ));
      let mut double_ob_claim = vec![
        json!({ "op": "ob-claim", "ob": "atomic-ob-open:0", "preimage": "secret" }),
        json!({ "op": "ob-claim", "ob": "atomic-ob-open:0", "preimage": "secret" }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut double_ob_claim,
        Some(&link),
        "atomic-double-ob-claim",
        11,
        1000
      ));
      assert!(updater
        .tap_get::<serde_json::Value>("obc/atomic-ob-open:0")
        .unwrap()
        .is_none());
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>("ob/atomic-ob-open:0")
          .unwrap()
          .unwrap()
          .get("st")
          .and_then(|v| v.as_str()),
        Some("open")
      );

      updater
        .tap_put(
          "ah/authority-inscription",
          &json!({
            "id": "authority-inscription",
            "k": "stk",
            "stk": "tap",
            "rt": [],
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "seq": 0,
            "r": { "cm": "arps", "rnd": "flr", "aw": false, "ep": "hold", "tr": [{ "id": "3m", "dur": 10, "w": "1" }] },
            "blck": 10,
            "tx": "authority-tx",
            "vo": 0,
            "val": "0",
            "ins": "authority-inscription",
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      let mut lock_then_stake = vec![
        json!({
          "op": "lock",
          "kind": "htlc",
          "tick": "tap",
          "amt": "60",
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "condition": { "type": "hashlock", "hash": hash },
          "refund_after": "20"
        }),
        json!({
          "op": "stake",
          "auth": "authority-inscription",
          "tick": "tap",
          "amt": "50",
          "tier": "3m",
          "claim": USER_ADDRESS
        }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut lock_then_stake,
        Some(&link),
        "atomic-lock-stake-overcommit",
        10,
        1000
      ));
      assert!(updater
        .tap_get::<TokenLockRecord>("l/atomic-lock-stake-overcommit:0")
        .unwrap()
        .is_none());
      assert!(updater
        .tap_get::<StakePositionRecord>("sp/atomic-lock-stake-overcommit:1")
        .unwrap()
        .is_none());
      assert_eq!(
        get_string(updater, &format!("b/{}/{}", USER_ADDRESS, tick_key)).as_deref(),
        Some("100")
      );

      let sale_auth = "atomic-sale-authority:0";
      updater
        .tap_put(
          &format!("ah/{sale_auth}"),
          &json!({
            "id": sale_auth,
            "k": "sale",
            "st": "tap",
            "pt": "tap",
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "tre": { "tt": "a", "to": USER_ADDRESS },
            "seq": 0,
            "r": { "cm": "fix", "pa": "1", "sa": "1", "rnd": "flr" },
            "s": { "sc": "1", "eh": "9" },
            "blck": 10,
            "tx": "sale-tx",
            "vo": 0,
            "val": "0",
            "ins": sale_auth,
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      updater
        .tap_put(
          &format!("ab/{}/{}", sale_auth, tick_key),
          &"100".to_string(),
        )
        .unwrap();
      updater
        .tap_put(
          &format!("sale/{sale_auth}"),
          &json!({
            "auth": sale_auth,
            "st": "tap",
            "pt": "tap",
            "tc": "100",
            "inv": "100",
            "alc": "80",
            "clm": "0",
            "ref": "0",
            "wdr": "0",
            "fin": true,
            "can": false,
            "pp": true
          }),
        )
        .unwrap();
      updater
        .tap_put(
          "scon/atomic-contribution",
          &json!({
            "auth": sale_auth,
            "status": "open",
            "claim": USER_ADDRESS,
            "sa": "10",
            "amt": "10"
          }),
        )
        .unwrap();
      let mut double_sale_claim = vec![
        json!({ "op": "claim-sale", "auth": sale_auth, "cid": "atomic-contribution" }),
        json!({ "op": "claim-sale", "auth": sale_auth, "cid": "atomic-contribution" }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut double_sale_claim,
        Some(&link),
        "atomic-double-sale-claim",
        10,
        1000
      ));
      assert_eq!(
        updater
          .tap_get::<serde_json::Value>("scon/atomic-contribution")
          .unwrap()
          .unwrap()
          .get("status")
          .and_then(|v| v.as_str()),
        Some("open")
      );

      let mut double_cancel_delegation = vec![
        json!({ "op": "cancel-delegation", "nonce": "delegation-nonce-1" }),
        json!({ "op": "cancel-delegation", "nonce": "delegation-nonce-1" }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut double_cancel_delegation,
        Some(&link),
        "atomic-double-cancel-delegation",
        10,
        1000
      ));
      assert!(updater
        .tap_get::<String>("tdc/authority-inscription/delegation-nonce-1")
        .unwrap()
        .is_none());
	    });
	  }

	  #[test]
	  fn sale_public_resolution_finalizes_or_unlocks_refunds() {
	    with_test_updater(BtcNetwork::Signet, 1, |updater| {
	      put_deploy(updater, "sale", 0);
	      put_deploy(updater, "pay", 0);
	      let sale_key = InscriptionUpdater::json_stringify_lower("sale");
	      let pay_key = InscriptionUpdater::json_stringify_lower("pay");

	      let final_auth = "resolve-sale-authority:0";
	      updater
	        .tap_put(
	          &format!("ah/{final_auth}"),
	          &json!({
	            "id": final_auth,
	            "k": "sale",
	            "st": "sale",
	            "pt": "pay",
	            "ctl": { "ty": "ta", "auth": "authority-inscription" },
	            "tre": { "tt": "a", "to": USER_ADDRESS },
	            "seq": 0,
	            "s": { "sh": "0", "eh": "20", "hc": "100", "sc": "50", "mn": null, "mx": null },
	            "blck": 10,
	            "tx": "sale-tx",
	            "vo": 0,
	            "val": "0",
	            "ins": final_auth,
	            "num": 0,
	            "ts": 1000
	          }),
	        )
	        .unwrap();
	      updater
	        .tap_put(&format!("ab/{final_auth}/{sale_key}"), &"80".to_string())
	        .unwrap();
	      updater
	        .tap_put(&format!("ab/{final_auth}/{pay_key}"), &"100".to_string())
	        .unwrap();
	      updater
	        .tap_put(
	          &format!("sale/{final_auth}"),
	          &json!({
	            "auth": final_auth,
	            "st": "sale",
	            "pt": "pay",
	            "tc": "100",
	            "inv": "80",
	            "alc": "80",
	            "clm": "0",
	            "ref": "0",
	            "wdr": "0",
	            "fin": false,
	            "can": false,
	            "fail": false,
	            "pp": false
	          }),
	        )
	        .unwrap();
	      let mut resolve = vec![json!({ "op": "resolve-sale", "auth": final_auth })];
	      assert!(updater.validate_token_proof_actions(&mut resolve, None, "resolve-final", 10, 1000));
	      let redeem = json!({ "actions": resolve.clone() });
	      assert!(updater.process_token_proof_actions(
	        &mut resolve,
	        None,
	        "",
	        &redeem,
	        &json!({}),
	        "",
	        "",
	        &"44".repeat(32),
	        0,
	        0,
	        "resolve-final",
	        1,
	        10,
	        1000,
	      ));
	      let final_status = updater
	        .tap_get::<serde_json::Value>(&format!("sale/{final_auth}"))
	        .unwrap()
	        .unwrap();
	      assert_eq!(final_status.get("fin").and_then(|v| v.as_bool()), Some(true));
	      assert_eq!(final_status.get("pp").and_then(|v| v.as_bool()), Some(true));
	      assert_eq!(final_status.get("res").and_then(|v| v.as_str()), Some("finalized"));
	      assert_eq!(
	        get_string(updater, &format!("ab/{final_auth}/{pay_key}")).as_deref(),
	        Some("0")
	      );
	      assert_eq!(
	        get_string(updater, &format!("b/{USER_ADDRESS}/{pay_key}")).as_deref(),
	        Some("100")
	      );

	      let fail_auth = "resolve-fail-sale-authority:0";
	      updater
	        .tap_put(
	          &format!("ah/{fail_auth}"),
	          &json!({
	            "id": fail_auth,
	            "k": "sale",
	            "st": "sale",
	            "pt": "pay",
	            "ctl": { "ty": "ta", "auth": "authority-inscription" },
	            "tre": { "tt": "a", "to": USER_ADDRESS },
	            "seq": 0,
	            "s": { "sh": "0", "eh": "9", "hc": "100", "sc": "50", "mn": null, "mx": null },
	            "blck": 10,
	            "tx": "sale-tx",
	            "vo": 0,
	            "val": "0",
	            "ins": fail_auth,
	            "num": 0,
	            "ts": 1000
	          }),
	        )
	        .unwrap();
	      updater
	        .tap_put(&format!("ab/{fail_auth}/{sale_key}"), &"40".to_string())
	        .unwrap();
	      updater
	        .tap_put(&format!("ab/{fail_auth}/{pay_key}"), &"100".to_string())
	        .unwrap();
	      updater
	        .tap_put(
	          &format!("sale/{fail_auth}"),
	          &json!({
	            "auth": fail_auth,
	            "st": "sale",
	            "pt": "pay",
	            "tc": "100",
	            "inv": "40",
	            "alc": "80",
	            "clm": "0",
	            "ref": "0",
	            "wdr": "0",
	            "fin": false,
	            "can": false,
	            "fail": false,
	            "pp": false
	          }),
	        )
	        .unwrap();
	      updater
	        .tap_put(
	          "scon/resolve-fail-contribution",
	          &json!({
	            "auth": fail_auth,
	            "status": "open",
	            "claim": USER_ADDRESS,
	            "sa": "40",
	            "amt": "50"
	          }),
	        )
	        .unwrap();
	      let mut fail_resolve = vec![json!({ "op": "resolve-sale", "auth": fail_auth })];
	      assert!(updater.validate_token_proof_actions(&mut fail_resolve, None, "resolve-fail", 10, 1000));
	      let redeem = json!({ "actions": fail_resolve.clone() });
	      assert!(updater.process_token_proof_actions(
	        &mut fail_resolve,
	        None,
	        "",
	        &redeem,
	        &json!({}),
	        "",
	        "",
	        &"45".repeat(32),
	        0,
	        0,
	        "resolve-fail",
	        1,
	        10,
	        1000,
	      ));
	      let fail_status = updater
	        .tap_get::<serde_json::Value>(&format!("sale/{fail_auth}"))
	        .unwrap()
	        .unwrap();
	      assert_eq!(fail_status.get("can").and_then(|v| v.as_bool()), Some(true));
	      assert_eq!(fail_status.get("fail").and_then(|v| v.as_bool()), Some(true));
	      assert_eq!(
	        fail_status.get("res").and_then(|v| v.as_str()),
	        Some("inventory-underfunded")
	      );
	      let link = auth_link(USER_ADDRESS, "authority-inscription");
	      assert!(apply_actions_at(
	        updater,
	        &link,
	        "resolve-fail-refund",
	        vec![json!({ "op": "refund-sale", "auth": fail_auth, "cid": "resolve-fail-contribution" })],
	        11,
	      ));
	      assert_eq!(
	        get_string(updater, &format!("b/{USER_ADDRESS}/{pay_key}")).as_deref(),
	        Some("150")
	      );
	      assert_eq!(
	        updater
	          .tap_get::<serde_json::Value>("scon/resolve-fail-contribution")
	          .unwrap()
	          .unwrap()
	          .get("status")
	          .and_then(|v| v.as_str()),
	        Some("refunded")
	      );

	      let atomic_auth = "resolve-atomic-sale-authority:0";
	      updater
	        .tap_put(
	          &format!("ah/{atomic_auth}"),
	          &json!({
	            "id": atomic_auth,
	            "k": "sale",
	            "st": "sale",
	            "pt": "pay",
	            "ctl": { "ty": "ta", "auth": "authority-inscription" },
	            "tre": { "tt": "a", "to": USER_ADDRESS },
	            "seq": 0,
	            "s": { "sh": "0", "eh": "20", "hc": "100", "sc": "50", "mn": null, "mx": null },
	            "blck": 10,
	            "tx": "sale-tx",
	            "vo": 0,
	            "val": "0",
	            "ins": atomic_auth,
	            "num": 0,
	            "ts": 1000
	          }),
	        )
	        .unwrap();
	      updater
	        .tap_put(&format!("ab/{atomic_auth}/{sale_key}"), &"80".to_string())
	        .unwrap();
	      updater
	        .tap_put(&format!("ab/{atomic_auth}/{pay_key}"), &"100".to_string())
	        .unwrap();
	      updater
	        .tap_put(
	          &format!("sale/{atomic_auth}"),
	          &json!({
	            "auth": atomic_auth,
	            "st": "sale",
	            "pt": "pay",
	            "tc": "100",
	            "inv": "80",
	            "alc": "80",
	            "clm": "0",
	            "ref": "0",
	            "wdr": "0",
	            "fin": false,
	            "can": false,
	            "fail": false,
	            "pp": false
	          }),
	        )
	        .unwrap();
	      let mut double_resolve = vec![
	        json!({ "op": "resolve-sale", "auth": atomic_auth }),
	        json!({ "op": "resolve-sale", "auth": atomic_auth }),
	      ];
	      assert!(!updater.validate_token_proof_actions(
	        &mut double_resolve,
	        None,
	        "resolve-double",
	        10,
	        1000
	      ));
	      let atomic_status = updater
	        .tap_get::<serde_json::Value>(&format!("sale/{atomic_auth}"))
	        .unwrap()
	        .unwrap();
	      assert_eq!(atomic_status.get("fin").and_then(|v| v.as_bool()), Some(false));
	    });
	  }

	  #[test]
	  fn amm_rejects_malformed_values_duplicate_refs_and_unavailable_balances() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "dmt", 0);
      put_balance(updater, USER_ADDRESS, "tap", "10000");
      put_balance(updater, USER_ADDRESS, "dmt", "10000");
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let pool_id = "amm-bad:0";

      for bad_fee in [
        json!(1000),
        json!("1001"),
        json!("-1"),
        json!("1e3"),
        json!("abc0.9999"),
      ] {
        let mut action = amm_config(&link, "30", "0");
        action["c"]["fee"] = bad_fee;
        assert!(updater
          .validate_amm_authority_config_action(
            &action,
            Some(&link),
            "bad-cfg",
            0,
            "",
            0,
            0,
            0,
            10,
            1000
          )
          .is_none());
      }

      assert!(apply_actions(
        updater,
        &link,
        "amm-bad",
        vec![
          amm_config(&link, "30", "0"),
          json!({
            "op": "add-liq",
            "auth": pool_id,
            "amts": ["2000", "2000"],
            "min": "1",
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "init"
          })
        ],
      ));

      let mut duplicate_refs = vec![
        json!({
          "op": "swap",
          "auth": pool_id,
          "m": "xin",
          "i": 0,
          "amt": "1",
          "min": "1",
          "to": { "tt": "a", "to": USER_ADDRESS },
          "exp": "20",
          "ref": "dupe"
        }),
        json!({
          "op": "swap",
          "auth": pool_id,
          "m": "xin",
          "i": 0,
          "amt": "1",
          "min": "1",
          "to": { "tt": "a", "to": USER_ADDRESS },
          "exp": "20",
          "ref": "dupe"
        }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut duplicate_refs,
        Some(&link),
        "dupe-redeem",
        10,
        1000
      ));

      for bad_amount in [
        json!("abc0.9999"),
        json!("1e3"),
        json!("-1"),
        json!("1,000"),
        json!(1),
      ] {
        let mut actions = vec![json!({
          "op": "add-liq",
          "auth": pool_id,
          "amts": [bad_amount, "1"],
          "min": "1",
          "to": { "tt": "a", "to": USER_ADDRESS },
          "exp": "20",
          "ref": "bad-amount"
        })];
        assert!(!updater.validate_token_proof_actions(
          &mut actions,
          Some(&link),
          "bad-amount-redeem",
          10,
          1000
        ));
      }

      updater
        .tap_put(
          &format!(
            "t/{}/{}",
            USER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          ),
          &"8000".to_string(),
        )
        .unwrap();
      let mut unavailable = vec![json!({
        "op": "swap",
        "auth": pool_id,
        "m": "xin",
        "i": 0,
        "amt": "1",
        "min": "1",
        "to": { "tt": "a", "to": USER_ADDRESS },
        "exp": "20",
        "ref": "unavailable"
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut unavailable,
        Some(&link),
        "unavailable-redeem",
        10,
        1000
      ));
    });
  }

  #[test]
  fn amm_protocol_fee_routes_atomically_and_external_policy_validates() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "dmt", 0);
      put_balance(updater, USER_ADDRESS, "tap", "101000");
      put_balance(updater, USER_ADDRESS, "dmt", "100000");
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let pool_id = "amm-fee:0";

      assert!(apply_actions(
        updater,
        &link,
        "amm-fee",
        vec![
          amm_config(&link, "100", "5000"),
          json!({
            "op": "add-liq",
            "auth": pool_id,
            "amts": ["100000", "100000"],
            "min": "99000",
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "init"
          }),
          json!({
            "op": "swap",
            "auth": pool_id,
            "m": "xin",
            "i": 0,
            "amt": "1000",
            "min": "980",
            "to": { "tt": "a", "to": USER_ADDRESS },
            "exp": "20",
            "ref": "swap"
          })
        ],
      ));

      let calc = InscriptionUpdater::calculate_amm_exact_in(
        &BigInt::from(1000),
        &BigInt::from(100000),
        &BigInt::from(100000),
        &BigInt::from(100),
        &BigInt::from(5000),
      )
      .unwrap();
      assert_eq!(calc.gross_fee.to_string(), "10");
      assert_eq!(calc.protocol_fee.to_string(), "5");
      assert_eq!(calc.amount_out.to_string(), "980");
      assert_eq!(
        get_string(
          updater,
          &format!(
            "b/{}/{}",
            RECEIVER_ADDRESS,
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("5")
      );
      let pool = updater
        .tap_get::<AuthorityConfigRecord>(&format!("amm/{pool_id}"))
        .unwrap()
        .unwrap();
      assert_eq!(pool.r, json!(["100995", "99020"]));

      let ext_config = json!({
        "op": "auth-cfg",
        "k": "amm",
        "a": [
          { "ty": "tap", "tick": "tap" },
          { "ty": "ext", "ns": "eip155", "cid": "56", "aid": "native", "dec": "18", "pool": "0xabc" }
        ],
        "att": {
          "thr": 2,
          "signers": [
            "021111111111111111111111111111111111111111111111111111111111111111",
            "032222222222222222222222222222222222222222222222222222222222222222"
          ],
          "max_age": "24",
          "reorg": "12"
        },
        "c": { "ty": "cpmm", "fee": "30", "pf": "0", "min": "1000", "pause": false },
        "ctl": { "ty": "ta", "auth": link.ins },
        "seq": 0
      });
      assert!(updater
        .validate_amm_authority_config_action(
          &ext_config,
          Some(&link),
          "amm-ext",
          0,
          "",
          0,
          0,
          0,
          10,
          1000
        )
        .is_some());

      let mut bad_ext = ext_config;
      bad_ext["att"]["thr"] = json!(3);
      assert!(updater
        .validate_amm_authority_config_action(
          &bad_ext,
          Some(&link),
          "amm-ext-bad",
          0,
          "",
          0,
          0,
          0,
          10,
          1000
        )
        .is_none());

      for (field, value) in [
        ("ns", "eip/155"),
        ("cid", "56/1"),
        ("aid", "native/sol"),
        ("pool", "0xabc/1"),
      ] {
        let mut bad_id = json!({
          "op": "auth-cfg",
          "k": "amm",
          "a": [
            { "ty": "tap", "tick": "tap" },
            { "ty": "ext", "ns": "eip155", "cid": "56", "aid": "native", "dec": "18", "pool": "0xabc" }
          ],
          "att": {
            "thr": 2,
            "signers": [
              "021111111111111111111111111111111111111111111111111111111111111111",
              "032222222222222222222222222222222222222222222222222222222222222222"
            ],
            "max_age": "24",
            "reorg": "12"
          },
          "c": { "ty": "cpmm", "fee": "30", "pf": "0", "min": "1000", "pause": false },
          "ctl": { "ty": "ta", "auth": link.ins },
          "seq": 0
        });
        bad_id["a"][1][field] = json!(value);
        assert!(updater
          .validate_amm_authority_config_action(
            &bad_id,
            Some(&link),
            "amm-ext-bad-id",
            0,
            "",
            0,
            0,
            0,
            10,
            1000
          )
          .is_none());
      }
    });
  }

  #[test]
  fn obligation_locks_preserve_existing_spend_surfaces_and_sale_reserves() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_deploy(updater, "dmt", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      put_balance(updater, USER_ADDRESS, "dmt", "1000");
      put_authority_config(updater, "authority-inscription");
      updater
        .tap_put("tains/authority-inscription", &"".to_string())
        .unwrap();
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let tick_key = InscriptionUpdater::json_stringify_lower("tap");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));
      updater
        .tap_put(
          &format!("oll/a/{}/{}", USER_ADDRESS, tick_key),
          &"95".to_string(),
        )
        .unwrap();

      let mut lock_actions = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "6",
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20"
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut lock_actions,
        Some(&link),
        "obligation-spend-lock",
        10,
        1000
      ));

      updater
        .tap_put(
          "ah/authority-inscription",
          &json!({
            "id": "authority-inscription",
            "k": "stk",
            "stk": "tap",
            "rt": [],
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "seq": 0,
            "r": { "cm": "arps", "rnd": "flr", "aw": false, "ep": "hold", "tr": [{ "id": "3m", "dur": 10, "w": "1" }] },
            "blck": 10,
            "tx": "authority-tx",
            "vo": 0,
            "val": "0",
            "ins": "authority-inscription",
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      let mut stake_actions = vec![json!({
        "op": "stake",
        "auth": "authority-inscription",
        "tick": "tap",
        "amt": "6",
        "tier": "3m",
        "claim": USER_ADDRESS
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut stake_actions,
        Some(&link),
        "obligation-spend-stake",
        10,
        1000
      ));

      updater
        .tap_put(
          "ah/authority-inscription",
          &json!({ "id": "authority-inscription", "k": "test" }),
        )
        .unwrap();
      let mut non_staking_reward = vec![json!({
        "op": "lock",
        "kind": "htlc",
        "tick": "tap",
        "amt": "1",
        "al": [{ "tt": "h", "to": "authority-inscription", "amt": "1", "rl": "sr" }],
        "claim": RECEIVER_ADDRESS,
        "refund": USER_ADDRESS,
        "condition": { "type": "hashlock", "hash": hash },
        "refund_after": "20"
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut non_staking_reward,
        Some(&link),
        "non-staking-reward",
        10,
        1000
      ));

      let pool_id = "amm-obligation:0";
      updater
        .tap_put(
          &format!("amm/{pool_id}"),
          &json!({
            "id": pool_id,
            "k": "amm",
            "p": false,
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "seq": 0,
            "r": ["50", "200"],
            "a": [{ "ty": "tap", "tick": "tap" }, { "ty": "tap", "tick": "dmt" }],
            "ak": [InscriptionUpdater::json_stringify_lower("tap"), InscriptionUpdater::json_stringify_lower("dmt")],
            "sh": "100",
            "fee": "30",
            "pf": "0",
            "min": "0",
            "blck": 10,
            "tx": "amm-tx",
            "vo": 0,
            "val": "0",
            "ins": pool_id,
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      updater
        .tap_put(&format!("ab/{}/{}", pool_id, tick_key), &"50".to_string())
        .unwrap();
      updater
        .tap_put(
          &format!(
            "ab/{}/{}",
            pool_id,
            InscriptionUpdater::json_stringify_lower("dmt")
          ),
          &"200".to_string(),
        )
        .unwrap();
      updater
        .tap_put(
          &format!("ammp/{}/a/{}", pool_id, USER_ADDRESS),
          &"100".to_string(),
        )
        .unwrap();
      assert_eq!(
        updater.tap_get_obligation_locked_bigint(
          &json!({ "tt": "amm", "pid": pool_id, "i": 0 }),
          &tick_key
        ),
        Some(BigInt::from(0))
      );
      let mut rm_no_lock = vec![json!({
        "op": "rm-liq",
        "auth": pool_id,
        "sh": "20",
        "min": ["1", "1"],
        "to": { "tt": "a", "to": USER_ADDRESS },
        "exp": "20",
        "ref": "rm-no-lock"
      })];
      assert!(updater.validate_token_proof_actions(
        &mut rm_no_lock,
        Some(&link),
        "amm-no-obligation-rm",
        10,
        1000
      ));
      let mut swap_no_lock = vec![json!({
        "op": "swap",
        "auth": pool_id,
        "m": "xin",
        "i": 1,
        "amt": "100",
        "min": "16",
        "to": { "tt": "a", "to": USER_ADDRESS },
        "exp": "20",
        "ref": "swap-no-lock"
      })];
      assert!(updater.validate_token_proof_actions(
        &mut swap_no_lock,
        Some(&link),
        "amm-no-obligation-swap",
        10,
        1000
      ));
      let mut rm_pending_lock = vec![
        json!({
          "op": "ob-open",
          "src": { "tt": "amm", "pid": pool_id, "i": 0, "tick": "tap" },
          "amt": "45",
          "cl": { "tt": "a", "to": RECEIVER_ADDRESS },
          "rf": { "tt": "amm", "pid": pool_id, "i": 0 },
          "cond": { "ty": "hash", "h": hash },
          "ra": "20",
          "exp": "30",
          "ctx": { "app": "test", "ref": "pending-amm-lock" }
        }),
        json!({
          "op": "rm-liq",
          "auth": pool_id,
          "sh": "20",
          "min": ["1", "1"],
          "to": { "tt": "a", "to": USER_ADDRESS },
          "exp": "20",
          "ref": "rm-pending-lock"
        }),
      ];
      assert!(!updater.validate_token_proof_actions(
        &mut rm_pending_lock,
        Some(&link),
        "amm-pending-obligation-rm",
        10,
        1000
      ));
      updater
        .tap_put(
          &format!("oll/amm/{}/0/{}", pool_id, tick_key),
          &"45".to_string(),
        )
        .unwrap();
      let mut rm_actions = vec![json!({
        "op": "rm-liq",
        "auth": pool_id,
        "sh": "20",
        "min": ["1", "1"],
        "to": { "tt": "a", "to": USER_ADDRESS },
        "exp": "20",
        "ref": "rm-drain"
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut rm_actions,
        Some(&link),
        "amm-obligation-rm",
        10,
        1000
      ));
      let mut swap_actions = vec![json!({
        "op": "swap",
        "auth": pool_id,
        "m": "xin",
        "i": 1,
        "amt": "100",
        "min": "16",
        "to": { "tt": "a", "to": USER_ADDRESS },
        "exp": "20",
        "ref": "swap-drain"
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut swap_actions,
        Some(&link),
        "amm-obligation-swap",
        10,
        1000
      ));

      let sale_auth = "sale-authority:0";
      updater
        .tap_put(
          &format!("ah/{sale_auth}"),
          &json!({
            "id": sale_auth,
            "k": "sale",
            "st": "tap",
            "pt": "tap",
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "tre": { "tt": "a", "to": USER_ADDRESS },
            "seq": 0,
            "r": { "cm": "fix", "pa": "1", "sa": "1", "rnd": "flr" },
            "s": { "sc": "1", "eh": "9" },
            "blck": 10,
            "tx": "sale-tx",
            "vo": 0,
            "val": "0",
            "ins": sale_auth,
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      updater
        .tap_put(
          &format!("ab/{}/{}", sale_auth, tick_key),
          &"100".to_string(),
        )
        .unwrap();
      updater
        .tap_put(
          &format!("sale/{sale_auth}"),
          &json!({
            "auth": sale_auth,
            "st": "tap",
            "pt": "tap",
            "tc": "100",
            "inv": "100",
            "alc": "80",
            "clm": "30",
            "ref": "0",
            "wdr": "0",
            "fin": true,
            "can": false,
            "pp": true
          }),
        )
        .unwrap();
      updater
        .tap_put(&format!("ab/{}/{}", sale_auth, tick_key), &"40".to_string())
        .unwrap();
      let mut withdraw_reserved_underflow = vec![json!({
        "op": "withdraw-sale",
        "auth": sale_auth,
        "tick": "tap",
        "amt": "1",
        "tt": "a",
        "to": USER_ADDRESS
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut withdraw_reserved_underflow,
        Some(&link),
        "sale-withdraw-reserve-over-balance",
        10,
        1000
      ));
      updater
        .tap_put(
          &format!("ab/{}/{}", sale_auth, tick_key),
          &"100".to_string(),
        )
        .unwrap();
      let mut withdraw_too_much = vec![json!({
        "op": "withdraw-sale",
        "auth": sale_auth,
        "tick": "tap",
        "amt": "51",
        "tt": "a",
        "to": USER_ADDRESS
      })];
      assert!(!updater.validate_token_proof_actions(
        &mut withdraw_too_much,
        Some(&link),
        "sale-withdraw-reserved",
        10,
        1000
      ));
      let mut withdraw_available = vec![json!({
        "op": "withdraw-sale",
        "auth": sale_auth,
        "tick": "tap",
        "amt": "50",
        "tt": "a",
        "to": USER_ADDRESS
      })];
      assert!(updater.validate_token_proof_actions(
        &mut withdraw_available,
        Some(&link),
        "sale-withdraw-available",
        10,
        1000
      ));
    });
  }

  #[test]
  fn staking_reward_allocations_retain_all_dust() {
    with_test_updater(BtcNetwork::Signet, 1, |updater| {
      put_deploy(updater, "tap", 0);
      put_balance(updater, USER_ADDRESS, "tap", "100");
      updater
        .tap_put(
          "ah/authority-inscription",
          &json!({
            "id": "authority-inscription",
            "k": "stk",
            "stk": "tap",
            "rt": ["tap"],
            "ctl": { "ty": "ta", "auth": "authority-inscription" },
            "seq": 0,
            "r": { "cm": "arps", "rnd": "flr", "aw": false, "ep": "hold", "tr": [{ "id": "3m", "dur": 10, "w": "1" }] },
            "blck": 10,
            "tx": "authority-tx",
            "vo": 0,
            "val": "0",
            "ins": "authority-inscription",
            "num": 0,
            "ts": 1000
          }),
        )
        .unwrap();
      updater
        .tap_put("ahs/authority-inscription", &"3".to_string())
        .unwrap();
      updater
        .tap_put("tains/authority-inscription", &"".to_string())
        .unwrap();
      let link = auth_link(USER_ADDRESS, "authority-inscription");
      let claim_link = auth_link(RECEIVER_ADDRESS, "claim-auth");
      let hash = InscriptionUpdater::tap_hash_proof_preimage(&json!("secret"));
      assert!(apply_actions(
        updater,
        &link,
        "reward-carry-lock",
        vec![json!({
          "op": "lock",
          "kind": "htlc",
          "tick": "tap",
          "amt": "1",
          "al": [{ "tt": "h", "to": "authority-inscription", "amt": "4", "rl": "sr" }],
          "claim": RECEIVER_ADDRESS,
          "refund": USER_ADDRESS,
          "condition": { "type": "hashlock", "hash": hash },
          "refund_after": "20"
        })],
      ));
      assert!(apply_actions_at(
        updater,
        &claim_link,
        "reward-carry-claim",
        vec![json!({ "op": "claim", "lock": "reward-carry-lock:0", "preimage": "secret" })],
        11,
      ));
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ahrps/{}/{}",
            "authority-inscription",
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("1333333333333333333")
      );
      assert_eq!(
        get_string(
          updater,
          &format!(
            "ahrc/{}/{}",
            "authority-inscription",
            InscriptionUpdater::json_stringify_lower("tap")
          )
        )
        .as_deref(),
        Some("1")
      );
    });
  }
}
