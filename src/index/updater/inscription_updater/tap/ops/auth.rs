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
}

// START TAP-DELEGATED-LOCKS
struct TokenDelegatedLockValidation {
  action: serde_json::Value,
  link: TokenAuthCreateRecord,
  normalized: TokenProofLockValidation,
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
          | "finalize-sale"
          | "withdraw-sale"
          | "rm-liq"
          | "ob-claim"
          | "ob-refund"
          | "ob-final"
          | "cancel-delegation"
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
        .all(Self::token_proof_is_delegated_execute_action)
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
  ) -> bool {
    let key = format!("{}/{}", address, tick_key);
    let pending = BigInt::from(*pending_locks.get(&key).unwrap_or(&0))
      + pending_amm_debits
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
        if block < self.feature_height(TapFeature::TokenDelegationBlockOffsetActivation) {
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
    if has_final_shape && !self.tap_feature_enabled(TapFeature::TokenDelegationFinalFillActivation)
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
    if self.tap_feature_enabled(TapFeature::TokenDelegationFinalFillActivation) {
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

  fn token_proof_primary_delegated_link(
    &mut self,
    actions: &[serde_json::Value],
  ) -> Option<TokenAuthCreateRecord> {
    let mut auth_id: Option<String> = None;
    for action in actions {
      if !Self::token_proof_is_delegated_execute_action(action) {
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
    if action.get("fee").is_some() {
      return None;
    }
    let lock_id = action.get("lock").and_then(|v| v.as_str())?;
    if self
      .tap_get::<String>(&format!("lc/{}", lock_id))
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
    if !self.tap_feature_enabled(TapFeature::TokenLockActivation) || actions.is_empty() {
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
    let mut consumed_obligations: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    // START TAP-DELEGATED-LOCKS
    let mut consumed_delegation_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    let mut cancelled_delegation_nonces: std::collections::HashSet<String> =
      std::collections::HashSet::new();
    // END TAP-DELEGATED-LOCKS

    for (i, action) in actions.iter_mut().enumerate() {
      let op = action
        .get("op")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase();
      if op == "lock" {
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
        ) {
          return false;
        }
        pending_locks.insert(pending_key, pending + delegated.normalized.total_amount);
        consumed_delegation_nonces.insert(delegated.nonce_key);
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
          || self
            .validate_finalize_sale_action(action, link, block)
            .is_none()
        {
          return false;
        }
        sale_finalizes.insert(auth);
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
        if auth.is_empty() || sale_cancels.contains(&auth) {
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
        if consumed_locks.contains(&lock_id)
          || self
            .validate_token_proof_release_action(action, link, block)
            .is_none()
        {
          return false;
        }
        consumed_locks.insert(lock_id);
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
      blck: block,
      tx: transaction.to_string(),
      vo: vout,
      val: value.to_string(),
      ins: inscription.to_string(),
      num: number,
      ts: timestamp,
    };
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
  ) {
    if !self.tap_feature_enabled(TapFeature::TokenLockActivation) {
      return;
    }
    for (i, action) in actions.iter_mut().enumerate() {
      let op = action
        .get("op")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase();
      if op == "lock" {
        if let Some(link) = link {
          let _ = self.process_token_proof_lock_action(
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
          );
        }
      // START TAP-DELEGATED-LOCKS
      } else if op == "execute" {
        if link.is_none() {
          if let Some(mut delegated) =
            self.validate_token_proof_delegated_execute_action(action, inscription, i, block)
          {
            if self.process_token_proof_lock_action(
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
            ) {
              let _ = self.tap_put(&delegated.nonce_key, &"".to_string());
            }
          }
        }
      } else if op == "cancel-delegation" {
        let _ = self.process_token_proof_delegation_cancel_action(
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
        );
      // END TAP-DELEGATED-LOCKS
      } else if op == "ob-open" {
        let _ = self.process_obligation_open_action(
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
        );
      } else if op == "ob-claim" || op == "ob-refund" || op == "ob-final" {
        let _ = self.process_obligation_settle_action(
          action,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        );
      } else if op == "auth-cfg" {
        let _ = self.process_authority_config_action(
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
        );
      } else if op == "sync-ext" {
        let _ = self.process_amm_external_snapshot_action(
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
        );
      } else if op == "add-liq" {
        let _ = self.process_amm_add_liquidity_action(
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
        );
      } else if op == "rm-liq" {
        let _ = self.process_amm_remove_liquidity_action(
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
        );
      } else if op == "swap" {
        let _ = self.process_amm_swap_action(
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
        );
      } else if op == "stake" {
        let _ = self.process_stake_action(
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
        );
      } else if op == "claim-rwd" {
        let _ = self.process_claim_reward_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        );
      } else if op == "unstake" {
        let _ = self.process_unstake_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        );
      } else if op == "fund-sale" {
        let _ = self.process_fund_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        );
      } else if op == "contribute" {
        let _ = self.process_sale_contribution_action(
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
        );
      } else if op == "finalize-sale" {
        let _ = self.process_finalize_sale_action(
          action,
          link,
          transaction,
          vout,
          value,
          inscription,
          number,
          block,
          timestamp,
        );
      } else if op == "claim-sale" {
        let _ = self.process_claim_sale_action(
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
        );
      } else if op == "refund-sale" {
        let _ = self.process_refund_sale_action(
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
        );
      } else if op == "cancel-sale" {
        let _ = self.process_cancel_sale_action(
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
        );
      } else if op == "withdraw-sale" {
        let _ = self.process_withdraw_sale_action(
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
        );
      } else if op == "claim" || op == "refund" {
        if let Some(link) = link {
          let _ = self.process_token_proof_release_action(
            action,
            link,
            transaction,
            vout,
            value,
            inscription,
            number,
            block,
            timestamp,
          );
        }
      }
    }
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
      // Ensure transfer-time execution isn't skipped by preflight bloom
      if let Some(bloom) = &self.any_bloom {
        bloom.borrow_mut().insert_str(&inscription_id.to_string());
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
      let actions_enabled = self.tap_feature_enabled(TapFeature::TokenLockActivation);
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
        if let Some(actions) = redeem_norm
          .get_mut("actions")
          .and_then(|v| v.as_array_mut())
        {
          self.process_token_proof_actions(
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
          );
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
          // Lock creation spends the authority owner's token balance, so it uses the same ticker whitelist as legacy items.
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
          self.process_token_proof_actions(
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
          );
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
    // Ensure transfer-time execution is not skipped by preflight bloom
    if let Some(bloom) = &self.any_bloom {
      bloom.borrow_mut().insert_str(&inscription_id.to_string());
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
