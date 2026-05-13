use super::super::super::*;
// START TAP-PROOFS
use crate::index::updater::inscription_updater::tap::{
  AuthorityConfigRecord, RewardClaimRecord, StakePositionRecord, TokenAllocationRecord,
  TokenDelegationCancelRecord, TokenLockConsumeRecord, TokenLockRecord,
};
use num_bigint::BigInt;
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
// END TAP-DELEGATED-LOCKS
// END TAP-PROOFS

impl InscriptionUpdater<'_, '_> {
  // START TAP-PROOFS
  fn token_proof_action_tick(action: &serde_json::Value) -> Option<&str> {
    action.get("tick").and_then(|v| v.as_str())
  }

  fn token_proof_refund_only_actions(
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
      action
        .get("op")
        .and_then(|v| v.as_str())
        .map(|op| op.eq_ignore_ascii_case("refund"))
        .unwrap_or(false)
    })
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

  fn normalize_token_allocation_role(role: &str) -> Option<String> {
    let normalized = role.to_lowercase();
    match normalized.as_str() {
      "of" | "sr" | "bn" => Some(normalized),
      _ => None,
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
        serde_json::from_str::<String>(tick_key).unwrap_or_else(|_| tick_key.to_string());
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
      return config.rt.iter().map(|tick| tick.to_lowercase()).collect();
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
        ticks.push(tick.to_lowercase());
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
    if empty_policy == "carry" {
      let carry = self.tap_get_authority_reward_carry(auth_id, tick_key);
      let distributable = carry + BigInt::from(amount);
      if total_shares == BigInt::from(0) {
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
      return self.tap_set_authority_reward_carry(auth_id, tick_key, &remaining);
    }
    if total_shares == BigInt::from(0) {
      return empty_policy == "hold";
    }
    let acc = self.tap_get_authority_acc_reward(auth_id, tick_key);
    let delta = BigInt::from(amount) * Self::authority_reward_precision() / total_shares;
    let next = acc + delta;
    self.tap_set_authority_acc_reward(auth_id, tick_key, &next)
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
    match value {
      serde_json::Value::Number(n) => {
        if let Some(u) = n.as_u64() {
          i128::try_from(u).ok()
        } else if let Some(i) = n.as_i64() {
          if i < 0 {
            None
          } else {
            Some(i128::from(i))
          }
        } else {
          let f = n.as_f64()?;
          if !f.is_finite() || f < 0.0 || f.fract() != 0.0 || f > i128::MAX as f64 {
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
        s.parse::<i128>().ok()
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
        let mut seen_dot = false;
        if s.is_empty() {
          return false;
        }
        for c in s.chars() {
          if c.is_ascii_digit() {
            continue;
          }
          if c == '.' && !seen_dot {
            seen_dot = true;
            continue;
          }
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

  fn validate_token_proof_lock_action(
    &mut self,
    action: &mut serde_json::Value,
    link: &TokenAuthCreateRecord,
  ) -> Option<TokenProofLockValidation> {
    let kind = action.get("kind")?.as_str()?.to_lowercase();
    if !matches!(
      kind.as_str(),
      "htlc" | "vesting" | "staking" | "escrow" | "otc" | "launchpad" | "cooldown"
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

    let condition = action.get("condition")?;
    let condition_type = condition.get("type")?.as_str()?.to_lowercase();
    if condition_type == "hashlock" {
      let hash = condition.get("hash")?.as_str()?;
      if !Self::tap_is_valid_sha256_hex(hash) {
        return None;
      }
      if action.get("refund").and_then(|v| v.as_str()).is_none() {
        return None;
      }
      if action
        .get("refund_after")
        .and_then(Self::js_parse_int)
        .is_none()
      {
        return None;
      }
    } else if condition_type == "height" {
      if condition.get("min").and_then(Self::js_parse_int).is_none() {
        return None;
      }
    } else if condition_type == "authority" {
      let auth = condition.get("auth")?.as_str()?;
      if self
        .tap_get::<String>(&format!("tains/{}", auth))
        .ok()
        .flatten()
        .is_none()
      {
        return None;
      }
      if self
        .tap_get::<String>(&format!("tac/{}", auth))
        .ok()
        .flatten()
        .is_some()
      {
        return None;
      }
      if action.get("refund").and_then(|v| v.as_str()).is_none() {
        return None;
      }
      if action
        .get("refund_after")
        .and_then(Self::js_parse_int)
        .is_none()
      {
        return None;
      }
    } else {
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
    if balance - transferable - locked - total_amount < 0 {
      return None;
    }

    Some(TokenProofLockValidation {
      kind,
      tick_key,
      tick: tick.to_lowercase(),
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
        .and_then(Self::js_parse_int)
        .and_then(|n| u32::try_from(n).ok()),
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
          .and_then(Self::js_parse_int)
          .and_then(|n| u32::try_from(n).ok())?;
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
  ) -> bool {
    if !self.tap_feature_enabled(TapFeature::TokenLockActivation) || actions.is_empty() {
      return false;
    }

    let mut pending_locks: std::collections::HashMap<String, i128> =
      std::collections::HashMap::new();
    let mut consumed_locks: std::collections::HashSet<String> = std::collections::HashSet::new();
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
        let balance = self
          .tap_get::<String>(&format!("b/{}/{}", link.addr, &normalized.tick_key))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let transferable = self
          .tap_get::<String>(&format!("t/{}/{}", link.addr, &normalized.tick_key))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let locked = self.tap_get_locked_amount(&link.addr, &normalized.tick_key);
        if balance - transferable - locked - pending - normalized.total_amount < 0 {
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
        let balance = self
          .tap_get::<String>(&format!(
            "b/{}/{}",
            delegated.link.addr, &delegated.normalized.tick_key
          ))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let transferable = self
          .tap_get::<String>(&format!(
            "t/{}/{}",
            delegated.link.addr, &delegated.normalized.tick_key
          ))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let locked =
          self.tap_get_locked_amount(&delegated.link.addr, &delegated.normalized.tick_key);
        if balance - transferable - locked - pending - delegated.normalized.total_amount < 0 {
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
      } else if op == "auth-cfg" {
        if self
          .validate_authority_config_action(action, link, inscription, i, "", 0, 0, 0, block, 0)
          .is_none()
        {
          return false;
        }
      } else if op == "stake" {
        let Some(normalized) = self.validate_stake_action(action, link, inscription, i, block)
        else {
          return false;
        };
        let pending_key = format!("{}/{}", normalized.addr, normalized.tick_key);
        let pending = *pending_locks.get(&pending_key).unwrap_or(&0);
        let balance = self
          .tap_get::<String>(&format!("b/{}/{}", normalized.addr, normalized.tick_key))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let transferable = self
          .tap_get::<String>(&format!("t/{}/{}", normalized.addr, normalized.tick_key))
          .ok()
          .flatten()
          .and_then(|s| s.parse::<i128>().ok())
          .unwrap_or(0);
        let locked = self.tap_get_locked_amount(&normalized.addr, &normalized.tick_key);
        if balance - transferable - locked - pending - normalized.amt < 0 {
          return false;
        }
        pending_locks.insert(pending_key, pending + normalized.amt);
      } else if op == "claim-rwd" {
        let auth = action.get("auth").and_then(|v| v.as_str()).unwrap_or("");
        let pos_id = action.get("pos").and_then(|v| v.as_str()).unwrap_or("");
        let reward_tick = action
          .get("rt")
          .and_then(|v| v.as_str())
          .map(|s| s.to_lowercase())
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
    let subtype = if role == "sk" { "as" } else { "aa" }.to_string();
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
    let subtype = if role == "us" { "au" } else { "ac" }.to_string();
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
            &lock.tick.to_lowercase(),
          );
          let _ = self.tap_put(&format!("ato/{}/{}", address, tick_key), &"".to_string());
        }
      }
    }

    let consume = TokenLockConsumeRecord {
      lock: lock_id.to_string(),
      action: action_name.clone(),
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
    let stake_tick = action.get("stk")?.as_str()?.to_lowercase();
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
      let tick = rt.as_str()?.to_lowercase();
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
      ctl: serde_json::json!({ "ty": "ta", "auth": link.ins }),
      seq: 0,
      r: serde_json::json!({
        "cm": "arps",
        "rnd": "flr",
        "aw": false,
        "ud": update_delay,
        "ep": empty_policy,
        "tr": tiers
      }),
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
    let _ = self.tap_put(&format!("ah/{}", config.id), &config);
    let _ = self.tap_set_list_record("ahl", "ahli", &config);
    let _ = self.tap_set_list_record(
      &format!("ahk/{}", config.k),
      &format!("ahki/{}", config.k),
      &config,
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
    let tick = action.get("tick")?.as_str()?.to_lowercase();
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
    if balance - transferable - locked - amount < 0 {
      return None;
    }
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
      Some(s) => s.to_lowercase(),
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
    let receiver_before = self
      .tap_get::<String>(&format!("b/{}/{}", position.claim, reward_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let receiver_after = receiver_before + pending;
    if !self.tap_set_authority_balance(&auth, &reward_key, auth_balance - pending) {
      return false;
    }
    let _ = self.tap_put(
      &format!("b/{}/{}", position.claim, reward_key),
      &receiver_after.to_string(),
    );
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
    if action.get("rt").and_then(|v| v.as_str()).is_some() {
      let _ = self.process_claim_reward_action(
        &serde_json::json!({
          "op": "claim-rwd",
          "auth": auth,
          "pos": pos_id,
          "rt": action.get("rt").and_then(|v| v.as_str()).unwrap_or("")
        }),
        link,
        transaction,
        vout,
        value,
        inscription,
        number,
        block,
        timestamp,
      );
      position =
        match self.get_stake_position(action.get("pos").and_then(|v| v.as_str()).unwrap_or("")) {
          Some(p) => p,
          None => return false,
        };
    }
    let tick_key = Self::json_stringify_lower(&position.tick);
    let amount = position.amt.parse::<i128>().ok().unwrap_or(0);
    let auth_balance = self.tap_get_authority_balance(&position.auth, &tick_key);
    if amount <= 0 || auth_balance < amount {
      return false;
    }
    let receiver_before = self
      .tap_get::<String>(&format!("b/{}/{}", position.claim, tick_key))
      .ok()
      .flatten()
      .and_then(|s| s.parse::<i128>().ok())
      .unwrap_or(0);
    let receiver_after = receiver_before + amount;
    if !self.tap_set_authority_balance(&position.auth, &tick_key, auth_balance - amount) {
      return false;
    }
    let _ = self.tap_put(
      &format!("b/{}/{}", position.claim, tick_key),
      &receiver_after.to_string(),
    );
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
          self.validate_token_proof_actions(actions, None, &inscription_id.to_string(), self.height)
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
      // Cancellation must not strand already-expired locks. A cancelled authority can only run refund actions.
      let auth_cancelled = self
        .tap_get::<String>(&format!("tac/{}", link.ins))
        .ok()
        .flatten()
        .is_some();
      let cancelled_refund_only = actions_enabled
        && auth_cancelled
        && Self::token_proof_refund_only_actions(&items_norm, &redeem_norm);
      if auth_cancelled && !cancelled_refund_only {
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
      return;
    };
    let Some(hash_str) = acc.json.get("hash").and_then(|v| v.as_str()) else {
      return;
    };
    let Some(salt_val) = acc.json.get("salt") else {
      return;
    };
    let Some(auth_arr) = acc.json.get("auth").and_then(|v| v.as_array()) else {
      return;
    };
    let salt_str = Self::js_value_to_string(salt_val);
    let msg_hash =
      Self::build_sha256_json_plus_salt(&serde_json::Value::Array(auth_arr.clone()), &salt_str);
    let Some((ok, compact_sig, _pub)) =
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
    for t in auth_arr.iter() {
      let Some(ts) = t.as_str() else {
        return;
      };
      if self
        .tap_get::<DeployRecord>(&format!("d/{}", Self::json_stringify_lower(ts)))
        .ok()
        .flatten()
        .is_none()
      {
        return;
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
