use super::*;
mod tap;
pub(crate) use tap::{
  TapBloomFilter,
  TapBatch,
  DmtElementRecord,
  // records
  BitmapRecord,
  DeployRecord,
  MintRecord,
  MintFlatRecord,
  MintSuperflatRecord,
  TransferInitRecord,
  TransferInitFlatRecord,
  TransferInitSuperflatRecord,
  TransferSendSenderRecord,
  TransferSendReceiverRecord,
  TransferSendFlatRecord,
  TransferSendSuperflatRecord,
  TradeOfferRecord,
  TradeBuySellerRecord,
  TradeBuyBuyerRecord,
  PrivilegeVerifiedRecord,
  TapAccumulatorEntry,
  TokenAuthCreateRecord,
  TokenAuthRedeemRecord,
  TAP_BITMAP_START_HEIGHT,
  TAP_DMT_NAT_REWARDS_HEIGHT,
  TAP_BLOOM_K,
  TAP_BLOOM_DMT_BITS,
  TAP_BLOOM_PRIV_BITS,
  TAP_BLOOM_ANY_BITS,
  TAP_BLOOM_DIR,
  MAX_DEC_U64_STR,
  BURN_ADDRESS,
  TapFeature,
};
use hex;
use secp256k1::{Secp256k1, Message, ecdsa::{RecoverableSignature, RecoveryId, Signature as SecpSignature}};
use std::collections::HashMap;
use std::str::FromStr;
// address/segmentation helpers live in tap::mod; no direct imports needed here

use serde::{Serialize, Deserialize};
use std::rc::Rc;
use std::cell::RefCell;

const BRC20_PRIVILEGE_AUTHORITY: &str = "c14d3de97cecc573d86592240ef38bf5ba298c8c2eaf68e17b99dbbeedbab7e4i0";

#[derive(Debug, PartialEq, Copy, Clone)]
enum Curse {
  DuplicateField,
  IncompleteField,
  NotAtOffsetZero,
  NotInFirstInput,
  Pointer,
  Pushnum,
  Reinscription,
  Stutter,
  UnrecognizedEvenField,
}

#[derive(Debug, Clone)]
pub(super) struct Flotsam {
  inscription_id: InscriptionId,
  offset: u64,
  origin: Origin,
}

#[derive(Debug, Clone)]
enum Origin {
  New {
    cursed: bool,
    fee: u64,
    hidden: bool,
    parents: Vec<InscriptionId>,
    reinscription: bool,
    unbound: bool,
    vindicated: bool,
    payload: Inscription,
  },
  Old {
    sequence_number: u32,
    old_satpoint: SatPoint,
  },
}

pub(super) struct InscriptionUpdater<'a, 'tx> {
  pub(super) blessed_inscription_count: u64,
  pub(super) cursed_inscription_count: u64,
  pub(super) flotsam: Vec<Flotsam>,
  pub(super) height: u32,
  // Height when this indexing run started; used to guard early bloom gating.
  pub(super) run_start_height: u32,
  pub(super) home_inscription_count: u64,
  pub(super) home_inscriptions: &'a mut Table<'tx, u32, InscriptionIdValue>,
  pub(super) id_to_sequence_number: &'a mut Table<'tx, InscriptionIdValue, u32>,
  pub(super) inscription_number_to_sequence_number: &'a mut Table<'tx, i32, u32>,
  pub(super) lost_sats: u64,
  pub(super) next_sequence_number: u32,
  pub(super) reward: u64,
  pub(super) transaction_buffer: Vec<u8>,
  pub(super) transaction_id_to_transaction: &'a mut Table<'tx, &'static TxidValue, &'static [u8]>,
  pub(super) sat_to_sequence_number: &'a mut MultimapTable<'tx, u64, u32>,
  pub(super) sequence_number_to_children: &'a mut MultimapTable<'tx, u32, u32>,
  pub(super) sequence_number_to_entry: &'a mut Table<'tx, u32, InscriptionEntryValue>,
  pub(super) timestamp: u32,
  pub(super) unbound_inscriptions: u64,
  pub(super) tap_db: TapBatch<'a, 'tx>,
  // Fast membership filters (shared with block updater via Rc)
  pub(super) dmt_bloom: Option<Rc<RefCell<TapBloomFilter>>>,
  pub(super) priv_bloom: Option<Rc<RefCell<TapBloomFilter>>>,
  // Cached list lengths within the current block to avoid repeated length reads
  pub(super) list_len_cache: HashMap<String, usize>,
  pub(super) any_bloom: Option<Rc<RefCell<TapBloomFilter>>>,
  // TAP profiling
  pub(super) profile: bool,
  pub(super) prof_bm_tr_ms: u128,
  pub(super) prof_bm_tr_ct: u64,
  pub(super) prof_dmt_tr_ms: u128,
  pub(super) prof_dmt_tr_ct: u64,
  pub(super) prof_prv_tr_ms: u128,
  pub(super) prof_prv_tr_ct: u64,
  pub(super) prof_ttr_ex_ms: u128,
  pub(super) prof_ttr_ex_ct: u64,
  pub(super) prof_tsend_ex_ms: u128,
  pub(super) prof_tsend_ex_ct: u64,
  pub(super) prof_ttrade_ex_ms: u128,
  pub(super) prof_ttrade_ex_ct: u64,
  pub(super) prof_tauth_ex_ms: u128,
  pub(super) prof_tauth_ex_ct: u64,
  pub(super) prof_pra_ex_ms: u128,
  pub(super) prof_pra_ex_ct: u64,
  pub(super) prof_blk_ex_ms: u128,
  pub(super) prof_blk_ex_ct: u64,
  pub(super) prof_unblk_ex_ms: u128,
  pub(super) prof_unblk_ex_ct: u64,
  // Created-path profiling
  pub(super) prof_created_total_ms: u128,
  pub(super) prof_created_ct: u64,
  pub(super) prof_bm_cr_ms: u128,
  pub(super) prof_bm_cr_ct: u64,
  pub(super) prof_dmt_el_cr_ms: u128,
  pub(super) prof_dmt_el_cr_ct: u64,
  pub(super) prof_dpl_cr_ms: u128,
  pub(super) prof_dpl_cr_ct: u64,
  pub(super) prof_dmtmint_cr_ms: u128,
  pub(super) prof_dmtmint_cr_ct: u64,
  pub(super) prof_mint_cr_ms: u128,
  pub(super) prof_mint_cr_ct: u64,
  pub(super) prof_ttr_cr_ms: u128,
  pub(super) prof_ttr_cr_ct: u64,
  pub(super) prof_tsend_cr_ms: u128,
  pub(super) prof_tsend_cr_ct: u64,
  pub(super) prof_ttrade_cr_ms: u128,
  pub(super) prof_ttrade_cr_ct: u64,
  pub(super) prof_tauth_cr_ms: u128,
  pub(super) prof_tauth_cr_ct: u64,
  pub(super) prof_dmtdep_cr_ms: u128,
  pub(super) prof_dmtdep_cr_ct: u64,
  pub(super) prof_pra_cr_ms: u128,
  pub(super) prof_pra_cr_ct: u64,
  pub(super) prof_prv_cr_ms: u128,
  pub(super) prof_prv_cr_ct: u64,
  pub(super) prof_blk_cr_ms: u128,
  pub(super) prof_blk_cr_ct: u64,
  pub(super) prof_unblk_cr_ms: u128,
  pub(super) prof_unblk_cr_ct: u64,
  // Core inscription-path profiling (deeper than TAP)
  pub(super) prof_core_env_ms: u128,
  pub(super) prof_core_old_ms: u128,
  pub(super) prof_core_new_ms: u128,
  pub(super) prof_core_parent_ms: u128,
  pub(super) prof_core_txdb_ms: u128,
  pub(super) prof_core_addr_ms: u128,
  pub(super) prof_core_update_ms: u128,
  pub(super) prof_core_event_ms: u128,
  pub(super) prof_core_event_ct: u64,
  pub(super) prof_core_old_ct: u64,
  pub(super) prof_core_new_ct: u64,
  pub(super) prof_core_txdb_ct: u64,
  pub(super) prof_core_addr_ct: u64,
  pub(super) prof_core_update_ct: u64,
  // Fine-grained update breakdown
  pub(super) prof_core_up_old_ms: u128,
  pub(super) prof_core_up_old_ct: u64,
  pub(super) prof_core_up_new_ms: u128,
  pub(super) prof_core_up_new_ct: u64,
  // microsecond-resolution counters for sub-steps
  pub(super) prof_core_up_new_parents_us: u128,
  pub(super) prof_core_up_new_entry_us: u128,
  pub(super) prof_core_up_new_serialize_us: u128,
  pub(super) prof_core_up_new_maps_us: u128,
  pub(super) prof_core_up_new_num_us: u128,
  pub(super) prof_core_up_new_sat_us: u128,
  pub(super) prof_core_up_new_delegate_us: u128,
  pub(super) prof_core_up_tap_us: u128,
  pub(super) prof_core_up_utxo_us: u128,
  // Per-block memoization for delegate lookups (nested delegate detection)
  pub(super) delegate_cache: HashMap<InscriptionId, bool>,
  // Active Bitcoin network for address validation in TAP
  pub(super) btc_network: bitcoin::Network,
}

impl InscriptionUpdater<'_, '_> {
  pub(super) fn is_dmt_nat_rewards_enabled(&self) -> bool {
    self.tap_feature_enabled(TapFeature::DmtNatRewards)
  }
  fn parse_inscription_id_str(s: &str) -> Option<InscriptionId> {
    InscriptionId::from_str(s).ok()
  }

  fn ordinal_available(&mut self, s: &str) -> bool {
    let Some(id) = Self::parse_inscription_id_str(s) else { return false; };
    self.id_to_sequence_number.get(&id.store()).ok().flatten().is_some()
  }

  pub(super) fn index_dmt_nat_rewards_for_block(
    &mut self,
    coinbase: &bitcoin::Transaction,
    bits: u32,
    index: &Index,
  ) -> Result {
    // Only apply on/after NAT rewards activation height
    if self.height < TAP_DMT_NAT_REWARDS_HEIGHT { return Ok(()); }
    // NAT ticker and keys
    let tick_lower = "dmt-nat".to_string();
    let tick_key = Self::json_stringify_lower(&tick_lower);

    // Must have a deployment and tokens-left counter
    let Some(deployed) = self.tap_get::<DeployRecord>(&format!("d/{}", tick_key)).ok().flatten() else { return Ok(()); };
    // Verify deployment inscription exists in ord view
    if !self.ordinal_available(&deployed.ins) { return Ok(()); }
    // Tokens left
    let mut tokens_left: i128 = match self.tap_get::<String>(&format!("dc/{}", tick_key)).ok().flatten().and_then(|s| s.parse::<i128>().ok()) { Some(v) => v, None => return Ok(()) };

    // Coinbase outputs share (exclude OP_RETURN)
    let mut tot_btc: u128 = 0;
    let mut outs: Vec<(usize, String, u64)> = Vec::new();
    for (i, txout) in coinbase.output.iter().enumerate() {
      if txout.script_pubkey.is_op_return() { continue; }
      let addr = self.resolve_owner_address(txout, index);
      if addr.trim() == "-" { continue; }
      let val_sat = txout.value.to_sat();
      outs.push((i, addr, val_sat));
      tot_btc = tot_btc.saturating_add(val_sat as u128);
    }
    if tot_btc == 0 { return Ok(()); }

    // NAT total = header bits as integer
    let nat_total: u128 = u128::from(bits);

    for (vout, address, val_sat) in outs {
      // Compute nat share: floor(nat_total * (val_sat/tot_btc))
      let amount_calc = (nat_total.saturating_mul(val_sat as u128)) / tot_btc;
      let mut amount: i128 = i128::try_from(amount_calc).unwrap_or(0);

      let mut fail = false;
      // Limit and tokens-left
      let limit: i128 = deployed.lim.parse::<i128>().unwrap_or(0);
      if limit > 0 && amount > limit { fail = true; }
      if !fail {
        if tokens_left - amount < 0 { amount = tokens_left; }
        if amount <= 0 { fail = true; }
      }

      // Balance update and holder updates
      let bal_key = format!("b/{}/{}", address, tick_key);
      let mut balance: i128 = self.tap_get::<String>(&bal_key).ok().flatten().and_then(|s| s.parse::<i128>().ok()).unwrap_or(0);
      if !fail {
        tokens_left = tokens_left.saturating_sub(amount);
        let _ = self.tap_put(&format!("dc/{}", tick_key), &tokens_left.to_string());
        balance = balance.saturating_add(amount);
        let _ = self.tap_put(&bal_key, &balance.to_string());
        // holders list
        if self.tap_get::<String>(&format!("he/{}/{}", address, tick_key)).ok().flatten().is_none() {
          let _ = self.tap_put(&format!("he/{}/{}", address, tick_key), &"".to_string());
          let _ = self.tap_set_list_record(&format!("h/{}", tick_key), &format!("hi/{}", tick_key), &address);
        }
        // account token owned
        if self.tap_get::<String>(&format!("ato/{}/{}", address, tick_key)).ok().flatten().is_none() {
          let tick_lower_for_list = serde_json::from_str::<String>(&tick_key).unwrap_or_else(|_| tick_lower.clone());
          let _ = self.tap_set_list_record(&format!("atl/{}", address), &format!("atli/{}", address), &tick_lower_for_list);
          let _ = self.tap_put(&format!("ato/{}/{}", address, tick_key), &"".to_string());
        }
        // mark block as minted to prevent duplicates
        let _ = self.tap_put(&format!("dmt-blk/{}/{}", tick_lower, self.height), &"".to_string());
      }

      // Record shapes (typed CBOR structs) — ins/num are None for rewards
      // Parity with compendium + writer: only write NAT reward mint records when amount > 0.
      if amount > 0 {
        let ts = self.timestamp;
        let txid = coinbase.compute_txid().to_string();
        let val_str = (val_sat as u128).to_string();
        let mint_rec = MintRecord {
          addr: address.clone(),
          blck: self.height,
          amt: amount.to_string(),
          bal: balance.to_string(),
          tx: Some(txid.clone()),
          vo: vout as u32,
          val: val_str.clone(),
          ins: None,
          num: None,
          ts,
          fail,
          dmtblck: Some(self.height),
          dta: None,
        };
        let _ = self.tap_set_list_record(&format!("aml/{}/{}", address, tick_key), &format!("amli/{}/{}", address, tick_key), &mint_rec);
        let flat_rec = MintFlatRecord { addr: mint_rec.addr.clone(), blck: mint_rec.blck, amt: mint_rec.amt.clone(), bal: mint_rec.bal.clone(), tx: Some(txid.clone()), vo: mint_rec.vo, val: mint_rec.val.clone(), ins: None, num: None, ts: mint_rec.ts, fail: mint_rec.fail, dmtblck: mint_rec.dmtblck, dta: None };
        let _ = self.tap_set_list_record(&format!("fml/{}", tick_key), &format!("fmli/{}", tick_key), &flat_rec);
        let super_rec = MintSuperflatRecord { tick: tick_lower.clone(), addr: address.clone(), blck: self.height, amt: amount.to_string(), bal: balance.to_string(), tx: Some(txid.clone()), vo: vout as u32, val: val_str, ins: None, num: None, ts, fail, dmtblck: Some(self.height), dta: None };
        if let Ok(list_len) = self.tap_set_list_record("sfml", "sfmli", &super_rec) {
          let ptr = format!("sfmli/{}", list_len - 1);
          // mirror writer pointers for NAT rewards too
          let _ = self.tap_set_list_record(&format!("tx/mnt/{}", txid), &format!("txi/mnt/{}", txid), &ptr);
          let _ = self.tap_set_list_record(&format!("txt/mnt/{}/{}", tick_key, txid), &format!("txti/mnt/{}/{}", tick_key, txid), &ptr);
          let _ = self.tap_set_list_record(&format!("blck/mnt/{}", self.height), &format!("blcki/mnt/{}", self.height), &ptr);
          let _ = self.tap_set_list_record(&format!("blckt/mnt/{}/{}", tick_key, self.height), &format!("blckti/mnt/{}/{}", tick_key, self.height), &ptr);
        }
      }
    }

    Ok(())
  }

  fn resolve_owner_address(&self, txout: &TxOut, index: &Index) -> String {
    if txout.script_pubkey.is_op_return() {
      return "-".to_string();
    }

    match index
      .settings
      .chain()
      .address_from_script(&txout.script_pubkey)
    {
      Ok(addr) => addr.to_string(),
      Err(_) => "-".to_string(),
    }
  }
  pub(super) fn index_inscriptions(
    &mut self,
    tx: &Transaction,
    txid: Txid,
    input_utxo_entries: &[ParsedUtxoEntry],
    output_utxo_entries: &mut [UtxoEntryBuf],
    utxo_cache: &mut HashMap<OutPoint, UtxoEntryBuf>,
    index: &Index,
    input_sat_ranges: Option<&Vec<&[u8]>>,
  ) -> Result {
    // TAP batch is available via self.tap_put/self.tap_get if needed here.
    let __core_start_env = std::time::Instant::now();
    let mut floating_inscriptions = Vec::new();
    let mut id_counter = 0;
    let mut inscribed_offsets = BTreeMap::new();
    let jubilant = self.height >= index.settings.chain().jubilee_height();
    let mut total_input_value = 0;
    let total_output_value = tx
      .output
      .iter()
      .map(|txout| txout.value.to_sat())
      .sum::<u64>();

    let envelopes = ParsedEnvelope::from_transaction(tx);
    let has_new_inscriptions = !envelopes.is_empty();
    let mut envelopes = envelopes.into_iter().peekable();
    if self.profile { self.prof_core_env_ms += __core_start_env.elapsed().as_millis(); }

    for (input_index, txin) in tx.input.iter().enumerate() {
      // skip subsidy since no inscriptions possible
      if txin.previous_output.is_null() {
        total_input_value += Height(self.height).subsidy();
        continue;
      }

      let __core_start_old = std::time::Instant::now();
      let mut transferred_inscriptions = input_utxo_entries[input_index].parse_inscriptions();

      transferred_inscriptions.sort_by_key(|(sequence_number, _)| *sequence_number);

      for (sequence_number, old_satpoint_offset) in transferred_inscriptions {
        let old_satpoint = SatPoint {
          outpoint: txin.previous_output,
          offset: old_satpoint_offset,
        };

        let inscription_id = InscriptionEntry::load(
          self
            .sequence_number_to_entry
            .get(sequence_number)?
            .unwrap()
            .value(),
        )
        .id;

        let offset = total_input_value + old_satpoint_offset;
        floating_inscriptions.push(Flotsam {
          offset,
          inscription_id,
          origin: Origin::Old {
            sequence_number,
            old_satpoint,
          },
        });

        inscribed_offsets
          .entry(offset)
          .or_insert((inscription_id, 0))
          .1 += 1;
      }
      if self.profile { self.prof_core_old_ms += __core_start_old.elapsed().as_millis(); }

      let offset = total_input_value;

      let input_value = input_utxo_entries[input_index].total_value();
      total_input_value += input_value;

      // go through all inscriptions in this input
      let __core_start_new_scan = std::time::Instant::now();
      let mut __core_new_local_ct: u64 = 0;
      while let Some(inscription) = envelopes.peek() {
        if inscription.input != u32::try_from(input_index).unwrap() {
          break;
        }

        let inscription_id = InscriptionId {
          txid,
          index: id_counter,
        };

        let curse = if inscription.payload.unrecognized_even_field {
          Some(Curse::UnrecognizedEvenField)
        } else if inscription.payload.duplicate_field {
          Some(Curse::DuplicateField)
        } else if inscription.payload.incomplete_field {
          Some(Curse::IncompleteField)
        } else if inscription.input != 0 {
          Some(Curse::NotInFirstInput)
        } else if inscription.offset != 0 {
          Some(Curse::NotAtOffsetZero)
        } else if inscription.payload.pointer.is_some() {
          Some(Curse::Pointer)
        } else if inscription.pushnum {
          Some(Curse::Pushnum)
        } else if inscription.stutter {
          Some(Curse::Stutter)
        } else if let Some((id, count)) = inscribed_offsets.get(&offset) {
          if *count > 1 {
            Some(Curse::Reinscription)
          } else {
            let initial_inscription_sequence_number =
              self.id_to_sequence_number.get(id.store())?.unwrap().value();

            let entry = InscriptionEntry::load(
              self
                .sequence_number_to_entry
                .get(initial_inscription_sequence_number)?
                .unwrap()
                .value(),
            );

            let initial_inscription_was_cursed_or_vindicated =
              entry.inscription_number < 0 || Charm::Vindicated.is_set(entry.charms);

            if initial_inscription_was_cursed_or_vindicated {
              None
            } else {
              Some(Curse::Reinscription)
            }
          }
        } else {
          None
        };

        let offset = inscription
          .payload
          .pointer()
          .filter(|&pointer| pointer < total_output_value)
          .unwrap_or(offset);

        floating_inscriptions.push(Flotsam {
          inscription_id,
          offset,
          origin: Origin::New {
            cursed: curse.is_some() && !jubilant,
            fee: 0,
            hidden: inscription.payload.hidden(),
            parents: inscription.payload.parents(),
            reinscription: inscribed_offsets.contains_key(&offset),
            unbound: input_value == 0
              || curse == Some(Curse::UnrecognizedEvenField)
              || inscription.payload.unrecognized_even_field,
            vindicated: curse.is_some() && jubilant,
            payload: inscription.payload.clone(),
          },
        });

        inscribed_offsets
          .entry(offset)
          .or_insert((inscription_id, 0))
          .1 += 1;

        envelopes.next();
        id_counter += 1;
        __core_new_local_ct += 1;
      }
      if self.profile {
        self.prof_core_new_ms += __core_start_new_scan.elapsed().as_millis();
        self.prof_core_new_ct += __core_new_local_ct;
      }
    }

    if index.index_transactions && has_new_inscriptions {
      let __core_start_txdb = std::time::Instant::now();
      tx.consensus_encode(&mut self.transaction_buffer)
        .expect("in-memory writers don't error");

      self
        .transaction_id_to_transaction
        .insert(&txid.store(), self.transaction_buffer.as_slice())?;

      self.transaction_buffer.clear();
      if self.profile { self.prof_core_txdb_ms += __core_start_txdb.elapsed().as_millis(); self.prof_core_txdb_ct += 1; }
    }

    let potential_parents = floating_inscriptions
      .iter()
      .map(|flotsam| flotsam.inscription_id)
      .collect::<HashSet<InscriptionId>>();

    let __core_start_parent = std::time::Instant::now();
    for flotsam in &mut floating_inscriptions {
      if let Flotsam {
        origin: Origin::New {
          parents: purported_parents,
          ..
        },
        ..
      } = flotsam
      {
        let mut seen = HashSet::new();
        purported_parents
          .retain(|parent| seen.insert(*parent) && potential_parents.contains(parent));
      }
    }
    if self.profile { self.prof_core_parent_ms += __core_start_parent.elapsed().as_millis(); }

    // still have to normalize over inscription size
    for flotsam in &mut floating_inscriptions {
      if let Flotsam {
        origin: Origin::New { fee, .. },
        ..
      } = flotsam
      {
        *fee = (total_input_value - total_output_value) / u64::from(id_counter);
      }
    }

    let is_coinbase = tx
      .input
      .first()
      .map(|tx_in| tx_in.previous_output.is_null())
      .unwrap_or_default();

    if is_coinbase {
      floating_inscriptions.append(&mut self.flotsam);
    }

    floating_inscriptions.sort_by_key(|flotsam| flotsam.offset);
    let mut inscriptions = floating_inscriptions.into_iter().peekable();

    let mut new_locations = Vec::new();
    let mut output_value = 0;
    for (vout, txout) in tx.output.iter().enumerate() {
      let end = output_value + txout.value.to_sat();

      while let Some(flotsam) = inscriptions.peek() {
        if flotsam.offset >= end {
          break;
        }

        let new_satpoint = SatPoint {
          outpoint: OutPoint {
            txid,
            vout: vout.try_into().unwrap(),
          },
          offset: flotsam.offset - output_value,
        };

        // Resolve owner address string for this output (or '-' if OP_RETURN)
        let __core_start_addr = std::time::Instant::now();
        let owner_address = self.resolve_owner_address(txout, index);
        if self.profile { self.prof_core_addr_ms += __core_start_addr.elapsed().as_millis(); self.prof_core_addr_ct += 1; }

        let receiving_value = txout.value.to_sat();

        new_locations.push((
          new_satpoint,
          inscriptions.next().unwrap(),
          txout.script_pubkey.is_op_return(),
          owner_address,
          receiving_value,
        ));
      }

      output_value = end;
    }

    for (new_satpoint, flotsam, op_return, owner_address, receiving_value) in new_locations.into_iter() {
      let output_utxo_entry =
        &mut output_utxo_entries[usize::try_from(new_satpoint.outpoint.vout).unwrap()];

      let __core_start_update = std::time::Instant::now();
      self.update_inscription_location(
        input_sat_ranges,
        flotsam,
        new_satpoint,
        op_return,
        Some(output_utxo_entry),
        utxo_cache,
        index,
        &owner_address,
        receiving_value,
      )?;
      if self.profile { self.prof_core_update_ms += __core_start_update.elapsed().as_millis(); self.prof_core_update_ct += 1; }
    }

    if is_coinbase {
      for flotsam in inscriptions {
        let new_satpoint = SatPoint {
          outpoint: OutPoint::null(),
          offset: self.lost_sats + flotsam.offset - output_value,
        };
        self.update_inscription_location(
          input_sat_ranges,
          flotsam,
          new_satpoint,
          false,
          None,
          utxo_cache,
          index,
          "-",
          0,
        )?;
      }
      self.lost_sats += self.reward - output_value;
      Ok(())
    } else {
      self.flotsam.extend(inscriptions.map(|flotsam| Flotsam {
        offset: self.reward + flotsam.offset - output_value,
        ..flotsam
      }));
      self.reward += total_input_value - output_value;
      // TAP batch remains open; no explicit tx end marker needed.
      Ok(())
    }
  }

  fn calculate_sat(input_sat_ranges: Option<&Vec<&[u8]>>, input_offset: u64) -> Option<Sat> {
    let input_sat_ranges = input_sat_ranges?;

    let mut offset = 0;
    for chunk in input_sat_ranges
      .iter()
      .flat_map(|slice| slice.chunks_exact(11))
    {
      let (start, end) = SatRange::load(chunk.try_into().unwrap());
      let size = end - start;
      if offset + size > input_offset {
        let n = start + input_offset - offset;
        return Some(Sat(n));
      }
      offset += size;
    }

    unreachable!()
  }

  fn update_inscription_location(
    &mut self,
    input_sat_ranges: Option<&Vec<&[u8]>>,
    flotsam: Flotsam,
    new_satpoint: SatPoint,
    op_return: bool,
    mut normal_output_utxo_entry: Option<&mut UtxoEntryBuf>,
    utxo_cache: &mut HashMap<OutPoint, UtxoEntryBuf>,
    index: &Index,
    // TAP additions: resolved owner address for the receiving output ("-" for OP_RETURN or special), and receiving output value in sats
    owner_address: &str,
    output_value_sat: u64,
  ) -> Result {
    let inscription_id = flotsam.inscription_id;
    let __upd_start_total = if self.profile { Some(std::time::Instant::now()) } else { None };
    let (unbound, sequence_number) = match flotsam.origin {
      Origin::Old {
        sequence_number,
        old_satpoint,
      } => {
        let __upd_old_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        if op_return {
          let entry = InscriptionEntry::load(
            self
              .sequence_number_to_entry
              .get(&sequence_number)?
              .unwrap()
              .value(),
          );

          let mut charms = entry.charms;
          Charm::Burned.set(&mut charms);

          self.sequence_number_to_entry.insert(
            sequence_number,
            &InscriptionEntry { charms, ..entry }.store(),
          )?;
        }

        if let Some(ref sender) = index.event_sender {
          let __core_start_evt = std::time::Instant::now();
          sender.blocking_send(Event::InscriptionTransferred {
            block_height: self.height,
            inscription_id,
            new_location: new_satpoint,
            old_location: old_satpoint,
            sequence_number,
          })?;
          if self.profile { self.prof_core_event_ms += __core_start_evt.elapsed().as_millis(); self.prof_core_event_ct += 1; }
        }

        // TAP hook: inscription transferred (bitmap processing)
        let __upd_tap_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        self.tap_on_inscription_transferred(
          inscription_id,
          sequence_number,
          old_satpoint,
          new_satpoint,
          op_return,
          owner_address,
          output_value_sat,
        );
          if let Some(st) = __upd_tap_start { if self.profile { self.prof_core_up_tap_us += st.elapsed().as_micros(); } }
        if let Some(st) = __upd_old_start { if self.profile { self.prof_core_up_old_ms += st.elapsed().as_millis(); self.prof_core_up_old_ct += 1; } }

        (false, sequence_number)
      }
      Origin::New {
        cursed,
        fee,
        hidden,
        parents,
        reinscription,
        unbound,
        vindicated,
        payload,
      } => {
        let inscription_number = if cursed {
          let number: i32 = self.cursed_inscription_count.try_into().unwrap();
          self.cursed_inscription_count += 1;
          -(number + 1)
        } else {
          let number: i32 = self.blessed_inscription_count.try_into().unwrap();
          self.blessed_inscription_count += 1;
          number
        };

        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        let __upd_new_num_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        self
          .inscription_number_to_sequence_number
          .insert(inscription_number, sequence_number)?;
        if let Some(st) = __upd_new_num_start { if self.profile { self.prof_core_up_new_num_us += st.elapsed().as_micros(); } }

        let sat = if unbound {
          None
        } else {
          Self::calculate_sat(input_sat_ranges, flotsam.offset)
        };

        let mut charms = 0;

        if cursed {
          Charm::Cursed.set(&mut charms);
        }

        if reinscription {
          Charm::Reinscription.set(&mut charms);
        }

        if let Some(sat) = sat {
          charms |= sat.charms();
        }

        if op_return {
          Charm::Burned.set(&mut charms);
        }

        if new_satpoint.outpoint == OutPoint::null() {
          Charm::Lost.set(&mut charms);
        }

        if unbound {
          Charm::Unbound.set(&mut charms);
        }

        if vindicated {
          Charm::Vindicated.set(&mut charms);
        }

        if let Some(Sat(n)) = sat {
          let __upd_new_sat_start = if self.profile { Some(std::time::Instant::now()) } else { None };
          self.sat_to_sequence_number.insert(&n, &sequence_number)?;
          if let Some(st) = __upd_new_sat_start { if self.profile { self.prof_core_up_new_sat_us += st.elapsed().as_micros(); } }
        }

        let __upd_new_parents_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        let parent_sequence_numbers = parents
          .iter()
          .map(|parent| {
            let parent_sequence_number = self
              .id_to_sequence_number
              .get(&parent.store())?
              .unwrap()
              .value();

            self
              .sequence_number_to_children
              .insert(parent_sequence_number, sequence_number)?;

            Ok(parent_sequence_number)
          })
          .collect::<Result<Vec<u32>>>()?;
        if let Some(st) = __upd_new_parents_start { if self.profile { self.prof_core_up_new_parents_us += st.elapsed().as_micros(); } }

        // serialize and then insert entry separately for profiling
        let __upd_new_serialize_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        let entry_store = InscriptionEntry {
            charms,
            fee,
            height: self.height,
            id: inscription_id,
            inscription_number,
            parents: parent_sequence_numbers,
            sat,
            sequence_number,
            timestamp: self.timestamp,
          }
          .store();
        if let Some(st) = __upd_new_serialize_start { if self.profile { self.prof_core_up_new_serialize_us += st.elapsed().as_micros(); } }

        let __upd_new_entry_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        self.sequence_number_to_entry.insert(
          sequence_number,
          &entry_store,
        )?;
        if let Some(st) = __upd_new_entry_start { if self.profile { self.prof_core_up_new_entry_us += st.elapsed().as_micros(); } }

        let __upd_new_maps_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        self
          .id_to_sequence_number
          .insert(&inscription_id.store(), sequence_number)?;

        if !hidden {
          self
            .home_inscriptions
            .insert(&sequence_number, inscription_id.store())?;

          if self.home_inscription_count == 100 {
            self.home_inscriptions.pop_first()?;
          } else {
          self.home_inscription_count += 1;
          }
        }
        if let Some(st) = __upd_new_maps_start { if self.profile { self.prof_core_up_new_maps_us += st.elapsed().as_micros(); } }

        // Compute the satpoint that will be used for this inscription for TAP hook purposes.
        let satpoint_for_hook = if unbound {
          SatPoint {
            outpoint: unbound_outpoint(),
            offset: self.unbound_inscriptions,
          }
        } else {
          new_satpoint
        };

        // TAP hook: inscription created (delegate guard handled inside)
        let __upd_tap_start = if self.profile { Some(std::time::Instant::now()) } else { None };
        self.tap_on_inscription_created(
          inscription_id,
          sequence_number,
          inscription_number,
          satpoint_for_hook,
          &payload,
          &parents,
          cursed,
          hidden,
          unbound,
          vindicated,
          reinscription,
          op_return,
          fee,
          owner_address,
          output_value_sat,
          index,
        );
        if let Some(st) = __upd_tap_start { if self.profile { self.prof_core_up_tap_us += st.elapsed().as_micros(); } }

        if let Some(ref sender) = index.event_sender {
          let __core_start_evt = std::time::Instant::now();
          sender.blocking_send(Event::InscriptionCreated {
            block_height: self.height,
            charms,
            inscription_id,
            location: (!unbound).then_some(new_satpoint),
            parent_inscription_ids: parents,
            sequence_number,
          })?;
          if self.profile { self.prof_core_event_ms += __core_start_evt.elapsed().as_millis(); self.prof_core_event_ct += 1; }
        }

        if let Some(st) = __upd_start_total { if self.profile { self.prof_core_up_new_ms += st.elapsed().as_millis(); self.prof_core_up_new_ct += 1; } }
        (unbound, sequence_number)
      }
    };

    let satpoint = if unbound {
      let new_unbound_satpoint = SatPoint {
        outpoint: unbound_outpoint(),
        offset: self.unbound_inscriptions,
      };
      self.unbound_inscriptions += 1;
      normal_output_utxo_entry = None;
      new_unbound_satpoint
    } else {
      new_satpoint
    };

    // The special outpoints, i.e., the null outpoint and the unbound outpoint,
    // don't follow the normal rules. Unlike real outputs they get written to
    // more than once. So we create a new UTXO entry here and commit() will
    // merge it with any existing entry.
    let output_utxo_entry = normal_output_utxo_entry.unwrap_or_else(|| {
      assert!(Index::is_special_outpoint(satpoint.outpoint));
      utxo_cache
        .entry(satpoint.outpoint)
        .or_insert(UtxoEntryBuf::empty(index))
    });

    let __upd_utxo_start = if self.profile { Some(std::time::Instant::now()) } else { None };
    output_utxo_entry.push_inscription(sequence_number, satpoint.offset, index);
    if let Some(st) = __upd_utxo_start { if self.profile { self.prof_core_up_utxo_us += st.elapsed().as_micros(); } }

    Ok(())
  }

  pub(super) fn tap_finalize_block(&mut self) -> Result {
    self.tap_db.flush()?;
    // Clear per-block caches
    self.list_len_cache.clear();
    Ok(())
  }
  
  fn tap_on_inscription_created(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    inscription_number: i32,
    satpoint: SatPoint,
    payload: &Inscription,
    parents: &[InscriptionId],
    _cursed: bool,
    _hidden: bool,
    _unbound: bool,
    _vindicated: bool,
    _reinscription: bool,
    _op_return: bool,
    _fee: u64,
    owner_address: &str,
    output_value_sat: u64,
    index: &Index,
  ) {
    let __cr_total = std::time::Instant::now();
    // No TAP work before bitmap activation
    if !self.tap_feature_enabled(TapFeature::Bitmap) { return; }

    // content-type guard for text/json only
    let ct_ok = payload
      .content_type()
      .map(|ct| ct.starts_with("text/") || ct.starts_with("application/json"))
      .unwrap_or(false);
    if !ct_ok { return; }

    // Before TAP start, index bitmap and BRC-20 deployments only; skip other TAP logic
    if !self.tap_feature_enabled(TapFeature::TapStart) {
      // bitmap
      let __st = std::time::Instant::now();
      self.index_bitmap_created(
        inscription_id,
        inscription_number,
        satpoint,
        payload,
        owner_address,
        output_value_sat,
      );
      if self.profile { self.prof_bm_cr_ms += __st.elapsed().as_millis(); self.prof_bm_cr_ct += 1; }

      // BRC-20 deployments
      let __st = std::time::Instant::now();
      self.index_deployments(
        inscription_id,
        inscription_number,
        satpoint,
        payload,
        owner_address,
        output_value_sat,
      );
      if self.profile { self.prof_dpl_cr_ms += __st.elapsed().as_millis(); self.prof_dpl_cr_ct += 1; }
      return;
    }

    // Delegate guard for TAP parsing (after TAP start): only 1-level delegates allowed
    if let Some(delegate_id) = payload.delegate() {
      let __upd_new_delegate_start = if self.profile { Some(std::time::Instant::now()) } else { None };
      let has_nested = if let Some(hit) = self.delegate_cache.get(&delegate_id) { *hit } else {
        let nested = match index.get_inscription_by_id(delegate_id) { Ok(Some(insc)) => insc.delegate().is_some(), _ => false };
        self.delegate_cache.insert(delegate_id, nested);
        nested
      };
      if let Some(st) = __upd_new_delegate_start { if self.profile { self.prof_core_up_new_delegate_us += st.elapsed().as_micros(); } }
      if has_nested { return; }
    }

    let __st = std::time::Instant::now();
    self.index_bitmap_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_bm_cr_ms += __st.elapsed().as_millis(); self.prof_bm_cr_ct += 1; }

    // DMT element creation (string inscriptions ending with .element)
    let __st = std::time::Instant::now();
    self.index_dmt_element_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_dmt_el_cr_ms += __st.elapsed().as_millis(); self.prof_dmt_el_cr_ct += 1; }

    let __st = std::time::Instant::now();
    self.index_deployments(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_dpl_cr_ms += __st.elapsed().as_millis(); self.prof_dpl_cr_ct += 1; }

    // DMT mint
    let __st = std::time::Instant::now();
    self.index_dmt_mint(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
      parents,
    );
    if self.profile { self.prof_dmtmint_cr_ms += __st.elapsed().as_millis(); self.prof_dmtmint_cr_ct += 1; }

    // TAP token mints (new inscriptions only)
    let __st = std::time::Instant::now();
    self.index_mints(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_mint_cr_ms += __st.elapsed().as_millis(); self.prof_mint_cr_ct += 1; }

    // TAP token transfers (initial transfer inscription)
    let __st = std::time::Instant::now();
    self.index_token_transfer_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_ttr_cr_ms += __st.elapsed().as_millis(); self.prof_ttr_cr_ct += 1; }

    // TAP token send (internal send intent)
    let __st = std::time::Instant::now();
    self.index_token_send_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_tsend_cr_ms += __st.elapsed().as_millis(); self.prof_tsend_cr_ct += 1; }

    // TAP token trade (internal trade intent)
    let __st = std::time::Instant::now();
    self.index_token_trade_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_ttrade_cr_ms += __st.elapsed().as_millis(); self.prof_ttrade_cr_ct += 1; }

    // TAP token auth (create/cancel or immediate redeem)
    let __st = std::time::Instant::now();
    self.index_token_auth_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_tauth_cr_ms += __st.elapsed().as_millis(); self.prof_tauth_cr_ct += 1; }

    // DMT deploy
    let __st = std::time::Instant::now();
    self.index_dmt_deploy(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_dmtdep_cr_ms += __st.elapsed().as_millis(); self.prof_dmtdep_cr_ct += 1; }

    // TAP privilege auth (create/cancel; accumulate only on creation)
    let __st = std::time::Instant::now();
    self.index_privilege_auth_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_pra_cr_ms += __st.elapsed().as_millis(); self.prof_pra_cr_ct += 1; }

    // TAP transferables block/unblock (accumulate on creation)
    let __st = std::time::Instant::now();
    self.index_block_transferables_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_blk_cr_ms += __st.elapsed().as_millis(); self.prof_blk_cr_ct += 1; }
    let __st = std::time::Instant::now();
    self.index_unblock_transferables_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_unblk_cr_ms += __st.elapsed().as_millis(); self.prof_unblk_cr_ct += 1; }

    // TAP privilege verification (verify on creation without accumulator)
    let __st = std::time::Instant::now();
    self.index_privilege_verify_created(
      inscription_id,
      inscription_number,
      satpoint,
      payload,
      owner_address,
      output_value_sat,
    );
    if self.profile { self.prof_prv_cr_ms += __st.elapsed().as_millis(); self.prof_prv_cr_ct += 1; }
    if self.profile { self.prof_created_total_ms += __cr_total.elapsed().as_millis(); self.prof_created_ct += 1; }
  }

  fn tap_on_inscription_transferred(
    &mut self,
    inscription_id: InscriptionId,
    _sequence_number: u32,
    _old_satpoint: SatPoint,
    new_satpoint: SatPoint,
    _op_return: bool,
    owner_address: &str,
    output_value_sat: u64,
  ) {
    // No TAP work before bitmap activation
    if !self.tap_feature_enabled(TapFeature::Bitmap) { return; }

    // Before TAP start, only bitmap transfers are relevant; route directly
    if !self.tap_feature_enabled(TapFeature::TapStart) {
      self.index_bitmap_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      return;
    }

    // Do not apply union-bloom preflight yet; first check cheap DB hints so we never
    // skip true positives when a stale bloom snapshot is loaded.
    // Fast routing by kind if available; otherwise, lazily detect and cache kind
    if let Some(kind) = self.tap_get::<String>(&format!("kind/{}", inscription_id)).ok().flatten() {
      match kind.as_str() {
        "bm" => {
          let __st = std::time::Instant::now();
          self.index_bitmap_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
          if self.profile { self.prof_bm_tr_ms += __st.elapsed().as_millis(); self.prof_bm_tr_ct += 1; }
          return;
        }
        "dmtmh" => {
          let __st = std::time::Instant::now();
          self.index_dmt_mint_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
          if self.profile { self.prof_dmt_tr_ms += __st.elapsed().as_millis(); self.prof_dmt_tr_ct += 1; }
          return;
        }
        "prvins" => {
          let __st = std::time::Instant::now();
          self.index_privilege_verify_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
          if self.profile { self.prof_prv_tr_ms += __st.elapsed().as_millis(); self.prof_prv_tr_ct += 1; }
          return;
        }
        "tl" => {
          let __st = std::time::Instant::now();
          self.index_token_transfer_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
          if self.profile { self.prof_ttr_ex_ms += __st.elapsed().as_millis(); self.prof_ttr_ex_ct += 1; }
          return;
        }
        _ => {}
      }
    } else {
      // Lazy detection by presence; set kind for future fast routing
      // Fast early negative-skip via union bloom when snapshot is fresh enough
      if let Some(bloom) = &self.any_bloom {
        let b = bloom.borrow();
        if b.ready && b.coverage_height >= self.run_start_height {
          if !b.contains_str(&inscription_id.to_string()) { return; }
        }
      }
      if self.tap_db.get(format!("bmh/{}", inscription_id).as_bytes()).ok().flatten().is_some() {
        let _ = self.tap_put(&format!("kind/{}", inscription_id), &"bm".to_string());
        let __st = std::time::Instant::now();
        self.index_bitmap_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
        if self.profile { self.prof_bm_tr_ms += __st.elapsed().as_millis(); self.prof_bm_tr_ct += 1; }
        return;
      }
      if self.tap_db.get(format!("dmtmh/{}", inscription_id).as_bytes()).ok().flatten().is_some() {
        let _ = self.tap_put(&format!("kind/{}", inscription_id), &"dmtmh".to_string());
        let __st = std::time::Instant::now();
        self.index_dmt_mint_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
        if self.profile { self.prof_dmt_tr_ms += __st.elapsed().as_millis(); self.prof_dmt_tr_ct += 1; }
        return;
      }
      if self.tap_db.get(format!("prvins/{}", inscription_id).as_bytes()).ok().flatten().is_some() {
        let _ = self.tap_put(&format!("kind/{}", inscription_id), &"prvins".to_string());
        let __st = std::time::Instant::now();
        self.index_privilege_verify_transferred(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
        if self.profile { self.prof_prv_tr_ms += __st.elapsed().as_millis(); self.prof_prv_tr_ct += 1; }
        return;
      }
      if let Some(val) = self.tap_db.get(format!("tl/{}", inscription_id).as_bytes()).ok().flatten() {
        if let Ok(ptr) = ciborium::de::from_reader::<String, _>(std::io::Cursor::new(&val)) {
          if !ptr.is_empty() {
            let _ = self.tap_put(&format!("kind/{}", inscription_id), &"tl".to_string());
            let __st = std::time::Instant::now();
            self.index_token_transfer_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
            if self.profile { self.prof_ttr_ex_ms += __st.elapsed().as_millis(); self.prof_ttr_ex_ct += 1; }
            return;
          }
        }
      }
    }

    // Accumulator-backed ops: if an accumulator exists for this inscription, dispatch
    // without bloom preflight to avoid false negatives when filter is stale.
    if let Ok(Some(_acc_any)) = self.tap_get::<TapAccumulatorEntry>(&format!("a/{}", inscription_id)) {
      let __st = std::time::Instant::now();
      self.index_token_send_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      if self.profile { self.prof_tsend_ex_ms += __st.elapsed().as_millis(); self.prof_tsend_ex_ct += 1; }
      let __st = std::time::Instant::now();
      self.index_token_trade_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      if self.profile { self.prof_ttrade_ex_ms += __st.elapsed().as_millis(); self.prof_ttrade_ex_ct += 1; }
      let __st = std::time::Instant::now();
      self.index_token_auth_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      if self.profile { self.prof_tauth_ex_ms += __st.elapsed().as_millis(); self.prof_tauth_ex_ct += 1; }
      let __st = std::time::Instant::now();
      self.index_privilege_auth_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      if self.profile { self.prof_pra_ex_ms += __st.elapsed().as_millis(); self.prof_pra_ex_ct += 1; }
      let __st = std::time::Instant::now();
      self.index_block_transferables_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      if self.profile { self.prof_blk_ex_ms += __st.elapsed().as_millis(); self.prof_blk_ex_ct += 1; }
      let __st = std::time::Instant::now();
      self.index_unblock_transferables_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
      if self.profile { self.prof_unblk_ex_ms += __st.elapsed().as_millis(); self.prof_unblk_ex_ct += 1; }
      return;
    }

    // Union preflight bloom: skip non-TAP inscriptions fast when snapshot is ready.
    // After all cheap presence checks; safe to skip negatives from here.
    if let Some(bloom) = &self.any_bloom {
      let b = bloom.borrow();
      if b.should_skip_negatives(self.height) {
        if !b.contains_str(&inscription_id.to_string()) { return; }
      }
    }

    // Fallback: execute other accumulators keyed by inscription id
    let __st = std::time::Instant::now();
    self.index_token_send_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
    if self.profile { self.prof_tsend_ex_ms += __st.elapsed().as_millis(); self.prof_tsend_ex_ct += 1; }
    let __st = std::time::Instant::now();
    self.index_token_trade_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
    if self.profile { self.prof_ttrade_ex_ms += __st.elapsed().as_millis(); self.prof_ttrade_ex_ct += 1; }
    let __st = std::time::Instant::now();
    self.index_token_auth_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
    if self.profile { self.prof_tauth_ex_ms += __st.elapsed().as_millis(); self.prof_tauth_ex_ct += 1; }
    let __st = std::time::Instant::now();
    self.index_privilege_auth_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
    if self.profile { self.prof_pra_ex_ms += __st.elapsed().as_millis(); self.prof_pra_ex_ct += 1; }
    let __st = std::time::Instant::now();
    self.index_block_transferables_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
    if self.profile { self.prof_blk_ex_ms += __st.elapsed().as_millis(); self.prof_blk_ex_ct += 1; }
    let __st = std::time::Instant::now();
    self.index_unblock_transferables_executed(inscription_id, _sequence_number, new_satpoint, owner_address, output_value_sat);
    if self.profile { self.prof_unblk_ex_ms += __st.elapsed().as_millis(); self.prof_unblk_ex_ct += 1; }
  }

  // --- Mint handlers moved to tap::ops::mint ---

  // --- Token trade (Internal) ---
  fn validate_trade_main_ticker_len(&self, tick: &str) -> bool {
    let vis_len = Self::visible_length(tick);
    Self::valid_transfer_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, self.feature_height(TapFeature::Jubilee), tick, vis_len)
  }

  fn validate_trade_accept_ticker_len(&self, tick: &str) -> bool {
    let t = Self::strip_prefix_for_len_check(tick);
    let vis_len = Self::visible_length(t);
    Self::valid_tap_ticker_visible_len(self.feature_height(TapFeature::FullTicker), self.height, vis_len)
  }

  fn verify_sig_obj_against_msg_with_hash(&self, sig_obj: &serde_json::Value, recovery_hash_hex: &str, msg_hash: &[u8; 32]) -> Option<(bool, String, String)> {
    // returns (is_valid, compact_sig_hex_lower, recovered_pubkey_hex)
    let sig = sig_obj.get("v")?;
    let r_val = sig_obj.get("r")?;
    let s_val = sig_obj.get("s")?;
    let v_i = if sig.is_string() { sig.as_str()?.parse::<i32>().ok()? } else { sig.as_i64()? as i32 };
    let r_str = if r_val.is_string() { r_val.as_str()?.to_string() } else { r_val.to_string() };
    let s_str = if s_val.is_string() { s_val.as_str()?.to_string() } else { s_val.to_string() };
    let r_bytes = Self::parse_sig_component_to_32(&r_str)?;
    let s_bytes = Self::parse_sig_component_to_32(&s_str)?;
    let compact_sig_hex = Self::secp_compact_hex(&r_bytes, &s_bytes).to_lowercase();

    // Recover pubkey from provided recovery hash (32-byte hex)
    let rec_hash_bytes = hex::decode(recovery_hash_hex.trim_start_matches("0x")).ok()?;
    if rec_hash_bytes.len() != 32 { return None; }
    let mut rec_hash_arr = [0u8; 32];
    rec_hash_arr.copy_from_slice(&rec_hash_bytes);
    let secp = Secp256k1::new();
    let rec_id = if let Ok(id) = RecoveryId::from_i32(v_i) { id } else { RecoveryId::from_i32(v_i - 27).ok()? };
    let mut sig_bytes = [0u8; 64];
    sig_bytes[..32].copy_from_slice(&r_bytes);
    sig_bytes[32..].copy_from_slice(&s_bytes);
    let rec_sig = RecoverableSignature::from_compact(&sig_bytes, rec_id).ok()?;
    let rec_msg = Message::from_digest_slice(&rec_hash_arr).ok()?;
    let pubkey = secp.recover_ecdsa(&rec_msg, &rec_sig).ok()?;
    let pubkey_uncompressed = pubkey.serialize_uncompressed();
    let pubkey_hex = hex::encode(pubkey_uncompressed);
    let norm_sig = SecpSignature::from_compact(&sig_bytes).ok()?;
    let verify_msg = Message::from_digest_slice(msg_hash).ok()?;
    let ok = secp.verify_ecdsa(&verify_msg, &norm_sig, &pubkey).is_ok();
    Some((ok, compact_sig_hex, pubkey_hex))
  }

  // (removed unused verify_sig_obj_against_msg shim; all callers use explicit hash)
}
