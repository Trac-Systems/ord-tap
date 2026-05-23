use super::super::*;
use super::ops::dmt_mint::DmtMintMetaRecord;
use crate::index::entry::InscriptionIdValue;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TapRoute {
  Bitmap { block: Option<u64> },
  DmtMint,
  Privilege,
  TransferLink,
  Accumulator,
}

#[derive(Clone, Debug)]
pub(crate) struct BitmapHotEntry {
  pub(crate) block: u64,
  pub(crate) current_owner: String,
}

#[derive(Clone, Debug)]
pub(crate) struct DmtHotEntry {
  pub(crate) meta: DmtMintMetaRecord,
  pub(crate) current_owner: String,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct TapRouteRebuildStats {
  pub(crate) bitmap: u64,
  pub(crate) dmt: u64,
  pub(crate) transfer_link: u64,
  pub(crate) privilege: u64,
  pub(crate) accumulator: u64,
  pub(crate) kind_hint: u64,
}

struct Stamped<T> {
  stamp: u64,
  value: T,
}

pub(crate) struct TapRouteIndex {
  routes: HashMap<InscriptionIdValue, TapRoute>,
  bitmap_hot: HashMap<InscriptionIdValue, Stamped<BitmapHotEntry>>,
  dmt_hot: HashMap<InscriptionIdValue, Stamped<DmtHotEntry>>,
  bitmap_lru: VecDeque<(InscriptionIdValue, u64)>,
  dmt_lru: VecDeque<(InscriptionIdValue, u64)>,
  clock: u64,
  max_hot_entries: usize,
  ready: bool,
  coverage_height: u32,
}

impl TapRouteIndex {
  pub(crate) fn new(max_hot_entries: usize) -> Self {
    Self {
      routes: HashMap::new(),
      bitmap_hot: HashMap::new(),
      dmt_hot: HashMap::new(),
      bitmap_lru: VecDeque::new(),
      dmt_lru: VecDeque::new(),
      clock: 0,
      max_hot_entries,
      ready: false,
      coverage_height: 0,
    }
  }

  pub(crate) fn is_ready(&self) -> bool {
    self.ready
  }

  pub(crate) fn coverage_height(&self) -> u32 {
    self.coverage_height
  }

  pub(crate) fn len(&self) -> usize {
    self.routes.len()
  }

  pub(crate) fn clear_for_rebuild(&mut self) {
    self.routes.clear();
    self.bitmap_hot.clear();
    self.dmt_hot.clear();
    self.bitmap_lru.clear();
    self.dmt_lru.clear();
    self.ready = false;
  }

  pub(crate) fn mark_ready(&mut self, coverage_height: u32) {
    self.coverage_height = coverage_height;
    self.ready = true;
  }

  pub(crate) fn route_for(&self, inscription_id: InscriptionId) -> Option<TapRoute> {
    if !self.ready {
      return None;
    }
    self.routes.get(&inscription_id.store()).cloned()
  }

  pub(crate) fn insert_route(&mut self, inscription_id: InscriptionId, route: TapRoute) {
    self.insert_route_key(inscription_id.store(), route);
  }

  fn insert_route_key(&mut self, key: InscriptionIdValue, route: TapRoute) {
    if let Some(existing) = self.routes.get_mut(&key) {
      match route {
        TapRoute::Accumulator => {}
        TapRoute::Bitmap { block: new_block } => match existing {
          TapRoute::Accumulator => {
            *existing = TapRoute::Bitmap { block: new_block };
          }
          TapRoute::Bitmap { block } => {
            if block.is_none() {
              *block = new_block;
            }
          }
          _ => {
            *existing = TapRoute::Bitmap { block: new_block };
          }
        },
        new_route => {
          if matches!(existing, TapRoute::Accumulator) || *existing != new_route {
            *existing = new_route;
          }
        }
      }
    } else {
      self.routes.insert(key, route);
    }
  }

  pub(crate) fn remove_route_if(&mut self, inscription_id: InscriptionId, route: TapRoute) {
    let key = inscription_id.store();
    if self.routes.get(&key) == Some(&route) {
      self.routes.remove(&key);
      self.bitmap_hot.remove(&key);
      self.dmt_hot.remove(&key);
    }
  }

  pub(crate) fn observe_put(&mut self, key: &str, value: &serde_json::Value) {
    if let Some(id_str) = key.strip_prefix("kind/") {
      let Some(id) = Self::parse_inscription_id(id_str) else {
        return;
      };
      let Some(kind) = value.as_str() else {
        return;
      };
      match kind {
        "bm" => self.insert_route(id, TapRoute::Bitmap { block: None }),
        "dmtmh" => self.insert_route(id, TapRoute::DmtMint),
        "prvins" => self.insert_route(id, TapRoute::Privilege),
        "tl" => self.insert_route(id, TapRoute::TransferLink),
        _ => {}
      }
      return;
    }

    if let Some(id_str) = key.strip_prefix("bmh/") {
      let Some(id) = Self::parse_inscription_id(id_str) else {
        return;
      };
      let block = value.as_str().and_then(Self::bitmap_block_from_mapping);
      self.insert_route(id, TapRoute::Bitmap { block });
      return;
    }

    if let Some(id_str) = key
      .strip_prefix("dmtmhm/")
      .or_else(|| key.strip_prefix("dmtmho/"))
    {
      if let Some(id) = Self::parse_inscription_id(id_str) {
        self.insert_route(id, TapRoute::DmtMint);
      }
      return;
    }

    if let Some(id_str) = key.strip_prefix("prvins/") {
      if let Some(id) = Self::parse_inscription_id(id_str) {
        self.insert_route(id, TapRoute::Privilege);
      }
      return;
    }

    if let Some(id_str) = key.strip_prefix("tl/") {
      let Some(id) = Self::parse_inscription_id(id_str) else {
        return;
      };
      if value.as_str().is_some_and(|s| !s.is_empty()) {
        self.insert_route(id, TapRoute::TransferLink);
      } else {
        self.remove_route_if(id, TapRoute::TransferLink);
      }
      return;
    }

    if let Some(id_str) = key.strip_prefix("a/") {
      if let Some(id) = Self::parse_inscription_id(id_str) {
        self.insert_route(id, TapRoute::Accumulator);
      }
    }
  }

  pub(crate) fn observe_del(&mut self, key: &str) {
    if let Some(id_str) = key.strip_prefix("a/") {
      if let Some(id) = Self::parse_inscription_id(id_str) {
        self.remove_route_if(id, TapRoute::Accumulator);
      }
      return;
    }

    if let Some(id_str) = key.strip_prefix("tl/") {
      if let Some(id) = Self::parse_inscription_id(id_str) {
        self.remove_route_if(id, TapRoute::TransferLink);
      }
    }
  }

  pub(crate) fn put_bitmap_hot(
    &mut self,
    inscription_id: InscriptionId,
    block: u64,
    current_owner: String,
  ) {
    let key = inscription_id.store();
    self.clock = self.clock.saturating_add(1);
    let stamp = self.clock;
    self.bitmap_hot.insert(
      key,
      Stamped {
        stamp,
        value: BitmapHotEntry {
          block,
          current_owner,
        },
      },
    );
    self.bitmap_lru.push_back((key, stamp));
    self.evict_bitmap_hot();
  }

  pub(crate) fn bitmap_hot(&mut self, inscription_id: InscriptionId) -> Option<BitmapHotEntry> {
    let key = inscription_id.store();
    let value = self.bitmap_hot.get(&key).map(|entry| entry.value.clone())?;
    self.clock = self.clock.saturating_add(1);
    let stamp = self.clock;
    if let Some(entry) = self.bitmap_hot.get_mut(&key) {
      entry.stamp = stamp;
    }
    self.bitmap_lru.push_back((key, stamp));
    Some(value)
  }

  pub(crate) fn put_dmt_hot(
    &mut self,
    inscription_id: InscriptionId,
    meta: DmtMintMetaRecord,
    current_owner: String,
  ) {
    let key = inscription_id.store();
    self.clock = self.clock.saturating_add(1);
    let stamp = self.clock;
    self.dmt_hot.insert(
      key,
      Stamped {
        stamp,
        value: DmtHotEntry {
          meta,
          current_owner,
        },
      },
    );
    self.dmt_lru.push_back((key, stamp));
    self.evict_dmt_hot();
  }

  pub(crate) fn dmt_hot(&mut self, inscription_id: InscriptionId) -> Option<DmtHotEntry> {
    let key = inscription_id.store();
    let value = self.dmt_hot.get(&key).map(|entry| entry.value.clone())?;
    self.clock = self.clock.saturating_add(1);
    let stamp = self.clock;
    if let Some(entry) = self.dmt_hot.get_mut(&key) {
      entry.stamp = stamp;
    }
    self.dmt_lru.push_back((key, stamp));
    Some(value)
  }

  fn evict_bitmap_hot(&mut self) {
    while self.bitmap_hot.len() > self.max_hot_entries {
      let Some((key, stamp)) = self.bitmap_lru.pop_front() else {
        break;
      };
      if self
        .bitmap_hot
        .get(&key)
        .is_some_and(|entry| entry.stamp == stamp)
      {
        self.bitmap_hot.remove(&key);
      }
    }
  }

  fn evict_dmt_hot(&mut self) {
    while self.dmt_hot.len() > self.max_hot_entries {
      let Some((key, stamp)) = self.dmt_lru.pop_front() else {
        break;
      };
      if self
        .dmt_hot
        .get(&key)
        .is_some_and(|entry| entry.stamp == stamp)
      {
        self.dmt_hot.remove(&key);
      }
    }
  }

  pub(crate) fn parse_inscription_id(s: &str) -> Option<InscriptionId> {
    InscriptionId::from_str(s).ok()
  }

  pub(crate) fn bitmap_block_from_mapping(s: &str) -> Option<u64> {
    let mut parts = s.split('/');
    if parts.next() != Some("bm") {
      return None;
    }
    parts.next()?.parse::<u64>().ok()
  }
}
