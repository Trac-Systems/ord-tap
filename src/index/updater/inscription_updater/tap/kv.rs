use super::super::super::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};

// Batch-like overlay DB for TAP that buffers writes and provides
// read-your-writes semantics before flushing to redb.
pub(crate) struct TapBatch<'a, 'tx> {
  pub table: &'a mut Table<'tx, &'static [u8], &'static [u8]>,
  pub overlay: HashMap<Vec<u8>, Vec<u8>>,
}

impl<'a, 'tx> TapBatch<'a, 'tx> {
  pub fn new(table: &'a mut Table<'tx, &'static [u8], &'static [u8]>) -> Self {
    Self {
      table,
      overlay: HashMap::new(),
    }
  }

  pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    if let Some(val) = self.overlay.get(key) {
      return Ok(Some(val.clone()));
    }
    Ok(self.table.get(&key)?.map(|v| v.value().to_vec()))
  }

  pub fn put(&mut self, key: &[u8], value: &[u8]) {
    self.overlay.insert(key.to_vec(), value.to_vec());
  }

  pub fn flush(&mut self) -> Result {
    for (k, v) in self.overlay.drain() {
      self.table.insert(&k.as_slice(), v.as_slice())?;
    }
    Ok(())
  }

  pub fn del(&mut self, key: &[u8]) -> Result {
    self.overlay.remove(key);
    self.table.remove(key)?;
    Ok(())
  }
}

pub(crate) struct TapDeltaBatch<'a, 'tx> {
  delta_dir: PathBuf,
  writer: Option<BufWriter<std::fs::File>>,
  block_state_table: Option<&'a mut Table<'tx, &'static [u8], &'static [u8]>>,
  height: u32,
  sequence: u64,
  rolling_state: Option<crate::index::TapExportRollingState>,
}

impl<'a, 'tx> TapDeltaBatch<'a, 'tx> {
  pub fn new(delta_dir: PathBuf, height: u32) -> Result<Self> {
    Ok(Self {
      delta_dir,
      writer: None,
      block_state_table: None,
      height,
      sequence: 0,
      rolling_state: None,
    })
  }

  pub fn with_rolling_state(
    delta_dir: PathBuf,
    block_state_table: &'a mut Table<'tx, &'static [u8], &'static [u8]>,
    height: u32,
    rolling_state: crate::index::TapExportRollingState,
  ) -> Result<Self> {
    Ok(Self {
      delta_dir,
      writer: None,
      block_state_table: Some(block_state_table),
      height,
      sequence: 0,
      rolling_state: Some(rolling_state),
    })
  }

  pub fn needs_old_value(&self) -> bool {
    self.rolling_state.is_some()
  }

  pub fn delta_file_path(delta_dir: &Path, height: u32) -> PathBuf {
    delta_dir.join(format!("{height:010}.jsonl"))
  }

  pub fn delta_file_height(path: &Path) -> Option<u32> {
    let file_name = path.file_name()?.to_str()?;
    let height = file_name.strip_suffix(".jsonl")?;
    if height.len() != 10 || !height.bytes().all(|byte| byte.is_ascii_digit()) {
      return None;
    }
    height.parse().ok()
  }

  pub fn delete_files_from_height(delta_dir: &Path, height: u32) -> Result {
    let Ok(entries) = std::fs::read_dir(delta_dir) else {
      return Ok(());
    };
    for entry in entries {
      let path = entry?.path();
      if Self::delta_file_height(&path).is_some_and(|delta_height| delta_height >= height) {
        std::fs::remove_file(path)?;
      }
    }
    Ok(())
  }

  pub fn delete_block_states_from_height(
    table: &mut Table<'tx, &'static [u8], &'static [u8]>,
    height: u32,
  ) -> Result {
    let start_key = format!("{height:010}");
    let mut keys = Vec::new();
    for result in table.range(start_key.as_bytes()..)? {
      let (key, _) = result?;
      keys.push(key.value().to_vec());
    }
    for key in keys {
      table.remove(key.as_slice())?;
    }
    Ok(())
  }

  fn record(&mut self, op: &str, key: &str, value: Option<String>) -> Result {
    let sequence = self.sequence;
    self.sequence += 1;
    let hash_payload = serde_json::to_vec(&serde_json::json!([
      self.height,
      sequence,
      op,
      key,
      value.as_deref()
    ]))?;
    let row_hash = hex::encode(Sha256::digest(&hash_payload));
    let row = crate::index::TapExportDeltaRecord {
      height: self.height,
      sequence,
      op: op.to_string(),
      key: key.to_string(),
      value,
      row_hash,
      block_hash: None,
      parent_block_hash: None,
    };
    if self.writer.is_none() {
      std::fs::create_dir_all(&self.delta_dir)?;
      self.writer = Some(BufWriter::new(
        std::fs::OpenOptions::new()
          .create(true)
          .write(true)
          .truncate(true)
          .open(Self::delta_file_path(&self.delta_dir, self.height))?,
      ));
    }
    let writer = self
      .writer
      .as_mut()
      .ok_or_else(|| anyhow!("TAP export delta writer missing"))?;
    serde_json::to_writer(&mut *writer, &row)?;
    writer.write_all(b"\n")?;
    Ok(())
  }

  pub fn put(
    &mut self,
    key: &str,
    old_encoded_value: Option<&[u8]>,
    encoded_value: &[u8],
  ) -> Result {
    let value = crate::index::Index::tap_export_value_string(encoded_value)
      .ok_or_else(|| anyhow!("failed to decode TAP export value for key `{key}`"))?;
    if let Some(rolling_state) = &mut self.rolling_state {
      let old_value = old_encoded_value
        .map(|bytes| {
          crate::index::Index::tap_export_value_string(bytes)
            .ok_or_else(|| anyhow!("failed to decode old TAP export value for key `{key}`"))
        })
        .transpose()?;
      crate::index::Index::tap_export_rolling_state_apply(
        rolling_state,
        key,
        old_value.as_deref(),
        Some(&value),
      )?;
    }
    self.record("put", key, Some(value))
  }

  pub fn del(&mut self, key: &str, old_encoded_value: Option<&[u8]>) -> Result {
    if let Some(rolling_state) = &mut self.rolling_state {
      let old_value = old_encoded_value
        .map(|bytes| {
          crate::index::Index::tap_export_value_string(bytes)
            .ok_or_else(|| anyhow!("failed to decode old TAP export value for key `{key}`"))
        })
        .transpose()?;
      crate::index::Index::tap_export_rolling_state_apply(
        rolling_state,
        key,
        old_value.as_deref(),
        None,
      )?;
    }
    self.record("del", key, None)
  }

  pub fn finalize_block(&mut self) -> Result<Option<crate::index::TapExportRollingState>> {
    if let Some(writer) = self.writer.as_mut() {
      writer.flush()?;
    }
    let Some(rolling_state) = &self.rolling_state else {
      return Ok(None);
    };
    let block_state_table = self
      .block_state_table
      .as_mut()
      .ok_or_else(|| anyhow!("TAP export rolling block state table missing"))?;
    let block_state = crate::index::TapExportBlockState {
      height: self.height,
      rolling_state_row_count: rolling_state.row_count,
      rolling_state_digest: rolling_state.state_digest.clone(),
    };
    let key = format!("{:010}", self.height);
    let value = serde_json::to_vec(&block_state)?;
    block_state_table.insert(key.as_bytes(), value.as_slice())?;
    Ok(Some(rolling_state.clone()))
  }
}

impl InscriptionUpdater<'_, '_> {
  fn tap_encode_value<T: serde::Serialize>(value: &T) -> Result<(Vec<u8>, serde_json::Value)> {
    let mut buf = Vec::new();
    let json_value = serde_json::to_value(value)?;
    match &json_value {
      serde_json::Value::String(s) if Self::js_string_contains_internal_marker(s) => {
        buf.extend_from_slice(&Self::js_string_node_utf8_bytes(s));
      }
      _ if Self::js_value_contains_internal_marker(&json_value) => {
        buf.extend_from_slice(Self::js_json_stringify(&json_value).as_bytes());
      }
      _ => {
        ciborium::into_writer(value, &mut buf)?;
      }
    }
    Ok((buf, json_value))
  }

  fn tap_decode_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    match ciborium::from_reader(Cursor::new(bytes)) {
      Ok(value) => Ok(value),
      Err(_) => {
        let raw = std::str::from_utf8(bytes)?;
        let compat = Self::preprocess_js_json_for_serde(raw);
        Ok(
          serde_json::from_str(&compat)
            .or_else(|_| serde_json::from_value(serde_json::Value::String(raw.to_string())))?,
        )
      }
    }
  }

  fn tap_atomic_store(
    &mut self,
    key: &str,
    value: Option<(Vec<u8>, serde_json::Value)>,
  ) -> bool {
    let Some(overlay) = self.tap_atomic_overlay.as_mut() else {
      return false;
    };
    if !overlay.contains_key(key) {
      self
        .tap_atomic_writes
        .as_mut()
        .expect("TAP atomic write order missing")
        .push(key.to_string());
    }
    overlay.insert(key.to_string(), value);
    true
  }

  fn tap_put_encoded_committed(
    &mut self,
    key: &str,
    buf: &[u8],
    json_value: &serde_json::Value,
  ) -> Result {
    let old_value = if self
      .tap_delta_db
      .as_ref()
      .is_some_and(TapDeltaBatch::needs_old_value)
    {
      self.tap_db.get(key.as_bytes())?
    } else {
      None
    };
    self.tap_db.put(key.as_bytes(), buf);
    if let Some(delta_db) = &mut self.tap_delta_db {
      delta_db.put(key, old_value.as_deref(), buf)?;
    }
    if let Some(route_index) = &self.tap_route_index {
      route_index.borrow_mut().observe_put(key, json_value);
    }
    Ok(())
  }

  fn tap_del_committed(&mut self, key: &str) -> Result {
    if let Some(route_index) = &self.tap_route_index {
      route_index.borrow_mut().observe_del(key);
    }
    let old_value = if self
      .tap_delta_db
      .as_ref()
      .is_some_and(TapDeltaBatch::needs_old_value)
    {
      self.tap_db.get(key.as_bytes())?
    } else {
      None
    };
    if let Some(delta_db) = &mut self.tap_delta_db {
      delta_db.del(key, old_value.as_deref())?;
    }
    self.tap_db.del(key.as_bytes())
  }

  pub(crate) fn tap_atomic_begin(&mut self) {
    assert!(self.tap_atomic_overlay.is_none(), "nested TAP atomic batch");
    self.tap_atomic_writes = Some(Vec::new());
    self.tap_atomic_overlay = Some(HashMap::new());
    self.tap_atomic_list_len_cache = Some(HashMap::new());
  }

  pub(crate) fn tap_atomic_abort(&mut self) {
    self.tap_atomic_writes = None;
    self.tap_atomic_overlay = None;
    self.tap_atomic_list_len_cache = None;
  }

  pub(crate) fn tap_atomic_commit(&mut self) -> Result {
    let writes = self.tap_atomic_writes.take().unwrap_or_default();
    let mut overlay = self.tap_atomic_overlay.take().unwrap_or_default();
    let list_cache = self.tap_atomic_list_len_cache.take().unwrap_or_default();
    for key in writes {
      let Some(value) = overlay.remove(&key) else {
        continue;
      };
      match value {
        Some((buf, json_value)) => self.tap_put_encoded_committed(&key, &buf, &json_value)?,
        None => self.tap_del_committed(&key)?,
      }
    }
    for (key, value) in list_cache {
      self.list_len_cache.insert(key, value);
    }
    Ok(())
  }

  pub(crate) fn tap_put<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result {
    let (buf, json_value) = Self::tap_encode_value(value)?;
    if self.tap_atomic_overlay.is_some() {
      self.tap_atomic_store(key, Some((buf, json_value)));
      return Ok(());
    }
    self.tap_put_encoded_committed(key, &buf, &json_value)
  }

  pub(crate) fn tap_put_json_object_row(&mut self, key: &str, value: &serde_json::Value) -> Result {
    let buf = serde_json::to_vec(value)?;
    if self.tap_atomic_overlay.is_some() {
      self.tap_atomic_store(key, Some((buf, value.clone())));
      return Ok(());
    }
    self.tap_put_encoded_committed(key, &buf, value)
  }

  pub(crate) fn tap_get<T: serde::de::DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>> {
    if let Some(overlay) = &self.tap_atomic_overlay {
      if let Some(value) = overlay.get(key) {
        return match value {
          Some((bytes, _)) => Ok(Some(Self::tap_decode_value(bytes)?)),
          None => Ok(None),
        };
      }
    }
    if let Some(bytes) = self.tap_db.get(key.as_bytes())? {
      Ok(Some(Self::tap_decode_value(&bytes)?))
    } else {
      Ok(None)
    }
  }

  pub(crate) fn tap_del(&mut self, key: &str) -> Result {
    if self.tap_atomic_store(key, None) {
      return Ok(());
    }
    self.tap_del_committed(key)
  }

  pub(crate) fn tap_set_list_record<T: serde::Serialize>(
    &mut self,
    length_key: &str,
    iterator_key: &str,
    data: &T,
  ) -> Result<usize> {
    let length: usize = if self.tap_atomic_list_len_cache.is_some() {
      if let Some(len) = self
        .tap_atomic_list_len_cache
        .as_mut()
        .expect("TAP atomic list cache missing")
        .get_mut(length_key)
      {
        *len += 1;
        *len
      } else {
        let length = match self.tap_get::<String>(length_key)? {
          Some(s) => s.parse::<usize>().unwrap_or(0) + 1,
          None => 1,
        };
        self
          .tap_atomic_list_len_cache
          .as_mut()
          .expect("TAP atomic list cache missing")
          .insert(length_key.to_string(), length);
        length
      }
    } else if let Some(len) = self.list_len_cache.get_mut(length_key) {
      *len += 1;
      *len
    } else {
      let length = match self.tap_get::<String>(length_key)? {
        Some(s) => s.parse::<usize>().unwrap_or(0) + 1,
        None => 1,
      };
      self.list_len_cache.insert(length_key.to_string(), length);
      length
    };
    self.tap_put(length_key, &length.to_string())?;
    self.tap_put(&format!("{}/{}", iterator_key, length - 1), data)?;
    Ok(length)
  }

  pub(crate) fn tap_set_list_record_json_object_row(
    &mut self,
    length_key: &str,
    iterator_key: &str,
    data: &serde_json::Value,
  ) -> Result<usize> {
    let length: usize = if self.tap_atomic_list_len_cache.is_some() {
      if let Some(len) = self
        .tap_atomic_list_len_cache
        .as_mut()
        .expect("TAP atomic list cache missing")
        .get_mut(length_key)
      {
        *len += 1;
        *len
      } else {
        let length = match self.tap_get::<String>(length_key)? {
          Some(s) => s.parse::<usize>().unwrap_or(0) + 1,
          None => 1,
        };
        self
          .tap_atomic_list_len_cache
          .as_mut()
          .expect("TAP atomic list cache missing")
          .insert(length_key.to_string(), length);
        length
      }
    } else if let Some(len) = self.list_len_cache.get_mut(length_key) {
      *len += 1;
      *len
    } else {
      let length = match self.tap_get::<String>(length_key)? {
        Some(s) => s.parse::<usize>().unwrap_or(0) + 1,
        None => 1,
      };
      self.list_len_cache.insert(length_key.to_string(), length);
      length
    };
    self.tap_put(length_key, &length.to_string())?;
    self.tap_put_json_object_row(&format!("{}/{}", iterator_key, length - 1), data)?;
    Ok(length)
  }
}

#[cfg(test)]
mod tests {
  use std::path::{Path, PathBuf};

  fn rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
      return;
    };

    for entry in entries.flatten() {
      let path = entry.path();
      if path.is_dir() {
        rust_sources(&path, out);
      } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
        out.push(path);
      }
    }
  }

  #[test]
  fn tap_kv_mutations_are_delta_aware() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
      .join("src")
      .join("index")
      .join("updater")
      .join("inscription_updater");
    let allowed = root.join("tap").join("kv.rs");
    let mut files = Vec::new();
    rust_sources(&root, &mut files);

    let mut offenders = Vec::new();
    for path in files {
      if path == allowed {
        continue;
      }
      let Ok(source) = std::fs::read_to_string(&path) else {
        continue;
      };
      for (line_idx, line) in source.lines().enumerate() {
        if line.contains("tap_db.put(") || line.contains("tap_db.del(") {
          offenders.push(format!(
            "{}:{}: {}",
            path
              .strip_prefix(env!("CARGO_MANIFEST_DIR"))
              .unwrap_or(&path)
              .display(),
            line_idx + 1,
            line.trim()
          ));
        }
      }
    }

    assert!(
      offenders.is_empty(),
      "TAP_KV writes must go through tap_put/tap_del so export deltas, route index, and storage shape stay synchronized:\n{}",
      offenders.join("\n")
    );
  }

  #[test]
  fn tap_export_delta_cleanup_is_block_scoped() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(super::TapDeltaBatch::delta_file_path(dir.path(), 1), b"a").unwrap();
    std::fs::write(super::TapDeltaBatch::delta_file_path(dir.path(), 2), b"b").unwrap();
    std::fs::write(super::TapDeltaBatch::delta_file_path(dir.path(), 3), b"c").unwrap();

    super::TapDeltaBatch::delete_files_from_height(dir.path(), 2).unwrap();

    assert!(super::TapDeltaBatch::delta_file_path(dir.path(), 1).exists());
    assert!(!super::TapDeltaBatch::delta_file_path(dir.path(), 2).exists());
    assert!(!super::TapDeltaBatch::delta_file_path(dir.path(), 3).exists());
  }
}
