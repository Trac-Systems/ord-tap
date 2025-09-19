use super::super::super::*;
use std::collections::HashMap;
use std::io::Cursor;

// Batch-like overlay DB for TAP that buffers writes and provides
// read-your-writes semantics before flushing to redb.
pub(crate) struct TapBatch<'a, 'tx> {
  pub table: &'a mut Table<'tx, &'static [u8], &'static [u8]>,
  pub overlay: HashMap<Vec<u8>, Vec<u8>>,
}

impl<'a, 'tx> TapBatch<'a, 'tx> {
  pub fn new(table: &'a mut Table<'tx, &'static [u8], &'static [u8]>) -> Self {
    Self { table, overlay: HashMap::new() }
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

impl InscriptionUpdater<'_, '_> {
  pub(crate) fn tap_put<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)?;
    self.tap_db.put(key.as_bytes(), &buf);
    Ok(())
  }

  pub(crate) fn tap_get<T: serde::de::DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>> {
    if let Some(bytes) = self.tap_db.get(key.as_bytes())? {
      let val: T = ciborium::from_reader(Cursor::new(bytes))?;
      Ok(Some(val))
    } else {
      Ok(None)
    }
  }

  pub(crate) fn tap_del(&mut self, key: &str) -> Result {
    self.tap_db.del(key.as_bytes())
  }

  pub(crate) fn tap_set_list_record<T: serde::Serialize>(
    &mut self,
    length_key: &str,
    iterator_key: &str,
    data: &T,
  ) -> Result<usize> {
    let length: usize = if let Some(len) = self.list_len_cache.get_mut(length_key) {
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
}
