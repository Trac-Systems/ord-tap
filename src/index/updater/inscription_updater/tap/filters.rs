use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TapFilterSnapshot {
  pub v: u8,
  pub kind: String,
  pub m: u64,
  pub k: u8,
  pub covh: u32,
  pub bits: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct TapBloomFilter {
  pub m_bits: u64,
  pub k: u8,
  pub bits: Vec<u8>,
  pub coverage_height: u32,
  pub ready: bool,
  pub dirty: bool,
}

impl TapBloomFilter {
  pub fn new(m_bits: u64, k: u8) -> Self {
    let byte_len = ((m_bits + 7) / 8) as usize;
    Self { m_bits, k, bits: vec![0u8; byte_len], coverage_height: 0, ready: false, dirty: false }
  }

  fn idx_pair(digest: &[u8; 32]) -> (u64, u64) {
    let mut h1 = [0u8; 8];
    h1.copy_from_slice(&digest[0..8]);
    let mut h2 = [0u8; 8];
    h2.copy_from_slice(&digest[8..16]);
    (u64::from_be_bytes(h1), u64::from_be_bytes(h2))
  }

  fn bit_ops(&mut self, idx: u64, set: bool) -> bool {
    let byte_index = (idx / 8) as usize;
    let bit_in_byte = (idx % 8) as u8;
    if byte_index >= self.bits.len() {
      return false;
    }
    let mask = 1u8 << bit_in_byte;
    let prev = (self.bits[byte_index] & mask) != 0;
    if set {
      self.bits[byte_index] |= mask;
      if !prev {
        self.dirty = true;
      }
    }
    prev
  }

  pub fn insert_str(&mut self, key: &str) {
    let digest = Self::sha256(key);
    let (h1, h2) = Self::idx_pair(&digest);
    for i in 0..self.k as u64 {
      let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % self.m_bits;
      self.bit_ops(idx, true);
    }
  }

  pub fn contains_str(&self, key: &str) -> bool {
    let digest = Self::sha256(key);
    let (h1, h2) = Self::idx_pair(&digest);
    for i in 0..self.k as u64 {
      let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % self.m_bits;
      let byte_index = (idx / 8) as usize;
      if byte_index >= self.bits.len() {
        return false;
      }
      let bit_in_byte = (idx % 8) as u8;
      let mask = 1u8 << bit_in_byte;
      if (self.bits[byte_index] & mask) == 0 {
        return false;
      }
    }
    true
  }

  #[allow(dead_code)]
  pub fn mark_ready_at(&mut self, height: u32) {
    self.coverage_height = height;
    self.ready = true;
  }

  pub fn should_skip_negatives(&self, current_height: u32) -> bool {
    self.ready && current_height >= self.coverage_height
  }

  pub fn save_snapshot(&self, dir: &Path, kind: &str) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.bloom.cbor", kind));
    let tmp_path = dir.join(format!("{}.bloom.cbor.tmp", kind));
    let snap = TapFilterSnapshot { v: 1, kind: kind.to_string(), m: self.m_bits, k: self.k, covh: self.coverage_height, bits: self.bits.clone() };
    let mut buf = Vec::with_capacity(16 + self.bits.len());
    ciborium::ser::into_writer(&snap, &mut buf)
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("cbor serialize: {e}")))?;
    {
      let mut f = File::create(&tmp_path)?;
      f.write_all(&buf)?;
      f.flush()?;
      f.sync_all()?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
  }

  pub fn load_snapshot(dir: &Path, kind: &str) -> Option<Self> {
    let path = dir.join(format!("{}.bloom.cbor", kind));
    let mut f = File::open(&path).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    let snap: TapFilterSnapshot = ciborium::de::from_reader(std::io::Cursor::new(buf)).ok()?;
    if snap.v != 1 || snap.kind != kind {
      return None;
    }
    let filt = TapBloomFilter { m_bits: snap.m, k: snap.k, bits: snap.bits, coverage_height: snap.covh, ready: true, dirty: false };
    let expected = ((filt.m_bits + 7) / 8) as usize;
    if filt.bits.len() != expected {
      return None;
    }
    Some(filt)
  }

  fn sha256(s: &str) -> [u8; 32] {
    let mut h = sha2::Sha256::new();
    h.update(s.as_bytes());
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
  }
}
