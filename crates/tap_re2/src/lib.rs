use libc::{c_char, c_int};
use std::ffi::CString;

extern "C" {
  fn re2_compile_ok(pattern: *const c_char) -> c_int;
}

pub fn is_re2_valid(pattern: &str) -> bool {
  if pattern.is_empty() {
    return true;
  }
  match CString::new(pattern) {
    Ok(cstr) => unsafe { re2_compile_ok(cstr.as_ptr()) != 0 },
    Err(_) => false,
  }
}

#[inline]
pub fn is_stub() -> bool {
  cfg!(tap_re2_stub)
}

#[inline]
pub fn backend_name() -> &'static str {
  if cfg!(tap_re2_vendored_20240601) {
    "vendored-re2-2024-06-01"
  } else if cfg!(tap_re2_system) {
    "system-re2"
  } else if cfg!(tap_re2_stub) {
    "stub"
  } else {
    "unknown-re2"
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn reports_exact_vendored_backend_by_default() {
    assert_eq!(backend_name(), "vendored-re2-2024-06-01");
    assert!(!is_stub());
  }

  #[test]
  fn dmt_pattern_acceptance_matches_pinned_re2_1_21_4_snapshot() {
    assert!(is_re2_valid("a.*b.*c"));
    assert!(is_re2_valid("\\C"));
    assert!(!is_re2_valid("(?<=a)b"));
    assert!(!is_re2_valid("(?=a)a"));
    assert!(!is_re2_valid("(a)\\1"));
    assert!(!is_re2_valid("[\\p{Letter}]"));
  }
}
