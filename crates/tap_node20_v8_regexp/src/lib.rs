use std::os::raw::{c_char, c_int, c_uchar};

const EXPECTED_V8_VERSION: &str = "11.3.244.8-node.25";

extern "C" {
  fn tap_node20_v8_version() -> *const c_char;
  fn tap_node20_v8_global_match_count(
    pattern: *const c_uchar,
    pattern_len: usize,
    haystack: *const c_uchar,
    haystack_len: usize,
    out_count: *mut usize,
  ) -> c_int;
}

/// Return the linked Node 20.10.0 V8 runtime version.
///
/// TAP DMT mint parity depends on this staying at Node.js 20.10.0's embedded
/// V8 version, not a nearby public V8 release.
pub fn embedded_v8_version() -> &'static str {
  unsafe {
    let version = tap_node20_v8_version();
    assert!(!version.is_null(), "tap_node20_v8_version returned null");
    std::ffi::CStr::from_ptr(version)
      .to_str()
      .expect("V8 version must be ASCII")
  }
}

/// Count matches exactly like tap-writer's DMT mint runtime path:
///
/// ```js
/// String(haystack).match(new RegExp(pattern, "g"))?.length
/// ```
///
/// `None` means either V8 rejected the pattern or `.match(...)` returned
/// `null`. Element registration remains gated separately by RE2.
pub fn js_global_match_count(pattern: &str, haystack: &str) -> Option<usize> {
  debug_assert_eq!(embedded_v8_version(), EXPECTED_V8_VERSION);

  let mut count = 0usize;
  let matched = unsafe {
    tap_node20_v8_global_match_count(
      pattern.as_ptr(),
      pattern.len(),
      haystack.as_ptr(),
      haystack.len(),
      &mut count,
    )
  };

  if matched == 1 {
    Some(count)
  } else {
    None
  }
}

#[cfg(test)]
mod tests {
  use super::{embedded_v8_version, js_global_match_count, EXPECTED_V8_VERSION};

  #[test]
  fn reports_exact_node20_v8_version() {
    assert_eq!(embedded_v8_version(), EXPECTED_V8_VERSION);
  }

  #[test]
  fn counts_zero_width_like_js_global_match() {
    assert_eq!(js_global_match_count("", ""), Some(1));
    assert_eq!(js_global_match_count("", "1"), Some(2));
    assert_eq!(js_global_match_count("", "123"), Some(4));
    assert_eq!(js_global_match_count("^", "123"), Some(1));
    assert_eq!(js_global_match_count("$", "123"), Some(1));
  }

  #[test]
  fn returns_none_for_no_match_or_v8_rejection() {
    assert_eq!(js_global_match_count("\\b", ""), None);
    assert_eq!(js_global_match_count("[", "123"), None);
    assert_eq!(js_global_match_count("(?i)a", "a"), None);
  }

  #[test]
  fn counts_numeric_matches() {
    assert_eq!(js_global_match_count("[0-9]", "123"), Some(3));
    assert_eq!(js_global_match_count("1|2", "123"), Some(2));
    assert_eq!(js_global_match_count("ff", "00ffff"), Some(2));
  }

  #[test]
  fn repeated_calls_are_deterministic() {
    for _ in 0..1024 {
      assert_eq!(js_global_match_count("[0-9]", "817798"), Some(6));
      assert_eq!(js_global_match_count("(?i)a", "a"), None);
    }
  }
}
