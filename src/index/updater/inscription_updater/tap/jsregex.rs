// JS/RE2 regex helpers for protocol DMT handling.
// - RE2 is used only as an acceptance gate for DMT element patterns.
// - V8 is used for matching at DMT mint time via
//   `new RegExp(pattern, "g")` runtime semantics.

pub(crate) fn re2_accepts(pattern: &str) -> bool {
  // Compile with RE2; if it accepts, the DMT element pattern is accepted.
  tap_re2::is_re2_valid(pattern)
}

pub(crate) fn js_count_global_matches(pattern: &str, haystack: &str) -> Option<usize> {
  tap_node20_v8_regexp::js_global_match_count(pattern, haystack)
}

#[cfg(test)]
mod tests {
  use super::js_count_global_matches;

  #[test]
  fn js_global_match_counts_match_node20_truth_vectors() {
    let cases = [
      ("", "", Some(1)),
      ("", "1", Some(2)),
      ("", "123", Some(4)),
      ("a*", "", Some(1)),
      ("a*", "1", Some(2)),
      ("a*", "123", Some(4)),
      ("a*", "a", Some(2)),
      ("a?", "123", Some(4)),
      ("^", "123", Some(1)),
      ("$", "123", Some(1)),
      ("\\b", "", None),
      ("\\b", "1", Some(2)),
      ("\\b", "123", Some(2)),
      ("\\b", "abc", Some(2)),
      ("[0-9]", "123", Some(3)),
    ];

    for (pattern, haystack, expected) in cases {
      assert_eq!(
        js_count_global_matches(pattern, haystack),
        expected,
        "pattern={pattern:?} haystack={haystack:?}"
      );
    }
  }
}
