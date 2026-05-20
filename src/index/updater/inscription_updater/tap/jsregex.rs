// JS/RE2 regex helpers to mirror tap-writer behavior
// - RE2 is used only as an acceptance gate for DMT element patterns
// - ECMAScript engine (regress) is used for matching at mint time

pub(crate) fn re2_accepts(pattern: &str) -> bool {
  // Compile with RE2; if it accepts, we accept the pattern (parity with tap-writer)
  tap_re2::is_re2_valid(pattern)
}

fn advance_one_js_global_index(text: &str, index: usize) -> usize {
  if index >= text.len() {
    return text.len() + 1;
  }
  text[index..]
    .char_indices()
    .nth(1)
    .map(|(offset, _)| index + offset)
    .unwrap_or(text.len())
}

pub(crate) fn js_count_global_matches(pattern: &str, haystack: &str) -> Option<usize> {
  // Current TAP DMT call sites pass only decimal or hex ASCII strings
  // (`blk`, `nonce`, `bits`). Do not reuse this helper for arbitrary user text
  // without first proving ECMAScript UTF-16 code-unit boundary parity.
  debug_assert!(haystack.is_ascii());
  let re = regress::Regex::new(pattern).ok()?;
  let mut count = 0usize;
  let mut last_index = 0usize;
  while last_index <= haystack.len() {
    let Some(m) = re.find_from(haystack, last_index).next() else {
      break;
    };
    let (s, e) = (m.start(), m.end());
    count += 1;
    if e == s {
      last_index = advance_one_js_global_index(haystack, e);
    } else {
      last_index = e;
    }
  }
  if count == 0 {
    None
  } else {
    Some(count)
  }
}

#[cfg(test)]
mod tests {
  use super::js_count_global_matches;

  #[test]
  fn js_global_match_counts_match_tap_writer_truth_vectors() {
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
