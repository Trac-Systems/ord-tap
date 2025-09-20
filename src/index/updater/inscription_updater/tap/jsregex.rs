// JS/RE2 regex helpers to mirror tap-writer behavior
// - RE2 is used only as an acceptance gate for DMT element patterns
// - ECMAScript engine (regress) is used for matching at mint time

pub(crate) fn re2_accepts(pattern: &str) -> bool {
  // Compile with RE2; if it accepts, we accept the pattern (parity with tap-writer)
  tap_re2::is_re2_valid(pattern)
}

pub(crate) fn js_count_global_matches(pattern: &str, haystack: &str) -> Option<usize> {
  // Use ECMAScript semantics (similar to JS RegExp with /g)
  let re = regress::Regex::new(pattern).ok()?;
  // regress::Regex supports find_iter over &str
  let mut count = 0usize;
  let mut last_end = 0usize;
  for m in re.find_iter(haystack) {
    // Avoid infinite loops on zero-width matches by advancing one char
    let (s, e) = (m.start(), m.end());
    count += 1;
    if e == s {
      // advance one unicode scalar to mimic JS's lastIndex bump
      if let Some(next_idx) = haystack[last_end..].char_indices().nth(1).map(|(i, _)| last_end + i) {
        last_end = next_idx;
      } else {
        break;
      }
    } else {
      last_end = e;
    }
  }
  Some(count)
}
