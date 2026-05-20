use pretty_assertions::assert_eq;
use serde_json::Value;

#[test]
fn v8_matcher_matches_node20_oracle_for_dmt_haystacks() {
  let fixture: Value =
    serde_json::from_str(include_str!("node20-re2-1214-oracle.json")).expect("valid oracle");

  assert_eq!(fixture["node"].as_str(), Some("20.10.0"));
  assert_eq!(fixture["v8"].as_str(), Some("11.3.244.8-node.25"));
  assert_eq!(fixture["re2"].as_str(), Some("1.21.4"));

  let rows = fixture["rows"].as_array().expect("rows array");
  assert!(rows.iter().any(|row| row["re2Accepts"] == true && row["v8Accepts"] == false));
  assert!(rows.iter().any(|row| row["re2Accepts"] == false && row["v8Accepts"] == true));

  for row in rows {
    let pattern = row["pattern"].as_str().expect("pattern string");
    let haystack = row["haystack"].as_str().expect("haystack string");
    let expected = if row["v8Accepts"] == true && row["matchResultIsNull"] == false {
      row["count"].as_u64().map(|value| value as usize)
    } else {
      None
    };

    assert_eq!(
      tap_node20_v8_regexp::js_global_match_count(pattern, haystack),
      expected,
      "pattern={pattern:?} haystack={haystack:?}"
    );
  }
}
