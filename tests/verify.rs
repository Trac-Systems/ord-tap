use super::*;

const SEGWIT_ADDRESS: &str = "bc1q9vza2e8x573nczrlzms0wvx3gsqjx7vavgkx0l";
const NESTED_SEGWIT_ADDRESS: &str = "3HSVzEhCFuH9Z3wvoWTexy7BMVVp3PjS6f";
const WIF_PRIVATE_KEY: &str = "L3VFeEujGtevx9w18HD1fhRbCH67Az2dpCymeRE1SoPK6XQtaN2k";
const NESTED_SEGWIT_WIF_PRIVATE_KEY: &str = "KwTbAxmBXjoZM3bzbXixEr9nxLhyYSM4vp2swet58i19bw9sqk5z";

#[test]
fn verify() {
  assert_eq!(
    CommandBuilder::new([
      "verify", "--address", SEGWIT_ADDRESS, "--text", "Hello World", "--witness",
      "AkcwRAIgZRfIY3p7/DoVTty6YZbWS71bc5Vct9p9Fia83eRmw2QCICK/ENGfwLtptFluMGs2KsqoNSk89pO7F29zJLUx9a/sASECx/EgAxlkQpQ9hYjgGu6EBCPMVPwVIVJqO4XCsMvViHI="
    ])
    .run_and_extract_stdout(),
    ""
  );
}

#[test]
fn verify_fails() {
  CommandBuilder::new([
      "verify",
      "--address", SEGWIT_ADDRESS,
      "--text", "Hello World - this should fail",
      "--witness", "AkcwRAIgZRfIY3p7/DoVTty6YZbWS71bc5Vct9p9Fia83eRmw2QCICK/ENGfwLtptFluMGs2KsqoNSk89pO7F29zJLUx9a/sASECx/EgAxlkQpQ9hYjgGu6EBCPMVPwVIVJqO4XCsMvViHI="
  ])
  .expected_exit_code(1)
  .stderr_regex("error: Invalid signature.*")
  .run_and_extract_stdout();
}

#[test]
fn witness_and_transaction_conflict() {
  CommandBuilder::new([
      "verify",
      "--address", SEGWIT_ADDRESS,
      "--text", "Hello World",
      "--transaction", "asdf",
      "--witness", "AkcwRAIgZRfIY3p7/DoVTty6YZbWS71bc5Vct9p9Fia83eRmw2QCICK/ENGfwLtptFluMGs2KsqoNSk89pO7F29zJLUx9a/sASECx/EgAxlkQpQ9hYjgGu6EBCPMVPwVIVJqO4XCsMvViHI="
  ])
  .expected_exit_code(2)
  .stderr_regex(".*error.*")
  .run_and_extract_stdout();
}

#[test]
fn verify_with_transaction() {
  let tx = bip322::sign_full_encoded(SEGWIT_ADDRESS, "Hello World", WIF_PRIVATE_KEY).unwrap();

  assert_eq!(
    CommandBuilder::new([
      "verify",
      "--address",
      SEGWIT_ADDRESS,
      "--text",
      "Hello World",
      "--transaction",
      &tx,
    ])
    .run_and_extract_stdout(),
    ""
  );
}

#[test]
fn verify_rejects_p2wpkh_proof_signed_by_unrelated_key() {
  let witness =
    bip322::sign_simple_encoded(SEGWIT_ADDRESS, "Hello World", NESTED_SEGWIT_WIF_PRIVATE_KEY)
      .unwrap();

  CommandBuilder::new([
    "verify",
    "--address",
    SEGWIT_ADDRESS,
    "--text",
    "Hello World",
    "--witness",
    &witness,
  ])
  .expected_exit_code(1)
  .stderr_regex(".*BIP322 witness public key does not match address.*")
  .run_and_extract_stdout();
}

#[test]
fn verify_rejects_p2wpkh_full_proof_signed_by_unrelated_key() {
  let transaction =
    bip322::sign_full_encoded(SEGWIT_ADDRESS, "Hello World", NESTED_SEGWIT_WIF_PRIVATE_KEY)
      .unwrap();

  CommandBuilder::new([
    "verify",
    "--address",
    SEGWIT_ADDRESS,
    "--text",
    "Hello World",
    "--transaction",
    &transaction,
  ])
  .expected_exit_code(1)
  .stderr_regex(".*BIP322 witness public key does not match address.*")
  .run_and_extract_stdout();
}

#[test]
fn verify_rejects_p2sh_p2wpkh_proof_signed_by_unrelated_key() {
  let witness =
    bip322::sign_simple_encoded(NESTED_SEGWIT_ADDRESS, "Hello World", WIF_PRIVATE_KEY).unwrap();

  CommandBuilder::new([
    "verify",
    "--address",
    NESTED_SEGWIT_ADDRESS,
    "--text",
    "Hello World",
    "--witness",
    &witness,
  ])
  .expected_exit_code(1)
  .stderr_regex(".*BIP322 witness public key does not match address.*")
  .run_and_extract_stdout();
}
