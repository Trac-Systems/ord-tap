use {
  super::*,
  base64::{engine::general_purpose, Engine},
};

#[derive(Debug, Parser)]
#[clap(
group(
  ArgGroup::new("input")
    .required(true)
    .args(&["text", "file"])),
group(
  ArgGroup::new("signature")
    .required(true)
    .args(&["transaction", "witness"]))
)]
pub(crate) struct Verify {
  #[arg(long, help = "Verify signature made by <ADDRESS>.")]
  address: Address<NetworkUnchecked>,
  #[arg(long, help = "Verify signature over <TEXT>.")]
  text: Option<String>,
  #[arg(long, help = "Verify signature over contents of <FILE>.")]
  file: Option<PathBuf>,
  #[arg(long, help = "Verify base64-encoded <WITNESS>.")]
  witness: Option<String>,
  #[arg(long, help = "Verify base64-encoded <TRANSACTION>.")]
  transaction: Option<String>,
}

impl Verify {
  pub(crate) fn run(self) -> SubcommandResult {
    let message = if let Some(text) = &self.text {
      text.as_bytes()
    } else if let Some(file) = &self.file {
      &fs::read(file)?
    } else {
      unreachable!()
    };

    let address = self.address.assume_checked();

    if let Some(witness) = self.witness {
      let mut cursor = bitcoin::io::Cursor::new(general_purpose::STANDARD.decode(witness)?);
      let witness = Witness::consensus_decode_from_finite_reader(&mut cursor)?;
      verify_witness_pubkey_matches_address(&address, &witness)?;
      bip322::verify_simple(&address, message, witness)?;
    } else if let Some(transaction) = self.transaction {
      let mut cursor = bitcoin::io::Cursor::new(general_purpose::STANDARD.decode(transaction)?);
      let transaction = Transaction::consensus_decode_from_finite_reader(&mut cursor)?;
      let witness = &transaction
        .input
        .first()
        .ok_or_else(|| anyhow!("BIP322 transaction has no inputs"))?
        .witness;
      verify_witness_pubkey_matches_address(&address, witness)?;
      bip322::verify_full(&address, message, transaction)?;
    } else {
      unreachable!();
    }

    Ok(None)
  }
}

fn verify_witness_pubkey_matches_address(address: &Address, witness: &Witness) -> Result {
  let address_script = address.script_pubkey();

  if !address_script.is_p2wpkh() && !address_script.is_p2sh() {
    return Ok(());
  }

  if witness.len() != 2 {
    return Err(anyhow!(
      "BIP322 witness must contain signature and public key"
    ));
  }

  let witness_items = witness.to_vec();
  let public_key = bitcoin::PublicKey::from_slice(&witness_items[1])
    .map_err(|_| anyhow!("BIP322 witness public key is invalid"))?;
  let wpubkey_hash = public_key
    .wpubkey_hash()
    .map_err(|_| anyhow!("BIP322 witness public key must be compressed"))?;
  let p2wpkh_script = ScriptBuf::new_p2wpkh(&wpubkey_hash);
  let expected_script = if address_script.is_p2sh() {
    p2wpkh_script.to_p2sh()
  } else {
    p2wpkh_script
  };

  if address_script != expected_script {
    return Err(anyhow!("BIP322 witness public key does not match address"));
  }

  Ok(())
}
