const REQUIRED_NODE_VERSION = '20.10.0';
if (process.versions.node !== REQUIRED_NODE_VERSION) {
  console.error(`tap-writer must run on Node.js ${REQUIRED_NODE_VERSION}; current runtime is ${process.version}.`);
  process.exit(1);
}

const ACTIVATION_HEIGHT = 954543;

const tokenTradeEmergencyActive = (type, network, block) =>
  type === 0 && network === 'main' && block >= ACTIVATION_HEIGHT;

const runSameTickerTrade = (block) => {
  const balances = new Map([
    ['seller', 1000n],
    ['buyer', 1000n],
  ]);

  if (tokenTradeEmergencyActive(0, 'main', block)) {
    return balances;
  }

  const offerAmount = 1n;
  const acceptedAmount = 100n;
  const sellerBalanceOffered = balances.get('seller') ?? 0n;
  const buyerBalanceOffered = balances.get('buyer') ?? 0n;
  const sellerBalanceAccepted = balances.get('seller') ?? 0n;
  const buyerBalanceAccepted = balances.get('buyer') ?? 0n;

  if (sellerBalanceOffered - offerAmount < 0n || buyerBalanceAccepted - acceptedAmount < 0n) {
    return balances;
  }

  // Same key writes as tap-writer: offered-leg writes are overwritten by accepted-leg writes.
  balances.set('buyer', buyerBalanceOffered + offerAmount);
  balances.set('seller', sellerBalanceOffered - offerAmount);
  balances.set('seller', sellerBalanceAccepted + acceptedAmount);
  balances.set('buyer', buyerBalanceAccepted - acceptedAmount);

  return balances;
};

const pre = runSameTickerTrade(ACTIVATION_HEIGHT - 1);
if (pre.get('seller') !== 1100n || pre.get('buyer') !== 900n) {
  throw new Error(`pre-emergency same-ticker trade mismatch: seller=${pre.get('seller')} buyer=${pre.get('buyer')}`);
}

const post = runSameTickerTrade(ACTIVATION_HEIGHT);
if (post.get('seller') !== 1000n || post.get('buyer') !== 1000n) {
  throw new Error(`post-emergency same-ticker trade mismatch: seller=${post.get('seller')} buyer=${post.get('buyer')}`);
}

if (tokenTradeEmergencyActive(0, 'test', ACTIVATION_HEIGHT)) {
  throw new Error('testnet token-trade should not be disabled by the mainnet emergency gate');
}

const corrected = new Map([
  ['exploit:b', 999n],
  ['exploit:t', 888n],
  ['tamt/7c59b1a556f2b072685af397aeee2a1ac6d5fa994833d7009726d48bc1ccf548i0', 5000000000000000000000n],
  ['tl/7c59b1a556f2b072685af397aeee2a1ac6d5fa994833d7009726d48bc1ccf548i0', 'atrli/exploit/"tap"/0'],
]);
let correctionApplied = false;
const applyEmergencyCorrection = (state) => {
  if (correctionApplied) {
    return;
  }
  state.set('exploit:b', 0n);
  state.set('exploit:t', 0n);
  state.set('tamt/7c59b1a556f2b072685af397aeee2a1ac6d5fa994833d7009726d48bc1ccf548i0', 0n);
  state.set('tl/7c59b1a556f2b072685af397aeee2a1ac6d5fa994833d7009726d48bc1ccf548i0', '');
  correctionApplied = true;
};

applyEmergencyCorrection(corrected);
if (corrected.get('exploit:b') !== 0n || corrected.get('exploit:t') !== 0n) {
  throw new Error('one-time exploiter correction did not clear balance and transferable');
}
if (
  corrected.get('tamt/7c59b1a556f2b072685af397aeee2a1ac6d5fa994833d7009726d48bc1ccf548i0') !== 0n ||
  corrected.get('tl/7c59b1a556f2b072685af397aeee2a1ac6d5fa994833d7009726d48bc1ccf548i0') !== ''
) {
  throw new Error('one-time exploiter correction did not clear live transferable inscription');
}

corrected.set('exploit:b', 7n);
corrected.set('exploit:t', 3n);
applyEmergencyCorrection(corrected);
if (corrected.get('exploit:b') !== 7n || corrected.get('exploit:t') !== 3n) {
  throw new Error('one-time exploiter correction black-holed future inbound TAP');
}

console.log(`Node ${process.version} tap-writer token-trade emergency behavior verified`);
