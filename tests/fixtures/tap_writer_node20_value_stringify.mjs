const REQUIRED_NODE_VERSION = '20.10.0';
if (process.versions.node !== REQUIRED_NODE_VERSION) {
  console.error(`tap-writer must run on Node.js ${REQUIRED_NODE_VERSION}; current runtime is ${process.version}.`);
  process.exit(1);
}

const parseLikeTapWriter = (str) => JSON.parse(
  str,
  (_key, value, data) => (_key === 'max' || _key === 'lim' || _key === 'amt') && typeof value === 'number'
    ? data.source
    : value,
);

const cases = [
  ['raw transfer amt', '{"p":"tap","op":"token-transfer","tick":"tap","amt":74}', true],
  ['raw mint amt', '{"p":"tap","op":"token-mint","tick":"tap","amt":74.01}', true],
  ['raw deploy max', '{"p":"tap","op":"token-deploy","tick":"tap","max":21000000,"lim":"1000"}', true],
  ['raw deploy lim', '{"p":"tap","op":"token-deploy","tick":"tap","max":"21000000","lim":1000}', true],
  ['raw nested send amt', '{"p":"tap","op":"token-send","items":[{"tick":"tap","amt":1,"address":"bc1qexample"}]}', true],
  ['raw nested trade accept amt', '{"p":"tap","op":"token-trade","side":"0","tick":"tap","amt":"1","accept":[{"tick":"tap","amt":1}]}', true],
  ['escaped raw amt key', '{"\\u0061mt":1}', true],
  ['duplicate final string', '{"amt":1,"amt":"2"}', false],
  ['duplicate final number', '{"amt":"2","amt":1}', true],
  ['quoted deploy', '{"p":"tap","op":"token-deploy","tick":"tap","max":"21000000","lim":"1000"}', false],
  ['quoted transfer', '{"p":"tap","op":"token-transfer","tick":"tap","amt":"74.01"}', false],
  ['non-target number', '{"valid":100,"side":0,"dec":18}', false],
];

for (const [label, input, shouldThrow] of cases) {
  try {
    parseLikeTapWriter(input);
    if (shouldThrow) {
      throw new Error(`${label}: expected Node 20 tap-writer parse to throw`);
    }
  } catch (error) {
    if (!shouldThrow) {
      throw new Error(`${label}: expected parse success, got ${error.message}`);
    }
  }
}

console.log(`Node ${process.version} tap-writer value_stringify behavior verified`);
