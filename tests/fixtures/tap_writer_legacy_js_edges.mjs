const REQUIRED_NODE_VERSION = '20.10.0';
if (process.versions.node !== REQUIRED_NODE_VERSION) {
  console.error(`tap-writer must run on Node.js ${REQUIRED_NODE_VERSION}; current runtime is ${process.version}.`);
  process.exit(1);
}

import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const WAValidator = require('multicoin-address-validator');
const secp = await import(require.resolve('@noble/secp256k1'));

const lowerTestnet = 'tb1qakxxzv9n7706kc3xdcycrtfv8cqv62hnwexc0l';
const upperTestnet = lowerTestnet.toUpperCase();
const mixedTestnet = `Tb1${lowerTestnet.slice(3)}`;

if (!WAValidator.validate(lowerTestnet, 'bitcoin', 'testnet')) {
  throw new Error('lowercase testnet address must validate');
}
if (!WAValidator.validate(upperTestnet, 'bitcoin', 'testnet')) {
  throw new Error('uppercase testnet address must validate after tap-writer testnet fix');
}
if (WAValidator.validate(mixedTestnet, 'bitcoin', 'testnet')) {
  throw new Error('mixed-case testnet address must reject');
}

const assertThrows = (label, fn) => {
  let threw = false;
  try {
    fn();
  } catch (_error) {
    threw = true;
  }
  if (!threw) {
    throw new Error(`${label}: expected throw`);
  }
};

assertThrows('non-string fee_rcv trim', () => ({ fee_rcv: 1 }).fee_rcv.trim());
assertThrows('token-send non-string tick lowercase', () => ({ tick: 1 }).tick.toLowerCase());
assertThrows('token-send non-string address trim', () => ({ address: 1 }).address.trim());
assertThrows('token-auth sig null dereference', () => ({ sig: null }).sig.v);
assertThrows('token-auth redeem null dereference', () => ({ redeem: null }).redeem.data);
assertThrows('privilege auth null dereference', () => ({ auth: null }).auth.name);
assertThrows('privileged mint missing prv dereference', () => ({}).prv.sig);
assertThrows('privileged mint null prv dereference', () => ({ prv: null }).prv.sig);
assertThrows('privileged mint null sig dereference', () => ({ prv: { sig: null } }).prv.sig.v);

for (const prv of [{}, [], 'x', 5, true, { sig: {} }, { sig: [] }, { sig: 1 }]) {
  const entersSignatureValidation =
    typeof prv.sig === 'object' &&
    typeof prv.sig.v !== 'undefined' &&
    typeof prv.sig.r !== 'undefined' &&
    typeof prv.sig.s !== 'undefined' &&
    typeof prv.hash !== 'undefined' &&
    typeof prv.address !== 'undefined' &&
    typeof prv.salt !== 'undefined';
  if (entersSignatureValidation) {
    throw new Error(`privileged mint gate unexpectedly entered for ${JSON.stringify(prv)}`);
  }
  const observableOutcome = entersSignatureValidation ? 'signature-validation' : 'failed-row';
  if (observableOutcome !== 'failed-row') {
    throw new Error(`privileged mint gate should produce failed row for ${JSON.stringify(prv)}`);
  }
}

const parseIntCases = [
  ['1abc', 1],
  ['18abc', 18],
  ['0.00000000000000001', 0],
  ['+1', 1],
  ['0x10', 16],
  [[1], 1],
  [null, NaN],
  [true, NaN],
  [{}, NaN],
];

for (const [input, expected] of parseIntCases) {
  const actual = parseInt(input);
  if (Number.isNaN(expected)) {
    if (!Number.isNaN(actual)) {
      throw new Error(`parseInt(${JSON.stringify(input)}) expected NaN, got ${actual}`);
    }
  } else if (actual !== expected) {
    throw new Error(`parseInt(${JSON.stringify(input)}) expected ${expected}, got ${actual}`);
  }
}

const bigintCases = [
  ['', 0n],
  ['0x10', 16n],
  ['0b10', 2n],
  ['0o10', 8n],
  ['+1', 1n],
  [[], 0n],
  [[1], 1n],
  [true, 1n],
  [false, 0n],
];

for (const [input, expected] of bigintCases) {
  const actual = BigInt(input);
  if (actual !== expected) {
    throw new Error(`BigInt(${JSON.stringify(input)}) expected ${expected}, got ${actual}`);
  }
}

for (const input of [null, {}, [1, 2], '1.2', '+0x10']) {
  assertThrows(`BigInt(${JSON.stringify(input)})`, () => BigInt(input));
}
if (BigInt('-1') !== -1n) {
  throw new Error('BigInt("-1") must produce a negative bigint before signature range checks');
}

for (const args of [[-1n, 1n, 0], [1n, -1n, 0], [0n, 1n, 0], [1n, 0n, 0]]) {
  assertThrows(`secp.Signature(${args.map(String).join(',')})`, () => new secp.Signature(...args));
}
new secp.Signature(1n, 1n, 99);

if (JSON.stringify({ tick: 'a/b', elem: { x: 1 } }) !== '{"tick":"a/b","elem":{"x":1}}') {
  throw new Error('JSON.stringify object/key behavior changed');
}

console.log(`Node ${process.version} tap-writer legacy JS edge behavior verified`);
