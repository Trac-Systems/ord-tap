#!/usr/bin/env node
import fs from 'node:fs';
import { createRequire } from 'node:module';

const requireFromTapWriter = createRequire(new URL('../../tap-writer/package.json', import.meta.url));
const RE2 = requireFromTapWriter('re2');

const patterns = [
  '',
  '^',
  '$',
  '^$',
  '.*',
  '.+',
  '\\b',
  '\\B',
  '[0-9]',
  '\\d',
  '\\D',
  '\\s',
  '\\S',
  '[\\s\\S]',
  '1|2',
  'ff',
  'a*',
  'a?',
  'a+',
  'a{0}',
  'a{0,}',
  '[0-9]{0}',
  '(?:1)',
  '(?:)',
  '(1)',
  '((1))',
  '(?<d>1)',
  '(?<d>1)\\k<d>',
  '(?<d>[0-9]+)',
  '(?<d>1)(?<d>2)',
  '(?<π>1)',
  '(?<💩>1)',
  '(?=1)1',
  '(?!1)1',
  '(?=\\d)',
  '(?!\\d)',
  '(?<=1)2',
  '(?<!1)2',
  '(?<=\\d)\\d',
  '(?<!\\d)\\d',
  '(1)\\1',
  '([0-9])\\1',
  '(a)?\\1',
  '(?i)a',
  '(?-i)a',
  '(?m)^1',
  '(?s).',
  '(?U)1.*',
  '(?ims:.)',
  '(?P<name>1)',
  '[[:digit:]]',
  '[[:alpha:]]',
  '[[:^alpha:]]',
  '[[:space:]]',
  '\\p{L}',
  '\\p{Letter}',
  '\\p{Number}',
  '\\p{Decimal_Number}',
  '\\p{Greek}',
  '\\p{Script=Greek}',
  '\\p{Script=Han}',
  '\\P{L}',
  '\\u{1F600}',
  '\\u{D800}',
  '\\uD800',
  '\\u2028',
  '\\x{41}',
  '\\x41',
  '[z-a]',
  '[\\d-\\w]',
  '[\\w-\\d]',
  '[',
  ']',
  '(',
  ')',
  '*',
  '+',
  '?',
  '{1,0}',
  '\\',
  '\\C',
  '\\Qabc\\E',
  '\\K',
  '(?>1)',
  '(?|(1))',
  '(?#comment)',
  '([0-9]{1,3})',
  '([0-9]+)?',
  '0*',
  '[a-f0-9]{2}',
  '(?:ff|00)',
  '([0-9])(?=\\1)',
  '([0-9]{1,64})'.repeat(8),
];

const haystacks = [
  '',
  '0',
  '1',
  '12',
  '123',
  '100000',
  '817798',
  '4294967295',
  '000111222333',
  '00ffff',
  '1d00ffff',
  'abc123',
  '11111111111111111111111111111111111111111111111111',
  '99999999999999999999999999999999999999999999999999',
  '0'.repeat(256),
  '1234567890'.repeat(64),
];

function tryRe2(pattern) {
  try {
    new RE2(pattern);
    return { accepts: true, error: null };
  } catch (error) {
    return { accepts: false, error: error instanceof Error ? error.message : String(error) };
  }
}

function tryV8(pattern, haystack) {
  try {
    const matches = String(haystack).match(new RegExp(pattern, 'g'));
    return {
      accepts: true,
      matchResultIsNull: matches === null,
      count: matches === null ? null : matches.length,
      error: null,
    };
  } catch (error) {
    return {
      accepts: false,
      matchResultIsNull: null,
      count: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

const rows = [];
for (const pattern of patterns) {
  const re2 = tryRe2(pattern);
  for (const haystack of haystacks) {
    const v8 = tryV8(pattern, haystack);
    rows.push({
      pattern,
      haystack,
      re2Accepts: re2.accepts,
      re2Error: re2.error,
      v8Accepts: v8.accepts,
      v8Error: v8.error,
      matchResultIsNull: v8.matchResultIsNull,
      count: v8.count,
    });
  }
}

const out = {
  node: process.versions.node,
  v8: process.versions.v8,
  re2: JSON.parse(fs.readFileSync(new URL('../../tap-writer/node_modules/re2/package.json', import.meta.url))).version,
  rows,
};

console.log(JSON.stringify(out, null, 2));
