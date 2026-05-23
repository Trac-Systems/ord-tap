<h1 align=center><code>ord-tap</code></h1>

`ord-tap` is a modification of the `ord` client (Bitcoin Ordinals) and standalone indexer for `TAP Protocol`.

`TAP Protocol` ships with the following features:

- Token creation
- Token distribution
- Efficient transfers (single-TX transfers, mass-transfers, postage returns)
- Authority system to control token flows (transfers, minting)
- Built-in trading
- Low UTXO-footprint
- Digital Matter Theory support (DMT)
- Bitmap (DMT Blockdrops & indexing)
- Synthetic block rewards for Bitcoin miners

`ord-tap` provides an extensive REST API to pull spot and historic states from the index.

This modification is a standalone port of the peer-to-peer indexer & validator (based on Trac Network).

This port has been implemented with the help of Codex (GPT5 high: parity checks/fixes, heuristic account balance checks).

## Install And Run

Run these steps from the `ord-tap` source folder unless the command changes directory.

### 1. Install Rust And Cargo

macOS/Linux:

```bash
if ! command -v cargo >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
fi
. "$HOME/.cargo/env"
rustup default stable
rustup update stable
rustc -vV
cargo -V
```

Windows:

```powershell
winget install -e --id Rustlang.Rustup
rustup default stable-msvc
rustup update stable-msvc
rustc -vV
cargo -V
```

### 2. Install System Tools

macOS:

```bash
xcode-select --install || true
brew install git python@3.11 ninja
PYTHON311="$(brew --prefix python@3.11)/bin/python3.11"
export PATH="$(dirname "$PYTHON311"):$PATH"
export PYTHON="$PYTHON311"
"$PYTHON311" --version
```

Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y git curl xz-utils build-essential pkg-config libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libffi-dev liblzma-dev uuid-dev ninja-build

# Ubuntu 24.04 does not ship python3.11 in the default repos.
# This installs Python 3.11 alongside the system Python and does not replace python3.
cd /tmp
curl -fsSLO https://www.python.org/ftp/python/3.11.15/Python-3.11.15.tar.xz
tar -xf Python-3.11.15.tar.xz
cd Python-3.11.15
./configure --prefix=/usr/local --with-ensurepip=no
make -j"$(nproc)"
sudo make altinstall
PYTHON311=/usr/local/bin/python3.11
export PYTHON="$PYTHON311"
"$PYTHON311" --version
```

Windows:

```powershell
# Install Visual Studio Build Tools 2022 with "Desktop development with C++".
# Install Git for Windows.
winget install -e --id Python.Python.3.11
$python311 = py -3.11 -c "import sys; print(sys.executable)"
& $python311 --version
```

### 3. Build V8 Requirement

This is required once per target machine before `cargo build`.

macOS:

```bash
git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git /tmp/node-v20.10.0-src
cd /tmp/node-v20.10.0-src

PYTHON311="$(brew --prefix python@3.11)/bin/python3.11"
export PATH="$(dirname "$PYTHON311"):$PATH"
export PYTHON="$PYTHON311"

"$PYTHON311" - <<'PY'
from pathlib import Path
path = Path("/tmp/node-v20.10.0-src/deps/v8/third_party/zlib/zutil.h")
text = path.read_text()
text = text.replace("#if defined(MACOS) || defined(TARGET_OS_MAC)", "#if defined(MACOS)")
path.write_text(text)
PY

./configure --enable-static --without-npm --without-corepack --without-inspector --ninja
ninja -C out/Release \
  v8_snapshot \
  v8_initializers \
  v8_init \
  v8_compiler \
  v8_turboshaft \
  v8_base_without_compiler \
  v8_libplatform \
  v8_libbase \
  v8_zlib \
  v8_libsampler \
  icui18n \
  icuucx \
  icudata

cd /path/to/ord-tap
crates/tap_node20_v8_regexp/tools/package-node20-v8-artifact.sh \
  /tmp/node-v20.10.0-src \
  "$(rustc -vV | sed -n 's/^host: //p')" \
  crates/tap_node20_v8_regexp/vendor/node20-v8
```

Linux:

```bash
git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git /tmp/node-v20.10.0-src
cd /tmp/node-v20.10.0-src

PYTHON311=/usr/local/bin/python3.11
export PATH="$(dirname "$PYTHON311"):$PATH"
export PYTHON="$PYTHON311"
"$PYTHON311" --version

./configure --enable-static --without-npm --without-corepack --without-inspector --ninja
ninja -C out/Release \
  v8_snapshot \
  v8_initializers \
  v8_init \
  v8_compiler \
  v8_turboshaft \
  v8_base_without_compiler \
  v8_libplatform \
  v8_libbase \
  v8_zlib \
  v8_libsampler \
  icui18n \
  icuucx \
  icudata

cd /path/to/ord-tap
crates/tap_node20_v8_regexp/tools/package-node20-v8-artifact.sh \
  /tmp/node-v20.10.0-src \
  x86_64-unknown-linux-gnu \
  crates/tap_node20_v8_regexp/vendor/node20-v8
```

Use `aarch64-unknown-linux-gnu` instead of `x86_64-unknown-linux-gnu` on ARM Linux. The Linux packaging script finds Node's nested V8 archives, converts thin archives into normal archives, and writes a combined `libtap_node20_v8_bundle.a` for stable static linking.

Windows:

```powershell
git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git C:\tmp\node-v20.10.0-src
cd C:\tmp\node-v20.10.0-src

# Select the Python 3.11 installed in step 1 and put only that Python first.
$python311 = py -3.11 -c "import sys; print(sys.executable)"
$env:PATH = "$(Split-Path $python311);$env:PATH"
python --version

.\vcbuild.bat release static nonpm nocorepack no-cctest

cd C:\path\to\ord-tap
$target = (rustc -vV | Select-String '^host:').ToString().Split(' ')[1]
.\crates\tap_node20_v8_regexp\tools\package-node20-v8-artifact.ps1 `
  -NodeSourceDir C:\tmp\node-v20.10.0-src `
  -TargetTriple $target `
  -OutputRoot .\crates\tap_node20_v8_regexp\vendor\node20-v8
```

The packaged V8 files must exist here:

```text
crates/tap_node20_v8_regexp/vendor/node20-v8/<target-triple>/
```

On Linux, this directory must also contain `lib/libtap_node20_v8_bundle.a`; the packaging script creates it automatically.

### 4. Build ord-tap

```bash
cd /path/to/ord-tap
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### 5. Run ord-tap

```bash
./target/release/ord \
  --bitcoin-data-dir /path/to/.bitcoin \
  --index /path/to/ord-tap.redb \
  server \
  --http \
  --http-port 3333
```

Use `--regtest`, `--signet`, `--testnet`, or `--chain mainnet` if needed. Mainnet is the default.

### 6. Check That It Works

```bash
curl http://127.0.0.1:3333/r/tap/getCurrentBlock
curl http://127.0.0.1:3333/r/tap/getRegexBackend
```

The regex backend response should be:

```json
{ "result": "vendored-re2-2024-06-01" }
```

## Useful Runtime Options

- `--bitcoin-data-dir` points to the Bitcoin Core data directory.
- `--cookie-file` can be used if the Bitcoin RPC cookie is not in the data directory.
- `--index` points to the REDB index file.
- `--bitcoin-rpc-url`, `--bitcoin-rpc-username`, and `--bitcoin-rpc-password` override cookie-based RPC auth.
- `--tap-profile` prints TAP indexing timings per block.
- The exact TAP transfer route index is enabled by default. No environment variable is required for normal operation.
- `ORD_TAP_ROUTE_INDEX=off` disables the route index and uses the slower DB routing path.
- `ORD_TAP_ROUTE_INDEX=verify` is a debug/parity mode: it rebuilds the route index, compares fast routing against DB routing, and executes the DB path. Do not use it for normal indexing.
- `ORD_TAP_HOT_OWNER_CACHE_ENTRIES=250000` changes the bounded DMT/bitmap hot-owner cache size. The default is `250000`.
- `ORD_TAP_WRITER_EXPORT=1` enables the local TAP writer export service. It is disabled by default.
- `ORD_TAP_WRITER_EXPORT_CONSUMER_ID` and `ORD_TAP_WRITER_EXPORT_TOKEN` are required when writer export is enabled.
- `ORD_TAP_WRITER_EXPORT_ENDPOINT=unix:///tmp/ord-tap-export-mainnet.sock` serves writer export on a Unix socket on Linux/macOS. Keep the socket path short.
- `ORD_TAP_WRITER_EXPORT_ENDPOINT=npipe://./pipe/ord-tap-export-mainnet` serves writer export on a Windows named pipe.
- `ORD_TAP_WRITER_EXPORT_ENDPOINT=tcp://127.0.0.1:39091` serves writer export on loopback TCP. Non-loopback TCP requires `ORD_TAP_WRITER_EXPORT_PUBLIC_BIND=1` and should not be used for production.
- Writer export records coverage metadata when enabled. Existing mirrors with cursors before the reported export coverage start must resnapshot instead of following deltas.
- `ORD_TAP_WRITER_EXPORT_ROLLING_STATE=1` records an optional per-block rolling export digest. It lets mirrors verify each block's full reader-visible state in linear time while following deltas, without full keyspace scans. Leave it unset if no mirror needs that check.

## Build Notes

- Bitcoin Core must run with `-txindex=1`.
- No system RE2 package is required. The production build uses vendored RE2 sources.
- DMT mint execution requires the exact Node `20.10.0` V8 artifact.
- Builds fail if the V8 artifact is missing or its `SHA256SUMS` file does not verify.
- To use an external V8 artifact directory, set `TAP_NODE20_V8_ARTIFACT_DIR=/path/to/artifact`.
- Build from the packaged artifact for production. `TAP_NODE20_V8_SOURCE_DIR` is only for local test work with a compatible Node build tree.

## TAP REST API

The JSON API exposes TAP protocol data under the `/r/tap/*` namespace. Routes are grouped below by topic. Unless noted otherwise:

- Length endpoints return: `{ "result": <number> }`
- List endpoints return: `{ "result": [ <object> ] }`
- Single-record endpoints return: `{ "result": <object|null> }`
- Some records may include `null` fields when not applicable (e.g., miner rewards may have `ins` and `tx` as `null`).

<!-- BEGIN GENERATED TAP REST ENDPOINT INVENTORY -->
# Current TAP REST Endpoint Inventory

This inventory is generated from `src/subcommand/server.rs` and covers the 278 current TAP REST routes under `/r/tap`. List endpoints accept `offset` and `max` query parameters unless the route is a single-record lookup. Length endpoints return `{ "result": <number> }`.

### General/helpers
Current index state, reorg records, regex backend diagnostics, and low-level pagination helpers.

- GET `/r/tap/getCurrentBlock`
- GET `/r/tap/getLength/{*length_key}`
- GET `/r/tap/getListRecords`
- GET `/r/tap/getRegexBackend`
- GET `/r/tap/getReorgs`

### Bitmap and DMT
Bitmap ownership/events, DMT element discovery, DMT mint ownership, and DMT holder history.

- GET `/r/tap/getBitmap/{bitmap_block}`
- GET `/r/tap/getBitmapByInscription/{inscription}`
- GET `/r/tap/getBitmapEventByBlock/{block}`
- GET `/r/tap/getBitmapEventByBlockLength/{block}`
- GET `/r/tap/getBitmapWalletHistoricList/{address}`
- GET `/r/tap/getBitmapWalletHistoricListLength/{address}`
- GET `/r/tap/getDmtElementsList`
- GET `/r/tap/getDmtElementsListLength`
- GET `/r/tap/getDmtEventByBlock/{block}`
- GET `/r/tap/getDmtEventByBlockLength/{block}`
- GET `/r/tap/getDmtMintHolder/{inscription}`
- GET `/r/tap/getDmtMintHolderByBlock/{ticker}/{block}`
- GET `/r/tap/getDmtMintHoldersHistoryList/{inscription}`
- GET `/r/tap/getDmtMintHoldersHistoryListLength/{inscription}`
- GET `/r/tap/getDmtMintWalletHistoricList/{address}`
- GET `/r/tap/getDmtMintWalletHistoricListLength/{address}`

### Deployments and mints
Token deployment records, mint records, ticker-specific history, remaining supply, and block/transaction scoped deployment or mint views.

- GET `/r/tap/getAccountMintList/{address}/{ticker}`
- GET `/r/tap/getAccountMintListLength/{address}/{ticker}`
- GET `/r/tap/getDeployedList/{tx}`
- GET `/r/tap/getDeployedListByBlock/{block}`
- GET `/r/tap/getDeployedListByBlockLength/{block}`
- GET `/r/tap/getDeployedListLength/{tx}`
- GET `/r/tap/getDeployment/{ticker}`
- GET `/r/tap/getDeployments`
- GET `/r/tap/getDeploymentsLength`
- GET `/r/tap/getMintList`
- GET `/r/tap/getMintListLength`
- GET `/r/tap/getMintTokensLeft/{ticker}`
- GET `/r/tap/getMintedList/{tx}`
- GET `/r/tap/getMintedListByBlock/{block}`
- GET `/r/tap/getMintedListByBlockLength/{block}`
- GET `/r/tap/getMintedListLength/{tx}`
- GET `/r/tap/getTickerDeployedList/{ticker}/{tx}`
- GET `/r/tap/getTickerDeployedListByBlock/{ticker}/{block}`
- GET `/r/tap/getTickerDeployedListByBlockLength/{ticker}/{block}`
- GET `/r/tap/getTickerDeployedListLength/{ticker}/{tx}`
- GET `/r/tap/getTickerMintList/{ticker}`
- GET `/r/tap/getTickerMintListLength/{ticker}`
- GET `/r/tap/getTickerMintedList/{ticker}/{tx}`
- GET `/r/tap/getTickerMintedListByBlock/{ticker}/{block}`
- GET `/r/tap/getTickerMintedListByBlockLength/{ticker}/{block}`
- GET `/r/tap/getTickerMintedListLength/{ticker}/{tx}`

### Balances, holders, account views
Current balances, transferable amounts, locked balances, account token summaries, and holder lists.

- GET `/r/tap/getAccountBlockedTransferables/{address}`
- GET `/r/tap/getAccountTokenDetail/{address}/{ticker}`
- GET `/r/tap/getAccountTokens/{address}`
- GET `/r/tap/getAccountTokensBalance/{address}`
- GET `/r/tap/getAccountTokensLength/{address}`
- GET `/r/tap/getAmmObligationLockedBalance/{pool_id}/{side}/{ticker}`
- GET `/r/tap/getAuthorityBalanceByTick/{authority_id}/{ticker}`
- GET `/r/tap/getAuthorityBalances/{authority_id}`
- GET `/r/tap/getAuthorityBalancesLength/{authority_id}`
- GET `/r/tap/getBalance/{address}/{ticker}`
- GET `/r/tap/getHistoricHolders/{ticker}`
- GET `/r/tap/getHistoricHoldersLength/{ticker}`
- GET `/r/tap/getHolders/{ticker}`
- GET `/r/tap/getHoldersLength/{ticker}`
- GET `/r/tap/getLockedBalance/{address}/{ticker}`
- GET `/r/tap/getObligationLockedBalance/{source_type}/{source_id}/{ticker}`
- GET `/r/tap/getSingleTransferable/{inscription}`
- GET `/r/tap/getTransferable/{address}/{ticker}`

### Transfers and sends
Transfer inscription creation, executed transfers, token-send records, account send/receive history, and block/transaction/ticker scoped transfer views.

- GET `/r/tap/getAccountReceiveList/{address}/{ticker}`
- GET `/r/tap/getAccountReceiveListLength/{address}/{ticker}`
- GET `/r/tap/getAccountSentList/{address}/{ticker}`
- GET `/r/tap/getAccountSentListLength/{address}/{ticker}`
- GET `/r/tap/getAccountTransferList/{address}/{ticker}`
- GET `/r/tap/getAccountTransferListLength/{address}/{ticker}`
- GET `/r/tap/getInscribeTransferList/{tx}`
- GET `/r/tap/getInscribeTransferListByBlock/{block}`
- GET `/r/tap/getInscribeTransferListByBlockLength/{block}`
- GET `/r/tap/getInscribeTransferListLength/{tx}`
- GET `/r/tap/getSentList`
- GET `/r/tap/getSentListLength`
- GET `/r/tap/getTickerInscribeTransferList/{ticker}/{tx}`
- GET `/r/tap/getTickerInscribeTransferListByBlock/{ticker}/{block}`
- GET `/r/tap/getTickerInscribeTransferListByBlockLength/{ticker}/{block}`
- GET `/r/tap/getTickerInscribeTransferListLength/{ticker}/{tx}`
- GET `/r/tap/getTickerSentList/{ticker}`
- GET `/r/tap/getTickerSentListLength/{ticker}`
- GET `/r/tap/getTickerTransferList/{ticker}`
- GET `/r/tap/getTickerTransferListLength/{ticker}`
- GET `/r/tap/getTickerTransferredList/{ticker}/{tx}`
- GET `/r/tap/getTickerTransferredListByBlock/{ticker}/{block}`
- GET `/r/tap/getTickerTransferredListByBlockLength/{ticker}/{block}`
- GET `/r/tap/getTickerTransferredListLength/{ticker}/{tx}`
- GET `/r/tap/getTransferAmountByInscription/{inscription}`
- GET `/r/tap/getTransferList`
- GET `/r/tap/getTransferListLength`
- GET `/r/tap/getTransferredList/{tx}`
- GET `/r/tap/getTransferredListByBlock/{block}`
- GET `/r/tap/getTransferredListByBlockLength/{block}`
- GET `/r/tap/getTransferredListLength/{tx}`

### Trades
TAP token-trade offers, fills, account trade history, ticker trade history, and global trade lists.

- GET `/r/tap/getAccountReceiveTradesFilledList/{address}/{ticker}`
- GET `/r/tap/getAccountReceiveTradesFilledListLength/{address}/{ticker}`
- GET `/r/tap/getAccountTradesFilledList/{address}/{ticker}`
- GET `/r/tap/getAccountTradesFilledListLength/{address}/{ticker}`
- GET `/r/tap/getAccountTradesList/{address}/{ticker}`
- GET `/r/tap/getAccountTradesListLength/{address}/{ticker}`
- GET `/r/tap/getTickerTradesFilledList/{ticker}`
- GET `/r/tap/getTickerTradesFilledListLength/{ticker}`
- GET `/r/tap/getTickerTradesList/{ticker}`
- GET `/r/tap/getTickerTradesListLength/{ticker}`
- GET `/r/tap/getTrade/{inscription_id}`
- GET `/r/tap/getTradesFilledList`
- GET `/r/tap/getTradesFilledListLength`
- GET `/r/tap/getTradesList`
- GET `/r/tap/getTradesListLength`

### Auth and privilege authority
Token authorities, privilege authorities, authority cancellations, hash existence checks, verified privilege records, and authority-scoped reward/staking/sale views.

- GET `/r/tap/getAccountAuthList/{address}`
- GET `/r/tap/getAccountAuthListLength/{address}`
- GET `/r/tap/getAccountPrivilegeAuthList/{address}`
- GET `/r/tap/getAccountPrivilegeAuthListLength/{address}`
- GET `/r/tap/getAuthCancelled/{inscription_id}`
- GET `/r/tap/getAuthCompactHexExists/{hash}`
- GET `/r/tap/getAuthDelegationCancelList/{auth}`
- GET `/r/tap/getAuthDelegationCancelListLength/{auth}`
- GET `/r/tap/getAuthHashExists/{hash}`
- GET `/r/tap/getAuthList`
- GET `/r/tap/getAuthListLength`
- GET `/r/tap/getAuthoritiesByKind/{kind}`
- GET `/r/tap/getAuthoritiesByKindLength/{kind}`
- GET `/r/tap/getAuthorityById/{authority_id}`
- GET `/r/tap/getAuthorityList`
- GET `/r/tap/getAuthorityListLength`
- GET `/r/tap/getPrivilegeAuthCancelled/{inscription_id}`
- GET `/r/tap/getPrivilegeAuthCompactHexExists/{hash}`
- GET `/r/tap/getPrivilegeAuthHashExists/{hash}`
- GET `/r/tap/getPrivilegeAuthList`
- GET `/r/tap/getPrivilegeAuthListLength`
- GET `/r/tap/getPrivilegeAuthorityCollectionList/{privilege_inscription_id}/{collection_name}`
- GET `/r/tap/getPrivilegeAuthorityCollectionListLength/{privilege_inscription_id}/{collection_name}`
- GET `/r/tap/getPrivilegeAuthorityEventByBlock/{block}`
- GET `/r/tap/getPrivilegeAuthorityEventByBlockLength/{block}`
- GET `/r/tap/getPrivilegeAuthorityEventByPrivBlock/{privilege_authority_inscription_id}/{block}`
- GET `/r/tap/getPrivilegeAuthorityEventByPrivBlockLength/{privilege_authority_inscription_id}/{block}`
- GET `/r/tap/getPrivilegeAuthorityEventByPrivColBlock/{privilege_authority_inscription_id}/{collection_name}/{block}`
- GET `/r/tap/getPrivilegeAuthorityEventByPrivColBlockLength/{privilege_authority_inscription_id}/{collection_name}/{block}`
- GET `/r/tap/getPrivilegeAuthorityIsVerified/{privilege_inscription_id}/{collection_name}/{verified_hash}/{sequence}`
- GET `/r/tap/getPrivilegeAuthorityList/{privilege_inscription_id}`
- GET `/r/tap/getPrivilegeAuthorityListLength/{privilege_inscription_id}`
- GET `/r/tap/getPrivilegeAuthorityVerifiedByInscription/{verified_inscription_id}`
- GET `/r/tap/getPrivilegeAuthorityVerifiedInscription/{privilege_inscription_id}/{collection_name}/{verified_hash}/{sequence}`
- GET `/r/tap/getRewardClaimsByAuthority/{authority_id}`
- GET `/r/tap/getRewardClaimsByAuthorityLength/{authority_id}`
- GET `/r/tap/getSaleCancelsByAuthority/{authority_id}`
- GET `/r/tap/getSaleCancelsByAuthorityLength/{authority_id}`
- GET `/r/tap/getSaleClaimsByAuthority/{authority_id}`
- GET `/r/tap/getSaleClaimsByAuthorityLength/{authority_id}`
- GET `/r/tap/getSaleContributionsByAuthority/{authority_id}`
- GET `/r/tap/getSaleContributionsByAuthorityLength/{authority_id}`
- GET `/r/tap/getSaleRefundsByAuthority/{authority_id}`
- GET `/r/tap/getSaleRefundsByAuthorityLength/{authority_id}`
- GET `/r/tap/getSaleWithdrawalsByAuthority/{authority_id}`
- GET `/r/tap/getSaleWithdrawalsByAuthorityLength/{authority_id}`
- GET `/r/tap/getStakePositionsByAuthority/{authority_id}`
- GET `/r/tap/getStakePositionsByAuthorityLength/{authority_id}`

### Locks and delegation cancellation
Token lock records, lock-consume records, HTLC/OTC style lock history, delegation cancellation records, and block/transaction scoped lock events.

Certified-control locks are exposed through the same lock endpoints. A lock that opts into certified control includes a `control` object on the lock record. A certified claim or refund includes a `cert` object on the lock-consume record. Locks without certified control omit those fields.

- GET `/r/tap/getAccountDelegationCancelList/{address}`
- GET `/r/tap/getAccountDelegationCancelListLength/{address}`
- GET `/r/tap/getAccountLockConsumes/{address}`
- GET `/r/tap/getAccountLockConsumesByKind/{address}/{kind}`
- GET `/r/tap/getAccountLockConsumesByKindLength/{address}/{kind}`
- GET `/r/tap/getAccountLockConsumesLength/{address}`
- GET `/r/tap/getAccountLocks/{address}`
- GET `/r/tap/getAccountLocksByKind/{address}/{kind}`
- GET `/r/tap/getAccountLocksByKindLength/{address}/{kind}`
- GET `/r/tap/getAccountLocksLength/{address}`
- GET `/r/tap/getDelegationCancel/{auth}/{nonce}`
- GET `/r/tap/getDelegationCancelEventsByBlock/{block}`
- GET `/r/tap/getDelegationCancelEventsByBlockLength/{block}`
- GET `/r/tap/getDelegationCancelEventsByTransaction/{transaction_hash}`
- GET `/r/tap/getDelegationCancelEventsByTransactionLength/{transaction_hash}`
- GET `/r/tap/getDelegationCancelList`
- GET `/r/tap/getDelegationCancelListLength`
- GET `/r/tap/getLock/{lock_id}`
- GET `/r/tap/getLockConsume/{lock_id}`
- GET `/r/tap/getLockConsumeEventsByBlock/{block}`
- GET `/r/tap/getLockConsumeEventsByBlockLength/{block}`
- GET `/r/tap/getLockConsumeEventsByTransaction/{transaction_hash}`
- GET `/r/tap/getLockConsumeEventsByTransactionLength/{transaction_hash}`
- GET `/r/tap/getLockConsumeList`
- GET `/r/tap/getLockConsumeListLength`
- GET `/r/tap/getLockConsumesByKind/{kind}`
- GET `/r/tap/getLockConsumesByKindLength/{kind}`
- GET `/r/tap/getLockEventsByBlock/{block}`
- GET `/r/tap/getLockEventsByBlockLength/{block}`
- GET `/r/tap/getLockEventsByTransaction/{transaction_hash}`
- GET `/r/tap/getLockEventsByTransactionLength/{transaction_hash}`
- GET `/r/tap/getLockList`
- GET `/r/tap/getLockListLength`
- GET `/r/tap/getLocksByKind/{kind}`
- GET `/r/tap/getLocksByKindLength/{kind}`
- GET `/r/tap/getTickerLockConsumes/{ticker}`
- GET `/r/tap/getTickerLockConsumesByKind/{ticker}/{kind}`
- GET `/r/tap/getTickerLockConsumesByKindLength/{ticker}/{kind}`
- GET `/r/tap/getTickerLockConsumesLength/{ticker}`
- GET `/r/tap/getTickerLocks/{ticker}`
- GET `/r/tap/getTickerLocksByKind/{ticker}/{kind}`
- GET `/r/tap/getTickerLocksByKindLength/{ticker}/{kind}`
- GET `/r/tap/getTickerLocksLength/{ticker}`

### AMM
AMM pool metadata, pool lists, pool events, positions, asset-indexed pools, external snapshots, and AMM-side obligation views.

- GET `/r/tap/getAmmEventsByBlock/{block}`
- GET `/r/tap/getAmmEventsByBlockLength/{block}`
- GET `/r/tap/getAmmEventsByPool/{pool_id}`
- GET `/r/tap/getAmmEventsByPoolLength/{pool_id}`
- GET `/r/tap/getAmmEventsByTransaction/{transaction_hash}`
- GET `/r/tap/getAmmEventsByTransactionLength/{transaction_hash}`
- GET `/r/tap/getAmmExternalSnapshot/{pool_id}/{snapshot_id}`
- GET `/r/tap/getAmmObligationsBySource/{pool_id}/{side}`
- GET `/r/tap/getAmmObligationsBySourceLength/{pool_id}/{side}`
- GET `/r/tap/getAmmObligationsByTarget/{pool_id}/{side}`
- GET `/r/tap/getAmmObligationsByTargetLength/{pool_id}/{side}`
- GET `/r/tap/getAmmPool/{pool_id}`
- GET `/r/tap/getAmmPoolList`
- GET `/r/tap/getAmmPoolListLength`
- GET `/r/tap/getAmmPoolsByAsset/{asset_key}`
- GET `/r/tap/getAmmPoolsByAssetLength/{asset_key}`
- GET `/r/tap/getAmmPosition/{pool_id}/{target_type}/{target}`
- GET `/r/tap/getAmmPositionsByTarget/{target_type}/{target}`
- GET `/r/tap/getAmmPositionsByTargetLength/{target_type}/{target}`

### Obligations
Obligation records and consume records used by protocol applications to track pending and fulfilled duties between sources and targets.

- GET `/r/tap/getObligation/{obligation_id}`
- GET `/r/tap/getObligationConsume/{obligation_id}`
- GET `/r/tap/getObligationConsumeEventsByBlock/{block}`
- GET `/r/tap/getObligationConsumeEventsByBlockLength/{block}`
- GET `/r/tap/getObligationConsumeEventsByTransaction/{transaction_hash}`
- GET `/r/tap/getObligationConsumeEventsByTransactionLength/{transaction_hash}`
- GET `/r/tap/getObligationConsumeList`
- GET `/r/tap/getObligationConsumeListLength`
- GET `/r/tap/getObligationEventsByBlock/{block}`
- GET `/r/tap/getObligationEventsByBlockLength/{block}`
- GET `/r/tap/getObligationEventsByTransaction/{transaction_hash}`
- GET `/r/tap/getObligationEventsByTransactionLength/{transaction_hash}`
- GET `/r/tap/getObligationList`
- GET `/r/tap/getObligationListLength`
- GET `/r/tap/getObligationsByContext/{context_key}`
- GET `/r/tap/getObligationsByContextLength/{context_key}`
- GET `/r/tap/getObligationsBySource/{source_type}/{source_id}`
- GET `/r/tap/getObligationsBySourceLength/{source_type}/{source_id}`
- GET `/r/tap/getObligationsByTarget/{target_type}/{target_id}`
- GET `/r/tap/getObligationsByTargetLength/{target_type}/{target_id}`

### Staking and reward claims
Staking positions, pending rewards, reward claims, and address/authority scoped reward history.

- GET `/r/tap/getPendingRewardsByPosition/{position_id}`
- GET `/r/tap/getRewardClaimList`
- GET `/r/tap/getRewardClaimListLength`
- GET `/r/tap/getRewardClaimsByAddress/{address}`
- GET `/r/tap/getRewardClaimsByAddressLength/{address}`
- GET `/r/tap/getStakePositionById/{position_id}`
- GET `/r/tap/getStakePositionsByAddress/{address}`
- GET `/r/tap/getStakePositionsByAddressLength/{address}`

### Sales
Sale status, contributions, claims, refunds, withdrawals, and cancellation history.

- GET `/r/tap/getSaleCancels`
- GET `/r/tap/getSaleCancelsLength`
- GET `/r/tap/getSaleClaims`
- GET `/r/tap/getSaleClaimsByAddress/{address}`
- GET `/r/tap/getSaleClaimsByAddressLength/{address}`
- GET `/r/tap/getSaleClaimsLength`
- GET `/r/tap/getSaleContribution/{id}`
- GET `/r/tap/getSaleContributions`
- GET `/r/tap/getSaleContributionsByAddress/{address}`
- GET `/r/tap/getSaleContributionsByAddressLength/{address}`
- GET `/r/tap/getSaleContributionsByClaim/{address}`
- GET `/r/tap/getSaleContributionsByClaimLength/{address}`
- GET `/r/tap/getSaleContributionsLength`
- GET `/r/tap/getSaleRefunds`
- GET `/r/tap/getSaleRefundsByAddress/{address}`
- GET `/r/tap/getSaleRefundsByAddressLength/{address}`
- GET `/r/tap/getSaleRefundsLength`
- GET `/r/tap/getSaleStatus/{authority_id}`
- GET `/r/tap/getSaleWithdrawals`
- GET `/r/tap/getSaleWithdrawalsLength`

### Other TAP endpoints
Accumulator and redeem lists used by authority/redeem flows.

- GET `/r/tap/getAccountAccumulatorList/{address}`
- GET `/r/tap/getAccountAccumulatorListLength/{address}`
- GET `/r/tap/getAccountRedeemList/{address}`
- GET `/r/tap/getAccountRedeemListLength/{address}`
- GET `/r/tap/getAccumulator/{inscription}`
- GET `/r/tap/getAccumulatorList`
- GET `/r/tap/getAccumulatorListLength`
- GET `/r/tap/getRedeemList`
- GET `/r/tap/getRedeemListLength`
<!-- END GENERATED TAP REST ENDPOINT INVENTORY -->

General
- GET `/r/tap/getCurrentBlock`
  - Description: Returns current indexed block height.
 - GET `/r/tap/getRegexBackend`
  - Description: Returns which DMT regex backend is active, for example `"vendored-re2-2024-06-01"`.
  - Response: `{ "result": <string> }`
- GET `/r/tap/getReorgs?limit=100`
  - Description: Returns recent reorg events observed while this ord instance was running. Each item has the block height of the first divergent block and its orphaned hash.
  - Query: `limit` (optional, default 100) — maximum number of records to return.
  - Response: `{ "result": [ { "block": <number>, "blockhash": <string> }, ... ] }`
- GET `/r/tap/getLength/{*length_key}`
  - Description: Internal helper to get list lengths by key; useful for pagination.
  - Response: `{ "result": <number> }`
- GET `/r/tap/getListRecords?length_key=...&iterator_key=...&offset=0&max=500&return_json=true`
  - Description: Internal helper to read a window of records by list keys; `return_json=true` decodes items to JSON objects, otherwise returns strings.
  - Response: `{ "result": [ <object|string> ] }`

Deployments
- GET `/r/tap/getDeploymentsLength` → length of all deployments
- GET `/r/tap/getDeployments?offset=0&max=500` → list of deployments
  - Each item: `{ tick, max, lim, dec, blck, tx, vo, val, ins, num, ts, addr, crsd, dmt, elem?, prj?, dim?, dt?, prv?, dta? }`
- GET `/r/tap/getDeployment/{ticker}` → single deployment or `null`
- GET `/r/tap/getMintTokensLeft/{ticker}` → tokens-left as a string or `null`
- Deployed by transaction: length/list
  - GET `/r/tap/getDeployedListLength/{tx}`
  - GET `/r/tap/getDeployedList/{tx}?offset&max`
- Deployed by ticker+tx: length/list
  - GET `/r/tap/getTickerDeployedListLength/{ticker}/{tx}`
  - GET `/r/tap/getTickerDeployedList/{ticker}/{tx}?offset&max`
- Deployed by block: length/list
  - GET `/r/tap/getDeployedListByBlockLength/{block}`
  - GET `/r/tap/getDeployedListByBlock/{block}?offset&max`
- Deployed by ticker+block: length/list
  - GET `/r/tap/getTickerDeployedListByBlockLength/{ticker}/{block}`
  - GET `/r/tap/getTickerDeployedListByBlock/{ticker}/{block}?offset&max`

Mints
- Account mints: length/list
  - GET `/r/tap/getAccountMintListLength/{address}/{ticker}`
  - GET `/r/tap/getAccountMintList/{address}/{ticker}?offset&max`
  - Each item: `{ addr, blck, amt, bal, tx?, vo, val, ins?, num?, ts, fail, dmtblck?, dta? }`
- Ticker mints: length/list
  - GET `/r/tap/getTickerMintListLength/{ticker}`
  - GET `/r/tap/getTickerMintList/{ticker}?offset&max`
- Global mints (superflat): length/list
  - GET `/r/tap/getMintListLength`
  - GET `/r/tap/getMintList?offset&max`
- Minted by transaction/ticker/block: lengths/lists
  - GET `/r/tap/getMintedListLength/{tx}`
  - GET `/r/tap/getMintedList/{tx}?offset&max`
  - GET `/r/tap/getTickerMintedListLength/{ticker}/{tx}`
  - GET `/r/tap/getTickerMintedList/{ticker}/{tx}?offset&max`
  - GET `/r/tap/getMintedListByBlockLength/{block}`
  - GET `/r/tap/getMintedListByBlock/{block}?offset&max`
  - GET `/r/tap/getTickerMintedListByBlockLength/{ticker}/{block}`
  - GET `/r/tap/getTickerMintedListByBlock/{ticker}/{block}?offset&max`

Balances & Holders
- GET `/r/tap/getBalance/{address}/{ticker}` → `{ "result": <string|null> }`
- GET `/r/tap/getTransferable/{address}/{ticker}` → `{ "result": <string|null> }`
- GET `/r/tap/getTransferAmountByInscription/{inscription}` → `{ "result": <string|null> }` (alias: `/r/tap/getSingleTransferable/{inscription}`)
- Holders: lengths/lists (current and historic)
  - GET `/r/tap/getHoldersLength/{ticker}`
  - GET `/r/tap/getHolders/{ticker}?offset&max` → `{ "result": [ <address> ] }`
  - GET `/r/tap/getHistoricHoldersLength/{ticker}`
  - GET `/r/tap/getHistoricHolders/{ticker}?offset&max` → `{ "result": [ <address> ] }`

Transfers (Initial)
- Inscribe transfer by tx/ticker/block: lengths/lists
  - GET `/r/tap/getInscribeTransferListLength/{tx}`
  - GET `/r/tap/getInscribeTransferList/{tx}?offset&max`
  - GET `/r/tap/getTickerInscribeTransferListLength/{ticker}/{tx}`
  - GET `/r/tap/getTickerInscribeTransferList/{ticker}/{tx}?offset&max`
  - GET `/r/tap/getInscribeTransferListByBlockLength/{block}`
  - GET `/r/tap/getInscribeTransferListByBlock/{block}?offset&max`
  - GET `/r/tap/getTickerInscribeTransferListByBlockLength/{ticker}/{block}`
  - GET `/r/tap/getTickerInscribeTransferListByBlock/{ticker}/{block}?offset&max`
- Account/ticker/global (initial) transfer lists: lengths/lists
  - GET `/r/tap/getAccountTransferListLength/{address}/{ticker}`
  - GET `/r/tap/getAccountTransferList/{address}/{ticker}?offset&max`
  - GET `/r/tap/getTickerTransferListLength/{ticker}`
  - GET `/r/tap/getTickerTransferList/{ticker}?offset&max`
  - GET `/r/tap/getTransferListLength`
  - GET `/r/tap/getTransferList?offset&max`
  - Each item: `{ addr, blck, amt, trf, bal, tx, vo, val, ins, num, ts, fail, int, dta? }`

Transfers (Executed / Sent/Received)
- Executed transfers by tx/ticker/block: lengths/lists
  - GET `/r/tap/getTransferredListLength/{tx}`
  - GET `/r/tap/getTransferredList/{tx}?offset&max`
  - GET `/r/tap/getTickerTransferredListLength/{ticker}/{tx}`
  - GET `/r/tap/getTickerTransferredList/{ticker}/{tx}?offset&max`
  - GET `/r/tap/getTransferredListByBlockLength/{block}`
  - GET `/r/tap/getTransferredListByBlock/{block}?offset&max`
  - GET `/r/tap/getTickerTransferredListByBlockLength/{ticker}/{block}`
  - GET `/r/tap/getTickerTransferredListByBlock/{ticker}/{block}?offset&max`
- Sent/Received, account-scoped: lengths/lists
  - GET `/r/tap/getAccountSentListLength/{address}/{ticker}`
  - GET `/r/tap/getAccountSentList/{address}/{ticker}?offset&max`
  - GET `/r/tap/getAccountReceiveListLength/{address}/{ticker}`
  - GET `/r/tap/getAccountReceiveList/{address}/{ticker}?offset&max`
  - Sent/received items include sender/receiver perspectives and balances.

Trades
- Single trade
  - GET `/r/tap/getTrade/{inscription_id}` → `{ "result": <object|null> }`
- Offers and fills (account/ticker/global): lengths/lists
  - Offers: `/getAccountTradesListLength`, `/getAccountTradesList`, `/getTickerTradesListLength`, `/getTickerTradesList`, `/getTradesListLength`, `/getTradesList`
  - Fills: `/getAccountReceiveTradesFilledListLength`, `/getAccountReceiveTradesFilledList`, `/getAccountTradesFilledListLength`, `/getAccountTradesFilledList`, `/getTickerTradesFilledListLength`, `/getTickerTradesFilledList`, `/getTradesFilledListLength`, `/getTradesFilledList`
  - Records include addresses, tickers, amounts, fees, tx/vo/val/ins/num/timestamps, and `fail` flags.

Accumulators
- Single accumulator: GET `/r/tap/getAccumulator/{inscription}` → `{ "result": <object|null> }`
- Account/global accumulator lists: lengths/lists
  - GET `/r/tap/getAccountAccumulatorListLength/{address}`
  - GET `/r/tap/getAccountAccumulatorList/{address}?offset&max`
  - GET `/r/tap/getAccumulatorListLength`
  - GET `/r/tap/getAccumulatorList?offset&max`
- Blocked transferables: GET `/r/tap/getAccountBlockedTransferables/{address}` → `{ "result": <string|null> }`

Token Auth
- Status helpers
  - GET `/r/tap/getAuthCancelled/{inscription_id}` → `{ "result": <string|null> }`
  - GET `/r/tap/getAuthHashExists/{hash}` → `{ "result": <string|null> }`
  - GET `/r/tap/getAuthCompactHexExists/{hash}` → `{ "result": <string|null> }`
- Lists (global/account)
  - GET `/r/tap/getAuthListLength`, `/r/tap/getAuthList?offset&max` (superflat)
  - GET `/r/tap/getAccountAuthListLength/{address}`, `/r/tap/getAccountAuthList/{address}?offset&max`

Privilege Auth
- Status helpers
  - GET `/r/tap/getPrivilegeAuthCancelled/{inscription_id}` → `{ "result": <string|null> }`
  - GET `/r/tap/getPrivilegeAuthHashExists/{hash}` → `{ "result": <string|null> }`
  - GET `/r/tap/getPrivilegeAuthCompactHexExists/{hash}` → `{ "result": <string|null> }`
- Lists (global/account)
  - GET `/r/tap/getPrivilegeAuthListLength`, `/r/tap/getPrivilegeAuthList?offset&max` (superflat)
  - GET `/r/tap/getAccountPrivilegeAuthListLength/{address}`, `/r/tap/getAccountPrivilegeAuthList/{address}?offset&max`

Privilege Verification
- Single verification by verified inscription:
  - GET `/r/tap/getPrivilegeAuthorityVerifiedByInscription/{verified_inscription_id}`
- Verified status:
  - GET `/r/tap/getPrivilegeAuthorityIsVerified/{privilege_inscription_id}/{collection_name}/{verified_hash}/{sequence}` → `{ "result": <object|null> }`
- Privilege authority lists:
  - GET `/r/tap/getPrivilegeAuthorityListLength/{privilege_inscription_id}`
  - GET `/r/tap/getPrivilegeAuthorityList/{privilege_inscription_id}?offset&max`
  - GET `/r/tap/getPrivilegeAuthorityVerifiedInscription/{privilege_inscription_id}/{collection_name}/{verified_hash}/{sequence}` → `{ "result": <object|null> }`
- Event lists by block/privilege/collection: lengths/lists
  - `/r/tap/getPrivilegeAuthorityEventByPrivBlockLength/{privilege_inscription_id}/{block}`
  - `/r/tap/getPrivilegeAuthorityEventByPrivBlock/{privilege_inscription_id}/{block}?offset&max`
  - `/r/tap/getPrivilegeAuthorityEventByBlockLength/{block}`
  - `/r/tap/getPrivilegeAuthorityEventByBlock/{block}?offset&max`
  - `/r/tap/getPrivilegeAuthorityEventByPrivColBlockLength/{privilege_inscription_id}/{collection_name}/{block}`
  - `/r/tap/getPrivilegeAuthorityEventByPrivColBlock/{privilege_inscription_id}/{collection_name}/{block}?offset&max`

DMT
- Elements: length/list
  - GET `/r/tap/getDmtElementsListLength`
  - GET `/r/tap/getDmtElementsList?offset&max`
  - Each item: `{ tick, blck, tx, vo, ins, num, ts, addr, pat?, fld }`
- Events by block: length/list
  - GET `/r/tap/getDmtEventByBlockLength/{block}`
  - GET `/r/tap/getDmtEventByBlock/{block}?offset&max`
- DMT mint holder history & wallet:
  - GET `/r/tap/getDmtMintHoldersHistoryListLength/{inscription}`
  - GET `/r/tap/getDmtMintHoldersHistoryList/{inscription}?offset&max` → `{ "result": [ <object> ] }`
  - GET `/r/tap/getDmtMintHolder/{inscription}` → `{ "result": <object|null> }`
  - GET `/r/tap/getDmtMintHolderByBlock/{ticker}/{block}` → `{ "result": <object|null> }`
  - GET `/r/tap/getDmtMintWalletHistoricListLength/{address}`
  - GET `/r/tap/getDmtMintWalletHistoricList/{address}?offset&max` → `{ "result": [ <inscription_id> ] }`

Bitmap
- Single bitmap by block or inscription:
  - GET `/r/tap/getBitmap/{bitmap_block}` → `{ "result": <object|null> }`
  - GET `/r/tap/getBitmapByInscription/{inscription}` → `{ "result": <object|null> }`
- Wallet historic list (addresses that ever owned a bitmap):
  - GET `/r/tap/getBitmapWalletHistoricListLength/{address}`
  - GET `/r/tap/getBitmapWalletHistoricList/{address}?offset&max` → `{ "result": [ <inscription_id> ] }`
- Bitmap events by block: lengths/lists
  - GET `/r/tap/getBitmapEventByBlockLength/{block}`
  - GET `/r/tap/getBitmapEventByBlock/{block}?offset&max`

Account Tokens (Summary)
- GET `/r/tap/getAccountTokensLength/{address}` → `{ "result": <number> }`
- GET `/r/tap/getAccountTokens/{address}?offset&max` → `{ "result": [ <ticker> ] }`
- GET `/r/tap/getAccountTokensBalance/{address}?offset&max`
  - Response: `{ "data": { "total": <number>, "list": [ { "ticker": <string>, "overallBalance": <string|null>, "transferableBalance": <string|null> } ] } }`
- GET `/r/tap/getAccountTokenDetail/{address}/{ticker}`
  - Response: `{ "data": { "tokenInfo": <object|null>, "tokenBalance": { "ticker": <string>, "overallBalance": <string|null>, "transferableBalance": <string|null> }, "transferList": [ <object> ] } }`

Donate
------

Ordinals is open-source and community funded. The current lead maintainer of
`ord` is [raphjaph](https://github.com/raphjaph/). Raph's work on `ord` is
entirely funded by donations. If you can, please consider donating!

The donation address is
[bc1qguzk63exy7h5uygg8m2tcenca094a8t464jfyvrmr0s6wkt74wls3zr5m3](https://mempool.space/address/bc1qguzk63exy7h5uygg8m2tcenca094a8t464jfyvrmr0s6wkt74wls3zr5m3).

This address is 2 of 4 multisig wallet with keys held by
[raphjaph](https://twitter.com/raphjaph),
[erin](https://twitter.com/realizingerin),
[rodarmor](https://twitter.com/rodarmor), and
[ordinally](https://twitter.com/veryordinally).

Bitcoin received will go towards funding maintenance and development of `ord`,
as well as hosting costs for [ordinals.com](https://ordinals.com).

Thank you for donating!

Wallet
------

`ord` relies on Bitcoin Core for private key management and transaction signing.
This has a number of implications that you must understand in order to use
`ord` wallet commands safely:

- Bitcoin Core is not aware of inscriptions and does not perform sat
  control. Using `bitcoin-cli` commands and RPC calls with `ord` wallets may
  lead to loss of inscriptions.

- `ord wallet` commands automatically load the `ord` wallet given by the
  `--name` option, which defaults to 'ord'. Keep in mind that after running
  an `ord wallet` command, an `ord` wallet may be loaded.

- Because `ord` has access to your Bitcoin Core wallets, `ord` should not be
  used with wallets that contain a material amount of funds. Keep ordinal and
  cardinal wallets segregated.

Security
--------

The `ord server` explorer hosts untrusted HTML and JavaScript. This creates
potential security vulnerabilities, including cross-site scripting and spoofing
attacks. You are solely responsible for understanding and mitigating these
attacks. See the [documentation](docs/src/security.md) for more details.

Installation
------------

`ord` is written in Rust and can be built from
[source](https://github.com/ordinals/ord). Pre-built binaries are available on the
[releases page](https://github.com/ordinals/ord/releases).

You can install the latest pre-built binary from the command line with:

```sh
curl --proto '=https' --tlsv1.2 -fsLS https://ordinals.com/install.sh | bash -s
```

Once `ord` is installed, you should be able to run `ord --version` on the
command line.

Building
--------

On Linux, `ord` requires `libssl-dev` when building from source.

On Debian-derived Linux distributions, including Ubuntu:

```
sudo apt-get install pkg-config libssl-dev build-essential
```

On Red Hat-derived Linux distributions:

```
yum install -y pkgconfig openssl-devel
yum groupinstall "Development Tools"
```

Clone the `ord` repo:

```
git clone https://github.com/ordinals/ord.git
cd ord
```

To build a specific version of `ord`, first checkout that version:

```
git checkout <VERSION>
```

And finally to actually build `ord`:

```
cargo build --release
```

Once built, the `ord` binary can be found at `./target/release/ord`.

### Docker

A Docker image can be built with:

```
docker build -t ordinals/ord .
```

### Homebrew

`ord` is available in [Homebrew](https://brew.sh/):

```
brew install ord
```

### Debian Package

To build a `.deb` package:

```
cargo install cargo-deb
cargo deb
```

Contributing
------------

If you wish to contribute there are a couple things that are helpful to know. We
put a lot of emphasis on proper testing in the code base, with three broad
categories of tests: unit, integration and fuzz. Unit tests can usually be found at
the bottom of a file in a mod block called `tests`. If you add or modify a
function please also add a corresponding test. Integration tests try to test
end-to-end functionality by executing a subcommand of the binary. Those can be
found in the [tests](tests) directory. We don't have a lot of fuzzing but the
basic structure of how we do it can be found in the [fuzz](fuzz) directory.

We strongly recommend installing [just](https://github.com/casey/just) to make
running the tests easier. To run our CI test suite you would do:

```
just ci
```

This corresponds to the commands:

```
cargo fmt -- --check
cargo test --all
cargo test --all -- --ignored
```

Have a look at the [justfile](justfile) to see some more helpful recipes
(commands). Here are a couple more good ones:

```
just fmt
just fuzz
just doc
just watch ltest --all
```

If the tests are failing or hanging, you might need to increase the maximum
number of open files by running `ulimit -n 1024` in your shell before you run
the tests, or in your shell configuration.

We also try to follow a TDD (Test-Driven-Development) approach, which means we
use tests as a way to get visibility into the code. Tests have to run fast for that
reason so that the feedback loop between making a change, running the test and
seeing the result is small. To facilitate that we created a mocked Bitcoin Core
instance in [mockcore](./crates/mockcore)

Syncing
-------

`ord` requires a synced `bitcoind` node with `-txindex` to build the index of
satoshi locations. `ord` communicates with `bitcoind` via RPC.

If `bitcoind` is run locally by the same user, without additional
configuration, `ord` should find it automatically by reading the `.cookie` file
from `bitcoind`'s datadir, and connecting using the default RPC port.

If `bitcoind` is not on mainnet, is not run by the same user, has a non-default
datadir, or a non-default port, you'll need to pass additional flags to `ord`.
See `ord --help` for details.

`bitcoind` RPC Authentication
-----------------------------

`ord` makes RPC calls to `bitcoind`, which usually requires a username and
password.

By default, `ord` looks a username and password in the cookie file created by
`bitcoind`.

The cookie file path can be configured using `--cookie-file`:

```
ord --cookie-file /path/to/cookie/file server
```

Alternatively, `ord` can be supplied with a username and password on the
command line:

```
ord --bitcoin-rpc-username foo --bitcoin-rpc-password bar server
```

Using environment variables:

```
export ORD_BITCOIN_RPC_USERNAME=foo
export ORD_BITCOIN_RPC_PASSWORD=bar
ord server
```

Or in the config file:

```yaml
bitcoin_rpc_username: foo
bitcoin_rpc_password: bar
```

Logging
--------

`ord` uses [env_logger](https://docs.rs/env_logger/latest/env_logger/). Set the
`RUST_LOG` environment variable in order to turn on logging. For example, run
the server and show `info`-level log messages and above:

```
$ RUST_LOG=info cargo run server
```

Set the `RUST_BACKTRACE` environment variable in order to turn on full rust
backtrace. For example, run the server and turn on debugging and full backtrace:

```
$ RUST_BACKTRACE=1 RUST_LOG=debug ord server
```

New Releases
------------

Release commit messages use the following template:

```
Release x.y.z

- Bump version: x.y.z → x.y.z
- Update changelog
- Update changelog contributor credits
- Update dependencies
```

Translations
------------

To translate [the docs](https://docs.ordinals.com) we use
[mdBook i18n helper](https://github.com/google/mdbook-i18n-helpers).

See
[mdbook-i18n-helpers usage guide](https://github.com/google/mdbook-i18n-helpers/blob/main/i18n-helpers/USAGE.md)
for help.

Adding a new translations is somewhat involved, so feel free to start
translation and open a pull request, even if your translation is incomplete.

Take a look at
[this commit](https://github.com/ordinals/ord/commit/329f31bf6dac207dad001507dd6f18c87fdef355)
for an example of adding a new translation. A maintainer will help you integrate it
into our build system.

To start a new translation:

1. Install `mdbook`, `mdbook-i18n-helpers`, and `mdbook-linkcheck`:

   ```
   cargo install mdbook mdbook-i18n-helpers mdbook-linkcheck
   ```

2. Generate a new `pot` file named `messages.pot`:

   ```
   MDBOOK_OUTPUT='{"xgettext": {"pot-file": "messages.pot"}}'
   mdbook build -d po
   ```

3. Run `msgmerge` on `XX.po` where `XX` is the two-letter
   [ISO-639](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) code for
   the language you are translating into. This will update the `po` file with
   the text of the most recent English version:

   ```
   msgmerge --update po/XX.po po/messages.pot
   ```

4. Untranslated sections are marked with `#, fuzzy` in `XX.po`. Edit the
   `msgstr` string with the translated text.

5. Execute the `mdbook` command to rebuild the docs. For Chinese, whose
   two-letter ISO-639 code is `zh`:

   ```
   mdbook build docs -d build
   MDBOOK_BOOK__LANGUAGE=zh mdbook build docs -d build/zh
   mv docs/build/zh/html docs/build/html/zh
   python3 -m http.server --directory docs/build/html --bind 127.0.0.1 8080
   ```

6. If everything looks good, commit `XX.po` and open a pull request on GitHub.
   Other changed files should be omitted from the pull request.
