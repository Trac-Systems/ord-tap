<h1 align=center><code>ord</code></h1>

<div align=center>
  <a href=https://crates.io/crates/ord>
    <img src=https://img.shields.io/crates/v/ord.svg alt="crates.io version">
  </a>
  <a href=https://github.com/ordinals/ord/actions/workflows/ci.yaml>
    <img src=https://github.com/ordinals/ord/actions/workflows/ci.yaml/badge.svg alt="build status">
  </a>
  <a href=https://github.com/ordinals/ord/releases>
    <img src=https://img.shields.io/github/downloads/ordinals/ord/total.svg alt=downloads>
  </a>
  <a href=https://discord.gg/ordinals>
    <img src=https://img.shields.io/discord/987504378242007100?logo=discord alt="chat on discord">
  </a>
</div>
<br>

`ord` is an index, block explorer, and command-line wallet. It is experimental
software with no warranty. See [LICENSE](LICENSE) for more details.

Ordinal theory imbues satoshis with numismatic value, allowing them to
be collected and traded as curios.

Ordinal numbers are serial numbers for satoshis, assigned in the order in which
they are mined, and preserved across transactions.

See [the docs](https://docs.ordinals.com) for documentation and guides.

See [the BIP](bip.mediawiki) for a technical description of the assignment and
transfer algorithm.

See [the project board](https://github.com/orgs/ordinals/projects/1) for
currently prioritized issues.

Join [the Discord server](https://discord.gg/87cjuz4FYg) to chat with fellow
ordinal degenerates.

TAP REST API
------------

How To Compile And Run
----------------------

- Requirements
  - Bitcoin Core v29 or newer (with `-txindex=1`, RPC enabled, and an accessible cookie file).
  - Rust toolchain (stable) and Cargo installed.

- Build (native-optimized release)
  1. Unzip or clone the ord-tap package, then change into the `ord-tap/` directory.
  2. Compile with native CPU optimizations:
     - `RUSTFLAGS="-C target-cpu=native" cargo build --release`
  


- Run (HTTP server with TAP REST endpoints)
  1. From the project root (or copy the binary from `target/release/`):
     - `./target/release/ord --bitcoin-data-dir /path/to/.bitcoin/ --index /path/to/ord-tap-index.redb server --http --http-port 3333`

- Notes
  - Data and index paths:
    - `--bitcoin-data-dir` must point to your Bitcoin Core data directory (where `.cookie` and chain folders live). Use `--cookie-file` if you keep the cookie elsewhere.
    - `--index` points to the REDB index file; pick an empty path for first-time indexing.
  - Chain selection: pass `--regtest`, `--signet`, `--testnet`, or `--chain mainnet` (default is mainnet). Ensure your Bitcoin Core node matches the selected chain.
  - RPC configuration: by default, ord reads credentials from the cookie file in the Bitcoin data dir. To override, use `--bitcoin-rpc-url`, `--bitcoin-rpc-username`, and `--bitcoin-rpc-password`.
  - Performance flags:
    - `--tap-profile` prints per-block TAP profiling; helpful for diagnosing throughput.
    - `--disable-tap-blooms` disables TAP bloom prefilters; useful for A/B testing or constrained environments.
    - Build with `RUSTFLAGS="-C target-cpu=native"` as shown to enable CPU-specific optimizations.
    - Enable logging to see info-level output:
      - macOS/Linux (bash/zsh): `export RUST_LOG=info`
      - Windows (PowerShell): `$env:RUST_LOG='info'`
      - Windows (cmd.exe): `set RUST_LOG=info`
  - First run: the indexer will scan the chain and populate the index; this can take time. The server keeps indexing in the background and exposes endpoints as data becomes available.
  - REST base URL: once running, TAP endpoints are under `http://127.0.0.1:<port>/r/tap/*`.
  - DMT regex parity (RE2):
    - tap-ord uses Google RE2 for DMT element pattern validation (parity with tap-writer) and bundles sources for static builds.
    - If your build environment can’t build the bundled RE2 (e.g., missing CMake), the build falls back to using a system RE2.
    - If you see a build error about RE2 not found, install a system RE2 package:
      - macOS: `brew install re2`
      - Ubuntu/Debian: `sudo apt-get install -y libre2-dev`
      - Alpine: `apk add re2 re2-dev`
      - Windows (MSVC): install Visual C++ Build Tools + CMake, then install RE2 via vcpkg and set INCLUDE/LIB:
        - `git clone https://github.com/microsoft/vcpkg C:\\vcpkg && C:\\vcpkg\\bootstrap-vcpkg.bat`
        - `C:\\vcpkg\\vcpkg.exe install re2:x64-windows`
        - `set VCPKG_ROOT=C:\\vcpkg`
        - `set INCLUDE=%VCPKG_ROOT%\\installed\\x64-windows\\include;%INCLUDE%`
        - `set LIB=%VCPKG_ROOT%\\installed\\x64-windows\\lib;%LIB%`
        - Then `cargo build --release`


The JSON API exposes TAP protocol data under the `/r/tap/*` namespace. Routes are grouped below by topic. Unless noted otherwise:

- Length endpoints return: `{ "result": <number> }`
- List endpoints return: `{ "result": [ <object> ] }`
- Single-record endpoints return: `{ "result": <object|null> }`
- Some records may include `null` fields when not applicable (e.g., miner rewards may have `ins` and `tx` as `null`).

General
- GET `/r/tap/getCurrentBlock`
  - Description: Returns current indexed block height.
  - Response: `{ "result": <number> }`
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
- GET `/r/tap/getSingleTransferable/{inscription}` → `{ "result": <string|null> }`
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

You'll also need Rust:

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
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

`ord` requires `rustc` version 1.79.0 or later. Run `rustc --version` to ensure
you have this version. Run `rustup update` to get the latest stable release.

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
