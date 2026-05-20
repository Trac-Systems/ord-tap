# tap_node20_v8_regexp

This crate needs the exact V8 engine from Node.js `20.10.0`:

- Node: `20.10.0`
- V8: `11.3.244.8-node.25`

Do the one-time setup for your OS below. After that, normal `cargo build --release` works from `upgrade/ord-tap`.

## macOS Setup

Run this from anywhere:

```bash
xcode-select --install || true
brew install git python ninja

git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git /tmp/node-v20.10.0-src
cd /tmp/node-v20.10.0-src

python3 - <<'PY'
from pathlib import Path
path = Path("/tmp/node-v20.10.0-src/deps/v8/third_party/zlib/zutil.h")
text = path.read_text()
text = text.replace("#if defined(MACOS) || defined(TARGET_OS_MAC)", "#if defined(MACOS)")
path.write_text(text)
PY

./configure --enable-static --without-npm --without-corepack --without-inspector --ninja
ninja -C out/Release \
  libv8_base_without_compiler.a \
  libv8_compiler.a \
  libv8_init.a \
  libv8_initializers.a \
  libv8_libbase.a \
  libv8_libplatform.a \
  libv8_libsampler.a \
  libv8_snapshot.a \
  libv8_turboshaft.a \
  libv8_zlib.a
```

Then package the V8 artifact and build ord-tap:

```bash
cd /Applications/MAMP/htdocs/tap-indexer/upgrade/ord-tap
crates/tap_node20_v8_regexp/tools/package-node20-v8-artifact.sh \
  /tmp/node-v20.10.0-src \
  "$(rustc -vV | sed -n 's/^host: //p')" \
  crates/tap_node20_v8_regexp/vendor/node20-v8

cargo build --release
```

## Linux Setup

Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y git python3 build-essential pkg-config ninja-build

git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git /tmp/node-v20.10.0-src
cd /tmp/node-v20.10.0-src

./configure --enable-static --without-npm --without-corepack --without-inspector --ninja
ninja -C out/Release \
  libv8_base_without_compiler.a \
  libv8_compiler.a \
  libv8_init.a \
  libv8_initializers.a \
  libv8_libbase.a \
  libv8_libplatform.a \
  libv8_libsampler.a \
  libv8_snapshot.a \
  libv8_turboshaft.a \
  libv8_zlib.a
```

Then package the V8 artifact and build ord-tap:

```bash
cd /path/to/tap-indexer/upgrade/ord-tap
crates/tap_node20_v8_regexp/tools/package-node20-v8-artifact.sh \
  /tmp/node-v20.10.0-src \
  "$(rustc -vV | sed -n 's/^host: //p')" \
  crates/tap_node20_v8_regexp/vendor/node20-v8

cargo build --release
```

## Windows Setup

Install first:

- Rust MSVC toolchain.
- Visual Studio Build Tools 2022 with "Desktop development with C++".
- Git for Windows.
- Python 3 in `PATH`.

Open "x64 Native Tools Command Prompt for VS 2022" or a PowerShell that has the Visual Studio developer environment loaded.

```powershell
git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git C:\tmp\node-v20.10.0-src
cd C:\tmp\node-v20.10.0-src
.\vcbuild.bat release static nonpm nocorepack no-cctest
```

Then package the V8 artifact and build ord-tap:

```powershell
cd C:\path\to\tap-indexer\upgrade\ord-tap
$target = (rustc -vV | Select-String '^host:').ToString().Split(' ')[1]

.\crates\tap_node20_v8_regexp\tools\package-node20-v8-artifact.ps1 `
  -NodeSourceDir C:\tmp\node-v20.10.0-src `
  -TargetTriple $target `
  -OutputRoot .\crates\tap_node20_v8_regexp\vendor\node20-v8

cargo build --release
```

If packaging fails because one of the V8 `.lib` files is missing, the Node Windows build did not emit the required static V8 archive set. Build the missing V8 static targets from the generated Visual Studio solution, then run the packaging command again.

## Artifact Location

The packaging command creates this directory:

```text
crates/tap_node20_v8_regexp/vendor/node20-v8/<target-triple>/
```

Cargo uses that directory automatically. To use a different artifact directory:

```bash
TAP_NODE20_V8_ARTIFACT_DIR=/path/to/artifact cargo build --release
```

For temporary local testing without packaging:

```bash
TAP_NODE20_V8_SOURCE_DIR=/tmp/node-v20.10.0-src cargo build --release
```

## Verify

From `upgrade/ord-tap`:

```bash
TAP_NODE20_V8_SOURCE_DIR=/tmp/node-v20.10.0-src cargo test -p tap_node20_v8_regexp -- --nocapture
TAP_NODE20_V8_SOURCE_DIR=/tmp/node-v20.10.0-src cargo test js_global_match_counts_match_node20_truth_vectors --lib -- --nocapture
TAP_NODE20_V8_SOURCE_DIR=/tmp/node-v20.10.0-src cargo check
```

The tests must report V8 `11.3.244.8-node.25`.

## Oracle Fixture

Regenerate the Node oracle fixture only when changing test coverage:

```bash
source ~/.nvm/nvm.sh
nvm use 20.10.0
node tools/dmt-regexp-oracle.mjs > crates/tap_node20_v8_regexp/tests/node20-re2-1214-oracle.json
```
