# tap_node20_v8_regexp

This crate needs the exact V8 engine from Node.js `20.10.0`:

- Node: `20.10.0`
- V8: `11.3.244.8-node.25`

Do the one-time setup for your OS below. After that, normal `cargo build --release` works from `upgrade/ord-tap`.

## macOS Setup

Run this from anywhere:

```bash
xcode-select --install || true
brew install git python@3.11 ninja
if ! command -v cargo >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
fi
. "$HOME/.cargo/env"
PYTHON311="$(brew --prefix python@3.11)/bin/python3.11"
export PATH="$(dirname "$PYTHON311"):$PATH"
export PYTHON="$PYTHON311"
"$PYTHON311" --version
rustc -vV

git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git /tmp/node-v20.10.0-src
cd /tmp/node-v20.10.0-src

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
```

Then package the V8 artifact and build ord-tap:

```bash
cd /path/to/ord-tap
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
sudo apt-get install -y git curl xz-utils build-essential pkg-config libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libffi-dev liblzma-dev uuid-dev ninja-build
if ! command -v cargo >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
fi
. "$HOME/.cargo/env"

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
rustc -vV

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
```

Then package the V8 artifact and build ord-tap:

```bash
cd /path/to/ord-tap
crates/tap_node20_v8_regexp/tools/package-node20-v8-artifact.sh \
  /tmp/node-v20.10.0-src \
  x86_64-unknown-linux-gnu \
  crates/tap_node20_v8_regexp/vendor/node20-v8

cargo build --release
```

Use `aarch64-unknown-linux-gnu` instead of `x86_64-unknown-linux-gnu` on ARM Linux. The Linux packaging script finds Node's nested V8 archives, converts thin archives into normal archives, and writes `lib/libtap_node20_v8_bundle.a` for stable static linking.

## Windows Setup

Install first:

- Rust MSVC toolchain.
- Visual Studio Build Tools 2022 with "Desktop development with C++".
- Git for Windows.
- Python 3.11.

Open "x64 Native Tools Command Prompt for VS 2022" or a PowerShell that has the Visual Studio developer environment loaded.

```powershell
winget install -e --id Python.Python.3.11
$python311 = py -3.11 -c "import sys; print(sys.executable)"
& $python311 --version

git clone --branch v20.10.0 --depth 1 https://github.com/nodejs/node.git C:\tmp\node-v20.10.0-src
cd C:\tmp\node-v20.10.0-src

# Select the Python 3.11 installed above and put only that Python first.
$python311 = py -3.11 -c "import sys; print(sys.executable)"
$env:PATH = "$(Split-Path $python311);$env:PATH"
python --version

.\vcbuild.bat release static nonpm nocorepack no-cctest
```

Then package the V8 artifact and build ord-tap:

```powershell
cd C:\path\to\ord-tap
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

On Linux, this directory must contain `lib/libtap_node20_v8_bundle.a`; the packaging script creates it automatically.

Cargo uses that directory automatically. To use a different artifact directory:

```bash
TAP_NODE20_V8_ARTIFACT_DIR=/path/to/artifact cargo build --release
```

For temporary local testing with a compatible Node build tree:

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
