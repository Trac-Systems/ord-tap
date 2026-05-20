#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  cat >&2 <<'USAGE'
usage: package-node20-v8-artifact.sh <node-v20.10.0-source-dir> <target-triple> <output-root>

Example:
  package-node20-v8-artifact.sh /tmp/node-v20.10.0-src aarch64-apple-darwin \
    crates/tap_node20_v8_regexp/vendor/node20-v8
USAGE
  exit 2
fi

src_dir="$1"
target="$2"
out_root="$3"
artifact_dir="$out_root/$target"

release_dir="$src_dir/out/Release"
include_dir="$src_dir/deps/v8/include"

libs=(
  libv8_snapshot.a
  libv8_initializers.a
  libv8_init.a
  libv8_compiler.a
  libv8_turboshaft.a
  libv8_base_without_compiler.a
  libv8_libplatform.a
  libv8_libbase.a
  libv8_zlib.a
  libv8_libsampler.a
  libicui18n.a
  libicuucx.a
  libicudata.a
)

if [[ ! -f "$include_dir/v8.h" ]]; then
  echo "missing V8 headers at $include_dir" >&2
  exit 1
fi

rm -rf "$artifact_dir"
mkdir -p "$artifact_dir/include/v8" "$artifact_dir/lib"
cp -R "$include_dir"/. "$artifact_dir/include/v8/"

for lib in "${libs[@]}"; do
  if [[ ! -f "$release_dir/$lib" ]]; then
    echo "missing archive $release_dir/$lib" >&2
    exit 1
  fi
  cp "$release_dir/$lib" "$artifact_dir/lib/$lib"
done

(
  cd "$artifact_dir"
  shasum -a 256 lib/*.a > SHA256SUMS
)

echo "wrote $artifact_dir"
