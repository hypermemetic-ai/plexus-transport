#!/usr/bin/env bash
# LMLS-2: the native <-> browser cross-check of wire profile v1.
#
#   1. the dependency graph: the wire crate depends on no crate of ours, and
#      each fixture depends on it (cargo itself refuses a cycle);
#   2. the committed vector file is the one the builder writes;
#   3. the native decoder observes of every vector what the file expects;
#   4. the browser binding (wasm32-unknown-unknown, run by Node's WebAssembly)
#      does too;
#   5. the two reports are the same bytes.
#
# Needs the wasm32-unknown-unknown target and node. CARGO_FLAGS=--offline
# builds without the network.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo=${CARGO:-cargo}
flags=${CARGO_FLAGS:-}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

echo "== 1. dependency graph"
# shellcheck disable=SC2086
"$cargo" tree $flags -p plexus-transport-mls -e normal --prefix none >"$out/tree"
if grep -Ev '^(plexus-transport-mls|sha2|digest|block-buffer|crypto-common|generic-array|typenum|cfg-if|cpufeatures|libc|version_check) ' "$out/tree"; then
  echo "FAIL: the wire crate depends on something other than sha2's tree" >&2
  exit 1
fi
for fixture in mls-wire-native-fixture mls-wire-browser-fixture; do
  # shellcheck disable=SC2086
  "$cargo" tree $flags -p "$fixture" -e normal --prefix none >"$out/tree"
  grep -q '^plexus-transport-mls ' "$out/tree" || {
    echo "FAIL: $fixture does not depend on the wire crate" >&2
    exit 1
  }
done
echo "ok: plexus-transport-mls -> sha2 only; both fixtures -> plexus-transport-mls"

# shellcheck disable=SC2086
"$cargo" build $flags --release -p mls-wire-native-fixture
# shellcheck disable=SC2086
"$cargo" build $flags --release -p mls-wire-browser-fixture --target wasm32-unknown-unknown
# shellcheck disable=SC2086
target=$("$cargo" metadata $flags --format-version 1 --no-deps |
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).target_directory))')
native="$target/release/mls-wire-native"
wasm="$target/wasm32-unknown-unknown/release/mls_wire_browser.wasm"

echo "== 2. the vector file is the builder's"
"$native" generate "$out/v1.json"
cmp vectors/v1.json "$out/v1.json"
echo "ok: vectors/v1.json ($(sha256sum vectors/v1.json | cut -d' ' -f1))"

echo "== 3. native"
"$native" report vectors/v1.json >"$out/native.report"
tail -n 1 "$out/native.report"

echo "== 4. browser ($(basename "$wasm"), $(wc -c <"$wasm") bytes, node $(node --version))"
node fixtures/browser/web/run-node.mjs "$wasm" vectors/v1.json >"$out/browser.report"
tail -n 1 "$out/browser.report"

echo "== 5. byte for byte"
cmp "$out/native.report" "$out/browser.report"
echo "ok: native and browser reports are identical ($(sha256sum "$out/native.report" | cut -d' ' -f1), $(wc -l <"$out/native.report") lines)"
