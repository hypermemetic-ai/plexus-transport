#!/usr/bin/env bash
# TRANSPORT-23: rmcp only behind `mcp`, and one version of it.
#   - no `mcp`: no rmcp in `cargo tree`, on any edge, dev included
#   - `mcp` on: exactly one rmcp version, 3.x (the manifests ask "3.8", so
#     3.8 or a later 3.y; PLEXUS-46: rmcp 3.9.0 came out and a fresh resolve
#     takes it — still one stack)
# Run by tests/rmcp_boundary.rs, so it fails `cargo test`.
set -uo pipefail
cd "$(dirname "$0")/.."
cargo=${CARGO:-cargo}
status=0

versions() {
  local out
  # shellcheck disable=SC2086
  if ! out=$("$cargo" tree --prefix none -e normal,build,dev --target all $1 2>&1); then
    echo "cargo-tree-failed: $out"
    return 1
  fi
  echo "$out" | sed -n 's/^rmcp v\([^ ]*\).*/\1/p' | sort -u | paste -sd, -
}

for flags in "" "--no-default-features" "--features=http-gateway"; do
  label=${flags:-default}
  if ! found=$(versions "$flags"); then
    echo "FAIL [$label]: $found"; status=1; continue
  fi
  if [ -n "$found" ]; then
    echo "FAIL [$label]: rmcp $found without \`mcp\`"; status=1
  else
    echo "ok   [$label]: no rmcp"
  fi
done

for flags in "--features=mcp" "--features=mcp-gateway" "--features=sqlite-sessions" "--all-features"; do
  if ! found=$(versions "$flags"); then
    echo "FAIL [$flags]: $found"; status=1; continue
  fi
  case "$found" in
    *,*)         echo "FAIL [$flags]: more than one rmcp: $found"; status=1 ;;
    3.[0-7].*)   echo "FAIL [$flags]: rmcp $found, older than the 3.8 asked for"; status=1 ;;
    3.*)         echo "ok   [$flags]: rmcp $found" ;;
    "")          echo "FAIL [$flags]: \`mcp\` is on and there is no rmcp"; status=1 ;;
    *)           echo "FAIL [$flags]: rmcp $found, wanted 3.x"; status=1 ;;
  esac
done
exit $status
