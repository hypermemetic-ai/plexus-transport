---
id: UMB-1
title: "UMB — `plexus-rpc` umbrella crate with capability manifest"
status: Epic
type: epic
blocked_by: []
unlocks: []
---

## Goal

Ship a single crate `plexus-rpc` that consumers depend on instead of `plexus-core` + `plexus-macros` + `plexus-transport` separately. The umbrella does three things at once:

1. **One dependency, one version pin.** `cargo add plexus-rpc` brings the verified-compatible subcrate set. Migrating between framework versions is one bump, not three coordinated bumps.
2. **Compatibility receipt.** A const-time `CAPABILITIES` struct names the bundled subcrate versions and feature flags. Backends embed it in `_info` so synapse + synapse-cc know exactly what features the backend supports — they branch on capability rather than guessing from version strings.
3. **Cross-crate integration test surface.** A test in `plexus-rpc/tests/` exercises a real activation through builder + transport + dispatch. This is the canonical "do these three crates compose" check. Today nothing in CI exercises that combination.

The capability manifest pattern subsumes:
- **SAFE-S02** (plexus-core toolchain version exposure) — replaced by the broader manifest
- **SAFE-4 degraded mode** — version-gating becomes feature-gating
- The drift problem in general — tooling negotiates with backends instead of inferring from semver

## Dependency DAG

```
UMB-2  pub const VERSION on each subcrate  ───┐
UMB-3  Capabilities struct + CAPABILITIES const ─┼─► UMB-4  CAPABILITIES in _info
                                                 │
UMB-1  scaffold plexus-rpc crate ────────────────┴─► UMB-5  synapse decodes capabilities
                                                          │
                                                          ▼
                                                      UMB-6  synapse-cc passes flags
                                                          │
                                                          ▼
                                                      UMB-7  hub-codegen branches on capability

UMB-8  proc-macro-crate spike: auto-detect plexus-rpc as crate_path source
UMB-9  end-to-end integration test in plexus-rpc/tests/
UMB-10 migrate FormVeritasV2 + substrate to plexus-rpc
```

## Phase breakdown

### Phase 1 — Scaffold + version consts
UMB-1 (this), UMB-2, UMB-3. Pure plumbing. No consumer-visible behavior change. Lands the crate skeleton + per-subcrate `VERSION` consts + the `Capabilities` type.

### Phase 2 — Backend → tooling channel
UMB-4 wires `CAPABILITIES` into `_info`. UMB-5 has synapse decode it. UMB-6 plumbs through synapse-cc. UMB-7 makes hub-codegen branch on `per_method_x_plexus_source` etc.

### Phase 3 — Ergonomics + safety
UMB-8 makes the macros find `plexus-rpc` so `crate_path = "plexus_rpc::core"` is auto-detected. UMB-9 commits the integration test that fails CI when subcrates drift.

### Phase 4 — Migration
UMB-10 switches uscis (FormVeritasV2) and substrate over. Pin in their Cargo.toml goes from three lines to one.

## Tickets

| ID | Summary | Status |
|---|---|---|
| UMB-2  | Add `pub const VERSION` to plexus-core + plexus-macros + plexus-transport | Pending |
| UMB-3  | Define `Capabilities` struct + `CAPABILITIES` const in plexus-rpc | Pending |
| UMB-4  | Backend embeds `CAPABILITIES` in `_info` response | Pending |
| UMB-5  | Synapse decodes capabilities + stamps into IR | Pending |
| UMB-6  | Synapse-cc reads capabilities + passes feature flags to hub-codegen | Pending |
| UMB-7  | Hub-codegen branches on capability (skip REQ-9 JSDoc when backend lacks REQ-6) | Pending |
| UMB-8  | proc-macro-crate spike + fix: auto-detect `crate_path` through plexus-rpc | Pending |
| UMB-9  | End-to-end integration test in `plexus-rpc/tests/` | Pending |
| UMB-10 | Migrate FormVeritasV2 + substrate to depend on `plexus-rpc` | Pending |

## Out of scope

- **Re-exporting tokio / serde / schemars / async-trait.** These are fundamental ecosystem deps; consumers pick their own version. plexus-rpc only re-exports the plexus-* family.
- **Renaming `plexus-rpc` to `plexus`.** Keeping the descriptive name; revisit if there's appetite for the shorter form.
- **Yanking the individual subcrates from crates.io.** They keep being published independently for advanced users who want fine-grained control. plexus-rpc is the recommended path; the subcrates are the supported-but-not-recommended path.
- **A "plexus-client" companion crate** for consumers who only need the wire types (TS codegen folks etc.). Out of scope; can ship later if demand exists.

## Success criteria

Epic is Complete when:
1. `cargo add plexus-rpc` is sufficient to build a non-trivial activation (uscis-style)
2. Backend's `_info` exposes a parseable `CAPABILITIES` blob
3. synapse + synapse-cc + hub-codegen all read it and branch on at least one feature flag
4. CI integration test in `plexus-rpc/tests/` passes against a real activation
5. FormVeritasV2 builds with a single `plexus-rpc` dependency
