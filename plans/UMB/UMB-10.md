---
id: UMB-10
title: "Migrate FormVeritasV2 + substrate to depend on plexus-rpc"
status: Pending
type: implementation
blocked_by: [UMB-1, UMB-3, UMB-8, UMB-9]
unlocks: []
severity: Medium
---

## Problem

Existing consumers (FormVeritasV2/uscis, plexus-substrate) pin three subcrates with three separate version requirements + a `[patch.crates-io]` block when developing against local checkouts. Migration to `plexus-rpc` reduces this to one dependency line and one patch.

This is opt-in per consumer. Subcrates remain published independently for advanced users; consumers choose when to migrate.

## Required behavior

For each migrated consumer:

1. `Cargo.toml` `[dependencies]`: replace three lines (`plexus-core`, `plexus-macros`, `plexus-transport`) with one (`plexus-rpc`)
2. `Cargo.toml` `[patch.crates-io]` (if present): replace three patches with one
3. Source-level imports: change `use plexus_core::...` to `use plexus_rpc::core::...` (or use the prelude)
4. Macro `crate_path` overrides: remove (UMB-8 makes this auto-detected)
5. Verify the consumer still builds + tests pass

## What must NOT change

- Consumer's runtime behavior
- Consumer's CI workflow
- Wire format / RPC behavior
- Activation / method definitions (only the import paths change)

## Acceptance criteria (per consumer)

1. `Cargo.toml` declares `plexus-rpc` as the sole framework dependency
2. `cargo build` succeeds without source-code changes beyond import-path updates
3. `cargo test` (where applicable) passes
4. Generated client (where applicable) is byte-identical to pre-migration output
5. Smoke test against the deployed backend: behaves identically

## Per-consumer scope

**FormVeritasV2 (uscis-notifier)**: `~/dev/hyperforge/workspaces/sshmendez/orgs/OneBigMediaCo/FormVeritasV2/`. ~10 import path edits estimated. Touches `Cargo.toml` + a handful of `src/activations/*/activation.rs` files.

**plexus-substrate**: `~/dev/controlflow/hypermemetic/plexus-substrate/`. Larger surface; many activations. Could be split into a separate ticket if the diff is big.

## Coordination

- Blocked by UMB-1 (crate exists), UMB-3 (Capabilities exists), UMB-8 (crate_path auto-detection), UMB-9 (integration test passes)
- Optional: split the per-consumer migrations into UMB-10a (FormVeritasV2) and UMB-10b (substrate) if the diff sizes warrant it

## Completion

Per-consumer commit on each repo. Implementor verifies build + tests + smoke. Flips per-consumer tickets to Complete.
