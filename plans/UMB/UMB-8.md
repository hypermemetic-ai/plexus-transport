---
id: UMB-8
title: "Auto-detect `crate_path` through plexus-rpc re-export (proc-macro-crate spike + fix)"
status: Pending
type: implementation
blocked_by: []
unlocks: []
severity: Medium
---

## Problem

When a consumer crate depends on `plexus-rpc` (which re-exports `plexus-core` as `plexus_rpc::core`) but NOT on `plexus-core` directly, the macros emit code referencing `::plexus_core::*` paths that don't resolve. Today, every PlexusRequest user has to write `crate_path = "plexus_core"` explicitly to override the macro's path resolution. With UMB, the right path becomes `plexus_rpc::core` — the macro should detect which is in scope and emit the correct path automatically.

## Required behavior

The `proc-macro-crate` infrastructure that picks `crate_path` defaults gains a fallback chain:

1. If the consumer crate depends on `plexus-core` directly, use `::plexus_core` (today's behavior)
2. Else if it depends on `plexus-rpc`, use `::plexus_rpc::core`
3. Else fall back to the explicit `crate_path = "..."` if provided
4. Else compile error (today's behavior when no path resolves)

A spike first verifies that `proc-macro-crate` can identify whether `plexus-rpc` is in the consumer's `Cargo.toml`. The crate has supported this for years; the question is just confirming the API.

## What must NOT change

- Existing consumers that depend on `plexus-core` directly: macro emits identical code as today
- Explicit `crate_path = "..."` overrides take precedence (escape hatch)
- The macro's behavior in test fixtures (which use `crate_path = "plexus_core"` explicitly): unchanged

## Acceptance criteria

1. Spike: A test crate depending only on `plexus-rpc` (not `plexus-core`) compiles a `#[plexus_rpc::macros::activation]` impl without specifying `crate_path` — macro auto-resolves to `plexus_rpc::core`
2. A test crate depending on both `plexus-rpc` and `plexus-core` compiles with explicit `crate_path = "plexus_core"` overriding
3. A test crate depending on neither (which today errors) continues to error with the same message
4. Existing tests in plexus-macros that use `crate_path = "plexus_core"` explicitly continue to pass

## Coordination

- Independent of UMB-1..7 — this is a pure plexus-macros change
- Should land BEFORE UMB-10 (migration) so the migrated consumers don't have to write `crate_path` everywhere

## Completion

Implementor: spike (30 min) → fix (1-2 hours) → test fixtures → commit.
