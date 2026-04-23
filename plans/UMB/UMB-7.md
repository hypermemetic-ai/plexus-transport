---
id: UMB-7
title: "Hub-codegen branches on capability (skip REQ-9 JSDoc when backend lacks REQ-6)"
status: Pending
type: implementation
blocked_by: [UMB-6]
unlocks: []
severity: Medium
---

## Problem

REQ-9 emits per-method JSDoc breadcrumbs (`@requiresAuth`, `@server-derived`, etc.) by reading per-param `pd_source` annotations from the IR. Those annotations are emitted by REQ-6's macro path. If a backend uses an older `plexus-macros` (< 0.5.6, pre-REQ-6), the IR has no `pd_source` data — but tonight's REQ-9 (after the fallback was deleted, commit `387348e`) handles this gracefully by emitting nothing. So technically there's no bug today.

But once UMB lands, hub-codegen should be EXPLICIT about its assumptions: it requires `featPerMethodXPlexusSource = true` to emit the JSDoc breadcrumbs. With explicit feature gating, future codegen features can be added the same way without ambiguity about what the backend supports.

## Required behavior

Hub-codegen accepts a new CLI flag `--feature-per-method-source` (passed by synapse-cc from UMB-6). When set, JSDoc breadcrumb emission is enabled. When absent, JSDoc breadcrumb emission is disabled and the codegen emits a debug log noting that breadcrumbs were skipped because the backend doesn't advertise the feature.

For other features (typed errors, cookie auth marker), the codegen continues to always emit them — the flags would only gate them off if the backend explicitly opts out, which is a future case.

## What must NOT change

- TS client output for non-breadcrumb code paths is unchanged
- The `--feature-per-method-source` flag defaults to `true` for backward-compat with synapse-cc invocations that don't pass it (UMB-6 will pass it explicitly when reading from `irCapabilities`)
- Generated transport.ts SAFE-7 marker is unchanged

## Acceptance criteria

1. Hub-codegen invoked WITH `--feature-per-method-source` against a REQ-6-era IR produces JSDoc breadcrumbs as today
2. Hub-codegen invoked WITHOUT `--feature-per-method-source` against the same IR emits no `@requiresAuth`/`@server-derived` tags (graceful degradation)
3. The default behavior (no flag passed) is to emit breadcrumbs — preserves current behavior for callers that haven't migrated to capability-aware invocation
4. Debug logging makes it clear which mode is active

## Coordination

- Blocked by UMB-6 (which passes the flag)
- Last ticket in the capability-flow chain (UMB-3 → 4 → 5 → 6 → 7)

## Completion

Implementor adds the flag to hub-codegen's CLI parser; gates the JSDoc emission on it; tests both branches; commits.
