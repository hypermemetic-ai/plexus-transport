---
id: UMB-6
title: "Synapse-cc reads capabilities + passes feature flags to hub-codegen"
status: Pending
type: implementation
blocked_by: [UMB-5]
unlocks: [UMB-7]
severity: Medium
---

## Problem

After UMB-5, the IR JSON carries `irCapabilities`. synapse-cc orchestrates synapse + hub-codegen and is the natural place to translate capability flags into hub-codegen invocation arguments. Otherwise hub-codegen would have to know how to walk `irCapabilities` itself.

## Required behavior

`synapse-cc build` reads `ir.json`'s `irCapabilities.features` block. For each `true` feature relevant to codegen, it passes a corresponding flag on the hub-codegen command line:

| IR feature flag | hub-codegen flag |
|---|---|
| `featPerMethodXPlexusSource` | `--feature-per-method-source` |
| `featTypedJsonRpcErrorCodes` | (no flag — hub-codegen always emits typed errors; future-proof for compat checks) |
| `featCookieAuthMarkerTransport` | (no flag — same; cookie-auth marker is unconditional in current hub-codegen output) |

The flag set is intentionally small for v1 — only flags that change codegen output actually need to be passed. Future capability flags get added here as they affect codegen.

When `irCapabilities` is `null` (pre-UMB backend), synapse-cc passes nothing — hub-codegen uses its defaults, which today match the no-capability assumption.

## What must NOT change

- synapse-cc CLI surface: no new user-facing flags in v1; the wiring is automatic
- hub-codegen invocation succeeds whether the new flags are passed or not (graceful default)
- synapse-cc's existing flags (`--no-install`, `--force`, etc.) are unchanged

## Acceptance criteria

1. Building against a UMB-aware backend with `featPerMethodXPlexusSource = true` causes hub-codegen to be invoked with `--feature-per-method-source`
2. Building against a backend with the feature absent causes hub-codegen to be invoked WITHOUT that flag
3. Both invocations succeed and produce valid TS output
4. Debug log (`--debug`) shows the resolved feature set being passed

## Coordination

- Blocked by UMB-5
- Unlocks UMB-7 (hub-codegen actually branches on the new flag)

## Completion

Implementor adds the IR-decode + flag-passing logic in `SynapseCC.Pipeline`, tests against uscis, commits.
