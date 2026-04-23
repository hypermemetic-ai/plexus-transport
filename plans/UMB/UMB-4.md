---
id: UMB-4
title: "Backend embeds `CAPABILITIES` in `_info` response"
status: Pending
type: implementation
blocked_by: [UMB-3]
unlocks: [UMB-5]
severity: Medium
---

## Problem

The `_info` endpoint (registered in plexus-core's `Plexus::new`) currently returns just the backend name. To realize UMB's capability-negotiation goal, `_info` needs to include the `CAPABILITIES` blob.

## Required behavior

`_info`'s response payload changes from a bare string to a JSON object:

```json
{
  "name": "uscis",
  "capabilities": {
    "plexus_rpc_version": "0.1.0",
    "plexus_core_version": "0.5.2",
    "plexus_macros_version": "0.5.6",
    "plexus_transport_version": "0.2.2",
    "wire_format_version": "1.0",
    "features": {
      "per_method_x_plexus_source": true,
      "typed_jsonrpc_error_codes": true,
      "cookie_auth_marker_transport": true,
      "request_field_locking": true,
      "startup_auth_assertion": true
    }
  }
}
```

Backends that don't depend on `plexus-rpc` (i.e., still use the subcrates directly) emit `_info` with no `capabilities` field — clients must treat it as absent / pre-UMB.

## Implementation strategy

The cleanest path: `plexus-core` exposes a hook `set_info_capabilities(value: serde_json::Value)` on the Plexus builder. `plexus-rpc` re-exports a wrapper that calls it with `serde_json::to_value(CAPABILITIES).unwrap()` automatically when the backend builder is constructed.

Alternative: have `plexus-rpc` provide its own `Plexus::new_with_capabilities()` constructor that wraps plexus-core's. Less intrusive but adds API surface.

Either way, the wire format stays additive — old clients reading `_info` see the new field but ignore it; new clients look for it.

## What must NOT change

- The notification method name (`PLEXUS_NOTIF_METHOD`) is unchanged
- The single-item-stream + `done` event structure is unchanged
- Backends not using `plexus-rpc` continue to emit the bare-name `_info` — no breakage

## Acceptance criteria

1. A backend built via `plexus-rpc` emits `_info` content with both `name` and `capabilities` fields
2. The `capabilities` field deserializes back into a `plexus_rpc::Capabilities` struct without loss
3. A backend NOT using `plexus-rpc` emits `_info` with no `capabilities` field; clients treat it as `None`
4. synapse can call `_info` and round-trip the full payload (verifies decoding)

## Coordination

- Blocked by UMB-3
- Unlocks UMB-5 (synapse decodes the field)

## Completion

Implementor lands the hook in plexus-core (or the wrapper in plexus-rpc), exercises against uscis, commits.
