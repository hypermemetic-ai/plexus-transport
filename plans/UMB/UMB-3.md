---
id: UMB-3
title: "Define `Capabilities` struct + `CAPABILITIES` const in plexus-rpc"
status: Pending
type: implementation
blocked_by: [UMB-2]
unlocks: [UMB-4]
severity: Medium
---

## Problem

The umbrella crate needs to expose a typed, serializable manifest describing both bundled subcrate versions AND named feature flags that downstream tooling can branch on. Version strings alone are not enough — they require tooling to know which features ship in which versions, which is brittle.

## Required behavior

`plexus-rpc/src/lib.rs` defines:

```rust
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Capabilities {
    pub plexus_rpc_version:       &'static str,
    pub plexus_core_version:      &'static str,
    pub plexus_macros_version:    &'static str,
    pub plexus_transport_version: &'static str,
    pub wire_format_version:      &'static str,
    pub features: Features,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Features {
    /// REQ-6: methods carry x-plexus-source on params
    pub per_method_x_plexus_source: bool,
    /// SAFE-5: synapse decodes -32001/-32602/-32601/-32000 with semantic prefixes
    pub typed_jsonrpc_error_codes: bool,
    /// SAFE-7: generated TS transport carries cookie-auth marker, no URL tokens
    pub cookie_auth_marker_transport: bool,
    /// REQ-10: activation `required(...)` field-locking enforced at compile time
    pub request_field_locking: bool,
    /// RED-4: TransportServer::build() rejects auth-gated activations without auth middleware
    pub startup_auth_assertion: bool,
}

pub const CAPABILITIES: Capabilities = Capabilities {
    plexus_rpc_version:       env!("CARGO_PKG_VERSION"),
    plexus_core_version:      plexus_core::VERSION,
    plexus_macros_version:    plexus_macros::VERSION,
    plexus_transport_version: plexus_transport::VERSION,
    wire_format_version:      "1.0",
    features: Features {
        per_method_x_plexus_source:    true,
        typed_jsonrpc_error_codes:     true,
        cookie_auth_marker_transport:  true,
        request_field_locking:         true,
        startup_auth_assertion:        true,
    },
};
```

The struct and const are publicly exported. `Capabilities` derives `Serialize` so backends can include it verbatim in `_info`.

## What must NOT change

- `Capabilities` is `Copy` so it can be used as a const value without lifetime gymnastics
- All field types are `&'static str` or `bool` — no allocations needed
- Field names match a stable convention (snake_case JSON output via serde defaults)

## Acceptance criteria

1. `plexus_rpc::CAPABILITIES.plexus_macros_version` returns `plexus_macros`'s version string at compile time
2. Each `Features` flag corresponds to a real feature; flags are added when features ship, never silently flipped
3. `serde_json::to_string(&plexus_rpc::CAPABILITIES)` produces valid JSON suitable for embedding in `_info`
4. Adding a new feature flag is a one-field addition, doesn't break consumers (serde defaults handle missing-field deserialization gracefully)

## Coordination

- Blocked by UMB-2 (needs the `VERSION` consts)
- Unlocks UMB-4 (which embeds CAPABILITIES into `_info`)
- Future feature flags get added here; the change is additive

## Completion

Implementor adds the struct + const + serde derives; runs cargo build; commits.
