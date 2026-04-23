---
id: UMB-5
title: "Synapse decodes capabilities + stamps into IR"
status: Pending
type: implementation
blocked_by: [UMB-4]
unlocks: [UMB-6]
severity: Medium
---

## Problem

Once backends advertise capabilities via `_info`, synapse needs to decode them and surface them in the generated IR so downstream tools (synapse-cc, hub-codegen, custom audit tools) can branch on them without re-querying the backend.

## Required behavior

Synapse, when fetching `_info` for IR generation:

1. Decodes the `capabilities` field if present into a Haskell `Capabilities` mirror type
2. Stores it on the IR root as `irCapabilities :: Maybe Capabilities`
3. Encodes it on serialization so the wire IR carries it through to consumers

Haskell type matches the Rust shape:

```haskell
data Capabilities = Capabilities
  { capPlexusRpcVersion       :: Text
  , capPlexusCoreVersion      :: Text
  , capPlexusMacrosVersion    :: Text
  , capPlexusTransportVersion :: Text
  , capWireFormatVersion      :: Text
  , capFeatures               :: Features
  }

data Features = Features
  { featPerMethodXPlexusSource    :: Bool
  , featTypedJsonRpcErrorCodes    :: Bool
  , featCookieAuthMarkerTransport :: Bool
  , featRequestFieldLocking       :: Bool
  , featStartupAuthAssertion      :: Bool
  }
```

All fields default to `False` / empty when missing on the wire (handles pre-UMB backends gracefully).

## What must NOT change

- IR JSON wire format is additive: `irCapabilities: null` for legacy backends, populated for UMB-aware backends
- Synapse renderer behavior: capabilities don't affect human-readable output unless future renderer work decides to surface them
- The existing `psRequest` / `irPluginRequests` paths are independent

## Acceptance criteria

1. `synapse uscis -i` (emit IR) against a UMB-aware backend produces IR JSON with a populated `irCapabilities` block
2. The same against a pre-UMB backend produces `irCapabilities: null` (and synapse continues to work)
3. The Haskell `Capabilities` type round-trips losslessly via Aeson
4. Capabilities are visible in `synapse-cc build typescript <bk> --debug` debug output for verification

## Coordination

- Blocked by UMB-4
- Unlocks UMB-6 (synapse-cc reads the IR's capabilities and passes flags downstream)

## Completion

Implementor adds the Haskell types + decoder + IR field, tests against uscis, commits.
