---
id: REQ-S10
title: "Spike: Haskell PluginSchema decodes psRequest field losslessly"
status: Complete
type: spike
blocked_by: []
unlocks: [REQ-5]
---

**Result (2026-04-22): PASS — 10/10 assertions.** `psRequest :: Maybe Value` added to `Plexus.Schema.Recursive.PluginSchema`. Test suite `schema-decode-test` lives in `synapse/test/SchemaDecodeSpec.hs` and runs via `cabal test schema-decode-test`. `x-plexus-source` extension nodes survive roundtrip on cookie/header/derived field types. REQ-5 is unblocked.

## Question

Can the Haskell `PluginSchema` type decode a JSON wire schema with an optional `request` field (containing arbitrary JSON Schema with `x-plexus-source` extensions) into `psRequest :: Maybe Value`, and round-trip it through `encode . decode` without loss of any extension fields?

## Background

REQ-5 adds `psRequest :: Maybe Value` to the Haskell `PluginSchema` type so synapse can read the activation-level request schema and render help text. This spike validates the Aeson decoding before REQ-5 implementation begins. Originally listed as program S-10 inside REQ-0; promoted to its own ticket because the implementation lives in synapse, not the plexus-transport spike workspace.

## Setup

Add a test file at `synapse/test/SchemaDecodeSpec.hs` (or extend the existing test module). Use `aesonQQ` quasi-quotes for fixture JSON. Three fixtures:

1. Schema with full `request` block — `auth_token` cookie + `origin` header + `peer_addr` derived
2. Schema with no `request` field at all (legacy backend before REQ-4)
3. Schema with `request: {}` (empty object — edge case)

Add `psRequest :: Maybe Value` to `PluginSchema` with `(.:?)` parser. Verify decoding under each fixture.

## Pass condition

All four assertions hold under `cabal test`:

1. Fixture 1 decodes to `psRequest = Just _` and the decoded value's `properties.auth_token.x-plexus-source.from` is `String "cookie"`
2. Fixture 2 decodes to `psRequest = Nothing`
3. Fixture 3 decodes to `psRequest = Just (Object empty)`
4. `decode (encode schema) == Just schema` for all three fixtures (lossless round-trip)

## Fail → next

If `psRequest` round-trip drops `x-plexus-source` extension nodes: investigate whether a `Generic`-derived `ToJSON` instance is interfering. Replace with hand-written `toJSON` that preserves the raw `Value`.

## Fail → fallback

If `Maybe Value` cannot preserve arbitrary JSON Schema extensions for any reason, introduce a typed wrapper `RequestSchema { rsRaw :: Value }` with explicit `FromJSON`/`ToJSON` instances that pass the value through verbatim. Update REQ-5 to reference the wrapper instead of bare `Maybe Value`.

## Out of scope

- Any synapse renderer changes (those land in REQ-5)
- Any change to plexus-protocol or plexus-core (those land elsewhere in the REQ epic)
- Validation of the schema's structural correctness (synapse trusts the wire schema)
