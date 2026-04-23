---
id: REQ-8
title: "synapse renderer shows per-method auth and source annotations"
status: Complete
type: implementation
blocked_by: [REQ-6]
unlocks: []
severity: Medium
---

**Implemented Apr 23 2026 (autonomous run):** `Synapse.Algebra.Render.renderParamDoc`
now extracts `x-plexus-source` from each param's JSON Schema and renders
server-sourced params inline with a `← <source>` label instead of the
`--flag` form. Auth/cookie/header/query/derived sources each produce a
distinct label (`auth: <resolver>`, `cookie <key>`, `server-derived`,
etc.). RPC params render unchanged.

Example output:

    list    List things
      --search <string>?
      user                ← auth: self.db.validate_user
      origin              ← server-derived

Tested via `test/Req8RenderSpec.hs` (8 assertions against a synthetic
PluginSchema with three param source types). `cabal test req8-render-test`
passes.

## Problem

REQ-5 landed activation-level rendering: when a user runs `synapse <bk> <plugin>`, they see a "Request requirements:" block listing the activation's extractor fields (origin / transport / etc). That's useful but blunt — it doesn't tell the user *which methods* in the plugin require auth or which resolver guards them, and it lies about methods that override the activation's request.

Once REQ-6 lands, every method's `MethodSchema.params` carries `x-plexus-source` per param. The renderer should use that to give per-method precision.

## Goal

`synapse <bk> <plugin>` output shows, for each method, an inline annotation listing the method's non-RPC sources. Example:

```
clients v1.0.0
  Client management

methods

  list        List clients
    search    (optional)        ← rpc
    user                        ← auth: self.db.validate_user
    origin                      ← server-derived (ValidOrigin)
    transport                   ← server-derived
    client_ip                   ← server-derived

  get         Get a client by ID
    id        (required)        ← rpc
    user                        ← auth: self.db.validate_user
    origin                      ← server-derived (ValidOrigin)
    ...
```

Methods that have no auth and no derived params look identical to today.

## Required behavior

For each method in the plugin schema:

- Emit one line per param: `name [source-annotation]`
- RPC-sourced params include `(optional)` / `(required)` marker based on whether the method's `required` array contains them
- Non-RPC params NEVER show `(required)` (they're populated by the server) — show `← auth: <resolver>` or `← server-derived` or `← cookie <name>` etc.
- Methods whose `MethodSchema.params` schema has no `properties` (zero params) render as today (just the method name + description)

## What must NOT change

- Methods without any `x-plexus-source` annotations (pre-REQ-6 schemas, or activations without `request = ...`) render byte-identical to today
- The activation-level "Authentication required" notice (REQ-5) can remain, but now its trigger should move from "activation psRequest has required access_token cookie" to "any method in the plugin has `x-plexus-source.from = auth`". Update the trigger when REQ-8 lands.
- JSON output (`--json`) is unchanged — this is a human-readable renderer change only

## Risks

1. **Width constraints:** per-param lines add vertical density. If a method has many params, the output gets tall. Acceptable trade-off — users can still grep or use `--json` for programmatic access.

2. **Coordination with REQ-5's activation-level block:** REQ-5's "Request requirements:" block may become redundant once per-method annotations exist. Decision: keep the activation-level block as a summary; the per-method annotations are the detail. Implementor tightens the overlap during this work.

## Acceptance criteria

1. `synapse <bk> <plugin>` against a backend whose methods have `#[from_auth(expr)]` params shows `← auth: <expr>` on those params in the method's rendering.
2. `synapse <bk> <plugin>` against uscis shows `← server-derived` on `origin`, `transport`, `client_ip` for each method in a `request = FormVeritasRequest` activation.
3. `synapse <bk> <plugin>` against a method with only RPC params shows output byte-identical to today (modulo pre-REQ-6 schemas, in which case the renderer degrades gracefully).
4. The "Authentication required (use --token...)" notice fires whenever any method in the plugin has `x-plexus-source.from == "auth"`, not just when the psRequest has a cookie field.
5. `--json` output is unchanged.

## Coordination

- `blocked_by: [REQ-6]` — needs per-method `x-plexus-source` annotations to exist in the schema
- Synapse renderer currently in `src/Synapse/Algebra/Render.hs`. The existing `renderRequestSchemaDoc` (REQ-5) is augmented, not replaced — activation-level block stays as a summary; new per-method logic renders the detail.

## Completion

Implementor augments `Render.hs` to walk `MethodSchema.params` and emit source annotations. Manual verification against uscis (post-REQ-6). Flips status to Complete.
