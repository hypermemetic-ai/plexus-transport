---
id: RED-S12
title: "Spike: stdio transport — auth posture"
status: Complete
type: spike
blocked_by: []
unlocks: []
---

## Verdict (2026-04-22)

**SAFE with documentation gap.**

stdio dispatch invokes `Activation::call` with `auth = None`. Same shape as MCP and REST: on any `#[from_auth]` method the macro wrapper fails-closed. Substrate's stdio mode does not currently deploy activations with `#[from_auth]` methods, so the failure mode is theoretical in-tree. RED-4's build-time check would catch a future deploy.

Remaining concern is the same as MCP (RED-S11): the stdio trust model ("subprocess already authenticated by the parent process / OS") is not documented. A future dev exposing a mixed activation over stdio hits the same 401-on-everything surprise and may ship without gates. Covered under RED-12's broader "document non-WS trust models" mitigation.

## Question

The stdio transport (line-delimited JSON-RPC, used by substrate for local agent processes and by MCP for subprocess integration) doesn't involve HTTP headers, cookies, or any upgrade. What does `#[from_auth]` do when the method is called over stdio?

- (a) Always fails-closed (AuthContext never populated)
- (b) Silently succeeds with default/empty AuthContext
- (c) The stdio transport is intentionally treated as trusted (process already authenticated by the OS) and auth is bypassed by design

## Setup

1. Read `plexus-transport/src/stdio.rs`. Trace how `Activation::call` gets invoked — is `auth` always None?
2. Check whether substrate's stdio mode deploys activations with `#[from_auth]` methods. If so, every call to such a method via stdio would either 401 (fail-closed) or silently succeed — which is it?
3. Does stdio have any auth hooks (session validator, API key)?

## Pass condition

Spike **passes** (= hole confirmed) if stdio dispatch succeeds with `AuthContext = None` for methods that declare `#[from_auth]` — AND this isn't documented as intentional.

Spike **fails** (= safe) if either:
- Methods fail-closed on stdio (meaning stdio mode is only usable for public activations — which is limiting but consistent)
- stdio has an opt-in auth mechanism

## Fail → next

If confirmed + undocumented: file a ticket to document the stdio trust model explicitly. If deployment pattern is "stdio = trusted subprocess," that needs to be loud in docs.

## Out of scope

- stdio performance characteristics
- Non-JSON-RPC stdio protocols
