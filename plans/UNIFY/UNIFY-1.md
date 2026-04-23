---
id: UNIFY-1
title: "UNIFY — Activations as callable RPCs"
status: Idea
type: epic
blocked_by: []
unlocks: []
---

## Goal

Make activations and methods *non-distinct at the caller level*. Today an activation is a static namespace: `clients` is just a routing prefix and only `clients.list` etc. are callable. Under UNIFY, calling `clients(args)` is a first-class operation that returns a typed handle, and methods become continuations on that handle. The compositional shape `clients({tenant: "acme"}).list({search: "foo"})` becomes the canonical wire model.

The destination resolves several long-standing awkwardnesses:

- **Request forwarding** stops being a special "extraction" concept — it *is* the activation's call signature.
- **The `request = ()` per-method override debate** dissolves — methods just declare their own params; the activation hands them a handle that already encodes its params.
- **Handles** (already present as `plexus_core::Handle` and `HandleEnum`) gain a clear semantic role: the typed return of an activation call.
- **Auth posture** becomes an ordinary activation-call requirement — `auth_required` is just a `#[from_auth]` param on the activation's `activate` function.

## Status: Idea

This epic is captured but not ready for implementation. The design has open questions (below), the cost is substantial (touches macros, core, protocol, synapse, hub-codegen, every existing activation), and the intermediate REQ-6 model gets us most of the way without committing to the full architectural shift.

REQ-6 is a strict subset of UNIFY's eventual shape: per-method `x-plexus-source` annotations and merged param schemas survive intact under UNIFY. Implementing REQ-6 is therefore not wasted work — it's the substrate UNIFY will build on.

When this epic is ready, promote to `Epic` status and break into individual implementation tickets per the dependency DAG below.

## Dependency DAG (sketch)

```
REQ-6 (per-method param injection)  ───┐
                                       ▼
UNIFY-S01 (spike: Activation trait can dispatch its own call)
                                       │
                                       ▼
UNIFY-2 (plexus-core: Activation gains `activate` method;
         PluginSchema gains `params`/`returns` for the activation itself)
                                       │
                ┌──────────────────────┼──────────────────────┐
                ▼                      ▼                      ▼
         UNIFY-3              UNIFY-4                 UNIFY-5
   plexus-macros emits        synapse renders         hub-codegen TS
   activate handler from      activations as          exposes activations
   activation attrs;          callable; CLI does      as factory functions;
   merges with method         two-stage navigation    methods as members on
   schemas                                            the returned handle
                                       │
                                       ▼
                           UNIFY-6 (migration:
                           existing activations get
                           an `activate` shim;
                           remove `psRequest` wire field)
```

## Phase Breakdown

### Phase 1 — Substrate (already underway)
REQ-6: per-method merged param schemas with `x-plexus-source` annotations. Macro inheritance from activation's request struct. Field-level `required` locking. This phase exists independently of UNIFY and ships first.

### Phase 2 — Activation as callable
UNIFY-S01 (spike) verifies that `plexus_core::Activation` can carry its own dispatch handler (in addition to per-method handlers) without breaking the existing trait shape.

UNIFY-2: extend `PluginSchema` with `params: Option<Value>` (the activation's own call schema) and `returns: Option<Value>` (the handle type schema). Add a `callable: bool` discriminator if needed. Keep `psRequest` for backward compat during migration; remove in UNIFY-6.

UNIFY-3: `#[plexus::activation]` macro generates an `activate` handler from the activation's attribute config (`request = X`, `auth = required`, `required = [...]`). The handler runs extractors, validates auth, returns a typed handle. Methods receive the handle as their first parameter (or implicitly via context).

### Phase 3 — Caller surfaces
UNIFY-4: synapse navigation treats activations as terminal callable nodes when invoked without a method path. CLI grows two-stage: `synapse uscis clients --tenant acme list --search foo`.

UNIFY-5: hub-codegen TS clients expose activations as factory functions. `client.clients({tenant: "acme"}).list({search: "foo"})`. The handle type carries the methods.

### Phase 4 — Migration
UNIFY-6: existing activations get a default `activate` shim (returns a unit-handle, preserves current behavior). `psRequest` wire field removed. Activations explicitly opt into the callable surface via `params = Type` declaration; legacy ones still work.

## Out of scope

- Removing the existing namespace-routing model (activations are still routed by namespace; UNIFY just makes them callable too)
- Multi-stage method chains (e.g., `clients().filter().list()`) — out of scope; methods are leaf-callable continuations
- Streaming the activation call itself (today methods can stream; should the activation call?) — open question, not committed
- Changing the wire format for method invocation in fundamental ways — only `params` and `returns` get added at the activation level

## Open design questions

**Q1 — Activation `activate` declaration.** Is it written explicitly by the dev as a regular `async fn activate(&self, ...) -> Self::Handle`? Or is it inferred entirely from activation attrs (`request = X` becomes `activate(req: X) -> Handle`)? Probably both: explicit override beats inferred default.

**Q2 — Handle type lifecycle.** Does the handle live for one method call (like `Self`) or can a client hold it across multiple calls (like a session)? Single-call is simpler; multi-call requires server-side session state. Start single-call; add multi-call as a follow-up if needed.

**Q3 — `#[activation_param]` in the new model.** Today it pulls a field from the activation's request struct by name. Under UNIFY, the handle IS the activation's params, so methods access them via the handle (`handle.user`, `handle.client_ip`). `#[activation_param]` becomes redundant or a sugar over `handle.fieldName`.

**Q4 — Auth enforcement boundary.** Is auth checked in the activate handler (which receives `#[from_auth(resolver)]`) or by middleware before the activate handler runs? Both. Middleware populates AuthContext; activate handler optionally consumes it via `#[from_auth]`. If activation declares auth as required and method doesn't consume it, the handler must still validate (compile-time check that AuthContext is referenced).

**Q5 — Override semantics post-UNIFY.** Earlier discussion ("we are allowed to override the default request if the activation allows it; explicit allows if required=true") translates to: a method can override the handle's apparent params by declaring its own `#[from_auth]` etc. The activation's `required = [...]` list constrains what methods can shadow. Pin syntax during UNIFY-3.

**Q6 — Mixing auth-required and auth-optional methods in one activation.** Probably refactoring pressure: split into two activations. Pin during migration planning.

**Q7 — Schema for side-effect-only handle fields.** When activate extracts `origin: ValidOrigin` purely for validation (no method consumes it), should the handle's schema still surface it? Yes — useful documentation. Tag with `x-plexus-source: { from: "derived", consumed: false }` or similar.

**Q8 — How does a backwards-compatible `psRequest` shim work during UNIFY-6 migration?** Backends declaring `request = X` get an auto-generated `activate` whose params are `X`'s fields; `psRequest` is computed from the activate signature. No source-code change required for FormVeritasV2-style activations.

## Cost estimate

UNIFY is ~1-2 weeks of careful work spread across:
- plexus-core (Activation trait extension, PluginSchema fields)
- plexus-macros (activate handler generation)
- plexus-protocol (Haskell PluginSchema mirror)
- synapse (navigation, CLI two-stage parsing, IR captures activation params + returns)
- synapse-cc (no major change beyond passing IR through)
- hub-codegen (TS factory function emission, handle types)
- Migration: every existing activation in every consumer (FormVeritasV2, etc.) tested

REQ-6 (the substrate) is ~1-2 days. Building UNIFY on top is the bulk of the rest.

## Recommendation

Park this epic at `Idea`. Implement REQ-6 next session. Revisit UNIFY when REQ-6 is Complete and there's appetite for a multi-day architectural lift. Before then, this ticket is the canonical record of what the model SHOULD become.
