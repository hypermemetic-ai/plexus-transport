---
id: DISP-6
title: "In-process gateway (library mode)"
status: Pending
type: task
blocked_by: [DISP-1, DISP-3]
unlocks: [DISP-7]
---

## Context

Once `Dispatcher` owns the invariant dispatch work, calling a Plexus activation from Rust without any network transport is a 50-line wrapper. This is the shape the UMB epic (umbrella `plexus-rpc` crate) needs — users who want to drive an activation from a Rust test, a library caller, or an embedded context get the same posture guarantees as a networked caller, with zero serialization overhead.

## Goal

Expose `Dispatcher::dispatch` as a public library API with an ergonomic wrapper. Library-mode callers can construct a `RawRequestContext` programmatically (to drive `#[from_cookie]` / `#[from_header]` extractors deterministically in tests) or pass `RawRequestContext::empty()` for posture-bypass on public methods.

## Acceptance

- [ ] `InProcessGateway` type (likely in `plexus-transport` or re-exported via `plexus-rpc` / UMB):
  ```rust
  pub struct InProcessGateway {
      dispatcher: Arc<Dispatcher>,
  }
  impl InProcessGateway {
      pub async fn call(
          &self,
          method: &str,
          params: Value,
          ctx: RawRequestContext,
      ) -> Result<PlexusStream, PlexusError>;
  }
  ```
- [ ] Construction path from `TransportServerBuilder` — probably `.build_in_process()` returns the gateway without starting any network listener.
- [ ] Posture check runs — auth-gated methods with no auth fail-closed, same as any transport. This is the posture guarantee we want: library mode is not auto-bypass.
- [ ] `InProcessGateway::call_with_auth(method, params, auth)` convenience for tests where the caller has already resolved an `AuthContext` and wants to skip the AuthChain.
- [ ] Tests:
  - Call a public method, get a stream, collect items
  - Call an auth-gated method without auth → `PlexusError::AuthenticationError`
  - Call an auth-gated method via `call_with_auth(Some(ctx))` → succeeds
  - Call a `#[from_cookie("sid")]` method with a `RawRequestContext` carrying that cookie → extractor succeeds
- [ ] Doc: short "library mode" section in `plexus-transport` or `plexus-rpc` README.

## Out of scope

- A macro for auto-generating test helpers per activation (nice-to-have; separate ticket if demand materializes).
- Serialization-free fast path (calling Rust impl directly without `serde_json::Value` round-trip) — optimization, not correctness.

## Notes

The UMB epic is where this pays off: `plexus-rpc` users who want to write `let result = backend.my_method(args).await` from an embedded context get a typed wrapper generated from the activation schema over this gateway. That codegen is UMB's problem; this ticket just provides the dispatch seam it needs.

Blocked on DISP-3 specifically because library callers should not be able to regress onto the 2-arg `Activation::call` form. Once that's deleted, `InProcessGateway::call` is the only sanctioned library entry point.
