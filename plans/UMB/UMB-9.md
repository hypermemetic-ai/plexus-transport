---
id: UMB-9
title: "End-to-end integration test in plexus-rpc/tests/"
status: Pending
type: implementation
blocked_by: [UMB-1, UMB-3]
unlocks: []
severity: High
---

## Problem

Today nothing in framework CI exercises `plexus-core` × `plexus-macros` × `plexus-transport` simultaneously. Each subcrate's tests cover its own surface; cross-crate compatibility is verified only by FormVeritasV2 (a downstream consumer with its own release cadence). When framework crates drift apart (e.g., REQ-6 lands in plexus-macros 0.5.6 but plexus-core 0.4.x is still on the registry), the breakage surfaces in consumers, not in the framework's own test suite.

## Required behavior

`plexus-rpc/tests/end_to_end.rs` declares a real activation, builds a `TransportServer`, optionally starts it on a test port, and asserts that the schema introspection round-trips correctly. The test forces all three subcrates to compose at framework CI time — any drift breaks the test, not downstream consumers.

Minimal shape:

```rust
use plexus_rpc::prelude::*;

#[derive(PlexusRequest)]
struct TestReq {
    #[from_cookie("access_token")]
    auth_token: String,
}

#[derive(Clone)]
struct TestUser;

struct TestActivation { db: () }

#[activation(namespace = "test", request = TestReq, crate_path = "plexus_rpc::core")]
impl TestActivation {
    async fn validate(&self, _: &AuthContext) -> Result<TestUser, PlexusError> { Ok(TestUser) }

    #[method]
    async fn ping(&self, #[from_auth(self.validate)] _u: TestUser)
        -> impl futures::stream::Stream<Item = String> + Send + 'static
    {
        async_stream::stream! { yield "pong".into(); }
    }
}

#[tokio::test]
async fn full_stack_compiles_and_introspects() {
    let activation = std::sync::Arc::new(TestActivation { db: () });
    let server = TransportServer::builder(activation, /* rpc_converter */ todo!())
        .with_session_validator(/* test validator */ todo!())
        .build().await.unwrap();
    // Don't actually serve; just verify the build path passes RED-4 and produces a schema.
    let schema = /* extract from server */ todo!();
    assert_eq!(schema.namespace, "test");
}
```

## What must NOT change

- The test runs as part of `cargo test -p plexus-rpc`, no special harness
- The test does not actually bind a port or sleep — it exercises construction + schema introspection only
- A failure in this test is a framework-CI signal that the subcrates have drifted

## Acceptance criteria

1. `cargo test -p plexus-rpc --test end_to_end` passes against the current pinned subcrate versions
2. Bumping any subcrate's version in `plexus-rpc/Cargo.toml` to an incompatible release causes the test to fail at compile or runtime — caught in framework CI
3. The test's activation uses `#[from_auth]` so RED-4's startup assertion is exercised on the build path (with auth middleware configured)
4. Capabilities const usage in the test verifies CAPABILITIES is reachable through the umbrella

## Coordination

- Blocked by UMB-1 (crate scaffold) + UMB-3 (Capabilities const must exist)
- Should land before UMB-10 — gives uscis migration a known-good integration point to reference

## Completion

Implementor writes the test with placeholder rpc_converter / session validator, makes it compile, asserts on a meaningful invariant, commits.
