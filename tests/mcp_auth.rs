//! PLX-17 acceptance: the MCP gateway threads a per-user `AuthContext` into
//! dispatch instead of the hard-coded `None`.
//!
//! Two seams are exercised directly (rmcp's `RequestContext` is transport
//! plumbing we don't own and can't synthesize):
//!   1. `resolve_auth` — a validated Cookie/Bearer becomes an `AuthContext`
//!      (Cookie wins, then Bearer), exactly as `call_tool` derives it.
//!   2. `dispatch` — a resolved identity reaches the handler through `call`
//!      (public-read observes it; a force-auth method fails closed on `None`).
//!
//! Run: `cargo test -p plexus-transport --features mcp-gateway --test mcp_auth`

use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use jsonrpsee::core::server::Methods;
use plexus_core::plexus::{
    types::{PlexusStreamItem, StreamMetadata},
    Activation, AuthContext, PlexusError, PlexusStream, SessionValidator,
};
use plexus_core::request::RawRequestContext;
use serde_json::Value;

use plexus_transport::mcp::bridge::{testing, ActivationMcpBridge};

// ---------------------------------------------------------------------------
// Fixtures (hand-rolled — this crate aliases `schemars` to a no-op shim, so the
// `#[activation]` macro can't be used; `type Methods` reuses `HealthMethod`
// purely to satisfy the associated-type bound).
// ---------------------------------------------------------------------------

struct SecureApi;

fn one_string(s: String) -> PlexusStream {
    Box::pin(stream! {
        yield PlexusStreamItem::Data {
            metadata: StreamMetadata::new(vec!["secure".into()], "h".into()),
            content_type: "text".into(),
            content: Value::String(s),
        };
        yield PlexusStreamItem::Done {
            metadata: StreamMetadata::new(vec!["secure".into()], "h".into()),
        };
    })
}

#[async_trait]
impl Activation for SecureApi {
    type Methods = plexus_core::activations::health::HealthMethod;

    fn namespace(&self) -> &str {
        "secure"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn methods(&self) -> Vec<&str> {
        vec!["whoami", "admin"]
    }

    async fn call(
        &self,
        method: &str,
        _params: Value,
        auth: Option<&AuthContext>,
        _raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        match method {
            "whoami" => {
                let label = match auth {
                    Some(ctx) => format!("some:{}", ctx.user_id),
                    None => "none".to_string(),
                };
                Ok(one_string(label))
            }
            "admin" => match auth {
                Some(ctx) => Ok(one_string(ctx.user_id.clone())),
                None => Err(PlexusError::Unauthenticated(
                    "admin requires authentication".to_string(),
                )),
            },
            other => Err(PlexusError::MethodNotFound {
                activation: "secure".to_string(),
                method: other.to_string(),
            }),
        }
    }

    fn into_rpc_methods(self) -> Methods {
        Methods::new()
    }
}

#[derive(Clone, Default)]
struct TestValidator;

#[async_trait]
impl SessionValidator for TestValidator {
    async fn validate(&self, input: &str) -> Option<AuthContext> {
        if input.contains("valid") {
            Some(AuthContext::new(
                "alice".into(),
                "sess-1".into(),
                vec!["user".into()],
                Value::Null,
            ))
        } else {
            None
        }
    }
}

fn bridge_with_validator() -> ActivationMcpBridge<SecureApi> {
    ActivationMcpBridge::new(Arc::new(SecureApi))
        .with_session_validator(Arc::new(TestValidator) as Arc<dyn SessionValidator>)
}

fn parts(headers: &[(&str, &str)]) -> http::request::Parts {
    let mut b = http::Request::builder().method("GET").uri("/mcp");
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    b.body(()).unwrap().into_parts().0
}

async fn first_string(mut stream: PlexusStream) -> Option<String> {
    while let Some(item) = stream.next().await {
        if let PlexusStreamItem::Data { content, .. } = item {
            if let Value::String(s) = content {
                return Some(s);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// resolve_auth: validated session/Bearer → AuthContext
// ---------------------------------------------------------------------------

#[tokio::test]
async fn resolve_auth_from_cookie() {
    let bridge = bridge_with_validator();
    let p = parts(&[("cookie", "access_token=valid")]);
    let auth = testing::resolve_auth(&bridge, Some(&p)).await;
    assert_eq!(
        auth.map(|c| c.user_id),
        Some("alice".to_string()),
        "PLX-17: a valid Cookie must resolve to a per-user AuthContext",
    );
}

#[tokio::test]
async fn resolve_auth_from_bearer() {
    let bridge = bridge_with_validator();
    let p = parts(&[("authorization", "Bearer valid-token")]);
    let auth = testing::resolve_auth(&bridge, Some(&p)).await;
    assert_eq!(auth.map(|c| c.user_id), Some("alice".to_string()));
}

#[tokio::test]
async fn resolve_auth_none_without_credential() {
    let bridge = bridge_with_validator();
    let p = parts(&[]);
    assert!(
        testing::resolve_auth(&bridge, Some(&p)).await.is_none(),
        "no credential → anonymous (None)",
    );
}

#[tokio::test]
async fn resolve_auth_none_without_validator() {
    // No validator configured → never resolves an identity, even with a cookie.
    let bridge = ActivationMcpBridge::new(Arc::new(SecureApi));
    let p = parts(&[("cookie", "access_token=valid")]);
    assert!(testing::resolve_auth(&bridge, Some(&p)).await.is_none());
}

// ---------------------------------------------------------------------------
// dispatch: the resolved identity reaches the handler
// ---------------------------------------------------------------------------

/// Core PLX-17 claim: an authed MCP call reaches the handler with `Some(auth)`.
#[tokio::test]
async fn authed_dispatch_threads_identity_to_handler() {
    let bridge = ActivationMcpBridge::new(Arc::new(SecureApi));
    let auth = Some(Arc::new(AuthContext::new(
        "alice".into(),
        "s".into(),
        vec![],
        Value::Null,
    )));
    let stream = testing::dispatch(&bridge, "secure.whoami", serde_json::json!({}), auth, None)
        .await
        .expect("dispatch ok");
    assert_eq!(
        first_string(stream).await.as_deref(),
        Some("some:alice"),
        "PLX-17: the MCP gateway must thread Some(auth) into Activation::call",
    );
}

/// Anonymous MCP call to a public-read method still succeeds, observing None.
#[tokio::test]
async fn anonymous_dispatch_reaches_public_method_as_none() {
    let bridge = ActivationMcpBridge::new(Arc::new(SecureApi));
    let stream = testing::dispatch(&bridge, "secure.whoami", serde_json::json!({}), None, None)
        .await
        .expect("dispatch ok");
    assert_eq!(first_string(stream).await.as_deref(), Some("none"));
}

/// A required-auth MCP method returns Unauthenticated when dispatched anonymously.
#[tokio::test]
async fn anonymous_dispatch_to_force_auth_method_is_unauthenticated() {
    let bridge = ActivationMcpBridge::new(Arc::new(SecureApi));
    match testing::dispatch(&bridge, "secure.admin", serde_json::json!({}), None, None).await {
        Err(PlexusError::Unauthenticated(_)) => {}
        Err(other) => panic!("expected Unauthenticated, got {other:?}"),
        Ok(_) => panic!("force-auth method must fail closed on anonymous dispatch"),
    }
}
