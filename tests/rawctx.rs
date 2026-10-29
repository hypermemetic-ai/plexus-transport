//! PLX-18 acceptance: every gateway threads a populated `RawRequestContext`
//! (origin/headers) into dispatch, so the framework's origin/CSRF machinery
//! (`ValidOrigin`) is reachable end-to-end.
//!
//! Coverage:
//!   - MCP  (default features): raw_ctx reaches the handler; `ValidOrigin`
//!     enforcement fires through the gateway (allowed accepts, disallowed 403s).
//!   - WS   (default features): the upgrade middleware stashes an
//!     `Arc<RawRequestContext>` (carrying the Origin) into request extensions,
//!     where the plexus-core `{ns}.call` subscription picks it up.
//!   - REST (http-gateway): the handler builds a raw_ctx from the request and
//!     the handler observes the request Origin.
//!
//! Run: `cargo test -p plexus-transport` (MCP + WS) and add `--features
//! http-gateway` for the REST case.

use std::sync::{Arc, Once};

use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use jsonrpsee::core::server::Methods;
use plexus_core::plexus::{
    types::{PlexusStreamItem, StreamMetadata},
    Activation, AuthContext, PlexusError, PlexusRequestField, PlexusStream,
};
use plexus_core::request::RawRequestContext;
use plexus_transport::request::{init_allowed_origins, ValidOrigin};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Shared allowlist (OnceLock in ValidOrigin → set once per test binary).
// ---------------------------------------------------------------------------

const GOOD_ORIGIN: &str = "https://good.example";
const BAD_ORIGIN: &str = "https://evil.example";

fn init_origins() {
    static INIT: Once = Once::new();
    INIT.call_once(|| init_allowed_origins(vec![GOOD_ORIGIN.to_string()]));
}

// ---------------------------------------------------------------------------
// Test activation: methods that read the RawRequestContext.
// ---------------------------------------------------------------------------

struct OriginApi;

fn one_string(s: String) -> PlexusStream {
    Box::pin(stream! {
        yield PlexusStreamItem::Data {
            metadata: StreamMetadata::new(vec!["origin_api".into()], "h".into()),
            content_type: "text".into(),
            content: Value::String(s),
        };
        yield PlexusStreamItem::Done {
            metadata: StreamMetadata::new(vec!["origin_api".into()], "h".into()),
        };
    })
}

#[async_trait]
impl Activation for OriginApi {
    type Methods = plexus_core::activations::health::HealthMethod;

    fn namespace(&self) -> &str {
        "origin_api"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn methods(&self) -> Vec<&str> {
        vec!["echo_origin", "guarded"]
    }

    async fn call(
        &self,
        method: &str,
        _params: Value,
        _auth: Option<&AuthContext>,
        raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        match method {
            // Proves the gateway threaded a raw_ctx carrying request headers.
            "echo_origin" => {
                let origin = raw_ctx
                    .and_then(|r| r.headers.get("origin"))
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("<none>")
                    .to_string();
                Ok(one_string(origin))
            }
            // A method guarded by ValidOrigin — enforcement fires iff the
            // gateway threaded the raw_ctx and the origin is disallowed.
            "guarded" => {
                let raw = raw_ctx.ok_or_else(|| {
                    PlexusError::Unauthenticated("no request context".to_string())
                })?;
                let _origin = ValidOrigin::extract_from_raw(raw)?;
                Ok(one_string("guarded-ok".to_string()))
            }
            other => Err(PlexusError::MethodNotFound {
                activation: "origin_api".to_string(),
                method: other.to_string(),
            }),
        }
    }

    fn into_rpc_methods(self) -> Methods {
        Methods::new()
    }
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

fn parts_with_origin(origin: Option<&str>) -> http::request::Parts {
    let mut b = http::Request::builder().method("GET").uri("/mcp");
    if let Some(o) = origin {
        b = b.header("origin", o);
    }
    b.body(()).unwrap().into_parts().0
}

// ===========================================================================
// MCP gateway (default features — mcp module is always compiled)
// ===========================================================================

use plexus_transport::mcp::bridge::{testing as mcp_testing, ActivationMcpBridge};

#[tokio::test]
async fn mcp_threads_raw_ctx_to_handler() {
    let bridge = ActivationMcpBridge::new(Arc::new(OriginApi));
    let parts = parts_with_origin(Some(GOOD_ORIGIN));
    let stream = mcp_testing::call_from_parts(&bridge, Some(&parts), "origin_api.echo_origin", serde_json::json!({}))
        .await
        .expect("dispatch ok");
    assert_eq!(
        first_string(stream).await.as_deref(),
        Some(GOOD_ORIGIN),
        "PLX-18: the MCP gateway must build a RawRequestContext carrying the request Origin",
    );
}

/// origin-enforced through the MCP gateway: an allowed origin passes.
#[tokio::test]
async fn origin_enforced_through_mcp_allows_allowed() {
    init_origins();
    let bridge = ActivationMcpBridge::new(Arc::new(OriginApi));
    let parts = parts_with_origin(Some(GOOD_ORIGIN));
    let stream = mcp_testing::call_from_parts(&bridge, Some(&parts), "origin_api.guarded", serde_json::json!({}))
        .await
        .expect("allowed origin must pass the ValidOrigin gate");
    assert_eq!(first_string(stream).await.as_deref(), Some("guarded-ok"));
}

/// origin-enforced through the MCP gateway: a disallowed origin is rejected.
#[tokio::test]
async fn origin_enforced_through_mcp_rejects_disallowed() {
    init_origins();
    let bridge = ActivationMcpBridge::new(Arc::new(OriginApi));
    let parts = parts_with_origin(Some(BAD_ORIGIN));
    match mcp_testing::call_from_parts(&bridge, Some(&parts), "origin_api.guarded", serde_json::json!({})).await {
        Err(PlexusError::Unauthenticated(msg)) => {
            assert!(msg.contains("evil.example"), "error names the rejected origin: {msg}");
        }
        Err(other) => panic!("expected Unauthenticated for disallowed origin, got {other:?}"),
        Ok(_) => panic!(
            "PLX-18: a disallowed origin must be rejected — origin enforcement is reachable \
             through the MCP gateway"
        ),
    }
}

// ===========================================================================
// WebSocket gateway (default features — websocket module is always compiled)
// The upgrade middleware must stash an Arc<RawRequestContext> (carrying the
// Origin) into request extensions, where plexus-core's {ns}.call subscription
// reads it and dispatches via route_with_ctx.
// ===========================================================================

mod ws {
    use super::*;
    use std::convert::Infallible;
    use bytes::Bytes;
    use http_body_util::Empty;
    use plexus_core::plexus::SessionValidator;
    use tower::Service;

    use plexus_transport::websocket::testing::combined_auth_middleware;

    #[derive(Default, Clone)]
    struct Capture {
        origin: Arc<std::sync::Mutex<Option<Option<String>>>>,
    }

    #[derive(Clone)]
    struct CaptureService {
        cap: Capture,
    }

    impl Service<http::Request<Empty<Bytes>>> for CaptureService {
        type Response = http::Response<jsonrpsee::server::HttpBody>;
        type Error = Infallible;
        type Future = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
        >;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<Empty<Bytes>>) -> Self::Future {
            let cap = self.cap.clone();
            Box::pin(async move {
                // PLX-18: assert the middleware stashed a RawRequestContext and
                // read the Origin it carried.
                let origin = req.extensions().get::<Arc<RawRequestContext>>().map(|raw| {
                    raw.headers
                        .get("origin")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                });
                *cap.origin.lock().unwrap() = Some(origin);
                Ok(http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(jsonrpsee::server::HttpBody::from("ok"))
                    .unwrap())
            })
        }
    }

    fn header_name() -> http::HeaderName {
        http::HeaderName::from_static("x-plexus-api-key")
    }

    #[tokio::test]
    async fn ws_middleware_attaches_raw_ctx_with_origin() {
        let cap = Capture::default();
        let stub = CaptureService { cap: cap.clone() };
        // No api_key, no validator → the plain pass-through branch must still
        // attach the RawRequestContext.
        let mut svc = combined_auth_middleware(
            stub,
            None,
            header_name(),
            None::<Arc<dyn SessionValidator>>,
            false,
        );

        let req = http::Request::builder()
            .method("GET")
            .uri("/")
            .header("origin", GOOD_ORIGIN)
            .body(Empty::<Bytes>::new())
            .unwrap();

        let resp = Service::call(&mut svc, req).await.expect("service call");
        assert_eq!(resp.status(), http::StatusCode::OK);

        let captured = cap.origin.lock().unwrap().take();
        let raw_present = captured.expect("inner service was called");
        assert_eq!(
            raw_present.as_deref(),
            Some(GOOD_ORIGIN),
            "PLX-18: the WS upgrade middleware must attach a RawRequestContext carrying the Origin",
        );
    }
}

// ===========================================================================
// REST gateway (http-gateway feature)
// ===========================================================================

#[cfg(feature = "http-gateway")]
mod rest {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    use plexus_transport::http::server::testing::build_rest_app;

    #[tokio::test]
    async fn rest_threads_raw_ctx_to_handler() {
        let app = build_rest_app(Arc::new(OriginApi), None, None, None, None);
        let req = Request::builder()
            .method("POST")
            .uri("/rest/origin_api/echo_origin")
            .header("content-type", "application/json")
            .header("origin", GOOD_ORIGIN)
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body["data"][0], GOOD_ORIGIN,
            "PLX-18: the REST gateway must build a RawRequestContext carrying the request Origin; got {:?}",
            body["data"]
        );
    }
}
