//! RETURNS-47 · the bridges answer as the socket does.
//!
//! Through REST and through MCP, a method that reads `reach: Reach`
//! (`probe.reach`) and one that reads `plexus_core::call::caller()`
//! (`whom.whom`, a trait impl), both under `auth = "required"`, are answered
//! for an admitted caller and refused for an anonymous one; their `public`
//! twins (`probe.open`, `whom.seen`) are answered for anyone, in the call's
//! context.
//!
//! - REST over one activation (the leaf path: the bridge is the whole
//!   dispatch, no service and no gate — everywhere, as a service without a
//!   gate), and REST over a gated service (its route, the gate first: ADA's
//!   reach is MENGER, BOB is forbidden). The caller is the one the bridge's
//!   session validator names from the request (cookie, then `Bearer`), as
//!   the WebSocket's upgrade names it; a credential it refuses is a 401.
//! - MCP mounted over the service (`with_mcp`'s `ServiceUpstream`): the
//!   caller its validator names. MCP's legacy `ActivationMcpBridge` leaf: it
//!   names no caller, so every call is anonymous.

use std::sync::Arc;

use returns47_fixture::{gated, flat_schemas, ProbeActivation, Seer, Users, ADA};

#[cfg(feature = "http-gateway")]
mod rest {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use plexus_core::plexus::Activation;
    use plexus_transport::{ActivationRestBridge, RouteAsFn};
    use tower::ServiceExt;

    fn request(uri: &str, bearer: Option<&str>) -> Request<Body> {
        let mut r = Request::builder().method("POST").uri(uri).header(header::CONTENT_TYPE, "application/json");
        if let Some(token) = bearer {
            r = r.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        r.body(Body::from("{}")).unwrap()
    }

    async fn answer(router: &axum::Router, uri: &str, bearer: Option<&str>) -> (StatusCode, String) {
        let response = router.clone().oneshot(request(uri, bearer)).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }

    fn leaf<A: Activation + 'static>(activation: A) -> axum::Router {
        ActivationRestBridge::new(Arc::new(activation)).with_session_validator(Arc::new(Users)).into_router()
    }

    fn over_the_service() -> axum::Router {
        let service = gated();
        let route: RouteAsFn = {
            let service = service.clone();
            Arc::new(move |method, params, auth| {
                let service = service.clone();
                Box::pin(async move { service.route_with_ctx(&method, params, auth.as_ref(), None).await })
            })
        };
        ActivationRestBridge::with_server_info_and_schemas(service, None, None, Some(flat_schemas()))
            .with_session_validator(Arc::new(Users))
            .with_router_as(route)
            .into_router()
    }

    #[tokio::test]
    async fn the_leaf_path_answers_an_admitted_caller_everywhere_and_refuses_anonymous() {
        let probe = leaf(ProbeActivation::default());
        let (status, body) = answer(&probe, "/probe/reach", Some("ada")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains(&format!("{ADA} *")), "{body}");
        let (status, body) = answer(&probe, "/probe/reach", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        let (status, body) = answer(&probe, "/probe/open", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains("anyone *"), "{body}");

        let seer = leaf(Seer);
        let (status, body) = answer(&seer, "/whom/whom", Some("ada")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains(&format!("{ADA} *")), "{body}");
        let (status, body) = answer(&seer, "/whom/whom", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        let (status, body) = answer(&seer, "/whom/seen", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains("anonymous *"), "{body}");
    }

    #[tokio::test]
    async fn a_credential_the_validator_refuses_is_a_401() {
        let (status, body) = answer(&leaf(ProbeActivation::default()), "/probe/open", Some("mallory")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    }

    #[tokio::test]
    async fn over_a_gated_service_the_gate_decides_first() {
        let router = over_the_service();
        for uri in ["/probe/reach", "/whom/whom"] {
            let (status, body) = answer(&router, uri, Some("ada")).await;
            assert_eq!(status, StatusCode::OK, "{uri}: {body}");
            assert!(body.contains(&format!("{ADA} MENGER")), "{uri}: {body}");
            let (status, body) = answer(&router, uri, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: {body}");
            let (status, body) = answer(&router, uri, Some("bob")).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
        }
    }
}

#[cfg(feature = "mcp")]
mod mcp_mounted {
    use super::*;
    use returns47_fixture::plexus_client::{self, MethodRef, StreamItem};
    use plexus_transport::ServiceUpstream;
    use plexus_transport_mcp::upstream::{Credential, Upstream};
    use futures::StreamExt;

    async fn call(credential: Credential, ns: &'static str, method: &'static str) -> Vec<StreamItem> {
        let upstream = ServiceUpstream::new(gated(), Some(Arc::new(Users)));
        let transport = upstream.transport_for(&credential).await.unwrap();
        transport.call(MethodRef::new(ns, method), serde_json::json!({})).map(|i| i.unwrap()).collect().await
    }

    #[tokio::test]
    async fn the_mounted_bridge_answers_an_admitted_caller_and_refuses_anonymous() {
        for (ns, m) in [("probe", "reach"), ("whom", "whom")] {
            let items = call(Credential::Bearer("ada".into()), ns, m).await;
            let StreamItem::Data { content, .. } = &items[0] else { panic!("{ns}.{m}: {items:?}") };
            assert_eq!(content, &serde_json::json!(format!("{ADA} MENGER")), "{ns}.{m}");
            let items = call(Credential::Anonymous, ns, m).await;
            assert!(matches!(&items[0], StreamItem::Error { error: plexus_client::RemoteError::Unauthenticated, .. }), "{ns}.{m}: {items:?}");
        }
    }
}

#[cfg(feature = "__mcp-test-client")]
mod mcp_leaf {
    #![allow(deprecated)]
    use super::*;
    use plexus_core::plexus::Activation;
    use plexus_transport::ActivationMcpBridge;
    use rmcp::model::CallToolRequestParams;
    use rmcp::ServiceExt;

    /// One tool call to the legacy bridge over `activation`, as text.
    async fn call<A: Activation + 'static>(activation: A, tool: &'static str) -> (bool, String) {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let server = ActivationMcpBridge::new(Arc::new(activation)).serve(server_io).await.expect("server initializes");
            let _ = server.waiting().await;
        });
        let client = ().serve(client_io).await.expect("client initializes");
        let result = client.peer().call_tool(CallToolRequestParams::new(tool).with_arguments(serde_json::Map::new())).await;
        let out = match result {
            Ok(r) => (r.is_error != Some(true), serde_json::to_string(&r).unwrap()),
            Err(e) => (false, e.to_string()),
        };
        client.cancel().await.unwrap();
        out
    }

    #[tokio::test]
    async fn the_legacy_leaf_is_anonymous_answered_in_a_context_and_refused_when_required() {
        let (ok, text) = call(ProbeActivation::default(), "probe.open").await;
        assert!(ok && text.contains("anyone *"), "{text}");
        let (ok, text) = call(Seer, "whom.seen").await;
        assert!(ok && text.contains("anonymous *"), "{text}");
        let (ok, text) = call(ProbeActivation::default(), "probe.reach").await;
        assert!(!ok, "{text}");
        let (ok, text) = call(Seer, "whom.whom").await;
        assert!(!ok, "{text}");
    }
}
