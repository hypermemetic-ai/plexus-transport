//! Generic REST HTTP bridge for any Activation
//!
//! This module implements a RESTful HTTP API for Plexus activations,
//! exposing each method as a POST endpoint at `/rest/{namespace}/{method}`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post, put, delete, patch, MethodRouter},
    extract::{Path, State, Extension},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use plexus_core::plexus::{Activation, AuthContext, PlexusError, PlexusStream, PluginSchema, schema::HttpMethod};
use plexus_core::request::RawRequestContext;
use serde_json::Value;

use crate::http::handler::{handle_method_call, MethodInfo};

/// A function that routes a namespaced method call (e.g., "loopback.permit") to the
/// correct activation. Used by hub activations to dispatch child calls via `hub.route()`.
///
/// PLX-16: this is the **4-arg** dispatch signature — it carries the per-user
/// `AuthContext` and the `RawRequestContext` through hub routing so that neither
/// is dropped on the way to the leaf activation. It wraps
/// `DynamicHub::route_with_ctx(method, params, auth, raw_ctx)`.
///
/// Signature: `fn(method, params, auth, raw_ctx) -> Future<Output = Result<PlexusStream, PlexusError>>`.
/// `auth` / `raw_ctx` are owned (`Arc` / value) so the returned future is `'static`.
pub type RouteFn = Arc<
    dyn Fn(
            String,
            Value,
            Option<Arc<AuthContext>>,
            Option<RawRequestContext>,
        ) -> Pin<Box<dyn Future<Output = Result<PlexusStream, PlexusError>> + Send>>
        + Send
        + Sync,
>;

// =============================================================================
// Schema → REST Routes Transformation
// =============================================================================

/// Extended method info for REST routing
#[derive(Clone, Debug)]
pub struct RestMethodInfo {
    pub namespace: String,
    pub method: String,
    pub streaming: bool,
    pub http_method: HttpMethod,
}

/// Registry of method metadata for REST endpoint handling
#[derive(Clone)]
pub struct MethodRegistry {
    /// Map of "{namespace}.{method}" → RestMethodInfo
    methods: Arc<HashMap<String, RestMethodInfo>>,
}

impl MethodRegistry {
    /// Create a registry from a list of plugin schemas
    pub fn from_schemas(schemas: Vec<PluginSchema>) -> Self {
        let mut methods = HashMap::new();

        for schema in schemas {
            for method in schema.methods {
                let key = format!("{}.{}", schema.namespace, method.name);
                methods.insert(key, RestMethodInfo {
                    namespace: schema.namespace.clone(),
                    method: method.name.clone(),
                    streaming: method.streaming,
                    http_method: method.http_method,
                });
            }
        }

        Self {
            methods: Arc::new(methods),
        }
    }

    /// Look up method info by namespace and method name
    pub fn get(&self, namespace: &str, method: &str) -> Option<&RestMethodInfo> {
        let key = format!("{}.{}", namespace, method);
        self.methods.get(&key)
    }

    /// Get all methods for route registration
    pub fn all_methods(&self) -> Vec<&RestMethodInfo> {
        self.methods.values().collect()
    }
}

/// Convert activation schemas to REST routes.
///
/// Registers a **single parameterized route** `/{namespace}/{method}` that
/// dispatches every method through [`rest_method_handler`], which resolves the
/// `{namespace}/{method}` pair against the registry (404 on miss) and reads the
/// method's streaming flag.
///
/// PLX-16: this replaces the previous per-method registration at *literal*
/// paths (`format!("/{namespace}/{method}", …)`). Those literal segments
/// captured **zero** path parameters, so the handler's `Path<(String, String)>`
/// extractor failed with "Expected 2 but got 0" and every REST request 500'd —
/// the gateway never actually routed a call to an activation, let alone
/// threaded auth. All HTTP verbs are accepted on the parameterized route; the
/// method's declared verb is metadata, validated by the registry rather than by
/// the router (registering the same parameterized path once per verb is both
/// sufficient and avoids axum's duplicate-path panic).
fn schemas_to_rest_routes<A>(
    activation: Arc<A>,
    schemas: Vec<PluginSchema>,
    route_fn: Option<RouteFn>,
) -> Router
where
    A: Activation + 'static,
{
    let registry = MethodRegistry::from_schemas(schemas);
    let state = Arc::new(RestBridgeState {
        activation,
        route_fn,
        registry,
    });

    let method_router: MethodRouter<Arc<RestBridgeState<A>>, _> = get(rest_method_handler)
        .post(rest_method_handler)
        .put(rest_method_handler)
        .delete(rest_method_handler)
        .patch(rest_method_handler);

    Router::new()
        .route("/{namespace}/{method}", method_router)
        .with_state(state)
}

// =============================================================================
// REST Bridge State
// =============================================================================

/// Shared state for REST bridge handlers
struct RestBridgeState<A: Activation> {
    activation: Arc<A>,
    route_fn: Option<RouteFn>,
    registry: MethodRegistry,
}

// =============================================================================
// REST Method Handler
// =============================================================================

/// Axum handler for REST method calls
async fn rest_method_handler<A>(
    Path((namespace, method)): Path<(String, String)>,
    State(state): State<Arc<RestBridgeState<A>>>,
    // PLX-16: the per-user identity resolved by the REST session-auth
    // middleware (`http::server::rest_session_auth`) and stashed in the
    // request extensions. Absent when no `SessionValidator` is configured
    // or the request carried no valid credential — in which case dispatch
    // proceeds with `None` (anonymous) and `#[from_auth]` methods fail-close.
    auth: Option<Extension<Arc<AuthContext>>>,
    // PLX-18: capture the request's origin/headers so the framework's origin
    // (`ValidOrigin`) / CSRF machinery and header-derived extractors are
    // reachable through the REST gateway.
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    Json(params): Json<Value>,
) -> Response
where
    A: Activation + 'static,
{
    let auth: Option<Arc<AuthContext>> = auth.map(|Extension(a)| a);
    // Look up method info to determine if streaming
    let rest_method_info = match state.registry.get(&namespace, &method) {
        Some(info) => info.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("Method not found: {}.{}", namespace, method)
                }))
            ).into_response();
        }
    };

    // Convert to handler MethodInfo (which doesn't need http_method)
    let method_info = MethodInfo {
        namespace: rest_method_info.namespace.clone(),
        method: rest_method_info.method.clone(),
        streaming: rest_method_info.streaming,
    };

    // PLX-18: build the RawRequestContext (origin/headers/peer + resolved
    // identity) and thread it into dispatch so origin/CSRF enforcement fires.
    // Peer is `None` here (axum `ConnectInfo` isn't wired into this bridge);
    // origin/CSRF derive from headers, which are present.
    let raw_ctx = RawRequestContext {
        headers,
        uri,
        auth: auth.as_deref().cloned(),
        peer: None,
    };

    // Call the method via activation or route_fn.
    // PLX-16: thread the per-user `AuthContext` (4-arg dispatch) instead of
    // dropping it via the legacy 2-arg call. PLX-18: also thread `raw_ctx`.
    let stream_result = if let Some(route_fn) = &state.route_fn {
        // Hub activation: use route_fn to dispatch
        let full_method = format!("{}.{}", namespace, method);
        route_fn(full_method, params, auth.clone(), Some(raw_ctx)).await
    } else {
        // Leaf activation: call directly
        state
            .activation
            .call(&method, params, auth.as_deref(), Some(&raw_ctx))
            .await
    };

    // Handle the result
    match stream_result {
        Ok(stream) => handle_method_call(stream, method_info).await,
        Err(e) => plexus_error_to_response(e),
    }
}

// =============================================================================
// Error Mapping
// =============================================================================

/// Convert PlexusError to HTTP response
fn plexus_error_to_response(e: PlexusError) -> Response {
    let (status, error_msg) = match e {
        PlexusError::ActivationNotFound(name) => {
            (StatusCode::NOT_FOUND, format!("Unknown activation: {}", name))
        }
        PlexusError::MethodNotFound { activation, method } => {
            (StatusCode::NOT_FOUND, format!("Unknown method: {}.{}", activation, method))
        }
        PlexusError::InvalidParams(reason) => {
            (StatusCode::BAD_REQUEST, format!("Invalid parameters: {}", reason))
        }
        PlexusError::ExecutionError(error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Execution error: {}", error))
        }
        PlexusError::HandleNotSupported(activation) => {
            (StatusCode::BAD_REQUEST, format!("Handle resolution not supported: {}", activation))
        }
        PlexusError::TransportError(kind) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Transport error: {:?}", kind))
        }
        // PLX-16: auth errors reach the REST mapper now that the gateway
        // threads AuthContext. Unauthenticated → 401, Forbidden (R-5 scope
        // gate) → 403. The Forbidden Display names only the unmet scope
        // (no-enumeration posture, AUTHZ-CORE-1/5).
        PlexusError::Unauthenticated(reason) => {
            (StatusCode::UNAUTHORIZED, format!("Authentication required: {}", reason))
        }
        forbidden @ PlexusError::Forbidden { .. } => {
            (StatusCode::FORBIDDEN, forbidden.to_string())
        }
    };

    (
        status,
        Json(serde_json::json!({
            "error": error_msg
        }))
    ).into_response()
}

// =============================================================================
// Generic Activation REST Bridge
// =============================================================================

/// Generic REST bridge for any Activation
///
/// Exposes activation methods as REST endpoints at `POST /{namespace}/{method}`.
/// Supports both leaf activations and hub activations with routing.
#[derive(Clone)]
pub struct ActivationRestBridge<A: Activation> {
    activation: Arc<A>,
    schemas: Vec<PluginSchema>,
    route_fn: Option<RouteFn>,
    #[allow(dead_code)]
    server_name: Option<String>,
    #[allow(dead_code)]
    server_version: Option<String>,
}

impl<A: Activation> ActivationRestBridge<A> {
    /// Create a new REST bridge with default server info
    pub fn new(activation: Arc<A>) -> Self {
        let schemas = vec![activation.plugin_schema()];
        Self {
            activation,
            schemas,
            route_fn: None,
            server_name: None,
            server_version: None,
        }
    }

    /// Create a REST bridge with server info and custom schemas
    ///
    /// For hub activations, pass `hub.list_plugin_schemas()` as `flat_schemas`
    /// to expose all child activation methods.
    pub fn with_server_info_and_schemas(
        activation: Arc<A>,
        server_name: Option<String>,
        server_version: Option<String>,
        flat_schemas: Option<Vec<PluginSchema>>,
    ) -> Self {
        let schemas = flat_schemas.unwrap_or_else(|| vec![activation.plugin_schema()]);
        Self {
            activation,
            schemas,
            route_fn: None,
            server_name,
            server_version,
        }
    }

    /// Set the routing function for hub activations
    ///
    /// This enables the bridge to dispatch calls to child activations via `hub.route()`.
    pub fn with_router(mut self, route_fn: RouteFn) -> Self {
        self.route_fn = Some(route_fn);
        self
    }

    /// Convert this bridge into an Axum router
    pub fn into_router(self) -> Router {
        schemas_to_rest_routes(self.activation, self.schemas, self.route_fn)
    }
}
