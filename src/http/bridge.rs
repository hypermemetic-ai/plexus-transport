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
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use plexus_core::plexus::{Activation, PlexusError, PlexusStream, PluginSchema, schema::HttpMethod};
use serde_json::Value;

use crate::http::handler::{handle_method_call, MethodInfo};

/// A function that routes a namespaced method call (e.g., "loopback.permit") to the
/// correct activation. Used by hub activations to dispatch child calls via `hub.route()`.
///
/// Signature: `fn(method: String, params: Value) -> Future<Output = Result<PlexusStream, PlexusError>>`
pub type RouteFn = Arc<
    dyn Fn(String, Value) -> Pin<Box<dyn Future<Output = Result<PlexusStream, PlexusError>> + Send>>
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

/// Convert activation schemas to REST routes
///
/// Creates Axum routes for each method with the appropriate HTTP method (GET, POST, PUT, DELETE, PATCH).
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
        registry: registry.clone(),
    });

    let mut router = Router::new();

    // Register each method with its specific HTTP method
    for method_info in registry.all_methods() {
        let path = format!("/{}/{}", method_info.namespace, method_info.method);

        // Choose the appropriate routing method based on http_method
        let method_router: MethodRouter<Arc<RestBridgeState<A>>, _> = match method_info.http_method {
            HttpMethod::Get => get(rest_method_handler),
            HttpMethod::Post => post(rest_method_handler),
            HttpMethod::Put => put(rest_method_handler),
            HttpMethod::Delete => delete(rest_method_handler),
            HttpMethod::Patch => patch(rest_method_handler),
        };

        router = router.route(&path, method_router);
    }

    router.with_state(state)
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
    Json(params): Json<Value>,
) -> Response
where
    A: Activation + 'static,
{
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

    // Call the method via activation or route_fn
    let stream_result = if let Some(route_fn) = &state.route_fn {
        // Hub activation: use route_fn to dispatch
        let full_method = format!("{}.{}", namespace, method);
        route_fn(full_method, params).await
    } else {
        // Leaf activation: call directly
        state.activation.call(&method, params).await
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
