//! Generic REST HTTP bridge for any Activation
//!
//! This module implements a RESTful HTTP API for Plexus activations,
//! exposing each method as a POST endpoint at `/rest/{namespace}/{method}`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    routing::any,
    extract::{rejection::JsonRejection, FromRequest, Path, Request, State},
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use plexus_core::plexus::{
    schema::HttpMethod, Activation, AuthContext, Cardinality, ParamProblem, PlexusError, PluginSchema,
    RemoteError, SessionValidator,
};
use serde_json::Value;

use crate::http::handler::{handle_method_call, remote_error_response, MethodInfo};

/// A function that routes a namespaced method call (e.g., "loopback.permit") to the
/// correct activation. Used by hub activations to dispatch child calls via `hub.route()`.
///
/// Canonical definition lives in [`crate::server`] so it is available with or
/// without the `mcp` feature.
pub use crate::server::RouteFn;
pub use crate::server::RouteAsFn;

// =============================================================================
// Schema → REST Routes Transformation
// =============================================================================

/// Extended method info for REST routing
#[derive(Clone, Debug)]
pub struct RestMethodInfo {
    pub namespace: String,
    pub method: String,
    /// `one` → one JSON body; `many` → SSE (RETURNS-5's `cardinality`).
    pub cardinality: Cardinality,
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
                    cardinality: method.cardinality,
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

    /// Whether any registered method belongs to `namespace` — tells an
    /// unknown method from an unknown activation.
    pub fn has_namespace(&self, namespace: &str) -> bool {
        self.methods.values().any(|info| info.namespace == namespace)
    }
}

/// Convert activation schemas to REST routes
///
/// Creates Axum routes for each method with the appropriate HTTP method (GET, POST, PUT, DELETE, PATCH).
fn schemas_to_rest_routes<A>(
    activation: Arc<A>,
    schemas: Vec<PluginSchema>,
    route_fn: Option<RouteFn>,
    route_as: Option<RouteAsFn>,
    validator: Option<Arc<dyn SessionValidator>>,
) -> Router
where
    A: Activation + 'static,
{
    let registry = MethodRegistry::from_schemas(schemas);
    let state = Arc::new(RestBridgeState {
        activation,
        route_fn,
        route_as,
        validator,
        registry: registry.clone(),
    });

    // A single parameterized route serves every method. The handler resolves
    // (namespace, method) against the registry (404 for unknown methods) and
    // enforces the schema's HTTP verb (405 on mismatch). Literal per-method
    // paths cannot be combined with the `Path` extractor: axum rejects `Path`
    // extraction on routes without captures (every request would 500).
    Router::new()
        .route("/{namespace}/{method}", any(rest_method_handler))
        .with_state(state)
}

// =============================================================================
// REST Bridge State
// =============================================================================

/// Shared state for REST bridge handlers
struct RestBridgeState<A: Activation> {
    activation: Arc<A>,
    route_fn: Option<RouteFn>,
    route_as: Option<RouteAsFn>,
    validator: Option<Arc<dyn SessionValidator>>,
    registry: MethodRegistry,
}

/// RETURNS-47: the caller a request names, as the WebSocket's upgrade names
/// one (AUTHZ-BEARER-1): its `Cookie` header, then its `Authorization:
/// Bearer` token, each to the session validator; the first it accepts is
/// the caller. A `Bearer` it refuses (and no cookie it accepts) is a 401,
/// typed `unauthenticated`; no `Bearer` and no accepted cookie is the
/// anonymous caller. Without a validator every caller is anonymous.
async fn identify(
    validator: Option<&Arc<dyn SessionValidator>>,
    headers: &axum::http::HeaderMap,
) -> Result<Option<AuthContext>, Response> {
    let Some(validator) = validator else { return Ok(None) };
    if let Some(cookies) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) {
        if let Some(ctx) = validator.validate(cookies).await {
            return Ok(Some(ctx));
        }
    }
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim);
    match bearer {
        Some(token) => match validator.validate(token).await {
            Some(ctx) => Ok(Some(ctx)),
            None => Err(remote_error_response(RemoteError::Unauthenticated)),
        },
        None => Ok(None),
    }
}

// =============================================================================
// REST Method Handler
// =============================================================================

/// Map a schema-declared HTTP method to the axum/http verb type
fn http_method_to_verb(http_method: &HttpMethod) -> Method {
    match http_method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
        HttpMethod::Patch => Method::PATCH,
    }
}

/// Axum handler for REST method calls
async fn rest_method_handler<A>(
    Path((namespace, method)): Path<(String, String)>,
    State(state): State<Arc<RestBridgeState<A>>>,
    request: Request,
) -> Response
where
    A: Activation + 'static,
{
    // Look up method info (its verb and cardinality). Unknown → 404 with the
    // typed `RemoteError`, as a service's own route would answer it.
    let rest_method_info = match state.registry.get(&namespace, &method) {
        Some(info) => info.clone(),
        None => {
            let error = if state.registry.has_namespace(&namespace) {
                RemoteError::MethodNotFound { activation: namespace, method }
            } else {
                RemoteError::ActivationNotFound { activation: namespace }
            };
            return remote_error_response(error);
        }
    };

    // Enforce the per-method HTTP verb (the single parameterized route accepts
    // all verbs; the schema says which one is valid for this method).
    let expected_verb = http_method_to_verb(&rest_method_info.http_method);
    if request.method() != expected_verb {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, expected_verb.as_str().to_string())],
            Json(serde_json::json!({
                "error": format!(
                    "{} not allowed for {}.{}; use {}",
                    request.method(), namespace, method, expected_verb
                )
            })),
        ).into_response();
    }

    // RETURNS-47: who is calling, before anything of the call is read.
    let auth = match identify(state.validator.as_ref(), request.headers()).await {
        Ok(auth) => auth,
        Err(refused) => return refused,
    };

    // Parse the JSON body after the 404/405 checks so verb mismatches are
    // reported even without a body. A body that is not JSON → 400 with a
    // typed `invalid_params`; any other rejection (e.g. a missing
    // content-type, 415) keeps axum's own answer.
    let Json(params) = match Json::<Value>::from_request(request, &()).await {
        Ok(json) => json,
        Err(rejection @ (JsonRejection::JsonSyntaxError(_) | JsonRejection::JsonDataError(_))) => {
            return remote_error_response(RemoteError::InvalidParams {
                param: None,
                problem: ParamProblem::Malformed { detail: rejection.body_text() },
            });
        }
        Err(rejection) => return rejection.into_response(),
    };

    // Convert to handler MethodInfo (which doesn't need http_method)
    let method_info = MethodInfo {
        namespace: rest_method_info.namespace.clone(),
        method: rest_method_info.method.clone(),
        cardinality: rest_method_info.cardinality,
    };

    // Call the method: through the service's route (the gate first, the
    // caller handed over), through a caller-less route, or — one activation
    // and no service — as a service with no gate dispatches it.
    let stream_result = if let Some(route_as) = &state.route_as {
        // RETURNS-47: the service's route, as the caller the request names.
        route_as(format!("{}.{}", namespace, method), params, auth).await
    } else if let Some(route_fn) = &state.route_fn {
        // Hub activation: use route_fn to dispatch (it names no caller: the
        // call is anonymous).
        let full_method = format!("{}.{}", namespace, method);
        route_fn(full_method, params).await
    } else {
        // RETURNS-47: the leaf path. The bridge is the whole dispatch — no
        // service, so no gate — and it runs the call as a service with no
        // gate runs one: in a context admitting the caller everywhere
        // (`call::ungated`). `Activation::call` alone runs in no context, where
        // a method reading `reach` or `caller()` is refused.
        plexus_core::call::ungated(&*state.activation, &method, params, auth.as_ref(), None).await
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

/// Convert a `PlexusError` (the call was rejected before its stream started)
/// to an HTTP response: the typed `RemoteError` it maps to, its status per
/// [`crate::http::handler::remote_error_status`].
fn plexus_error_to_response(e: PlexusError) -> Response {
    remote_error_response(RemoteError::from(&e))
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
    route_as: Option<RouteAsFn>,
    validator: Option<Arc<dyn SessionValidator>>,
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
            route_as: None,
            validator: None,
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
            route_as: None,
            validator: None,
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

    /// RETURNS-47: route calls through the service as the caller each
    /// request names (`hub.route_with_ctx(&method, params, auth.as_ref(),
    /// None)`): the gate decides first, and the call runs in its context, as
    /// over the WebSocket. Preferred over [`Self::with_router`], which names
    /// no caller.
    pub fn with_router_as(mut self, route_as: RouteAsFn) -> Self {
        self.route_as = Some(route_as);
        self
    }

    /// RETURNS-47: name each request's caller with `validator` — its
    /// `Cookie`, then its `Authorization: Bearer` token, as the WebSocket's
    /// upgrade does (the same validator names the same caller on both). A
    /// `Bearer` it refuses is a 401; none is the anonymous caller. The static
    /// api key of `serve_rest_http` also rides `Authorization`, so a service
    /// that uses both names its REST callers by cookie.
    pub fn with_session_validator(mut self, validator: Arc<dyn SessionValidator>) -> Self {
        self.validator = Some(validator);
        self
    }

    /// Convert this bridge into an Axum router
    pub fn into_router(self) -> Router {
        schemas_to_rest_routes(self.activation, self.schemas, self.route_fn, self.route_as, self.validator)
    }
}
