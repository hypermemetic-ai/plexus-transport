//! AUTH-6: OAuth 2.0 Protected Resource Metadata (RFC 9728).
//!
//! A server configured with [`ProtectedResource`] (through
//! `TransportServerBuilder::with_protected_resource`) answers
//! `GET /.well-known/oauth-protected-resource` on its WebSocket port with
//! the metadata document, and every 401 it sends at the upgrade names that
//! document in `WWW-Authenticate` (`resource_metadata="…"`), so an HTTP
//! client — an MCP client, an OAuth library — learns where to sign in the
//! standard way.

use serde::Serialize;

/// The well-known path of RFC 9728 §3.
pub const WELL_KNOWN_PATH: &str = "/.well-known/oauth-protected-resource";

/// RFC 9728 protected-resource metadata, as served.
///
/// `bearer_methods_supported` defaults to `["header"]`, so
/// `ProtectedResource { resource, authorization_servers, ..Default::default() }`
/// or [`ProtectedResource::new`] is the whole configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProtectedResource {
    /// The resource identifier: the URL clients reach this server at, e.g.
    /// `http://127.0.0.1:4455`. RFC 9728 requires a client to check that it
    /// equals the URL it fetched the metadata for.
    pub resource: String,
    /// Issuer identifiers of the authorization servers that mint tokens for
    /// this resource.
    pub authorization_servers: Vec<String>,
    /// The scopes a token may carry here (for Plexus: the grants,
    /// `tickets.get`, …). Omitted from the document when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes_supported: Option<Vec<String>>,
    /// How a bearer token may be presented (RFC 6750 §2): `header` is the
    /// `Authorization: Bearer` header. Omitted from the document when empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bearer_methods_supported: Vec<String>,
}

impl Default for ProtectedResource {
    fn default() -> Self {
        Self {
            resource: String::new(),
            authorization_servers: Vec::new(),
            scopes_supported: None,
            bearer_methods_supported: vec!["header".to_string()],
        }
    }
}

impl ProtectedResource {
    /// The resource and its authorization servers; `bearer_methods_supported`
    /// `["header"]`, no `scopes_supported`.
    pub fn new<I, S>(resource: impl Into<String>, authorization_servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            resource: resource.into(),
            authorization_servers: authorization_servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Set `scopes_supported`.
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes_supported = Some(scopes.into_iter().map(Into::into).collect());
        self
    }

    /// The path the metadata is served at on this server: the well-known
    /// path, followed by the resource's own path when it has one (RFC 9728
    /// §3.1 — `https://h/api` → `/.well-known/oauth-protected-resource/api`).
    pub fn metadata_path(&self) -> String {
        let path = split_origin(&self.resource).1.trim_end_matches('/');
        format!("{WELL_KNOWN_PATH}{path}")
    }

    /// The absolute URL of the metadata document, as `resource_metadata`
    /// names it in `WWW-Authenticate`.
    pub fn metadata_url(&self) -> String {
        let origin = split_origin(&self.resource).0;
        format!("{origin}{}", self.metadata_path())
    }

    /// The document, serialized.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("metadata serializes")
    }
}

/// `("scheme://authority", "/path…")` — query and fragment dropped (a
/// resource identifier has neither, RFC 9728 §1.2).
fn split_origin(resource: &str) -> (&str, &str) {
    let resource = resource.split(['?', '#']).next().unwrap_or(resource);
    let after_scheme = resource.find("://").map(|i| i + 3).unwrap_or(0);
    match resource[after_scheme..].find('/') {
        Some(i) => resource.split_at(after_scheme + i),
        None => (resource, ""),
    }
}

/// The metadata, prepared once for the upgrade middleware.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) path: String,
    pub(crate) url: String,
    pub(crate) body: String,
}

impl From<&ProtectedResource> for Prepared {
    fn from(p: &ProtectedResource) -> Self {
        Self { path: p.metadata_path(), url: p.metadata_url(), body: p.to_json() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_origin_is_served_at_the_well_known_path() {
        let p = ProtectedResource::new("http://127.0.0.1:4455", ["https://as.example"]);
        assert_eq!(p.metadata_path(), "/.well-known/oauth-protected-resource");
        assert_eq!(p.metadata_url(), "http://127.0.0.1:4455/.well-known/oauth-protected-resource");
        let p = ProtectedResource::new("http://127.0.0.1:4455/", ["https://as.example"]);
        assert_eq!(p.metadata_url(), "http://127.0.0.1:4455/.well-known/oauth-protected-resource");
    }

    #[test]
    fn a_resource_with_a_path_appends_it() {
        let p = ProtectedResource::new("https://h.example/api/v1", Vec::<String>::new());
        assert_eq!(p.metadata_path(), "/.well-known/oauth-protected-resource/api/v1");
        assert_eq!(p.metadata_url(), "https://h.example/.well-known/oauth-protected-resource/api/v1");
    }

    #[test]
    fn the_document_defaults_to_header_and_omits_absent_scopes() {
        let p = ProtectedResource::new("http://127.0.0.1:4455", ["https://as.example"]);
        let v: serde_json::Value = serde_json::from_str(&p.to_json()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "resource": "http://127.0.0.1:4455",
                "authorization_servers": ["https://as.example"],
                "bearer_methods_supported": ["header"],
            })
        );
    }
}
