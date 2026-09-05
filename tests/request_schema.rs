//! REQ-1 acceptance tests: PlexusRequest derive — JSON Schema output.
//!
//! Run with: cargo test --test request_schema

use plexus_transport::request::PlexusRequest;
use schemars::PlexusRequest;
use std::net::SocketAddr;

#[derive(PlexusRequest, schemars::JsonSchema)]
struct TestRequest {
    /// JWT from Keycloak
    #[from_cookie("access_token")]
    auth_token: String,

    #[from_header("origin")]
    origin: Option<String>,

    /// Caller IP (server-derived)
    #[from_peer]
    peer_addr: Option<SocketAddr>,
}

fn schema_value() -> serde_json::Value {
    // `schemars` is aliased to plexus-macros' no-op JsonSchema derive in
    // dev-dependencies; the real schemars macros live at plexus_core::__schemars.
    let schema = plexus_core::__schemars::schema_for!(TestRequest);
    serde_json::to_value(&schema).unwrap()
}

#[test]
fn schema_has_x_plexus_source_on_cookie_field() {
    let v = schema_value();
    let source = &v["properties"]["auth_token"]["x-plexus-source"];
    assert_eq!(source["from"], "cookie", "auth_token x-plexus-source.from");
    assert_eq!(source["key"], "access_token", "auth_token x-plexus-source.key");
}

#[test]
fn schema_has_x_plexus_source_on_header_field() {
    let v = schema_value();
    let source = &v["properties"]["origin"]["x-plexus-source"];
    assert_eq!(source["from"], "header");
    assert_eq!(source["key"], "origin");
}

#[test]
fn schema_has_derived_source_on_peer_field() {
    let v = schema_value();
    let source = &v["properties"]["peer_addr"]["x-plexus-source"];
    assert_eq!(source["from"], "derived");
}

#[test]
fn schema_required_contains_non_option_fields_only() {
    let v = schema_value();
    let required = v["required"]
        .as_array()
        .expect("required array must exist");
    let names: Vec<&str> = required.iter().map(|v| v.as_str().unwrap()).collect();

    assert!(
        names.contains(&"auth_token"),
        "auth_token (String) must be required"
    );
    assert!(
        !names.contains(&"origin"),
        "origin (Option<String>) must NOT be required"
    );
    assert!(
        !names.contains(&"peer_addr"),
        "peer_addr (Option<SocketAddr>) must NOT be required"
    );
}

#[test]
fn schema_properties_all_present() {
    let v = schema_value();
    let props = v["properties"].as_object().unwrap();
    assert!(props.contains_key("auth_token"));
    assert!(props.contains_key("origin"));
    assert!(props.contains_key("peer_addr"));
}
