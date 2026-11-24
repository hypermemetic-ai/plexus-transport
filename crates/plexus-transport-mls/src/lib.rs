//! The MLS agent transport of Plexus.
//!
//! This crate is published on its own and depends on neither
//! `plexus-transport` nor an application runtime. Its default build is
//! `no_std` with `alloc` and has no platform dependency, so a browser client
//! compiles the same wire types and decoders as a native endpoint.
//!
//! [`wire`] is the v1 wire profile: what a signed intent is, byte for byte,
//! at every endpoint.

#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

pub mod wire;
