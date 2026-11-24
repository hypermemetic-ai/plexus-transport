//! The digests and signing inputs of the profile.
//!
//! Each is SHA-256 of, or simply is, a deterministic CBOR array that opens
//! with a domain text, so a value made for one purpose is never the input of
//! another.

use alloc::vec::Vec;

use sha2::{Digest as _, Sha256};

use super::cbor::Writer;
use super::limits::{COSE_ALG_ES256, COSE_HEADER_ALG, COSE_HEADER_KID};
use super::types::{CredentialRef, Digest, Fingerprint, Nonce};

/// Domain of the WebAuthn assertion challenge.
pub const WEBAUTHN_DOMAIN: &str = "plexus.agent.webauthn.v1";
/// Domain of the WebAuthn registration challenge.
pub const WEBAUTHN_REGISTER_DOMAIN: &str = "plexus.agent.webauthn.register.v1";
/// Domain of a credential reference.
pub const CREDENTIAL_DOMAIN: &str = "plexus.agent.credential.v1";
/// Domain of an MLS leaf identity.
pub const MLS_LEAF_DOMAIN: &str = "plexus.agent.mls-leaf.v1";
/// Domain of the PIV enrollment signature.
pub const ENROLL_DOMAIN: &str = "plexus.agent.enroll.v1";
/// The `profile` text of a PIV credential.
pub const PROFILE_PIV: &str = "piv";
/// The `profile` text of a WebAuthn credential.
pub const PROFILE_WEBAUTHN: &str = "webauthn";

fn sha256(bytes: &[u8]) -> Digest {
    Digest(Sha256::digest(bytes).into())
}

/// The digest of an intent: SHA-256 of its exact bytes. An intent opens with
/// its own domain, so the bytes are hashed as they are.
#[must_use]
pub fn intent_digest(intent_bytes: &[u8]) -> Digest {
    sha256(intent_bytes)
}

/// The WebAuthn challenge of an assertion over an intent: SHA-256 of
/// `["plexus.agent.webauthn.v1", bstr(intent_bytes)]`. The client data's
/// base64url `challenge` decodes to these 32 bytes.
#[must_use]
pub fn webauthn_challenge(intent_bytes: &[u8]) -> Digest {
    let mut w = Writer::new();
    w.array(2);
    w.text(WEBAUTHN_DOMAIN);
    w.bytes(intent_bytes);
    sha256(&w.into_bytes())
}

/// The reference of an enrolled credential: SHA-256 of
/// `["plexus.agent.credential.v1", profile, bstr(key)]`.
///
/// For [`PROFILE_PIV`], `key` is the 65-byte SEC1 uncompressed P-256 public
/// key; for [`PROFILE_WEBAUTHN`], the credential ID.
#[must_use]
pub fn credential_ref(profile: &str, key: &[u8]) -> CredentialRef {
    let mut w = Writer::new();
    w.array(3);
    w.text(CREDENTIAL_DOMAIN);
    w.text(profile);
    w.bytes(key);
    CredentialRef(sha256(&w.into_bytes()).0)
}

/// The identity of an MLS leaf: SHA-256 of
/// `["plexus.agent.mls-leaf.v1", ciphersuite, bstr(signature_key)]`.
#[must_use]
pub fn leaf_identity(ciphersuite: u16, signature_key: &[u8]) -> Fingerprint {
    let mut w = Writer::new();
    w.array(3);
    w.text(MLS_LEAF_DOMAIN);
    w.uint(u64::from(ciphersuite));
    w.bytes(signature_key);
    Fingerprint(sha256(&w.into_bytes()).0)
}

/// The bytes of a COSE protected header of a PIV proof: `{1: -7, 4: kid}`.
#[must_use]
pub fn cose_protected(kid: &CredentialRef) -> Vec<u8> {
    let mut w = Writer::new();
    w.map(2);
    w.uint(COSE_HEADER_ALG);
    w.nint(!(COSE_ALG_ES256 as u64));
    w.uint(COSE_HEADER_KID);
    w.bytes(&kid.0);
    w.into_bytes()
}

/// The COSE `Sig_structure` an ES256 command signature is made over (RFC 9052
/// §4.4): `["Signature1", bstr(protected), h'', bstr(intent_bytes)]`. ES256
/// hashes it once with SHA-256.
#[must_use]
pub fn cose_sig_structure(kid: &CredentialRef, intent_bytes: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array(4);
    w.text("Signature1");
    w.bytes(&cose_protected(kid));
    w.bytes(&[]);
    w.bytes(intent_bytes);
    w.into_bytes()
}

/// The bytes a PIV enrollment proof signs with ES256:
/// `["plexus.agent.enroll.v1", host, pairing, nonce, bstr(leaf_key),
/// bstr(signing_pub)]`.
#[must_use]
pub fn enroll_signing_input(
    host: &Fingerprint,
    pairing: &Digest,
    nonce: &Nonce,
    leaf_key: &[u8],
    signing_pub: &[u8],
) -> Vec<u8> {
    let mut w = Writer::new();
    w.array(6);
    w.text(ENROLL_DOMAIN);
    w.bytes(&host.0);
    w.bytes(&pairing.0);
    w.bytes(&nonce.0);
    w.bytes(leaf_key);
    w.bytes(signing_pub);
    w.into_bytes()
}

/// The WebAuthn challenge of a registration: SHA-256 of
/// `["plexus.agent.webauthn.register.v1", host, pairing, nonce,
/// bstr(leaf_key)]`.
#[must_use]
pub fn webauthn_register_challenge(
    host: &Fingerprint,
    pairing: &Digest,
    nonce: &Nonce,
    leaf_key: &[u8],
) -> Digest {
    let mut w = Writer::new();
    w.array(5);
    w.text(WEBAUTHN_REGISTER_DOMAIN);
    w.bytes(&host.0);
    w.bytes(&pairing.0);
    w.bytes(&nonce.0);
    w.bytes(leaf_key);
    sha256(&w.into_bytes())
}
