//! The v1 limits. Frozen: a change is a new wire version.
//!
//! Every length is checked against the bytes that remain before anything is
//! allocated for it, and every whole-record limit is checked before the
//! record's first byte is read.

/// The wire version every record carries in `v`.
pub const WIRE_VERSION: u64 = 1;

/// The RFC 9420 ciphersuite both ends of a v1 group use:
/// `MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519`.
pub const MLS_CIPHERSUITE: u16 = 0x0001;
/// The RFC 9420 protocol version (`mls10`) an MLSMessage opens with.
pub const MLS_VERSION: u16 = 0x0001;
/// Length of an MLS leaf signature public key under [`MLS_CIPHERSUITE`]
/// (Ed25519, RFC 8032 encoding).
pub const MLS_SIGNATURE_KEY_BYTES: usize = 32;

/// COSE algorithm identifier of a PIV command signature: ES256 (RFC 9053).
pub const COSE_ALG_ES256: i64 = -7;
/// COSE header label of `alg`.
pub const COSE_HEADER_ALG: u64 = 1;
/// COSE header label of `kid`.
pub const COSE_HEADER_KID: u64 = 4;
/// Length of a P-256 public key on the wire: SEC1 uncompressed,
/// `0x04 || X || Y`.
pub const P256_PUBLIC_KEY_BYTES: usize = 65;
/// Length of an ES256 signature on the wire: `R || S`, each 32 bytes
/// big-endian, as COSE prescribes (not DER).
pub const ES256_SIGNATURE_BYTES: usize = 64;

/// The largest value of any typed integer field (a time, a sequence number,
/// an offset, a length, a credit): 2^53 - 1, so that a browser holds every one
/// of them exactly in a JavaScript number. Integers inside `args` and stream
/// values are not typed fields and keep the full CBOR range.
pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// One relay delivery, whole.
pub const MAX_DELIVERY_BYTES: usize = 1_048_576;
/// What a [`DeliveryV1`](super::DeliveryV1) adds around its MLS message.
pub const DELIVERY_OVERHEAD_BYTES: usize = 44;
/// One TLS-serialized MLSMessage.
pub const MAX_MLS_MESSAGE_BYTES: usize = MAX_DELIVERY_BYTES - DELIVERY_OVERHEAD_BYTES;
/// The fewest bytes an MLSMessage can be read from: its version and wire format.
pub const MIN_MLS_MESSAGE_BYTES: usize = 4;
/// One decrypted application frame. The 64 KiB under [`MAX_DELIVERY_BYTES`] is
/// the room MLS framing, the AEAD tag and padding have.
pub const MAX_APPLICATION_BYTES: usize = 983_040;
/// One signed intent: what a person approves and a token signs.
pub const MAX_INTENT_BYTES: usize = 65_536;
/// Arrays and maps nested inside one open value (`args`, a stream `value`, a
/// result). The outermost container is depth 1.
pub const MAX_DEPTH: usize = 16;

/// Bytes of a `method`.
pub const MAX_METHOD_BYTES: usize = 128;
/// Bytes of a `resource`.
pub const MAX_RESOURCE_BYTES: usize = 256;
/// Seconds between an intent's `issued` and its `expires`.
pub const MAX_INTENT_LIFETIME_SECS: u64 = 300;

/// Entries of an intent's attachment manifest.
pub const MAX_ATTACHMENTS: usize = 16;
/// Bytes of one attachment, as its manifest entry declares them.
pub const MAX_ATTACHMENT_BYTES: u64 = 67_108_864;
/// Bytes of an attachment's name.
pub const MAX_ATTACHMENT_NAME_BYTES: usize = 255;
/// Bytes of an attachment's media type.
pub const MAX_MEDIA_TYPE_BYTES: usize = 127;
/// Content bytes of one attachment chunk frame.
pub const MAX_CHUNK_BYTES: usize = 262_144;

/// Bytes of the COSE_Sign1 of a PIV proof: the intent and its framing.
pub const MAX_SIGN1_BYTES: usize = MAX_INTENT_BYTES + 256;
/// Bytes of a WebAuthn credential ID.
pub const MAX_CREDENTIAL_ID_BYTES: usize = 1023;
/// The fewest bytes of WebAuthn authenticator data: RP ID hash, flags, counter.
pub const MIN_AUTHENTICATOR_DATA_BYTES: usize = 37;
/// Bytes of WebAuthn authenticator data in an assertion.
pub const MAX_AUTHENTICATOR_DATA_BYTES: usize = 1024;
/// Bytes of WebAuthn client data JSON.
pub const MAX_CLIENT_DATA_JSON_BYTES: usize = 4096;
/// Bytes of a WebAuthn assertion signature, in the credential algorithm's own
/// encoding.
pub const MAX_WEBAUTHN_SIGNATURE_BYTES: usize = 512;
/// Bytes of a WebAuthn user handle.
pub const MAX_USER_HANDLE_BYTES: usize = 64;
/// Bytes of a WebAuthn attestation object at enrollment.
pub const MAX_ATTESTATION_OBJECT_BYTES: usize = 16_384;

/// The fewest certificates of a PIV enrollment chain: the slot's attestation
/// and the device's attestation certificate (f9).
pub const MIN_CHAIN_CERTS: usize = 2;
/// The most certificates of a PIV enrollment chain.
pub const MAX_CHAIN_CERTS: usize = 4;
/// DER bytes of one certificate of the chain.
pub const MAX_CERT_BYTES: usize = 4096;

/// Bytes of a typed error's code.
pub const MAX_ERROR_CODE_BYTES: usize = 64;
/// Bytes of a typed error's message.
pub const MAX_ERROR_MESSAGE_BYTES: usize = 1024;
