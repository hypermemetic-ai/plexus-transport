//! The one error type of the wire profile.
//!
//! Every rejection has a stable code ([`WireError::code`]). The codes are part
//! of the frozen profile: the negative vectors name them, and the native and
//! browser adapters must report the same code for the same bytes.

use core::fmt;

/// Why a byte string is not a v1 record.
///
/// A decoder returns the first violation it meets reading the bytes front to
/// back; no partial record is ever returned alongside an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WireError {
    // --- deterministic CBOR -------------------------------------------------
    /// The input ends inside an item, or a length runs past the input.
    Truncated,
    /// Bytes follow the one item a record consists of (a second record, a
    /// second intent, padding).
    TrailingBytes,
    /// An integer or a length is not in its shortest form.
    NonMinimal,
    /// An indefinite-length item, or a stray break.
    Indefinite,
    /// A reserved additional-information value (28..=30).
    Reserved,
    /// A tag. v1 has none, a tagged COSE_Sign1 included.
    Tag,
    /// A floating-point number. v1 has none.
    Float,
    /// A simple value other than `false`, `true` and `null`.
    Simple,
    /// A text string that is not UTF-8.
    Utf8,
    /// An item of another major type than the field's.
    WrongType,
    /// A map key of the wrong type: a record's keys are unsigned integers, an
    /// argument map's keys are text strings.
    MapKey,
    /// A map key that occurs twice.
    DuplicateKey,
    /// Map keys out of the bytewise order of their encodings.
    UnsortedKeys,

    // --- limits -------------------------------------------------------------
    /// Arrays and maps nested deeper than [`MAX_DEPTH`](super::limits::MAX_DEPTH).
    Depth,
    /// A delivery longer than [`MAX_DELIVERY_BYTES`](super::limits::MAX_DELIVERY_BYTES).
    DeliveryBytes,
    /// An application frame longer than
    /// [`MAX_APPLICATION_BYTES`](super::limits::MAX_APPLICATION_BYTES).
    ApplicationBytes,
    /// An intent longer than [`MAX_INTENT_BYTES`](super::limits::MAX_INTENT_BYTES).
    IntentBytes,
    /// More attachments than [`MAX_ATTACHMENTS`](super::limits::MAX_ATTACHMENTS).
    Attachments,

    // --- record shape -------------------------------------------------------
    /// A `v` other than 1.
    UnsupportedVersion,
    /// A `domain` other than the record's.
    UnknownDomain,
    /// A `kind`, `type`, `profile`, `decision` or `status` v1 does not define.
    UnknownKind,
    /// A field label v1 does not define. Every field is critical.
    UnknownField,
    /// A field v1 defines, in a record of a kind that does not carry it (a
    /// `method` in an approval).
    FieldNotAllowed,
    /// A required field is absent.
    MissingField,
    /// A byte or text string, or an array, whose length is outside the field's.
    Length,
    /// An integer outside the field's range.
    Range,
    /// A text field with characters outside the field's set.
    Text,
    /// A public key that is not in the field's encoding.
    KeyEncoding,
    /// The bytes decode but are not the encoding of what they decode to.
    NonCanonical,

    // --- intent -------------------------------------------------------------
    /// A `method` that is empty, too long or outside the method character set
    /// (so no separator can carry a second one).
    MethodSyntax,
    /// A `resource` that is empty, too long or outside its character set.
    ResourceSyntax,
    /// `expires` is not after `issued`.
    TimeWindow,
    /// `expires - issued` is over
    /// [`MAX_INTENT_LIFETIME_SECS`](super::limits::MAX_INTENT_LIFETIME_SECS).
    Lifetime,

    // --- proof --------------------------------------------------------------
    /// The COSE payload is not the enclosed intent, byte for byte.
    PayloadMismatch,
    /// The COSE `kid` is not the intent's credential reference.
    CredentialMismatch,
    /// A COSE `alg` other than ES256.
    UnknownAlgorithm,
    /// A COSE unprotected header that is not the empty map.
    Unprotected,

    // --- MLS framing --------------------------------------------------------
    /// An MLSMessage whose version is not `mls10`.
    MlsVersion,
    /// An MLSMessage whose wire format RFC 9420 does not define.
    MlsWireFormat,
}

impl WireError {
    /// The stable code of this rejection, as the vectors and both adapters
    /// print it.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Truncated => "cbor.truncated",
            Self::TrailingBytes => "cbor.trailing_bytes",
            Self::NonMinimal => "cbor.non_minimal",
            Self::Indefinite => "cbor.indefinite",
            Self::Reserved => "cbor.reserved",
            Self::Tag => "cbor.tag",
            Self::Float => "cbor.float",
            Self::Simple => "cbor.simple",
            Self::Utf8 => "cbor.utf8",
            Self::WrongType => "cbor.wrong_type",
            Self::MapKey => "cbor.map_key",
            Self::DuplicateKey => "cbor.duplicate_key",
            Self::UnsortedKeys => "cbor.unsorted_keys",
            Self::Depth => "limit.depth",
            Self::DeliveryBytes => "limit.delivery_bytes",
            Self::ApplicationBytes => "limit.application_bytes",
            Self::IntentBytes => "limit.intent_bytes",
            Self::Attachments => "limit.attachments",
            Self::UnsupportedVersion => "wire.unsupported_version",
            Self::UnknownDomain => "wire.unknown_domain",
            Self::UnknownKind => "wire.unknown_kind",
            Self::UnknownField => "wire.unknown_field",
            Self::FieldNotAllowed => "wire.field_not_allowed",
            Self::MissingField => "wire.missing_field",
            Self::Length => "wire.length",
            Self::Range => "wire.range",
            Self::Text => "wire.text",
            Self::KeyEncoding => "wire.key_encoding",
            Self::NonCanonical => "wire.non_canonical",
            Self::MethodSyntax => "intent.method_syntax",
            Self::ResourceSyntax => "intent.resource_syntax",
            Self::TimeWindow => "intent.time_window",
            Self::Lifetime => "intent.lifetime",
            Self::PayloadMismatch => "proof.payload_mismatch",
            Self::CredentialMismatch => "proof.credential_mismatch",
            Self::UnknownAlgorithm => "proof.unknown_algorithm",
            Self::Unprotected => "proof.unprotected",
            Self::MlsVersion => "mls.version",
            Self::MlsWireFormat => "mls.wire_format",
        }
    }
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl core::error::Error for WireError {}

/// The result of every decoder and encoder of the profile.
pub type Result<T> = core::result::Result<T, WireError>;
