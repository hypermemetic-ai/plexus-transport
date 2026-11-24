//! The fixed-width identifiers of the profile.

use alloc::string::String;
use core::fmt;

/// Lower-case hex of `bytes`.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0x0f)] as char);
    }
    out
}

macro_rules! fixed_bytes {
    ($(#[$doc:meta])* $name:ident, $len:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; $len]);

        impl $name {
            /// Its length on the wire, in bytes.
            pub const LEN: usize = $len;

            /// The bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $len] {
                &self.0
            }
        }

        impl From<[u8; $len]> for $name {
            fn from(bytes: [u8; $len]) -> Self {
                Self(bytes)
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), hex(&self.0))
            }
        }
    };
}

fixed_bytes!(
    /// A call's identifier: a UUID as its 16 bytes. With the local principal
    /// and the final audience it names one command across retries, epochs and
    /// delivery paths.
    RequestId,
    16
);
fixed_bytes!(
    /// A SHA-256 fingerprint of an endpoint identity: a host, a client leaf, a
    /// presenter, a reply recipient.
    Fingerprint,
    32
);
fixed_bytes!(
    /// The pairwise reference to a user that the host resolves to its local
    /// principal. Opaque on the wire; it names nobody to a third party.
    PrincipalRef,
    32
);
fixed_bytes!(
    /// The stable reference to an enrolled credential
    /// ([`credential_ref`](super::credential_ref)). Also the COSE `kid`.
    CredentialRef,
    32
);
fixed_bytes!(
    /// A verifier's random, single-use value.
    Nonce,
    32
);
fixed_bytes!(
    /// A SHA-256 digest.
    Digest,
    32
);
fixed_bytes!(
    /// The random mailbox a delivery is addressed to. All the relay routes on.
    Mailbox,
    32
);
