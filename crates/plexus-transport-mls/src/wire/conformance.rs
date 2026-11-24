//! What an adapter observes of a byte string: the oracle of the conformance
//! vectors.
//!
//! A native and a browser adapter hand the same bytes to [`observe`] and must
//! print the same [`line()`]: the same intent bytes, digest and typed view for a
//! record that is accepted, the same code for one that is refused.

use alloc::string::String;
use alloc::vec::Vec;

use super::delivery::DeliveryV1;
use super::error::Result;
use super::frame::ApplicationV1;
use super::intent::SealedIntent;
use super::types::{hex, Digest};

/// Which decoder a vector is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Record {
    /// [`DeliveryV1::decode_bounded`].
    Delivery,
    /// [`ApplicationV1::decode`].
    Application,
    /// [`SealedIntent::decode`].
    Intent,
}

impl Record {
    /// Its name in the vector file.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delivery => "delivery",
            Self::Application => "application",
            Self::Intent => "intent",
        }
    }

    /// The record of that name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "delivery" => Some(Self::Delivery),
            "application" => Some(Self::Application),
            "intent" => Some(Self::Intent),
            _ => None,
        }
    }

    /// Its number across the browser binding's ABI.
    #[must_use]
    pub const fn code(self) -> u32 {
        match self {
            Self::Delivery => 0,
            Self::Application => 1,
            Self::Intent => 2,
        }
    }

    /// The record of that number.
    #[must_use]
    pub const fn from_code(code: u32) -> Option<Self> {
        match code {
            0 => Some(Self::Delivery),
            1 => Some(Self::Application),
            2 => Some(Self::Intent),
            _ => None,
        }
    }
}

/// What an accepted record is, as far as dispatch is concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    /// The exact intent bytes the record carries, when it carries an intent.
    pub intent: Option<Vec<u8>>,
    /// Their digest.
    pub digest: Option<Digest>,
    /// The canonical JSON view of the whole record, typed arguments included.
    pub view: String,
}

/// Decodes `bytes` as `record` and says what was found.
pub fn observe(record: Record, bytes: &[u8]) -> Result<Observation> {
    let sealed = |intent: &SealedIntent| (Some(intent.bytes().to_vec()), Some(intent.digest()));
    Ok(match record {
        Record::Delivery => Observation {
            intent: None,
            digest: None,
            view: DeliveryV1::decode_bounded(bytes)?.view(),
        },
        Record::Application => {
            let frame = ApplicationV1::decode(bytes)?;
            let (intent, digest) = match &frame {
                ApplicationV1::Invoke(invoke) => sealed(&invoke.intent),
                _ => (None, None),
            };
            Observation {
                intent,
                digest,
                view: frame.view(),
            }
        }
        Record::Intent => {
            let intent = SealedIntent::decode(bytes)?;
            let (bytes, digest) = sealed(&intent);
            Observation {
                intent: bytes,
                digest,
                view: intent.intent().view(),
            }
        }
    })
}

/// The observation as one line of text, the form both adapters report in:
/// `ok<TAB>intent hex<TAB>digest hex<TAB>view` with `-` for an absent intent
/// or digest, or `error<TAB>code`.
#[must_use]
pub fn line(observed: &Result<Observation>) -> String {
    match observed {
        Ok(o) => {
            let mut out = String::from("ok\t");
            match &o.intent {
                Some(bytes) => out.push_str(&hex(bytes)),
                None => out.push('-'),
            }
            out.push('\t');
            match &o.digest {
                Some(digest) => out.push_str(&hex(&digest.0)),
                None => out.push('-'),
            }
            out.push('\t');
            out.push_str(&o.view);
            out
        }
        Err(e) => {
            let mut out = String::from("error\t");
            out.push_str(e.code());
            out
        }
    }
}
