//! The native adapter fixture of the wire profile.
//!
//! It consumes `plexus-transport-mls` through its public API alone:
//! [`vectors`] assembles the v1 vectors with a byte-level builder of its own
//! ([`raw`]), [`file`] reads and writes the committed vector file, and
//! [`report`] is what the native decoder makes of each vector, in the form the
//! browser adapter reports too.

pub mod file;
pub mod raw;
pub mod vectors;

use plexus_transport_mls::wire::conformance::{line, observe};

use vectors::Vector;

/// What one adapter run found.
pub struct Report {
    /// One line per vector, `name<TAB>observation`, then a summary line.
    pub text: String,
    /// The vectors whose observation is not the expected one.
    pub mismatches: Vec<String>,
}

/// Runs every vector through the native decoder and an inert dispatcher.
///
/// The dispatcher stands where an executor would: it is reached only with an
/// accepted intent, and counts. A refused vector never reaches it.
pub fn report(vectors: &[Vector]) -> Report {
    let mut text = String::new();
    let mut mismatches = Vec::new();
    let (mut accepted, mut refused, mut dispatched) = (0, 0, 0);
    for vector in vectors {
        let observed = observe(vector.record, &vector.bytes());
        match &observed {
            Ok(o) => {
                accepted += 1;
                if o.intent.is_some() {
                    dispatched += 1;
                }
            }
            Err(_) => refused += 1,
        }
        let observed = line(&observed);
        if observed != vector.expected_line() {
            mismatches.push(format!(
                "{}: expected {:?}, observed {:?}",
                vector.name,
                vector.expected_line(),
                observed
            ));
        }
        text.push_str(&format!("{}\t{}\n", vector.name, observed));
    }
    text.push_str(&format!(
        "summary\tvectors={}\taccepted={accepted}\trefused={refused}\tdispatched={dispatched}\n",
        vectors.len()
    ));
    Report { text, mismatches }
}
