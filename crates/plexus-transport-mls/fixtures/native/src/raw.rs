//! A byte-level CBOR builder, written apart from the wire crate's encoder.
//!
//! The vectors are assembled with it so that their bytes do not come from the
//! code they test, and so that the malformed ones can be written at all: it
//! sorts nothing, checks nothing and will write any head it is asked for.

/// A head of `major` with argument `n`, in its shortest form.
pub fn head(major: u8, n: u64) -> Vec<u8> {
    let m = major << 5;
    match n {
        0..=23 => vec![m | n as u8],
        24..=0xff => vec![m | 24, n as u8],
        0x100..=0xffff => [&[m | 25][..], &(n as u16).to_be_bytes()].concat(),
        0x1_0000..=0xffff_ffff => [&[m | 26][..], &(n as u32).to_be_bytes()].concat(),
        _ => [&[m | 27][..], &n.to_be_bytes()].concat(),
    }
}

/// An unsigned integer.
pub fn u(n: u64) -> Vec<u8> {
    head(0, n)
}

/// The negative integer `-1 - n`.
pub fn neg(n: u64) -> Vec<u8> {
    head(1, n)
}

/// A byte string.
pub fn b(bytes: &[u8]) -> Vec<u8> {
    [head(2, bytes.len() as u64), bytes.to_vec()].concat()
}

/// A text string.
pub fn t(text: &str) -> Vec<u8> {
    [head(3, text.len() as u64), text.as_bytes().to_vec()].concat()
}

/// An array of items that are encoded already.
pub fn arr(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = head(4, items.len() as u64);
    items.iter().for_each(|i| out.extend_from_slice(i));
    out
}

/// A map of encoded keys and values, in the order given.
pub fn map(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = head(5, entries.len() as u64);
    for (k, v) in entries {
        out.extend_from_slice(k);
        out.extend_from_slice(v);
    }
    out
}

/// A map with text keys, put in wire order: shorter keys first, then bytewise.
pub fn tmap(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut sorted: Vec<_> = entries.to_vec();
    sorted.sort_by(|(a, _), (b, _)| (a.len(), a.as_bytes()).cmp(&(b.len(), b.as_bytes())));
    map(&sorted
        .into_iter()
        .map(|(k, v)| (t(k), v))
        .collect::<Vec<_>>())
}

/// `null`.
pub fn null() -> Vec<u8> {
    vec![0xf6]
}

/// `false` or `true`.
pub fn boolean(v: bool) -> Vec<u8> {
    vec![if v { 0xf5 } else { 0xf4 }]
}

/// A record under construction: integer-labelled fields in the order they
/// will be written, each value encoded already.
#[derive(Clone, Default)]
pub struct Rec(pub Vec<(Vec<u8>, Vec<u8>)>);

impl Rec {
    /// A record of these labelled values, in this order.
    pub fn of(fields: &[(u64, Vec<u8>)]) -> Self {
        Self(fields.iter().map(|(l, v)| (u(*l), v.clone())).collect())
    }

    /// The same record with the value at `label` replaced, or the field put
    /// where its label belongs if it was absent.
    pub fn with(mut self, label: u64, value: Vec<u8>) -> Self {
        let key = u(label);
        if let Some(entry) = self.0.iter_mut().find(|(k, _)| *k == key) {
            entry.1 = value;
        } else {
            let at = self
                .0
                .iter()
                .position(|(k, _)| k.len() > key.len() || (k.len() == key.len() && *k > key))
                .unwrap_or(self.0.len());
            self.0.insert(at, (key, value));
        }
        self
    }

    /// The same record without the field at `label`.
    pub fn without(mut self, label: u64) -> Self {
        let key = u(label);
        self.0.retain(|(k, _)| *k != key);
        self
    }

    /// The same record with one more entry at the end, whatever its key.
    pub fn then(mut self, key: Vec<u8>, value: Vec<u8>) -> Self {
        self.0.push((key, value));
        self
    }

    /// The same record with an entry put right after the field at `label`.
    pub fn after(mut self, label: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        let at = self.0.iter().position(|(k, _)| *k == u(label)).unwrap();
        self.0.insert(at + 1, (key, value));
        self
    }

    /// The value at `label`.
    pub fn get(&self, label: u64) -> &[u8] {
        &self.0.iter().find(|(k, _)| *k == u(label)).unwrap().1
    }

    /// The record's bytes.
    pub fn enc(&self) -> Vec<u8> {
        map(&self.0)
    }
}
