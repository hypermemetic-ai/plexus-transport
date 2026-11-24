//! The deterministic CBOR subset of the profile, and nothing more.
//!
//! RFC 8949 §4.2.1 core deterministic encoding, narrowed: definite lengths
//! only, shortest heads, map keys unique and in the bytewise order of their
//! encodings, no tags, no floats, and of the simple values only `false`,
//! `true` and `null`. Under those rules a value has exactly one encoding, so a
//! decoder that enforces them accepts exactly the bytes an encoder writes.
//!
//! Hand-written because the property being relied on is the rejection of every
//! other encoding, which general CBOR libraries do not promise.

use alloc::string::String;
use alloc::vec::Vec;

use super::error::{Result, WireError};
use super::limits::MAX_DEPTH;

/// An open value: the `args` of an intent, a stream item, a result.
///
/// Integers keep the whole CBOR range: [`Value::Uint`] is `n`,
/// [`Value::Nint`] is `-1 - n`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `null`.
    Null,
    /// `false` or `true`.
    Bool(bool),
    /// An unsigned integer.
    Uint(u64),
    /// A negative integer: the value is `-1 - n`.
    Nint(u64),
    /// A byte string.
    Bytes(Vec<u8>),
    /// A text string.
    Text(String),
    /// An array.
    Array(Vec<Value>),
    /// A map with text keys.
    Map(Map),
}

impl Value {
    /// A text value.
    #[must_use]
    pub fn text(s: &str) -> Self {
        Self::Text(String::from(s))
    }

    /// An integer value from a signed integer.
    #[must_use]
    pub fn int(i: i64) -> Self {
        if i >= 0 {
            Self::Uint(i as u64)
        } else {
            Self::Nint(!(i as u64))
        }
    }

    /// Decodes one value that is the whole of `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let v = r.value()?;
        r.finish()?;
        Ok(v)
    }

    /// The one encoding of this value.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.value(self);
        w.into_bytes()
    }

    /// How deep arrays and maps nest in this value; a scalar is 0.
    fn depth(&self) -> usize {
        match self {
            Self::Array(items) => 1 + items.iter().map(Self::depth).max().unwrap_or(0),
            Self::Map(map) => 1 + map.iter().map(|(_, v)| v.depth()).max().unwrap_or(0),
            _ => 0,
        }
    }

    /// Whether a decoder accepts this value: nesting within [`MAX_DEPTH`].
    pub(crate) fn check_depth(&self) -> Result<()> {
        if self.depth() > MAX_DEPTH {
            return Err(WireError::Depth);
        }
        Ok(())
    }
}

/// A map with text keys, held in the order of the wire: shorter keys first,
/// equal lengths bytewise. A `Map` cannot hold a key twice.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Map {
    entries: Vec<(String, Value)>,
}

/// The wire order of two text keys: the bytewise order of their encodings,
/// which for shortest-form heads is length first, then bytes.
fn key_order(a: &str, b: &str) -> core::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

impl Map {
    /// The empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Puts `key` in its place. A key already present is refused
    /// ([`WireError::DuplicateKey`]) and the map is unchanged.
    pub fn insert(&mut self, key: &str, value: Value) -> Result<()> {
        match self.entries.binary_search_by(|(k, _)| key_order(k, key)) {
            Ok(_) => Err(WireError::DuplicateKey),
            Err(at) => {
                self.entries.insert(at, (String::from(key), value));
                Ok(())
            }
        }
    }

    /// A map of these entries; a repeated key is refused.
    pub fn from_entries<'a>(entries: impl IntoIterator<Item = (&'a str, Value)>) -> Result<Self> {
        let mut map = Self::new();
        for (k, v) in entries {
            map.insert(k, v)?;
        }
        Ok(map)
    }

    /// The value at `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .binary_search_by(|(k, _)| key_order(k, key))
            .ok()
            .map(|at| &self.entries[at].1)
    }

    /// The entries in wire order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Whether a decoder accepts this map: nesting within [`MAX_DEPTH`], the
    /// map itself being one level.
    pub(crate) fn check_depth(&self) -> Result<()> {
        let inner = self.iter().map(|(_, v)| v.depth()).max().unwrap_or(0);
        if 1 + inner > MAX_DEPTH {
            return Err(WireError::Depth);
        }
        Ok(())
    }

    /// How many entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// The head of one item: its major type and argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Head {
    Uint(u64),
    Nint(u64),
    Bytes(u64),
    Text(u64),
    Array(u64),
    Map(u64),
    Bool(bool),
    Null,
}

/// Reads items off the front of a byte string, refusing every encoding the
/// profile does not write.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// The input is used up, or [`WireError::TrailingBytes`].
    pub(crate) fn finish(&self) -> Result<()> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(WireError::TrailingBytes)
        }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: u64) -> Result<&'a [u8]> {
        // Compared as u64 so that a length over the address space of a 32-bit
        // target is the same rejection it is on a 64-bit one.
        if n > self.remaining() as u64 {
            return Err(WireError::Truncated);
        }
        let n = n as usize;
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn be(&mut self, n: u64) -> Result<u64> {
        Ok(self
            .take(n)?
            .iter()
            .fold(0u64, |acc, b| (acc << 8) | u64::from(*b)))
    }

    /// Whether the next item is `null`, without reading it.
    pub(crate) fn at_null(&self) -> bool {
        self.buf.get(self.pos) == Some(&0xf6)
    }

    pub(crate) fn head(&mut self) -> Result<Head> {
        let initial = self.take(1)?[0];
        let major = initial >> 5;
        let info = initial & 0x1f;
        if major == 7 {
            return match info {
                20 => Ok(Head::Bool(false)),
                21 => Ok(Head::Bool(true)),
                22 => Ok(Head::Null),
                25..=27 => Err(WireError::Float),
                28..=30 => Err(WireError::Reserved),
                31 => Err(WireError::Indefinite),
                _ => Err(WireError::Simple),
            };
        }
        let arg = match info {
            0..=23 => u64::from(info),
            24 => self.minimal(1, 24)?,
            25 => self.minimal(2, 0x100)?,
            26 => self.minimal(4, 0x1_0000)?,
            27 => self.minimal(8, 0x1_0000_0000)?,
            28..=30 => return Err(WireError::Reserved),
            _ => return Err(WireError::Indefinite),
        };
        match major {
            0 => Ok(Head::Uint(arg)),
            1 => Ok(Head::Nint(arg)),
            2 => Ok(Head::Bytes(arg)),
            3 => Ok(Head::Text(arg)),
            4 => Ok(Head::Array(arg)),
            5 => Ok(Head::Map(arg)),
            _ => Err(WireError::Tag),
        }
    }

    /// An argument of `width` bytes, which is shortest only from `floor` up.
    fn minimal(&mut self, width: u64, floor: u64) -> Result<u64> {
        let arg = self.be(width)?;
        if arg < floor {
            return Err(WireError::NonMinimal);
        }
        Ok(arg)
    }

    pub(crate) fn uint(&mut self) -> Result<u64> {
        match self.head()? {
            Head::Uint(n) => Ok(n),
            _ => Err(WireError::WrongType),
        }
    }

    /// A byte string of any length the input holds.
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        match self.head()? {
            Head::Bytes(n) => self.take(n),
            _ => Err(WireError::WrongType),
        }
    }

    /// A byte string of `min..=max` bytes. The length is judged before the
    /// bytes are looked at.
    pub(crate) fn bytes_within(&mut self, min: usize, max: usize) -> Result<&'a [u8]> {
        match self.head()? {
            Head::Bytes(n) => {
                if n < min as u64 || n > max as u64 {
                    return Err(WireError::Length);
                }
                self.take(n)
            }
            _ => Err(WireError::WrongType),
        }
    }

    /// A byte string of exactly `N` bytes.
    pub(crate) fn bytes_exact<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.bytes_within(N, N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    /// A text string of any length the input holds.
    pub(crate) fn text(&mut self) -> Result<&'a str> {
        match self.head()? {
            Head::Text(n) => {
                let bytes = self.take(n)?;
                core::str::from_utf8(bytes).map_err(|_| WireError::Utf8)
            }
            _ => Err(WireError::WrongType),
        }
    }

    /// The length of an array.
    pub(crate) fn array(&mut self) -> Result<u64> {
        match self.head()? {
            Head::Array(n) => Ok(n),
            _ => Err(WireError::WrongType),
        }
    }

    /// The entry count of a map.
    pub(crate) fn map(&mut self) -> Result<u64> {
        match self.head()? {
            Head::Map(n) => Ok(n),
            _ => Err(WireError::WrongType),
        }
    }

    /// One open value.
    pub(crate) fn value(&mut self) -> Result<Value> {
        self.value_at(0)
    }

    /// One open value that must be a map.
    pub(crate) fn value_map(&mut self) -> Result<Map> {
        match self.value()? {
            Value::Map(map) => Ok(map),
            _ => Err(WireError::WrongType),
        }
    }

    fn value_at(&mut self, depth: usize) -> Result<Value> {
        Ok(match self.head()? {
            Head::Null => Value::Null,
            Head::Bool(b) => Value::Bool(b),
            Head::Uint(n) => Value::Uint(n),
            Head::Nint(n) => Value::Nint(n),
            Head::Bytes(n) => Value::Bytes(self.take(n)?.to_vec()),
            Head::Text(n) => {
                let bytes = self.take(n)?;
                Value::Text(String::from(
                    core::str::from_utf8(bytes).map_err(|_| WireError::Utf8)?,
                ))
            }
            Head::Array(n) => {
                if depth >= MAX_DEPTH {
                    return Err(WireError::Depth);
                }
                // Every element is at least one byte: a count over what
                // remains is refused before anything is reserved for it.
                if n > self.remaining() as u64 {
                    return Err(WireError::Truncated);
                }
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(self.value_at(depth + 1)?);
                }
                Value::Array(items)
            }
            Head::Map(n) => {
                if depth >= MAX_DEPTH {
                    return Err(WireError::Depth);
                }
                if n > self.remaining() as u64 / 2 {
                    return Err(WireError::Truncated);
                }
                let mut entries: Vec<(String, Value)> = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let key = match self.head()? {
                        Head::Text(len) => {
                            let bytes = self.take(len)?;
                            core::str::from_utf8(bytes).map_err(|_| WireError::Utf8)?
                        }
                        _ => return Err(WireError::MapKey),
                    };
                    if let Some((last, _)) = entries.last() {
                        if key_order(last, key) != core::cmp::Ordering::Less {
                            // Out of place. Say which fault it is: a key seen
                            // before is a duplicate wherever it sits.
                            let seen = entries.binary_search_by(|(k, _)| key_order(k, key)).is_ok();
                            return Err(if seen {
                                WireError::DuplicateKey
                            } else {
                                WireError::UnsortedKeys
                            });
                        }
                    }
                    let value = self.value_at(depth + 1)?;
                    entries.push((String::from(key), value));
                }
                Value::Map(Map { entries })
            }
        })
    }
}

/// The entries of a record: a map whose keys are small unsigned integers,
/// each at most once and in ascending order.
pub(crate) struct Fields {
    left: u64,
    seen: u64,
    last: Option<u64>,
}

impl Fields {
    /// Opens a record map.
    pub(crate) fn open(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            left: r.map()?,
            seen: 0,
            last: None,
        })
    }

    /// The label of the next entry, whose value the caller then reads; `None`
    /// when the record has no more.
    pub(crate) fn next(&mut self, r: &mut Reader<'_>) -> Result<Option<u64>> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        let label = match r.head()? {
            Head::Uint(n) => n,
            _ => return Err(WireError::MapKey),
        };
        // No table of v1 reaches label 64.
        if label >= 64 {
            return Err(WireError::UnknownField);
        }
        if self.seen & (1 << label) != 0 {
            return Err(WireError::DuplicateKey);
        }
        if self.last.is_some_and(|last| label < last) {
            return Err(WireError::UnsortedKeys);
        }
        self.seen |= 1 << label;
        self.last = Some(label);
        Ok(Some(label))
    }

    /// The next entry must be `label`: the fields a record opens with.
    pub(crate) fn expect(&mut self, r: &mut Reader<'_>, label: u64) -> Result<()> {
        match self.next(r)? {
            Some(found) if found == label => Ok(()),
            _ => Err(WireError::MissingField),
        }
    }
}

/// Writes the one encoding of what it is given.
pub(crate) struct Writer {
    out: Vec<u8>,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Self { out: Vec::new() }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.out
    }

    fn head(&mut self, major: u8, arg: u64) {
        let m = major << 5;
        if arg < 24 {
            self.out.push(m | arg as u8);
        } else if arg < 0x100 {
            self.out.push(m | 24);
            self.out.push(arg as u8);
        } else if arg < 0x1_0000 {
            self.out.push(m | 25);
            self.out.extend_from_slice(&(arg as u16).to_be_bytes());
        } else if arg < 0x1_0000_0000 {
            self.out.push(m | 26);
            self.out.extend_from_slice(&(arg as u32).to_be_bytes());
        } else {
            self.out.push(m | 27);
            self.out.extend_from_slice(&arg.to_be_bytes());
        }
    }

    pub(crate) fn uint(&mut self, n: u64) {
        self.head(0, n);
    }

    /// The negative integer `-1 - n`.
    pub(crate) fn nint(&mut self, n: u64) {
        self.head(1, n);
    }

    pub(crate) fn bytes(&mut self, b: &[u8]) {
        self.head(2, b.len() as u64);
        self.out.extend_from_slice(b);
    }

    pub(crate) fn text(&mut self, s: &str) {
        self.head(3, s.len() as u64);
        self.out.extend_from_slice(s.as_bytes());
    }

    pub(crate) fn array(&mut self, len: usize) {
        self.head(4, len as u64);
    }

    pub(crate) fn map(&mut self, len: usize) {
        self.head(5, len as u64);
    }

    pub(crate) fn null(&mut self) {
        self.out.push(0xf6);
    }

    pub(crate) fn value(&mut self, v: &Value) {
        match v {
            Value::Null => self.null(),
            Value::Bool(false) => self.out.push(0xf4),
            Value::Bool(true) => self.out.push(0xf5),
            Value::Uint(n) => self.uint(*n),
            Value::Nint(n) => self.nint(*n),
            Value::Bytes(b) => self.bytes(b),
            Value::Text(s) => self.text(s),
            Value::Array(items) => {
                self.array(items.len());
                for item in items {
                    self.value(item);
                }
            }
            Value::Map(map) => self.map_value(map),
        }
    }

    pub(crate) fn map_value(&mut self, map: &Map) {
        self.map(map.len());
        for (k, v) in map.iter() {
            self.text(k);
            self.value(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn err(bytes: &[u8]) -> WireError {
        Value::decode(bytes).unwrap_err()
    }

    #[test]
    fn integers_take_their_shortest_head() {
        for (n, hex) in [
            (0u64, &[0x00][..]),
            (23, &[0x17]),
            (24, &[0x18, 0x18]),
            (255, &[0x18, 0xff]),
            (256, &[0x19, 0x01, 0x00]),
            (65_535, &[0x19, 0xff, 0xff]),
            (65_536, &[0x1a, 0x00, 0x01, 0x00, 0x00]),
            (u64::from(u32::MAX), &[0x1a, 0xff, 0xff, 0xff, 0xff]),
            (
                1 << 32,
                &[0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                u64::MAX,
                &[0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
        ] {
            assert_eq!(Value::Uint(n).encode(), hex);
            assert_eq!(Value::decode(hex).unwrap(), Value::Uint(n));
        }
        assert_eq!(Value::int(-1).encode(), [0x20]);
        assert_eq!(Value::int(-500).encode(), [0x39, 0x01, 0xf3]);
        assert_eq!(Value::int(i64::MIN), Value::Nint(i64::MAX as u64));
    }

    #[test]
    fn longer_heads_are_refused() {
        assert_eq!(err(&[0x18, 0x17]), WireError::NonMinimal);
        assert_eq!(err(&[0x19, 0x00, 0xff]), WireError::NonMinimal);
        assert_eq!(err(&[0x1a, 0x00, 0x00, 0xff, 0xff]), WireError::NonMinimal);
        assert_eq!(
            err(&[0x1b, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff]),
            WireError::NonMinimal
        );
        // A length is an argument like any other.
        assert_eq!(err(&[0x58, 0x01, 0x00]), WireError::NonMinimal);
        assert_eq!(err(&[0x78, 0x01, 0x61]), WireError::NonMinimal);
    }

    #[test]
    fn what_the_profile_leaves_out_is_refused() {
        assert_eq!(err(&[0x5f, 0x41, 0x00, 0xff]), WireError::Indefinite);
        assert_eq!(err(&[0x9f, 0xff]), WireError::Indefinite);
        assert_eq!(err(&[0xbf, 0xff]), WireError::Indefinite);
        assert_eq!(err(&[0xff]), WireError::Indefinite);
        assert_eq!(err(&[0xc0, 0x60]), WireError::Tag);
        assert_eq!(err(&[0xd2, 0x80]), WireError::Tag);
        assert_eq!(err(&[0xf9, 0x3c, 0x00]), WireError::Float);
        assert_eq!(err(&[0xfa, 0, 0, 0, 0]), WireError::Float);
        assert_eq!(err(&[0xfb, 0, 0, 0, 0, 0, 0, 0, 0]), WireError::Float);
        assert_eq!(err(&[0xf7]), WireError::Simple);
        assert_eq!(err(&[0xe0]), WireError::Simple);
        assert_eq!(err(&[0xf8, 0x20]), WireError::Simple);
        assert_eq!(err(&[0x1c]), WireError::Reserved);
        assert_eq!(err(&[0xfc]), WireError::Reserved);
        assert_eq!(err(&[0x62, 0xc3, 0x28]), WireError::Utf8);
        assert_eq!(err(&[0xa1, 0x01, 0x02]), WireError::MapKey);
        assert_eq!(err(&[]), WireError::Truncated);
        assert_eq!(err(&[0x42, 0x00]), WireError::Truncated);
        assert_eq!(err(&[0x00, 0x00]), WireError::TrailingBytes);
    }

    #[test]
    fn a_length_over_the_input_reserves_nothing() {
        // 2^64 - 1 elements, entries or bytes: refused on the head.
        for major in [0x5b, 0x7b, 0x9b, 0xbb] {
            let mut bytes = vec![major];
            bytes.extend_from_slice(&[0xff; 8]);
            assert_eq!(err(&bytes), WireError::Truncated);
        }
    }

    #[test]
    fn map_keys_are_unique_and_in_wire_order() {
        // {"b": 1, "aa": 2}: shorter key first.
        let good = [0xa2, 0x61, 0x62, 0x01, 0x62, 0x61, 0x61, 0x02];
        let map = Map::from_entries([("aa", Value::Uint(2)), ("b", Value::Uint(1))]).unwrap();
        assert_eq!(Value::Map(map.clone()).encode(), good);
        assert_eq!(Value::decode(&good).unwrap(), Value::Map(map));
        // {"aa": 2, "b": 1}
        assert_eq!(
            err(&[0xa2, 0x62, 0x61, 0x61, 0x02, 0x61, 0x62, 0x01]),
            WireError::UnsortedKeys
        );
        // {"b": 1, "b": 2}
        assert_eq!(
            err(&[0xa2, 0x61, 0x62, 0x01, 0x61, 0x62, 0x02]),
            WireError::DuplicateKey
        );
        // {"a": 1, "b": 2, "a": 3}: a duplicate, though also out of order.
        assert_eq!(
            err(&[0xa3, 0x61, 0x61, 0x01, 0x61, 0x62, 0x02, 0x61, 0x61, 0x03]),
            WireError::DuplicateKey
        );
        let mut map = Map::new();
        map.insert("k", Value::Null).unwrap();
        assert_eq!(
            map.insert("k", Value::Bool(true)),
            Err(WireError::DuplicateKey)
        );
        assert_eq!(map.get("k"), Some(&Value::Null));
    }

    #[test]
    fn nesting_stops_at_the_limit() {
        let nested = |depth: usize| {
            let mut bytes = vec![0x81; depth];
            bytes.push(0x00);
            bytes
        };
        assert!(Value::decode(&nested(MAX_DEPTH)).is_ok());
        assert_eq!(err(&nested(MAX_DEPTH + 1)), WireError::Depth);
        let mut v = Value::Uint(0);
        for _ in 0..=MAX_DEPTH {
            v = Value::Array(vec![v]);
        }
        assert_eq!(v.check_depth(), Err(WireError::Depth));
    }

    #[test]
    fn every_decoded_value_encodes_to_its_input() {
        let bytes = [
            0xa3, 0x61, 0x61, 0x82, 0x01, 0x20, 0x61, 0x62, 0x43, 0x01, 0x02, 0x03, 0x62, 0x63,
            0x63, 0xa1, 0x61, 0x64, 0x83, 0xf4, 0xf5, 0xf6,
        ];
        assert_eq!(Value::decode(&bytes).unwrap().encode(), bytes);
    }
}
