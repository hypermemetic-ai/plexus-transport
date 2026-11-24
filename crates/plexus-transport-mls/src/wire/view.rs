//! The canonical JSON view: one deterministic text for one decoded record.
//!
//! It is what an approval surface renders from and what the conformance
//! fixtures compare. It is a rendering, never an input: authority stays with
//! the bytes. Compact JSON, fields in a fixed order; byte strings of typed
//! fields are lower-case hex strings. Inside an open value (`args`, a stream
//! `value`, a result), where a type cannot be read off the field:
//!
//! - a byte string is `{"$bytes": "<hex>"}`;
//! - an integer outside ±(2^53 − 1) is `{"$int": "<decimal>"}`;
//! - a map key that begins with `$` is written with one more `$` in front,
//!   so neither form above can be forged by a map.

use alloc::string::String;
use core::fmt::Write;

use super::cbor::{Map, Value};
use super::limits::MAX_SAFE_INTEGER;
use super::types::hex;

/// Appends `s` as a JSON string.
pub(crate) fn json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Appends an open value.
pub(crate) fn json_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Uint(n) => {
            if *n <= MAX_SAFE_INTEGER {
                let _ = write!(out, "{n}");
            } else {
                let _ = write!(out, "{{\"$int\":\"{n}\"}}");
            }
        }
        Value::Nint(n) => {
            let value = -1 - i128::from(*n);
            if *n < MAX_SAFE_INTEGER {
                let _ = write!(out, "{value}");
            } else {
                let _ = write!(out, "{{\"$int\":\"{value}\"}}");
            }
        }
        Value::Bytes(b) => {
            let _ = write!(out, "{{\"$bytes\":\"{}\"}}", hex(b));
        }
        Value::Text(s) => json_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_value(out, item);
            }
            out.push(']');
        }
        Value::Map(map) => json_map(out, map),
    }
}

/// Appends an open map.
pub(crate) fn json_map(out: &mut String, map: &Map) {
    out.push('{');
    for (i, (k, v)) in map.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if k.starts_with('$') {
            let mut escaped = String::with_capacity(k.len() + 1);
            escaped.push('$');
            escaped.push_str(k);
            json_string(out, &escaped);
        } else {
            json_string(out, k);
        }
        out.push(':');
        json_value(out, v);
    }
    out.push('}');
}

/// Writes one JSON object, field by field.
pub(crate) struct Object {
    out: String,
    any: bool,
}

impl Object {
    pub(crate) fn new() -> Self {
        Self {
            out: String::from("{"),
            any: false,
        }
    }

    fn key(&mut self, name: &str) {
        if self.any {
            self.out.push(',');
        }
        self.any = true;
        json_string(&mut self.out, name);
        self.out.push(':');
    }

    pub(crate) fn str(mut self, name: &str, value: &str) -> Self {
        self.key(name);
        json_string(&mut self.out, value);
        self
    }

    pub(crate) fn hex(mut self, name: &str, value: &[u8]) -> Self {
        self.key(name);
        self.out.push('"');
        self.out.push_str(&hex(value));
        self.out.push('"');
        self
    }

    pub(crate) fn opt_hex(mut self, name: &str, value: Option<&[u8]>) -> Self {
        match value {
            Some(bytes) => self.hex(name, bytes),
            None => {
                self.key(name);
                self.out.push_str("null");
                self
            }
        }
    }

    /// A typed integer: within 2^53 − 1 by the field's range.
    pub(crate) fn num(mut self, name: &str, value: u64) -> Self {
        self.key(name);
        let _ = write!(self.out, "{value}");
        self
    }

    pub(crate) fn value(mut self, name: &str, value: &Value) -> Self {
        self.key(name);
        json_value(&mut self.out, value);
        self
    }

    pub(crate) fn map(mut self, name: &str, value: &Map) -> Self {
        self.key(name);
        json_map(&mut self.out, value);
        self
    }

    /// A field whose value is JSON already.
    pub(crate) fn raw(mut self, name: &str, json: &str) -> Self {
        self.key(name);
        self.out.push_str(json);
        self
    }

    pub(crate) fn end(mut self) -> String {
        self.out.push('}');
        self.out
    }
}

/// A JSON array of values that are JSON already.
pub(crate) fn array(items: impl IntoIterator<Item = String>) -> String {
    let mut out = String::from("[");
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&item);
    }
    out.push(']');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn open_values_keep_their_types() {
        let map = Map::from_entries([
            ("n", Value::Uint(MAX_SAFE_INTEGER)),
            ("big", Value::Uint(MAX_SAFE_INTEGER + 1)),
            ("neg", Value::Nint(MAX_SAFE_INTEGER - 1)),
            ("negbig", Value::Nint(u64::MAX)),
            ("raw", Value::Bytes(vec![0xde, 0xad])),
            ("$bytes", Value::text("a \"map\"\n")),
            ("list", Value::Array(vec![Value::Null, Value::Bool(true)])),
        ])
        .unwrap();
        let mut out = String::new();
        json_map(&mut out, &map);
        assert_eq!(
            out,
            concat!(
                r#"{"n":9007199254740991,"big":{"$int":"9007199254740992"},"neg":-9007199254740991,"#,
                r#""raw":{"$bytes":"dead"},"list":[null,true],"$$bytes":"a \"map\"\u000a","#,
                r#""negbig":{"$int":"-18446744073709551616"}}"#
            )
        );
    }
}
