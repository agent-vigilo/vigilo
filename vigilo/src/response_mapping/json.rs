//! Borrowed JSON fragments with bounded structural parsing and exact numeric bytes.
//!
//! Syntax validation precedes bounded tree construction. Every nested fragment
//! consumes the shared parsing budget, including JSON decoded from strings.
//! Objects reject duplicate decoded keys; numbers remain borrowed tokens instead
//! of floating-point values. Pointer traversal distinguishes absent members from
//! invalid parent types. This module owns no response resources or host handles.

use std::collections::BTreeMap;

use serde::de::{
    self,
    DeserializeSeed,
    MapAccess,
    SeqAccess,
    Visitor,
};
use serde_json::value::RawValue;

use super::{
    Budget,
    ErrorKind,
    MappingError,
    Result,
};

pub(super) struct Node<'a> {
    pub raw: &'a RawValue,
    pub kind: Kind<'a>,
}
pub(super) enum Kind<'a> {
    Array(Vec<Node<'a>>),
    Object(BTreeMap<String, Node<'a>>),
    String(String),
    Bool(bool),
    Null,
    Number,
}
impl Node<'_> {
    /// Missing members may be optional; traversing a scalar is always a shape error.
    pub fn pointer(&self, pointer: &str) -> Result<Option<&Self>> {
        if pointer.is_empty() {
            return Ok(Some(self));
        }
        let mut node = self;
        for token in pointer[1..].split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            let selected = match &node.kind {
                Kind::Object(values) => values.get(&token),
                Kind::Array(values) => {
                    if token != "0"
                        && (token.starts_with('0')
                            || token.is_empty()
                            || !token.bytes().all(|b| b.is_ascii_digit()))
                    {
                        return Ok(None);
                    }
                    token
                        .parse::<usize>()
                        .ok()
                        .and_then(|index| values.get(index))
                }
                _ => return Err(MappingError::new(ErrorKind::WrongType)),
            };
            let Some(selected) = selected else {
                return Ok(None);
            };
            node = selected;
        }
        Ok(Some(node))
    }
}

pub(super) fn parse<'a>(bytes: &'a [u8], budget: &mut Budget) -> Result<Node<'a>> {
    budget.work(bytes.len())?;
    let raw = serde_json::from_slice::<&RawValue>(bytes)
        .map_err(|_| MappingError::new(ErrorKind::InvalidJson))?;
    node(raw, budget, 1)
}
fn node<'a>(raw: &'a RawValue, budget: &mut Budget, depth: usize) -> Result<Node<'a>> {
    budget.work(raw.get().len())?;
    if depth > budget.limits.max_json_depth {
        return Err(MappingError::new(ErrorKind::LimitExceeded));
    }
    Budget::add(&mut budget.nodes, 1, budget.limits.max_json_nodes)?;
    let kind = match raw.get().as_bytes()[0] {
        b'{' | b'[' => {
            let mut deserializer = serde_json::Deserializer::from_str(raw.get());
            Structure { budget, depth }
                .deserialize(&mut deserializer)
                .map_err(|_| {
                    MappingError::new(budget.parse_error.take().unwrap_or(ErrorKind::InvalidJson))
                })?
        }
        b'"' => Kind::String(
            serde_json::from_str(raw.get())
                .map_err(|_| MappingError::new(ErrorKind::InvalidJson))?,
        ),
        b't' => Kind::Bool(true),
        b'f' => Kind::Bool(false),
        b'n' => Kind::Null,
        _ => Kind::Number,
    };
    budget.check()?;
    Ok(Node { raw, kind })
}
struct Structure<'b> {
    budget: &'b mut Budget,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Structure<'_> {
    type Value = Kind<'de>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Structure<'_> {
    type Value = Kind<'de>;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("JSON structure")
    }
    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut access: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(raw) = access.next_element::<&RawValue>()? {
            let value = node(raw, self.budget, self.depth + 1).map_err(|error| {
                self.budget.parse_error = Some(error.kind);
                de::Error::custom("invalid structure")
            })?;
            values.push(value);
        }
        Ok(Kind::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(
        self,
        mut access: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut values = BTreeMap::new();
        while let Some(key) = access.next_key::<String>()? {
            if values.contains_key(&key) {
                self.budget.parse_error = Some(ErrorKind::DuplicateKey);
                return Err(de::Error::custom("duplicate field"));
            }
            let raw = access.next_value::<&RawValue>()?;
            let value = node(raw, self.budget, self.depth + 1).map_err(|error| {
                self.budget.parse_error = Some(error.kind);
                de::Error::custom("invalid structure")
            })?;
            values.insert(key, value);
        }
        Ok(Kind::Object(values))
    }
}
