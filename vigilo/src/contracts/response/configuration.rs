//! Versioned, declarative response mappings. Syntax validation is pure and bounded;
//! application to a response belongs to the mapping engine, not this module.
use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};

use super::{
    CompletionStatus,
    StopCategory,
};

/// Supported configuration contract, independent of profile revision and ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigurationVersion {
    #[serde(rename = "1.0.0")]
    V1,
}

/// Explicit response interpretation. No mapping means raw-only evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseConfiguration {
    pub schema_version: ConfigurationVersion,
    pub interpretation: Interpretation,
}

/// Raw bytes require no JSON parser. JSON mapping is explicitly selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
pub enum Interpretation {
    Raw {},
    Json { mapping: JsonMapping },
}

/// Bounded list of producers, evaluated in declaration order. Each array producer
/// preserves array order; unrelated arrays are never chronologically interleaved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedMapping")]
pub struct JsonMapping {
    pub items: Vec<ItemMapping>,
    pub status: Option<ValueMapping<CompletionStatus>>,
    pub stop_reason: Option<ValueMapping<StopCategory>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedMapping {
    items: Vec<ItemMapping>,
    status: Option<ValueMapping<CompletionStatus>>,
    stop_reason: Option<ValueMapping<StopCategory>>,
}

impl TryFrom<UncheckedMapping> for JsonMapping {
    type Error = String;
    fn try_from(value: UncheckedMapping) -> Result<Self, Self::Error> {
        let result = Self {
            items: value.items,
            status: value.status,
            stop_reason: value.stop_reason,
        };
        result.validate()?;
        Ok(result)
    }
}

impl JsonMapping {
    /// Validates authoring limits, including configurations constructed in Rust.
    /// Runtime budgets additionally bound expansion against response arrays.
    pub fn validate(&self) -> Result<(), String> {
        if self.items.len() > 128 {
            return Err("mapping.items exceeds 128 producers".into());
        }
        let mut parts = 0;
        for item in &self.items {
            let template = match item {
                ItemMapping::Single { item } | ItemMapping::Each { item, .. } => item,
            };
            let content = match template {
                ItemTemplate::Message { content, .. }
                | ItemTemplate::ToolResult { content, .. } => content.as_slice(),
                ItemTemplate::ToolCall { arguments, .. } => std::slice::from_ref(arguments),
            };
            parts += content.len();
            for part in content {
                let valid = part
                    .media_type
                    .split_once('/')
                    .is_some_and(|(kind, subtype)| {
                        [kind, subtype].iter().all(|token| {
                            !token.is_empty()
                                && token
                                    .bytes()
                                    .all(|b| b.is_ascii_alphanumeric() || b"!#$&^_.+-".contains(&b))
                        })
                    });
                if part.media_type.len() > 255 || !valid {
                    return Err(
                        "content media_type must be a bounded type/subtype without parameters"
                            .into(),
                    );
                }
            }
        }
        if parts > 256 {
            return Err("mapping exceeds 256 content templates".into());
        }
        Ok(())
    }
}

/// Exact RFC 6901 JSON Pointer. Empty points to the selected root, not a missing
/// field. URI fragments and malformed tilde escapes are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct JsonPointer(String);

impl JsonPointer {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn new(value: String) -> Result<Self, String> {
        if value.len() > 4096 || (!value.is_empty() && !value.starts_with('/')) {
            return Err("JSON pointer must be empty or start with '/', at most 4096 bytes".into());
        }
        let mut bytes = value.bytes();
        while let Some(b) = bytes.next() {
            if b == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
                return Err("JSON pointer contains an invalid '~' escape".into());
            }
        }
        Ok(Self(value))
    }
}
impl<'de> Deserialize<'de> for JsonPointer {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Selected values are required by default; optional absence is not JSON null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub pointer: JsonPointer,
    #[serde(default = "required")]
    pub required: bool,
}
fn required() -> bool {
    true
}

/// Explicit single answer or iteration over an array selected from the root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ItemMapping {
    Single {
        item: ItemTemplate,
    },
    Each {
        source: Selection,
        item: ItemTemplate,
    },
}

/// Item field pointers are relative to the current element for `each`, and the
/// response root for `single`. Omitted optional selectors produce no evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ItemTemplate {
    Message {
        id: Option<Selection>,
        role: Option<StringMapping>,
        content: Vec<ContentMapping>,
    },
    ToolCall {
        call_id: Option<Selection>,
        name: StringMapping,
        arguments: ContentMapping,
    },
    ToolResult {
        call_id: Option<Selection>,
        content: Vec<ContentMapping>,
        is_error: Option<Selection>,
    },
}

/// Constants are explicit configuration, never fabricated from an absent selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum StringMapping {
    Literal { value: String },
    Field { selection: Selection },
}

/// A typed content producer. Absent optional sources omit the entire content part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMapping {
    pub media_type: String,
    pub source: ContentMappingSource,
}

/// Operations are explicit and nonrecursive; the engine enforces decoded budgets.
/// JSON value serialization is an interpretation, never original-byte evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "representation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentMappingSource {
    InlineText { selection: Selection },
    JsonValue { selection: Selection },
    JsonString { selection: Selection },
    Base64 { selection: Selection },
    OriginalBody,
}

/// Exact string lookup for completion metadata. Unlisted values are errors;
/// mapping a value to `Other` must also be explicit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueMapping<T> {
    pub selection: Selection,
    pub values: BTreeMap<String, T>,
}
