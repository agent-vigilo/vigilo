//! Pure response interpretation with no network, database, or evaluator access.
//!
//! The caller retains immutable original evidence. Mapping builds a private result
//! and publishes it only on complete success. Decoded resources contain shared bytes,
//! never invocation handles; the evaluator host owns later authorization and binding.
//! Limits bound input, parsing work, expansion, output, and cooperative elapsed time.
//! Selectors and operations are explicit; unknown shapes never trigger fallback.

mod json;
#[cfg(test)]
mod tests;

use std::{
    fmt,
    time::{
        Duration,
        Instant,
    },
};

use base64::{
    Engine,
    engine::general_purpose::STANDARD,
};
use json::{
    Kind,
    Node,
};

use crate::response::*;

/// Mapping semantics identity, independent of application and configuration versions.
pub const IMPLEMENTATION_ID: &str = "vigilo-response-mapping-v1@1";

/// Host-selected ceilings. Counts and byte work are deterministic; the deadline
/// is checked cooperatively before/after bounded parsing and decoding operations.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_input_bytes: usize,
    pub max_json_nodes: usize,
    pub max_json_depth: usize,
    pub max_work_bytes: usize,
    pub max_items: usize,
    pub max_parts: usize,
    pub max_output_bytes: usize,
    pub max_resources: usize,
    pub max_duration: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_bytes: 4 * 1024 * 1024,
            max_json_nodes: 100_000,
            max_json_depth: 64,
            max_work_bytes: 32 * 1024 * 1024,
            max_items: 4096,
            max_parts: 8192,
            max_output_bytes: 16 * 1024 * 1024,
            max_resources: 63,
            max_duration: Duration::from_secs(1),
        }
    }
}

/// Content before evaluator-local resource registration. No variant fetches URLs.
#[derive(Debug, Clone)]
pub enum MappedSource {
    InlineText { text: String },
    BodyRange { offset: u64, byte_length: u64 },
    Resource { body: CapturedBody },
}

/// Entire successful mapping. None means no interpretation, not an empty sequence.
/// Live handles are absent, so this value can be shared across evaluator invocations.
#[derive(Debug, Clone)]
pub struct MappedResponse {
    pub original: CapturedBody,
    pub items: Option<Vec<OutputItem<MappedSource>>>,
    pub status: Option<CompletionStatus>,
    pub stop_reason: Option<StopReason>,
}

/// Stable classification without response data, provider values, or parser excerpts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidConfiguration,
    InvalidJson,
    DuplicateKey,
    MissingField,
    WrongType,
    UnknownValue,
    LimitExceeded,
}

/// Safe mapping error. Producer and element indexes locate failures without
/// copying potentially sensitive response strings or configuration literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingError {
    pub kind: ErrorKind,
    pub producer: Option<usize>,
    pub element: Option<usize>,
}
impl MappingError {
    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            producer: None,
            element: None,
        }
    }
}
impl fmt::Display for MappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "response mapping {:?} (producer {:?}, element {:?})",
            self.kind, self.producer, self.element
        )
    }
}
impl std::error::Error for MappingError {}
type Result<T> = std::result::Result<T, MappingError>;

struct Budget {
    limits: Limits,
    deadline: Instant,
    nodes: usize,
    work: usize,
    items: usize,
    parts: usize,
    output: usize,
    resources: usize,
    parse_error: Option<ErrorKind>,
}
impl Budget {
    fn new(limits: Limits) -> Result<Self> {
        if limits.max_json_depth == 0 || limits.max_json_depth > 64 || limits.max_resources > 63 {
            return Err(MappingError::new(ErrorKind::InvalidConfiguration));
        }
        let deadline = Instant::now()
            .checked_add(limits.max_duration)
            .ok_or_else(|| MappingError::new(ErrorKind::InvalidConfiguration))?;
        Ok(Self {
            limits,
            deadline,
            nodes: 0,
            work: 0,
            items: 0,
            parts: 0,
            output: 0,
            resources: 0,
            parse_error: None,
        })
    }
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err(MappingError::new(ErrorKind::LimitExceeded))
        } else {
            Ok(())
        }
    }
    fn add(count: &mut usize, amount: usize, limit: usize) -> Result<()> {
        *count = count
            .checked_add(amount)
            .filter(|value| *value <= limit)
            .ok_or_else(|| MappingError::new(ErrorKind::LimitExceeded))?;
        Ok(())
    }
    fn work(&mut self, amount: usize) -> Result<()> {
        self.check()?;
        Self::add(&mut self.work, amount, self.limits.max_work_bytes)
    }
    fn output(&mut self, amount: usize) -> Result<()> {
        self.check()?;
        Self::add(&mut self.output, amount, self.limits.max_output_bytes)
    }
    fn text(&mut self, text: &str) -> Result<String> {
        self.output(text.len())?;
        Ok(text.to_owned())
    }
}

/// Interpret exactly the configured response shape. Failure returns no partial
/// mapped output or resources and never mutates the caller's original evidence.
/// Content-Type does not choose or override interpretation. This synchronous API
/// should run on a bounded blocking executor when called from an async runtime.
pub fn map_response(
    body: &CapturedBody,
    configuration: Option<&ResponseConfiguration>,
    limits: Limits,
) -> Result<MappedResponse> {
    let mut budget = Budget::new(limits)?;
    budget.check()?;
    if body.bytes().len() > budget.limits.max_input_bytes {
        return Err(MappingError::new(ErrorKind::LimitExceeded));
    }
    let mut result = MappedResponse {
        original: body.clone(),
        items: None,
        status: None,
        stop_reason: None,
    };
    let mapping = match configuration.map(|value| &value.interpretation) {
        None | Some(Interpretation::Raw {}) => return Ok(result),
        Some(Interpretation::Json { mapping }) => mapping,
    };
    mapping
        .validate()
        .map_err(|_| MappingError::new(ErrorKind::InvalidConfiguration))?;
    let root = json::parse(body.bytes(), &mut budget)?;
    let mut items = Vec::new();
    for (producer, source) in mapping.items.iter().enumerate() {
        let mut emit = |node: &Node<'_>, template: &ItemTemplate, element| {
            budget.check()?;
            Budget::add(&mut budget.items, 1, budget.limits.max_items)?;
            map_item(node, template, body, &mut budget).map_err(|mut error| {
                error.producer = Some(producer);
                error.element = element;
                error
            })
        };
        match source {
            ItemMapping::Single { item } => items.push(emit(&root, item, None)?),
            ItemMapping::Each { source, item } => {
                let selected = select(&root, source)?;
                if let Some(selected) = selected {
                    let Kind::Array(elements) = &selected.kind else {
                        return Err(MappingError {
                            kind: ErrorKind::WrongType,
                            producer: Some(producer),
                            element: None,
                        });
                    };
                    for (index, element) in elements.iter().enumerate() {
                        items.push(emit(element, item, Some(index))?);
                    }
                }
            }
        }
    }
    result.status = map_value(&root, mapping.status.as_ref(), &mut budget)?.map(|(value, _)| value);
    result.stop_reason = map_value(&root, mapping.stop_reason.as_ref(), &mut budget)?.map(
        |(category, provider_value)| StopReason {
            category,
            provider_value: Some(provider_value),
        },
    );
    budget.check()?;
    result.items = Some(items);
    Ok(result)
}

fn select<'a, 'data>(
    node: &'a Node<'data>,
    selection: &Selection,
) -> Result<Option<&'a Node<'data>>> {
    let selected = node.pointer(selection.pointer.as_str())?;
    if selected.is_none() && selection.required {
        Err(MappingError::new(ErrorKind::MissingField))
    } else {
        Ok(selected)
    }
}
fn string<'a>(node: &'a Node<'_>) -> Result<&'a str> {
    if let Kind::String(value) = &node.kind {
        Ok(value)
    } else {
        Err(MappingError::new(ErrorKind::WrongType))
    }
}
fn optional_string(
    root: &Node<'_>,
    selection: Option<&Selection>,
    budget: &mut Budget,
) -> Result<Option<String>> {
    selection
        .map(|selection| {
            select(root, selection)?
                .map(|node| budget.text(string(node)?))
                .transpose()
        })
        .transpose()
        .map(Option::flatten)
}
fn map_string(
    root: &Node<'_>,
    mapping: &StringMapping,
    budget: &mut Budget,
) -> Result<Option<String>> {
    match mapping {
        StringMapping::Literal { value } => Ok(Some(budget.text(value)?)),
        StringMapping::Field { selection } => optional_string(root, Some(selection), budget),
    }
}
fn map_value<T: Copy>(
    root: &Node<'_>,
    mapping: Option<&ValueMapping<T>>,
    budget: &mut Budget,
) -> Result<Option<(T, String)>> {
    let Some(mapping) = mapping else {
        return Ok(None);
    };
    let Some(node) = select(root, &mapping.selection)? else {
        return Ok(None);
    };
    let value = string(node)?;
    let mapped = mapping
        .values
        .get(value)
        .copied()
        .ok_or_else(|| MappingError::new(ErrorKind::UnknownValue))?;
    Ok(Some((mapped, budget.text(value)?)))
}

fn map_item(
    root: &Node<'_>,
    template: &ItemTemplate,
    original: &CapturedBody,
    budget: &mut Budget,
) -> Result<OutputItem<MappedSource>> {
    Ok(match template {
        ItemTemplate::Message { id, role, content } => OutputItem::Message {
            id: optional_string(root, id.as_ref(), budget)?,
            role: role
                .as_ref()
                .map(|role| map_string(root, role, budget))
                .transpose()?
                .flatten(),
            content: map_parts(root, content, original, budget)?,
        },
        ItemTemplate::ToolCall {
            call_id,
            name,
            arguments,
        } => OutputItem::ToolCall {
            call_id: optional_string(root, call_id.as_ref(), budget)?,
            name: map_string(root, name, budget)?
                .ok_or_else(|| MappingError::new(ErrorKind::MissingField))?,
            arguments: map_part(root, arguments, original, budget)?
                .ok_or_else(|| MappingError::new(ErrorKind::MissingField))?,
        },
        ItemTemplate::ToolResult {
            call_id,
            content,
            is_error,
        } => OutputItem::ToolResult {
            call_id: optional_string(root, call_id.as_ref(), budget)?,
            content: map_parts(root, content, original, budget)?,
            is_error: is_error
                .as_ref()
                .map(|selection| {
                    select(root, selection)?
                        .map(|node| {
                            if let Kind::Bool(value) = node.kind {
                                Ok(value)
                            } else {
                                Err(MappingError::new(ErrorKind::WrongType))
                            }
                        })
                        .transpose()
                })
                .transpose()?
                .flatten(),
        },
    })
}

fn map_parts(
    root: &Node<'_>,
    templates: &[ContentMapping],
    original: &CapturedBody,
    budget: &mut Budget,
) -> Result<Vec<ContentPart<MappedSource>>> {
    let mut parts = Vec::new();
    for template in templates {
        if let Some(part) = map_part(root, template, original, budget)? {
            parts.push(part);
        }
    }
    Ok(parts)
}

fn map_part(
    root: &Node<'_>,
    template: &ContentMapping,
    original: &CapturedBody,
    budget: &mut Budget,
) -> Result<Option<ContentPart<MappedSource>>> {
    budget.check()?;
    let selection = match &template.source {
        ContentMappingSource::OriginalBody => None,
        ContentMappingSource::InlineText { selection }
        | ContentMappingSource::JsonValue { selection }
        | ContentMappingSource::JsonString { selection }
        | ContentMappingSource::Base64 { selection } => Some(selection),
    };
    let node = match selection {
        Some(selection) => match select(root, selection)? {
            Some(value) => Some(value),
            None => return Ok(None),
        },
        None => None,
    };
    Budget::add(&mut budget.parts, 1, budget.limits.max_parts)?;
    let media_type = budget.text(&template.media_type)?;
    let source = match (&template.source, node) {
        (ContentMappingSource::OriginalBody, _) => {
            budget.output(original.bytes().len())?;
            MappedSource::BodyRange {
                offset: 0,
                byte_length: original.bytes().len() as u64,
            }
        }
        (ContentMappingSource::InlineText { .. }, Some(node)) => MappedSource::InlineText {
            text: budget.text(string(node)?)?,
        },
        (ContentMappingSource::JsonValue { .. }, Some(node)) => {
            budget.output(node.raw.get().len())?;
            let offset = (node.raw.get().as_ptr() as usize)
                .checked_sub(original.bytes().as_ptr() as usize)
                .filter(|offset| {
                    offset
                        .checked_add(node.raw.get().len())
                        .is_some_and(|end| end <= original.bytes().len())
                })
                .ok_or_else(|| MappingError::new(ErrorKind::InvalidJson))?;
            MappedSource::BodyRange {
                offset: offset as u64,
                byte_length: node.raw.get().len() as u64,
            }
        }
        (ContentMappingSource::JsonString { .. }, Some(node)) => {
            let value = string(node)?;
            json::parse(value.as_bytes(), budget)?;
            budget.output(value.len())?;
            Budget::add(&mut budget.resources, 1, budget.limits.max_resources)?;
            MappedSource::Resource {
                body: CapturedBody::new(value.as_bytes().to_vec(), None),
            }
        }
        (ContentMappingSource::Base64 { .. }, Some(node)) => {
            let value = string(node)?;
            budget.work(value.len())?;
            // Strict padded RFC 4648: bound the exact maximum decoded size before allocation.
            if value.len() % 4 != 0 {
                return Err(MappingError::new(ErrorKind::WrongType));
            }
            let padding = value
                .as_bytes()
                .iter()
                .rev()
                .take_while(|byte| **byte == b'=')
                .count();
            if padding > 2 {
                return Err(MappingError::new(ErrorKind::WrongType));
            }
            let length = (value.len() / 4)
                .checked_mul(3)
                .and_then(|n| n.checked_sub(padding))
                .ok_or_else(|| MappingError::new(ErrorKind::WrongType))?;
            budget.output(length)?;
            Budget::add(&mut budget.resources, 1, budget.limits.max_resources)?;
            let bytes = STANDARD
                .decode(value)
                .map_err(|_| MappingError::new(ErrorKind::WrongType))?;
            budget.check()?;
            MappedSource::Resource {
                body: CapturedBody::new(bytes, None),
            }
        }
        _ => unreachable!("selected content always has a node"),
    };
    Ok(Some(ContentPart { media_type, source }))
}
