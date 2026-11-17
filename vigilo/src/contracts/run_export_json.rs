//! Database JSON projection decoding without shrinking a payload's depth budget.
//!
//! Export projections add one record object, or a record plus a tagged envelope.
//! These scaffolds must not consume the recursion budget of an already accepted
//! JSON field. The bounded reader admits bytes before calling this module. Input
//! comes from fixed JSON constructors (unique object keys), not arbitrary request JSON.
use serde::de::DeserializeOwned;

/// Decode one admitted projection. Only its explicit one/two object envelopes
/// are removed from the JSON parser's per-field recursion budget.
pub(crate) fn decode<T: DeserializeOwned>(
    body: &str,
    envelope_objects: u8,
) -> Result<T, serde_json::Error> {
    fn object(body: &str, remaining: u8) -> Result<serde_json::Value, serde_json::Error> {
        if remaining == 0 || !body.trim_start().starts_with('{') {
            return serde_json::from_str(body);
        }
        // RawValue skips values iteratively. Payloads are then parsed with their
        // own bounded recursion budget rather than the envelope's remaining one.
        let fields: std::collections::BTreeMap<&str, &serde_json::value::RawValue> =
            serde_json::from_str(body)?;
        let mut values = serde_json::Map::new();
        for (key, value) in fields {
            values.insert(key.to_owned(), object(value.get(), remaining - 1)?);
        }
        Ok(serde_json::Value::Object(values))
    }
    // Callers select only the known record or tagged-record scaffold; the
    // parameter is never taken from a request or stored payload.
    if !(1..=2).contains(&envelope_objects) {
        return Err(<serde_json::Error as serde::de::Error>::custom(
            "unsupported export JSON scaffold",
        ));
    }
    // Most records fit the normal parser budget. Decode them directly instead
    // of allocating an intermediate map/value for every scalar field. The same
    // bounded fallback preserves accepted deep payloads without relaxing errors.
    match serde_json::from_str(body) {
        Ok(record) => Ok(record),
        Err(_) => serde_json::from_value(object(body, envelope_objects)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(serde::Deserialize)]
    struct Record {
        payload: serde_json::Value,
    }
    #[derive(serde::Deserialize)]
    struct Tagged {
        data: Record,
    }
    #[test]
    fn shallow_records_preserve_fields_and_reject_invalid_shapes_and_scaffolds() {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Strict {
            value: u32,
        }
        assert_eq!(decode::<Strict>(r#"{"value":7}"#, 1).unwrap().value, 7);
        for body in [
            r#"{"value":"7"}"#,
            r#"{"value":7,"extra":1}"#,
            r#"{"value":7} trailing"#,
        ] {
            assert!(decode::<Strict>(body, 1).is_err());
        }
        for scaffold in [0, 3] {
            assert!(decode::<Strict>(r#"{"value":7}"#, scaffold).is_err());
        }
    }
    #[test]
    fn accepted_payload_depth_survives_record_and_tagged_envelopes() {
        let payload = format!("{}0{}", "[".repeat(127), "]".repeat(127));
        let expected: serde_json::Value = serde_json::from_str(&payload).unwrap();
        let record = format!("{{\"payload\":{payload}}}");
        assert_eq!(decode::<Record>(&record, 1).unwrap().payload, expected);
        let tagged = format!("{{\"data\":{record}}}");
        assert_eq!(decode::<Tagged>(&tagged, 2).unwrap().data.payload, expected);
        let excessive = format!("{{\"payload\":{}0{}}}", "[".repeat(140), "]".repeat(140));
        assert!(decode::<Record>(&excessive, 1).is_err());
    }
}
