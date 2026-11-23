//! Versioned payloads exposed by the run lifecycle outbox.

use serde::{
    Deserialize,
    Deserializer,
    de,
};
use uuid::Uuid;

/// JSON payload for a committed `run.completed` event. Delivery may repeat.
/// The tenant and run identify the committed work. Use an independently
/// authorized caller context when reading its results.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct RunCompletedEventV1 {
    #[serde(deserialize_with = "completion_version")]
    pub schema_version: u32,
    pub tenant_id: Uuid,
    pub run_id: Uuid,
    pub run_key: String,
    pub gate_status: String,
    pub terminal_execution_count: i32,
    pub passed_execution_count: i32,
    pub failed_execution_count: i32,
    pub errored_execution_count: i32,
}

impl RunCompletedEventV1 {
    /// RabbitMQ routing key on the configured event exchange.
    pub const ROUTING_KEY: &str = "run.completed";
}

fn completion_version<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let version = u32::deserialize(deserializer)?;
    if version != 1 {
        return Err(de::Error::custom(
            "unsupported run.completed schema version",
        ));
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_consumer_requires_v1_and_both_identities() {
        let mut payload = serde_json::json!({
            "schema_version": 1,
            "tenant_id": Uuid::from_u128(1),
            "run_id": Uuid::from_u128(2),
            "run_key": "example",
            "gate_status": "pass",
            "terminal_execution_count": 1,
            "passed_execution_count": 1,
            "failed_execution_count": 0,
            "errored_execution_count": 0,
        });
        let event: RunCompletedEventV1 = serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(event.tenant_id, Uuid::from_u128(1));
        assert_eq!(event.run_id, Uuid::from_u128(2));
        assert_eq!(event.schema_version, 1);
        for field in ["schema_version", "tenant_id", "run_id"] {
            let mut incomplete = payload.clone();
            incomplete.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<RunCompletedEventV1>(incomplete).is_err());
        }
        payload["schema_version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<RunCompletedEventV1>(payload).is_err());
    }
}
