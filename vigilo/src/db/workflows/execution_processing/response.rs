//! Attempt-level response interpretation and retention, independent of SQL.
//!
//! Frozen run identities are supplied from the worker's verified snapshot. Mapping
//! and packing run on the bounded case executor's blocking task, once per attempt;
//! evaluator invocations share the result and install their own resource handles.
use std::sync::Arc;

use crate::contracts::{
    evaluator::AgentOutput,
    response_retention::{
        Provenance,
        RetainedResponse,
    },
    run::RunProfile,
};

/// Immutable identities read from the existing verified run context.
#[derive(Debug)]
pub(crate) struct Identity {
    pub(crate) profile_hash: String,
    pub(crate) execution_plan_hash: String,
    pub(crate) runtime_artifact_digest: String,
}

/// Hash the running executable once per process, using bounded streaming I/O.
/// Keep deployment executables immutable while a worker process is running.
pub(crate) async fn runtime_digest() -> anyhow::Result<String> {
    static DIGEST: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    Ok(DIGEST
        .get_or_try_init(|| async {
            tokio::task::spawn_blocking(|| -> anyhow::Result<String> {
                let mut file = std::fs::File::open(std::env::current_exe()?)?;
                let mut hasher = blake3::Hasher::new();
                let mut buffer = [0; 64 * 1024];
                loop {
                    let count = std::io::Read::read(&mut file, &mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    hasher.update(&buffer[..count]);
                }
                Ok(hasher.finalize().to_hex().to_string())
            })
            .await?
        })
        .await?
        .clone())
}

/// Reject incompatible effective capture limits before invoking an agent. The
/// durable evidence contract bounds original bytes to the same 4 MiB ceiling.
pub(crate) fn validate_limits(
    limits: crate::agent_client::limits::Limits,
    profile: &RunProfile,
) -> anyhow::Result<()> {
    use crate::agent_client::failure::{
        Failure,
        Kind,
    };
    let effective = limits
        .resolve(&profile.agent.http)
        .map_err(|error| Failure::terminal(Kind::Configuration, error))?;
    if effective.max_response_bytes > 4 * 1024 * 1024 {
        return Err(Failure::terminal(Kind::Configuration,
            "response evidence supports at most 4 MiB; set agent.http.max_response_bytes accordingly").into());
    }
    Ok(())
}

/// Return atomic mapping (including an explicit mapping failure) and a single
/// durable packet. Interpretation failures are not invocation retry signals.
pub(crate) async fn prepare(
    mut output: AgentOutput,
    profile: &RunProfile,
    provenance: Provenance,
) -> anyhow::Result<(AgentOutput, RetainedResponse)> {
    let configuration = profile.agent.response.clone();
    let retention = profile.persistence.mode.clone();
    tokio::task::spawn_blocking(move || {
        let body = output
            .captured_body
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("agent returned no captured response"))?;
        let mut mapped = vigilo::response_mapping::map_response(
            body,
            configuration.as_ref(),
            Default::default(),
        );
        let packet = match RetainedResponse::capture(
            provenance.clone(),
            retention.clone(),
            Some(body),
            configuration.as_ref(),
            Some(mapped.as_ref()),
        ) {
            Ok(packet) => packet,
            Err(_) if mapped.is_ok() => {
                // Durable representation budgets are part of interpretation limits.
                // Reject the whole mapping, preserving raw evidence and preventing
                // an interpretation-size failure from retrying the agent.
                mapped = Err(vigilo::response_mapping::MappingError {
                    kind: vigilo::response_mapping::ErrorKind::LimitExceeded,
                    producer: None,
                    element: None,
                });
                RetainedResponse::capture(
                    provenance,
                    retention,
                    Some(body),
                    configuration.as_ref(),
                    Some(mapped.as_ref()),
                )?
            }
            Err(error) => return Err(error),
        };
        output.mapping = Some(Arc::new(mapped));
        Ok((output, packet))
    })
    .await?
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::contracts::response_retention::ExternalDependencies;
    #[tokio::test]
    async fn prepare_shares_mapping_retains_raw_failures_and_honors_summary() {
        let mut profile: RunProfile = serde_json::from_value(json!({
            "profile_id":"test","profile_version":"1","description":"",
            "defaults":{"max_attempts":1,"request_timeout_secs":2,"fail_on_any_blocking_failure":true,"min_execution_score":0.5},
            "agent":{"provider":"test","name":"test","http":{"url":"http://localhost"},"response":{"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[{"mode":"single","item":{"type":"message","content":[{"media_type":"text/plain","source":{"representation":"inline_text","selection":{"pointer":"/text"}}}]}}]}}}},
            "case_groups":[]
        })).unwrap();
        let ceilings = crate::agent_client::limits::Limits {
            max_response_bytes: 8 * 1024 * 1024,
            ..Default::default()
        };
        let failure = validate_limits(ceilings, &profile).unwrap_err();
        assert!(
            !failure
                .downcast_ref::<crate::agent_client::failure::Failure>()
                .unwrap()
                .retryable
        );
        profile.agent.http.max_response_bytes = Some(4 * 1024 * 1024);
        validate_limits(ceilings, &profile).unwrap();
        profile.agent.http.max_response_bytes = Some(0);
        assert!(validate_limits(ceilings, &profile).is_err());
        profile.agent.http.max_response_bytes = None;
        validate_limits(Default::default(), &profile).unwrap();
        let provenance = Provenance {
            run_id: uuid::Uuid::new_v4(),
            execution_id: uuid::Uuid::new_v4(),
            attempt_id: uuid::Uuid::new_v4(),
            case_hash: "a".repeat(64),
            profile_hash: "b".repeat(64),
            execution_plan_hash: "c".repeat(64),
            runtime_artifact_digest: "d".repeat(64),
            external_dependencies: ExternalDependencies::None,
        };
        for bytes in [br#"{"text":"hello"}"#.as_slice(), b"malformed", b""] {
            let mut output: AgentOutput = serde_json::from_value(json!({})).unwrap();
            output.captured_body = Some(vigilo::response::CapturedBody::new(bytes.to_vec(), None));
            let (output, packet) = prepare(output, &profile, provenance.clone()).await.unwrap();
            let authority =
                crate::contracts::response_retention::policy_identity(&profile).unwrap();
            let metadata = packet.metadata_json().unwrap();
            assert_eq!(
                authority["configuration_hash"],
                metadata["configuration_hash"]
            );
            assert_eq!(authority["retention"], metadata["retention"]);
            let replay = packet.reconstruct().unwrap();
            assert_eq!(replay.original.bytes(), bytes);
            assert_eq!(replay.items.is_some(), bytes.starts_with(b"{"));
            let clone = output.clone();
            assert!(Arc::ptr_eq(
                clone.mapping.as_ref().unwrap(),
                output.mapping.as_ref().unwrap()
            ));
            assert_eq!(
                output.captured_body.as_ref().unwrap().bytes().as_ptr(),
                clone.captured_body.as_ref().unwrap().bytes().as_ptr()
            );
        }
        // A small body can expand into too much retained metadata. Reject the
        // mapping atomically, while retaining the exact original for raw consumers.
        profile.agent.response = Some(serde_json::from_value(json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[{
            "mode":"each","source":{"pointer":"/rows"},"item":{"type":"message","id":{"pointer":"/id"},"content":[]}
        }]}}})).unwrap());
        let bytes =
            serde_json::to_vec(&json!({"rows": vec![json!({"id": "x".repeat(1024)}); 2048]}))
                .unwrap();
        let mut output: AgentOutput = serde_json::from_value(json!({})).unwrap();
        output.captured_body = Some(vigilo::response::CapturedBody::new(bytes.clone(), None));
        let (output, packet) = prepare(output, &profile, provenance.clone()).await.unwrap();
        assert_eq!(
            output
                .mapping
                .as_deref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .kind,
            vigilo::response_mapping::ErrorKind::LimitExceeded
        );
        assert_eq!(packet.reconstruct().unwrap().original.bytes(), bytes);
        assert!(packet.reconstruct().unwrap().items.is_none());
        profile.persistence.mode = crate::contracts::run::PersistenceMode::Summary;
        let mut output: AgentOutput = serde_json::from_value(json!({})).unwrap();
        output.captured_body = Some(vigilo::response::CapturedBody::new(
            b"private".to_vec(),
            None,
        ));
        let (_, packet) = prepare(output, &profile, provenance).await.unwrap();
        assert!(packet.payload().is_empty());
        assert!(packet.reconstruct().is_err());
    }
}
