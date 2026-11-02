//! Static registry of evaluator WIT adapters supported by this host binary.
//!
//! Each adapter owns one immutable WIT contract, its generated bindings, and
//! conversion to the host's canonical evaluator input and output contracts.

use wasmtime::component::{
    Component,
    InstancePre,
};

use crate::{
    context::wasm::{
        EvaluatorHost,
        Wasm,
    },
    contracts::{
        evaluator::{
            EvaluatorInput,
            EvaluatorOutput,
        },
        evaluator_abi::EvaluatorAbiIdentity,
    },
};

mod v1;

pub(crate) trait EvaluatorAbiAdapter: Send + Sync {
    fn identity(&self) -> &'static EvaluatorAbiIdentity;

    #[cfg(test)]
    fn fixture_artifact(&self) -> &'static str;

    fn prepare(
        &self,
        runtime: &Wasm,
        component: &Component,
    ) -> anyhow::Result<InstancePre<EvaluatorHost>>;

    fn execute(
        &self,
        runtime: &Wasm,
        instance_pre: &InstancePre<EvaluatorHost>,
        input: EvaluatorInput,
    ) -> anyhow::Result<EvaluatorOutput>;
}

static ADAPTERS: [&dyn EvaluatorAbiAdapter; 1] = [&v1::ADAPTER];

/// A compiled component paired with its already-resolved ABI adapter.
#[derive(Clone)]
pub(crate) struct PreparedEvaluator {
    component: Component,
    instance_pre: InstancePre<EvaluatorHost>,
    adapter: &'static dyn EvaluatorAbiAdapter,
}

impl PreparedEvaluator {
    pub(crate) fn compile(
        runtime: &Wasm,
        wasm_bytes: &[u8],
        identity: &EvaluatorAbiIdentity,
    ) -> anyhow::Result<Self> {
        let component = Component::new(runtime.engine(), wasm_bytes)?;
        let adapter = resolve_identity(identity)?;
        let instance_pre = adapter.prepare(runtime, &component)?;
        Ok(Self {
            component,
            instance_pre,
            adapter,
        })
    }

    pub(crate) fn execute(
        &self,
        runtime: &Wasm,
        input: EvaluatorInput,
    ) -> anyhow::Result<EvaluatorOutput> {
        self.adapter.execute(runtime, &self.instance_pre, input)
    }

    pub(crate) fn approximate_size(&self) -> usize {
        let range = self.component.image_range();
        range.end as usize - range.start as usize
    }

    pub(crate) fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.component.serialize()?)
    }
}

#[cfg(test)]
pub(crate) fn current_identity() -> EvaluatorAbiIdentity {
    v1::ADAPTER.identity().clone()
}

pub(crate) fn resolve_declaration(
    package: &str,
    world: &str,
    interface: &str,
    version: &str,
) -> anyhow::Result<&'static dyn EvaluatorAbiAdapter> {
    ADAPTERS
        .iter()
        .copied()
        .find(|adapter| {
            let identity = adapter.identity();
            identity.package == package
                && identity.world == world
                && identity.interface == interface
                && identity.version == version
        })
        .ok_or_else(|| {
            let supported = ADAPTERS
                .iter()
                .map(|adapter| adapter.identity().version.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::anyhow!(
                "unsupported evaluator ABI {}/{}@{} (world '{}'); supported versions: {}",
                package,
                interface,
                version,
                world,
                supported,
            )
        })
}

pub(crate) fn resolve_identity(
    identity: &EvaluatorAbiIdentity,
) -> anyhow::Result<&'static dyn EvaluatorAbiAdapter> {
    ADAPTERS
        .iter()
        .copied()
        .find(|adapter| adapter.identity() == identity)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unsupported evaluator ABI {}/{}@{} (world '{}', contract hash '{}', adapter '{}')",
                identity.package,
                identity.interface,
                identity.version,
                identity.world,
                identity.contract_hash,
                identity.adapter,
            )
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    #[cfg(feature = "evaluator-abi-fixtures")]
    use std::{
        collections::BTreeMap,
        fs,
        path::Path,
    };

    #[cfg(feature = "evaluator-abi-fixtures")]
    use serde_json::json;

    use super::*;
    #[cfg(feature = "evaluator-abi-fixtures")]
    use crate::{
        context::wasm::Config,
        contracts::evaluator::{
            AgentOutput,
            EvaluatorInput,
            EvaluatorOutcome,
            TestCase,
        },
    };

    #[cfg(feature = "evaluator-abi-fixtures")]
    fn evaluator_input() -> EvaluatorInput {
        EvaluatorInput {
            run_id: "run-1".to_string(),
            execution_id: "execution-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            case: TestCase {
                id: "case-1".to_string(),
                task_type: "classification".to_string(),
                case_group: Some("sentiment".to_string()),
                input: json!({"text": "neutral statement"}),
                expected: None,
                context: None,
                tags: Vec::new(),
                metadata: BTreeMap::new(),
            },
            actual: AgentOutput {
                captured_body: None,
                text: Some("neutral statement".to_string()),
                structured: None,
                tool_calls: Vec::new(),
                trace: Vec::new(),
                raw: json!({}),
                metadata: json!({}),
            },
            evaluator_config: json!({}),
        }
    }

    #[test]
    fn registry_identities_and_adapter_ids_are_unique() {
        let mut declarations = HashSet::new();
        let mut adapter_ids = HashSet::new();
        let mut fixture_artifacts = HashSet::new();

        for adapter in ADAPTERS {
            let identity = adapter.identity();
            assert!(declarations.insert((
                identity.package.clone(),
                identity.world.clone(),
                identity.interface.clone(),
                identity.version.clone(),
            )));
            assert!(adapter_ids.insert(identity.adapter.clone()));
            assert!(fixture_artifacts.insert(adapter.fixture_artifact()));
        }
    }

    #[test]
    fn declaration_and_exact_identity_resolve_through_the_registry() {
        let identity = current_identity();
        assert_eq!(
            resolve_declaration(
                &identity.package,
                &identity.world,
                &identity.interface,
                &identity.version,
            )
            .unwrap()
            .identity(),
            &identity,
        );
        assert_eq!(resolve_identity(&identity).unwrap().identity(), &identity);
    }

    #[test]
    fn changed_contract_or_adapter_fails_closed() {
        let mut identity = current_identity();
        identity.contract_hash = "different".to_string();
        assert!(resolve_identity(&identity).is_err());

        identity = current_identity();
        identity.adapter = "vigilo-evaluator-v1@2".to_string();
        assert!(resolve_identity(&identity).is_err());
    }

    #[cfg(feature = "evaluator-abi-fixtures")]
    #[test]
    fn all_registered_evaluator_abis_execute_in_one_host() {
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wasm32-wasip2/release");
        let runtime = Wasm::new(Config {
            timeout_ms: 0,
            ..Config::default()
        })
        .unwrap();

        for adapter in ADAPTERS {
            let identity = adapter.identity();
            let bytes = fs::read(target.join(adapter.fixture_artifact())).unwrap_or_else(|err| {
                panic!(
                    "missing compatibility fixture for evaluator ABI {}: {}",
                    identity.version, err
                )
            });
            let evaluator = PreparedEvaluator::compile(&runtime, &bytes, identity).unwrap();
            let output = evaluator.execute(&runtime, evaluator_input()).unwrap();

            assert!(matches!(output.outcome, EvaluatorOutcome::Completed(_)));
            assert_eq!(
                output.evaluator.interface_version.as_deref(),
                Some(identity.version.as_str())
            );
        }
    }
    #[cfg(feature = "evaluator-abi-fixtures")]
    #[test]
    fn response_resources_cross_real_wasm_and_are_scoped_to_each_execution() {
        let bytes = fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../target/wasm32-wasip2/release/response_resource_fixture.wasm"),
        )
        .expect("build response-resource-fixture for wasm32-wasip2");
        let runtime = Wasm::new(Config {
            timeout_ms: 0,
            ..Config::default()
        })
        .unwrap();
        let evaluator = PreparedEvaluator::compile(&runtime, &bytes, &current_identity()).unwrap();
        assert!(
            evaluator
                .execute(&runtime, evaluator_input())
                .unwrap_err()
                .to_string()
                .contains("evidence unavailable")
        );
        let mut previous_handle = None;
        for config in [
            json!({}),
            json!({"trap":true}),
            json!({}),
            json!({"error":true}),
            json!({}),
        ] {
            let mut input = evaluator_input();
            input.actual.captured_body = Some(vigilo::response::CapturedBody::new(
                vec![0, 255, 42, 10],
                Some("application/octet-stream".into()),
            ));
            input.evaluator_config = if config.as_object().is_some_and(|value| value.is_empty()) {
                previous_handle.map_or_else(|| config.clone(), |handle: u64| json!(handle))
            } else {
                config.clone()
            };
            let result = evaluator.execute(&runtime, input);
            if config.get("trap").is_some() || config.get("error").is_some() {
                assert!(result.is_err());
            } else {
                let output = result.unwrap();
                previous_handle = output.metadata.as_u64();
                assert!(previous_handle.is_some());
                assert!(matches!(
                    output.outcome,
                    EvaluatorOutcome::Completed(crate::contracts::evaluator::Measurement::Binary {
                        value: true
                    })
                ));
            }
        }
        assert!(evaluator.execute(&runtime, evaluator_input()).is_err());
    }
}
