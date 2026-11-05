//! Persisted evaluator WebAssembly interface identities and execution plans.
//!
//! Runtime support for individual WIT versions belongs to `crate::evaluator_abi`.

use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

/// Complete identity of one supported evaluator component contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluatorAbiIdentity {
    pub(crate) package: String,
    pub(crate) world: String,
    pub(crate) interface: String,
    pub(crate) version: String,
    pub(crate) contract_hash: String,
    pub(crate) adapter: String,
}

impl EvaluatorAbiIdentity {
    pub(crate) fn cache_key(&self, content_hash: &str) -> String {
        format!("{}:{}:{}", content_hash, self.contract_hash, self.adapter)
    }
}

/// Immutable evaluator artifact and ABI selected when a run is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolvedEvaluator {
    /// Immutable outbound permission ceiling; no rules means no HTTP authority.
    pub(crate) http_rules: Vec<crate::evaluator_http::Rule>,
    pub(crate) evaluator_ref: String,
    pub(crate) evaluator_id: Uuid,
    pub(crate) content_hash: String,
    pub(crate) abi: EvaluatorAbiIdentity,
    pub(crate) runtime: String,
    pub(crate) runtime_version: String,
    pub(crate) runtime_fingerprint: String,
}

/// Frozen evaluator execution plan stored in each run snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluatorExecutionPlan {
    pub(crate) version: u32,
    pub(crate) aggregation_policy_hash: String,
    pub(crate) evaluators: Vec<ResolvedEvaluator>,
}

impl EvaluatorExecutionPlan {
    pub(crate) fn new(
        aggregation_policy_hash: String,
        mut evaluators: Vec<ResolvedEvaluator>,
    ) -> Self {
        evaluators.sort_by(|left, right| left.evaluator_ref.cmp(&right.evaluator_ref));
        Self {
            version: 1,
            aggregation_policy_hash,
            evaluators,
        }
    }

    pub(crate) fn hash(&self) -> anyhow::Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn abi_identity() -> EvaluatorAbiIdentity {
        EvaluatorAbiIdentity {
            package: "test:evaluator".to_string(),
            world: "test-world".to_string(),
            interface: "evaluator".to_string(),
            version: "1.0.0".to_string(),
            contract_hash: "contract-hash".to_string(),
            adapter: "test-adapter@1".to_string(),
        }
    }

    #[test]
    fn execution_plan_hash_is_order_independent_and_artifact_specific() {
        let evaluator = |evaluator_ref: &str, content_hash: &str| ResolvedEvaluator {
            http_rules: vec![],
            evaluator_ref: evaluator_ref.to_string(),
            evaluator_id: Uuid::nil(),
            content_hash: content_hash.to_string(),
            abi: abi_identity(),
            runtime: "wasmtime".to_string(),
            runtime_version: "44.0.0".to_string(),
            runtime_fingerprint: "runtime-fingerprint".to_string(),
        };
        let first = EvaluatorExecutionPlan::new(
            "policy".to_string(),
            vec![evaluator("vigilo/b:1", "b"), evaluator("vigilo/a:1", "a")],
        );
        let reordered = EvaluatorExecutionPlan::new(
            "policy".to_string(),
            vec![evaluator("vigilo/a:1", "a"), evaluator("vigilo/b:1", "b")],
        );
        assert_eq!(first.hash().unwrap(), reordered.hash().unwrap());

        let changed = EvaluatorExecutionPlan::new(
            "policy".to_string(),
            vec![
                evaluator("vigilo/a:1", "changed"),
                evaluator("vigilo/b:1", "b"),
            ],
        );
        assert_ne!(first.hash().unwrap(), changed.hash().unwrap());

        let mut changed_adapter = first.clone();
        changed_adapter.evaluators[0].abi.adapter = "different-adapter".to_string();
        assert_ne!(first.hash().unwrap(), changed_adapter.hash().unwrap());

        let mut changed_grants = first.clone();
        changed_grants.evaluators[0]
            .http_rules
            .push(crate::evaluator_http::Rule {
                uri: "https://judge.example/score".into(),
                method: "POST".into(),
                action: crate::evaluator_http::Action::Allow,
                networks: vec![],
            });
        assert_ne!(first.hash().unwrap(), changed_grants.hash().unwrap());
    }
}
