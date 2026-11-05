//! Execution control values and observations, independent of interface policy.
//! Revisions order delivery; database application is not worker quiescence.
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

/// Mutable execution permission, separate from a run's lifecycle status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "execution_control", rename_all = "lowercase")]
pub enum ExecutionControl {
    Enabled,
    Paused,
    Revoked,
}
impl ExecutionControl {
    pub(crate) fn permits_transition(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Enabled, Self::Paused | Self::Revoked)
                    | (Self::Paused, Self::Enabled | Self::Revoked)
            )
    }
}
/// Observed durable delivery progress. Application does not promise that a remote
/// call has stopped; settlement requires completed cleanup on every placement.
#[derive(Clone, Debug, Serialize)]
pub struct ControlOutcome {
    pub run_id: Uuid,
    pub execution_control: ExecutionControl,
    pub revision: i64,
    pub placement_count: i64,
    pub applied_placement_count: i64,
    pub quiescent_placement_count: i64,
    pub settled: bool,
}

/// Process-local observation updated by the existing lease heartbeat.
#[derive(Clone)]
pub(crate) struct Observation {
    state: std::sync::Arc<std::sync::atomic::AtomicU8>,
    pub(crate) revoked: tokio_util::sync::CancellationToken,
}
impl Observation {
    pub(crate) fn new(state: ExecutionControl) -> Self {
        let s = Self {
            state: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
            revoked: Default::default(),
        };
        s.update(state);
        s
    }
    pub(crate) fn update(&self, state: ExecutionControl) {
        self.state.store(
            match state {
                ExecutionControl::Enabled => 0,
                ExecutionControl::Paused => 1,
                ExecutionControl::Revoked => 2,
            },
            std::sync::atomic::Ordering::Release,
        );
        if state == ExecutionControl::Revoked {
            self.revoked.cancel();
        }
    }
    pub(crate) fn enabled(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == 0 && !self.revoked.is_cancelled()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn revocation_is_terminal_and_observation_never_restores_withdrawn_authority() {
        for from in [
            ExecutionControl::Enabled,
            ExecutionControl::Paused,
            ExecutionControl::Revoked,
        ] {
            for to in [
                ExecutionControl::Enabled,
                ExecutionControl::Paused,
                ExecutionControl::Revoked,
            ] {
                assert_eq!(
                    from.permits_transition(to),
                    from != ExecutionControl::Revoked || to == from
                );
            }
        }
        let observation = Observation::new(ExecutionControl::Enabled);
        assert!(observation.enabled());
        observation.update(ExecutionControl::Paused);
        assert!(!observation.enabled());
        observation.update(ExecutionControl::Enabled);
        assert!(observation.enabled());
        observation.update(ExecutionControl::Revoked);
        assert!(observation.revoked.is_cancelled());
        observation.update(ExecutionControl::Enabled);
        assert!(!observation.enabled());
    }
}
