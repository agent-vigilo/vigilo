//! Public library surface for the Vigilo crate.
//!
//! The runtime currently lives in the binary crate, so this module is reserved
//! for reusable APIs that evaluator crates or external integrations should
//! import directly. Keep this surface small and stable; do not expose runtime,
//! database, or CLI internals here until they are intended as public contracts.

//! Public library surface for the Vigilo crate.
//!
//! The runtime currently lives in the binary crate, so this module is reserved
//! for reusable APIs that evaluator crates or external integrations should
//! import directly. Keep this surface small and stable; do not expose runtime,
//! database, or CLI internals here until they are intended as public contracts.

// todo - export traits and utilities for evaluators

/// Versioned agent response configuration and evidence contracts.
#[path = "contracts/response.rs"]
pub mod response;
