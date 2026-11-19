//! Profile and native JSON contracts for harness invocation. The types describe
//! messages independently of delivery, database ownership and evaluator execution.

#[path = "invocation/profile.rs"]
pub mod profile;
#[path = "invocation/wire.rs"]
pub mod wire;

#[cfg(test)]
#[path = "invocation/tests.rs"]
mod tests;
