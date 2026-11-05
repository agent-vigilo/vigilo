//! Database access layer.
//!
//! This module keeps persistence concerns behind three boundaries:
//! migrations for schema setup, tables for narrow row-oriented helpers, and
//! workflows for multi-table operations that need transactional or concurrency
//! semantics.

pub(crate) mod migrations;
pub(crate) mod shard_write_fence;
pub(crate) mod tables;
pub(crate) mod workflows;

pub(crate) mod scope;

#[cfg(test)]
pub(crate) mod test_support;
