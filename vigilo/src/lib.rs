//! Application operations and evaluator contracts for embedding Vigilo.
//!
//! Runtime implementation stays private. Hosts own process setup and presentation;
//! standalone operations expose bounded typed values without database internals.

extern crate self as vigilo;

#[cfg(test)]
#[path = "../build_support.rs"]
mod build_support;

mod agent_client;
mod blob_storage;
mod case_reader;
mod circuit_breaker;
pub mod cli;
mod context;
mod contracts;
mod core_evaluators;
mod dataset_ingestion;
mod db;
mod evaluator_abi;
mod evaluator_package;
mod evaluator_requests;
mod evaluators;
mod manifest;
mod models;
mod mq;
mod outbox;
mod run_admission;
mod run_planning;
mod runtime;

pub mod operations;

/// Versioned agent response configuration and evidence contracts.
#[path = "contracts/response.rs"]
pub mod response;

/// Pure, bounded interpretation of captured response evidence.
pub mod response_mapping;

/// Operator-authorized evaluator HTTP, snapshot caching and bounded transport.
pub mod evaluator_http;
