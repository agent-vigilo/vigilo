//! Evaluator registry table access.
//!
//! Evaluator rows store published WASM artifacts, identity metadata, runtime
//! compatibility fields, and lifecycle state. Runtime paths use the narrower
//! metadata projection to avoid loading WASM bytes when only identity and state
//! checks are needed.

use sqlx::PgPool;
use uuid::Uuid;

use crate::models::evaluator::{
    Evaluator,
    EvaluatorDraft,
    EvaluatorPatch,
    EvaluatorState,
    EvaluatorSummary,
};

/// Minimal evaluator metadata needed to bind a run profile to executable code.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct EvaluatorRuntimeMetadata {
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) id: Uuid,
    pub(crate) state: EvaluatorState,
    pub(crate) interface_version: String,
    pub(crate) interface_name: String,
    pub(crate) wit_world: String,
    pub(crate) content_hash: String,
    pub(crate) abi_contract_hash: String,
    pub(crate) abi_adapter: String,
    pub(crate) runtime: String,
    pub(crate) runtime_version: String,
    pub(crate) runtime_fingerprint: String,
}

/// Loads runtime metadata for a batch of fully qualified evaluator identities.
///
/// Identities are passed as `(namespace, name, version)` tuples and resolved in
/// one query so run creation and execution do not perform per-evaluator lookups.
pub(crate) async fn select_evaluator_runtime_metadata_by_identities<
    'e,
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
>(
    db: E,
    identities: &[(String, String, String)],
) -> anyhow::Result<Vec<EvaluatorRuntimeMetadata>> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }

    let namespaces = identities.iter().map(|v| v.0.clone()).collect::<Vec<_>>();
    let names = identities.iter().map(|v| v.1.clone()).collect::<Vec<_>>();
    let versions = identities.iter().map(|v| v.2.clone()).collect::<Vec<_>>();

    let rows = sqlx::query_as::<_, EvaluatorRuntimeMetadata>(
        r#"
        WITH requested AS (
            SELECT *
            FROM unnest($1::text[], $2::text[], $3::text[]) AS r(namespace, name, version)
        )
        SELECT
            e.namespace,
            e.name,
            e.version,
            e.id,
            e.state,
            e.interface_name,
            e.interface_version,
            e.wit_world,
            e.content_hash,
            e.abi_contract_hash,
            e.abi_adapter,
            e.runtime,
            e.runtime_version,
            e.runtime_fingerprint
        FROM requested r
        JOIN evaluators e
          ON e.namespace = r.namespace
         AND e.name = r.name
         AND e.version = r.version
        "#,
    )
    .bind(namespaces)
    .bind(names)
    .bind(versions)
    .fetch_all(db)
    .await?;

    Ok(rows)
}

/// Inserts a newly published evaluator artifact.
pub(crate) async fn insert_evaluator(
    db: &PgPool,
    draft: &EvaluatorDraft,
) -> anyhow::Result<Evaluator> {
    let wasm_size_bytes = draft.wasm_bytes.len() as i64;

    let evaluator = sqlx::query_as::<_, Evaluator>(
        r#"
        INSERT INTO evaluators (
            namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        RETURNING
            id, namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata, state, state_reason, created_at, updated_at
        "#,
    )
    .bind(&draft.namespace)
    .bind(&draft.name)
    .bind(&draft.version)
    .bind(&draft.content_hash)
    .bind(&draft.wasm_bytes)
    .bind(wasm_size_bytes)
    .bind(&draft.interface_name)
    .bind(&draft.interface_version)
    .bind(&draft.wit_world)
    .bind(&draft.abi_contract_hash)
    .bind(&draft.abi_adapter)
    .bind(&draft.runtime)
    .bind(&draft.runtime_version)
    .bind(&draft.runtime_fingerprint)
    .bind(&draft.description)
    .bind(&draft.tags)
    .bind(&draft.metadata)
    .fetch_one(db)
    .await?;

    Ok(evaluator)
}

/// Finds an evaluator by its database id.
pub(crate) async fn select_evaluator_by_id(
    db: &PgPool,
    id: Uuid,
) -> anyhow::Result<Option<Evaluator>> {
    let evaluator = sqlx::query_as::<_, Evaluator>(
        r#"
        SELECT
            id, namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata, state, state_reason, created_at, updated_at
        FROM evaluators
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await?;

    Ok(evaluator)
}

/// Finds the most recently created evaluator for a namespace/name pair.
pub(crate) async fn select_latest_evaluator_by_name(
    db: &PgPool,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<Evaluator>> {
    let evaluator = sqlx::query_as::<_, Evaluator>(
        r#"
        SELECT
            id, namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata, state, state_reason, created_at, updated_at
        FROM evaluators
        WHERE namespace = $1 AND name = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(name)
    .fetch_optional(db)
    .await?;

    Ok(evaluator)
}

/// Finds an evaluator by namespace, name, and version.
pub(crate) async fn select_evaluator(
    db: &PgPool,
    namespace: &str,
    name: &str,
    version: &str,
) -> anyhow::Result<Option<Evaluator>> {
    let evaluator = sqlx::query_as::<_, Evaluator>(
        r#"
        SELECT
            id, namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata, state, state_reason, created_at, updated_at
        FROM evaluators
        WHERE namespace = $1 AND name = $2 AND version = $3
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(name)
    .bind(version)
    .fetch_optional(db)
    .await?;

    Ok(evaluator)
}

/// Finds an evaluator by namespace and content hash.
pub(crate) async fn select_evaluator_by_content_hash(
    db: &PgPool,
    namespace: &str,
    content_hash: &str,
) -> anyhow::Result<Option<Evaluator>> {
    let evaluator = sqlx::query_as::<_, Evaluator>(
        r#"
        SELECT
            id, namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata, state, state_reason, created_at, updated_at
        FROM evaluators
        WHERE namespace = $1 AND content_hash = $2
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(content_hash)
    .fetch_optional(db)
    .await?;

    Ok(evaluator)
}

/// Lists evaluators in a namespace for management and discovery views.
pub(crate) async fn list_evaluators(
    db: &PgPool,
    namespace: &str,
) -> anyhow::Result<Vec<Evaluator>> {
    let evaluators = sqlx::query_as::<_, Evaluator>(
        r#"
        SELECT
            id, namespace, name, version, content_hash, wasm_bytes,
            wasm_size_bytes, interface_name, interface_version,
            wit_world, abi_contract_hash, abi_adapter,
            runtime, runtime_version, runtime_fingerprint,
            description, tags, metadata, state, state_reason, created_at, updated_at
        FROM evaluators
        WHERE namespace = $1
        ORDER BY name ASC, version DESC
        "#,
    )
    .bind(namespace)
    .fetch_all(db)
    .await?;

    Ok(evaluators)
}

/// Searches evaluator summaries in a namespace using a bounded text match.
pub(crate) async fn search_evaluator_summaries(
    db: &PgPool,
    namespace: &str,
    query: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<EvaluatorSummary>> {
    let limit = limit.clamp(1, 20);

    let pattern = query
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("%{}%", value));

    let evaluators = sqlx::query_as::<_, EvaluatorSummary>(
        r#"
        SELECT
            namespace, name, version, description, tags, metadata, state, state_reason
        FROM evaluators
        WHERE namespace = $1
          AND (
              $2::text IS NULL
              OR name ILIKE $2
              OR COALESCE(description, '') ILIKE $2
              OR COALESCE(state_reason, '') ILIKE $2
              OR tags::text ILIKE $2
              OR metadata::text ILIKE $2
          )
        ORDER BY name ASC, version DESC
        LIMIT $3
        "#,
    )
    .bind(namespace)
    .bind(pattern)
    .bind(limit)
    .fetch_all(db)
    .await?;

    Ok(evaluators)
}

/// Updates an evaluator lifecycle state unless it has already been removed.
pub(crate) async fn update_evaluator_state(
    db: &PgPool,
    namespace: &str,
    name: &str,
    version: &str,
    patch: &EvaluatorPatch,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        UPDATE evaluators
        SET state = $3,
            state_reason = $4,
            updated_at = now()
        WHERE namespace = $1 AND name = $2 AND version = $5
          AND state <> 'removed'::evaluator_state
        "#,
    )
    .bind(namespace)
    .bind(name)
    .bind(&patch.state)
    .bind(&patch.state_reason)
    .bind(version)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}
