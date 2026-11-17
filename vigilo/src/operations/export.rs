//! Bounded, live execution exports for operator and restricted hosts.
//!
//! Independent collections prevent child fan-out from defeating pagination.
//! Cursors retain logical position, never pools or authorization. Every page
//! resolves current ownership/routing before a short execution read; errors return
//! no successful partial page. Interfaces own serialization and delivery.

mod reader;
mod sql;
mod types;
pub use reader::{
    ExportClient,
    SensitiveExportClient,
};
use serde::{
    Deserialize,
    Serialize,
};
pub use types::{
    AggregateContent,
    AggregateMetadata,
    AttemptContent,
    AttemptMetadata,
    EvaluatorResultContent,
    EvaluatorResultMetadata,
    ExecutionContent,
    ExecutionMetadata,
    ExportDiagnostic,
    ExportRecord,
};
use uuid::Uuid;

use super::Error;

/// An independently paginated collection. Content and diagnostics require the
/// sensitive reader; ordinary readers cannot upgrade authority through this value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportCollection {
    Executions,
    Attempts,
    Aggregates,
    EvaluatorResults,
    ExecutionContent,
    AttemptContent,
    AggregateContent,
    EvaluatorResultContent,
    Diagnostics,
}
impl ExportCollection {
    pub(crate) fn sensitive(self) -> bool {
        !matches!(
            self,
            Self::Executions | Self::Attempts | Self::Aggregates | Self::EvaluatorResults
        )
    }
}

/// Per-page record and UTF-8 JSON projection budgets. The byte budget counts the
/// larger encoding of each database/compact-JSON record, including array separators, not
/// the outer page/cursor envelope. Limits apply before client decoding as well.
#[derive(Clone, Copy, Debug)]
pub struct ExportLimits {
    rows: u32,
    bytes: u32,
}
impl Default for ExportLimits {
    fn default() -> Self {
        Self {
            rows: 250,
            bytes: 4 * 1024 * 1024,
        }
    }
}
impl ExportLimits {
    /// Requires 1–10,000 records and 2 bytes–16 MiB per page. Invalid budgets
    /// return `Error::InvalidInput` before any database work.
    pub fn new(rows: u32, bytes: u32) -> Result<Self, Error> {
        if !(1..=10_000).contains(&rows) || !(2..=16 * 1024 * 1024).contains(&bytes) {
            return Err(Error::InvalidInput(
                "export limits exceed bounded page budgets",
            ));
        }
        Ok(Self { rows, bytes })
    }
    pub fn rows(self) -> u32 {
        self.rows
    }
    pub fn bytes(self) -> u32 {
        self.bytes
    }
}

/// Opaque logical continuation, suitable for storing or passing through an API.
/// This is not a signature or an access grant. Current authority is always checked.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct ExportCursor(String);
impl ExportCursor {
    /// Parses a logical cursor of at most 1,024 bytes. Malformed or unsupported
    /// identities fail locally; parsing does not establish permission to read.
    pub fn parse(token: &str) -> Result<Self, Error> {
        if token.len() > 1024 {
            return Err(Error::InvalidInput("export cursor exceeds 1024 bytes"));
        }
        let position: Position = serde_json::from_str(token)
            .map_err(|_| Error::InvalidInput("invalid export cursor"))?;
        if position.version != 1
            || !(0..128).contains(&position.shard)
            || position.tenant_id.is_some() != position.actor_id.is_some()
        {
            return Err(Error::InvalidInput(
                "invalid export cursor identity or position",
            ));
        }
        Ok(Self(token.to_owned()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    version: u8,
    run_id: Uuid,
    collection: ExportCollection,
    tenant_id: Option<Uuid>,
    actor_id: Option<Uuid>,
    shard: i16,
    after: Option<Uuid>,
}
impl Position {
    fn start(
        run_id: Uuid,
        collection: ExportCollection,
        context: Option<super::UserContext>,
        cursor: Option<&ExportCursor>,
    ) -> Result<Self, Error> {
        let tenant_id = context.map(|c| c.tenant_id());
        let actor_id = context.map(|c| c.actor_id());
        let position = match cursor {
            Some(cursor) => serde_json::from_str::<Self>(&cursor.0)
                .map_err(|_| Error::InvalidInput("invalid export cursor"))?,
            None => Self {
                version: 1,
                run_id,
                collection,
                tenant_id,
                actor_id,
                shard: 0,
                after: None,
            },
        };
        if position.run_id != run_id
            || position.collection != collection
            || position.tenant_id != tenant_id
            || position.actor_id != actor_id
        {
            return Err(Error::InvalidInput(
                "export cursor does not match this read",
            ));
        }
        Ok(position)
    }
}

pub(super) async fn validate_connection(
    connection: &mut sqlx::PgConnection,
    sensitive: bool,
    functions: &[String],
) -> Result<(), sqlx::Error> {
    reader::permissions(connection, sensitive, functions).await
}

pub(super) async fn read_operator(
    database: &crate::context::database::DatabaseRouter,
    run_id: Uuid,
    collection: ExportCollection,
    limits: ExportLimits,
    cursor: Option<&ExportCursor>,
) -> Result<ExportPage, Error> {
    reader::standalone(database, run_id, collection, limits, cursor).await
}

/// One bounded observation. A continuation may lead to an empty final page;
/// concurrent writes can change subsequent observations. No snapshot is implied.
#[derive(Clone, Debug, Serialize)]
pub struct ExportPage {
    pub records: Vec<ExportRecord>,
    pub next: Option<ExportCursor>,
    pub encoded_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn policy_function_bindings_are_bounded_and_require_known_aliases() {
        use std::collections::BTreeMap;
        let config = || crate::operations::UserConfig {
            control: crate::operations::Config::new("postgres://fixture@localhost/fixture".into()),
            database_role: "export_reader".into(),
            execution_databases: BTreeMap::new(),
        };
        let cases = [
            ("unknown", vec!["access.allowed(uuid)".into()]),
            ("primary", vec![String::new()]),
            ("primary", vec![" access.allowed(uuid)".into()]),
            ("primary", vec!["access.\nallowed(uuid)".into()]),
            ("primary", vec!["x".repeat(1025)]),
            ("primary", vec!["access.allowed(uuid)".into(); 2]),
            (
                "primary",
                (0..65)
                    .map(|i| format!("access.allowed_{i}(uuid)"))
                    .collect(),
            ),
        ];
        for (alias, functions) in cases {
            let functions = BTreeMap::from([(alias.to_string(), functions)]);
            assert!(matches!(
                ExportClient::with_trusted_policy_functions(config(), functions.clone()),
                Err(Error::InvalidConfiguration(
                    "invalid trusted policy function bindings"
                ))
            ));
            assert!(matches!(
                SensitiveExportClient::with_trusted_policy_functions(config(), functions),
                Err(Error::InvalidConfiguration(
                    "invalid trusted policy function bindings"
                ))
            ));
        }
        let reader = ExportClient::with_trusted_policy_functions(
            config(),
            BTreeMap::from([("primary".into(), Vec::new())]),
        )
        .unwrap();
        reader.close().await;
    }
    #[test]
    fn page_limits_reject_unbounded_or_empty_work() {
        for (rows, bytes) in [(0, 1024), (10_001, 1024), (1, 0), (1, 16 * 1024 * 1024 + 1)] {
            assert!(ExportLimits::new(rows, bytes).is_err());
        }
        assert!(ExportLimits::new(10_000, 16 * 1024 * 1024).is_ok());
        assert_eq!(ExportLimits::default().rows(), 250);
        assert_eq!(ExportLimits::default().bytes(), 4 * 1024 * 1024);
    }
    #[test]
    fn cursor_rejects_unknown_versions_invalid_shards_and_large_tokens() {
        let mut p = Position {
            version: 1,
            run_id: Uuid::now_v7(),
            collection: ExportCollection::Executions,
            tenant_id: None,
            actor_id: None,
            shard: 0,
            after: None,
        };
        assert!(ExportCursor::parse(&serde_json::to_string(&p).unwrap()).is_ok());
        p.version = 2;
        assert!(ExportCursor::parse(&serde_json::to_string(&p).unwrap()).is_err());
        p.version = 1;
        p.shard = 128;
        assert!(ExportCursor::parse(&serde_json::to_string(&p).unwrap()).is_err());
        assert!(ExportCursor::parse(&" ".repeat(1025)).is_err());
    }
    #[test]
    fn cursors_bind_collection_run_and_caller_without_database_access() {
        let id = Uuid::from_u128(1);
        let position = Position::start(id, ExportCollection::Executions, None, None).unwrap();
        let cursor = ExportCursor::parse(&serde_json::to_string(&position).unwrap()).unwrap();
        assert!(
            Position::start(
                Uuid::from_u128(2),
                ExportCollection::Executions,
                None,
                Some(&cursor)
            )
            .is_err()
        );
        assert!(Position::start(id, ExportCollection::Attempts, None, Some(&cursor)).is_err());
        let context =
            crate::operations::UserContext::new(Uuid::from_u128(2), Uuid::from_u128(3)).unwrap();
        assert!(
            Position::start(
                id,
                ExportCollection::Executions,
                Some(context),
                Some(&cursor)
            )
            .is_err()
        );
        let mut object = serde_json::to_value(position).unwrap();
        object["unknown"] = serde_json::json!(true);
        assert!(ExportCursor::parse(&object.to_string()).is_err());
    }

    #[test]
    fn encoded_counter_matches_utf8_quotes_and_control_character_encoding() {
        let value = serde_json::json!({"input":["雪","\n\t\"\\",null,true],"nested":{"x":1}});
        assert_eq!(
            encoded_size(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
    }
}

#[cfg(test)]
#[path = "export/postgres_tests.rs"]
mod postgres_tests;

/// Count encoding without allocating a second response-sized buffer.
fn encoded_size(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}
