//! Source and portable publication must converge on the same immutable registry row.
use sqlx::{
    ConnectOptions,
    PgPool,
};

use super::*;
use crate::{
    context::{
        database,
        output::OutputFormat,
        wasm,
    },
    evaluator_package::{
        self,
        PackageInput,
    },
};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn portable_publication_matches_source_and_preserves_conflict_guards(pool: PgPool) {
    let context = Context::new(
        database::Config::new(
            pool.connect_options().to_url_lossy().to_string(),
            5,
            std::time::Duration::from_secs(10),
            database::CircuitBreakerConfig::default(),
            database::PlacementConfig::default_single_database(),
        ),
        None,
        wasm::Config::default(),
        OutputFormat::Json,
    );
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../evaluators/sentiment-basic-en");
    let prepared = context
        .wasm()
        .await
        .unwrap()
        .prepare_evaluator(source.clone(), "release".into())
        .unwrap();
    let input = |bytes: Vec<u8>| PackageInput {
        name: prepared.name.clone(),
        version: prepared.version.clone(),
        description: prepared.description.clone(),
        tags: prepared.tags.clone(),
        metadata: prepared.metadata.clone(),
        abi: prepared.abi.clone(),
        wasm: bytes,
    };
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("package.tar.gz");
    evaluator_package::write(&archive, input(prepared.wasm_bytes.clone())).unwrap();
    let original = publish_vigilo_evaluator(&context, source, "release".into())
        .await
        .unwrap();
    assert!(matches!(original, PublishEvaluatorOutcome::Inserted(_)));
    for _ in 0..2 {
        let replay = publish_vigilo_evaluator(&context, archive.clone(), "dev".into())
            .await
            .unwrap();
        assert!(matches!(replay, PublishEvaluatorOutcome::Skipped(_)));
        assert_eq!(replay.evaluator().id, original.evaluator().id);
        assert_eq!(replay.evaluator().content_hash, prepared.wasm_hash);
        assert_eq!(replay.evaluator().wasm_bytes, prepared.wasm_bytes);
        assert_eq!(
            replay.evaluator().abi_contract_hash,
            prepared.abi.contract_hash
        );
    }
    let mut changed = prepared.wasm_bytes.clone();
    // An extra valid custom section changes content without changing the ABI.
    changed.extend_from_slice(&[0, 2, 1, b'x']);
    let conflict = dir.path().join("conflict.tar.gz");
    evaluator_package::write(&conflict, input(changed)).unwrap();
    let error = publish_vigilo_evaluator(&context, conflict, "dev".into())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("different content hash"));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM evaluators")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    context
        .dbr()
        .await
        .unwrap()
        .control()
        .await
        .unwrap()
        .close()
        .await;
}
