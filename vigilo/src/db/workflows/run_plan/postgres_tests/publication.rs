//! Publication validation retains distinct failures and atomic state transitions.
//! Faults are injected after durable object acknowledgement, immediately before
//! publication, so the workflow itself must reject inconsistent ledger evidence.
use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn publication_rejects_each_incomplete_ledger_condition_without_exposing_routes(db: PgPool) {
    let cases = [
        (
            "UPDATE run_plan_objects SET acknowledged=false WHERE run_id=NEW.run_id AND kind='configuration'",
            "plan root references an unacknowledged object",
        ),
        (
            "DELETE FROM run_plan_objects WHERE run_id=NEW.run_id AND kind='configuration'",
            "plan root references an unacknowledged object",
        ),
        (
            "UPDATE run_plan_objects SET reference=jsonb_set(reference,'{key}','\"wrong-key\"') WHERE run_id=NEW.run_id AND kind='configuration'",
            "plan root references an unacknowledged object",
        ),
        (
            "UPDATE run_plan_objects SET acknowledged=false WHERE run_id=NEW.run_id AND kind='chunks'",
            "incomplete run-plan object coverage",
        ),
        (
            "UPDATE run_plans SET chunk_cursor=0 WHERE run_id=NEW.run_id",
            "incomplete run-plan object coverage",
        ),
        (
            "UPDATE run_plan_objects SET acknowledged=false WHERE run_id=NEW.run_id AND kind='manifest'",
            "incomplete run-plan object coverage",
        ),
        (
            "UPDATE run_plan_objects SET kind='probe',range_start=NULL,range_end=NULL WHERE run_id=NEW.run_id AND kind='manifest'",
            "manifest object not acknowledged",
        ),
        (
            "DELETE FROM run_plan_objects WHERE run_id=NEW.run_id AND kind='manifest'",
            "manifest object not acknowledged",
        ),
        (
            "UPDATE run_plan_objects SET reference=jsonb_set(reference,'{key}','\"wrong-key\"') WHERE run_id=NEW.run_id AND kind='manifest'",
            "manifest object not acknowledged",
        ),
    ];
    for (mutation, expected) in cases {
        let f = Fixture::with_size(&db, 2, 2, 2).await;
        let lease = claim(&db, f.plan.run_id).await.unwrap();
        // Static SQL fixtures mutate only the run currently publishing its root.
        sqlx::raw_sql(&format!(
            "CREATE OR REPLACE FUNCTION corrupt_publication_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {mutation}; RETURN NEW; END $$;
             CREATE TRIGGER corrupt_publication_evidence AFTER UPDATE OF acknowledged ON run_plan_objects FOR EACH ROW WHEN (NEW.kind='manifest' AND NEW.acknowledged) EXECUTE FUNCTION corrupt_publication_evidence();"
        )).execute(&db).await.unwrap();
        let error = f.prepare(&db, lease).await.unwrap_err();
        assert!(
            format!("{error:#}").contains(expected),
            "{mutation}: {error:#}"
        );
        let state: (String, String, bool, i64, i64, i64) = sqlx::query_as(
            "SELECT r.status::text,p.status,p.manifest IS NULL,
              (SELECT count(*) FROM run_plan_shards WHERE run_id=$1),
              (SELECT count(*) FROM shard_placements WHERE run_id=$1),
              (SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1)
             FROM runs r JOIN run_plans p ON p.run_id=r.id WHERE r.id=$1",
        )
        .bind(f.plan.run_id)
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(
            state,
            ("creating".into(), "preparing".into(), true, 0, 0, 0)
        );
        sqlx::query("DROP TRIGGER corrupt_publication_evidence ON run_plan_objects")
            .execute(&db)
            .await
            .unwrap();
    }
    let healthy = Fixture::with_size(&db, 2, 2, 2).await;
    let lease = claim(&db, healthy.plan.run_id).await.unwrap();
    let root = healthy.prepare(&db, lease).await.unwrap();
    let plan = load(&db, healthy.plan.run_id).await.unwrap().unwrap();
    assert_eq!(plan.status, "ready");
    assert_eq!(plan.manifest.unwrap().0, root);
}
