//! Bounded SQL operations for run-plan ownership and publication.

pub(super) const SELECT_PLAN: &str = r#"
    SELECT run_id,dataset_upload_id,status,storage_binding_hash,limits,execution_plan,assignments,chunk_cursor,execution_requested,admission_cursor,admission_state,manifest
    FROM run_plans
    WHERE run_id=$1
"#;

pub(super) const LOCK_IDENTITY: &str = r#"
    SELECT pg_advisory_xact_lock(hashtextextended($1,0))
"#;

pub(super) const SELECT_RUN: &str = r#"
    SELECT profile_hash, aggregation_policy_hash, config_snapshot, status::text
    FROM runs
    WHERE id=$1
    FOR UPDATE
"#;

pub(super) const INSERT_PLAN: &str = r#"
    INSERT INTO run_plans(run_id,dataset_upload_id,storage_binding_hash,limits,execution_plan,assignments,execution_requested)
    VALUES($1,$2,$3,$4,$5,$6,$7)
"#;

pub(super) const REQUEST_EXECUTION: &str = r#"
    UPDATE run_plans SET execution_requested=true
    WHERE run_id=$1 AND EXISTS(SELECT 1 FROM runs WHERE id=$1 AND status='creating')
"#;

pub(super) const LOCK_RUN: &str = r#"
    SELECT id
    FROM runs
    WHERE id=$1
    FOR UPDATE
"#;

pub(super) const CLAIM_RUN: &str = r#"
    UPDATE runs
    SET coordinator_id=$2,coordinator_leased_until=clock_timestamp()+interval '300 seconds',coordinator_heartbeat_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE id=$1
    AND status='creating'
    AND (coordinator_leased_until IS NULL OR coordinator_leased_until<=clock_timestamp())
    AND EXISTS(SELECT 1
    FROM run_plans
    WHERE run_id=$1
    AND status='preparing')
"#;

pub(super) const NEXT_GENERATION: &str = r#"
    UPDATE run_plans
    SET generation=generation+1,updated_at=clock_timestamp()
    WHERE run_id=$1
    RETURNING generation
"#;

pub(super) const RENEW_RUN: &str = r#"
    UPDATE runs
    SET coordinator_leased_until=clock_timestamp()+interval '300 seconds',coordinator_heartbeat_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE id=$1
    AND status='creating'
    AND coordinator_id=$2
    AND coordinator_leased_until>clock_timestamp()
    AND EXISTS(SELECT 1
    FROM run_plans
    WHERE run_id=$1
    AND generation=$3
    AND status='preparing')
"#;

pub(super) const RELEASE_RUN: &str = r#"
    UPDATE runs
    SET coordinator_id=NULL,coordinator_leased_until=NULL,coordinator_heartbeat_at=NULL,error_message=COALESCE($4,error_message)
    WHERE id=$1
    AND coordinator_id=$2
    AND EXISTS(SELECT 1
    FROM run_plans
    WHERE run_id=$1
    AND generation=$3)
"#;

pub(super) const SELECT_OWNER: &str = r#"
    SELECT plan.limits,catalog.storage_alias
    FROM run_plans plan
    JOIN dataset_catalog catalog ON catalog.id=plan.dataset_upload_id
    WHERE plan.run_id=$1
"#;

pub(super) const ADVANCE_CHUNKS: &str = r#"
    UPDATE run_plans
    SET chunk_cursor=$3
    WHERE run_id=$1
    AND chunk_cursor=$2
"#;

pub(super) const SELECT_OBJECTS: &str = r#"
    SELECT object_key,kind,level,range_start,range_end,reference,acknowledged
    FROM run_plan_objects
    WHERE run_id=$1
    AND kind=$2
    AND level=$3
    AND range_start>$4
    AND acknowledged
    ORDER BY range_start
    LIMIT 256
"#;

pub(super) const PENDING_PROBES: &str = r#"
    SELECT object_key,kind,level,range_start,range_end,reference,acknowledged
    FROM run_plan_objects
    WHERE run_id=$1
    AND kind='probe'
    AND NOT acknowledged
    ORDER BY object_key
    LIMIT 64
"#;

pub(super) const SELECT_FROZEN_IDENTITY: &str = r#"
    SELECT catalog.manifest,run.profile_hash,run.expected_execution_count
    FROM run_plans plan
    JOIN dataset_catalog catalog ON catalog.id=plan.dataset_upload_id
    JOIN runs run ON run.id=plan.run_id
    WHERE plan.run_id=$1
"#;

// Independent publication predicates share one statement snapshot. Keep separate
// results so the workflow preserves its validation order and error context.
pub(super) const SELECT_PUBLICATION_CHECKS: &str = r#"
    SELECT NOT EXISTS (
        SELECT 1
        FROM jsonb_to_recordset($2::jsonb) AS expected(object_key text, reference jsonb)
        LEFT JOIN run_plan_objects actual
            ON actual.run_id=$1 AND actual.object_key=expected.object_key
            AND actual.reference=expected.reference AND actual.acknowledged
        WHERE actual.object_key IS NULL
    ) AS references_acknowledged,
    EXISTS (
        SELECT 1 FROM run_plan_objects
        WHERE run_id=$1 AND NOT acknowledged
    ) AS has_unacknowledged_objects,
    EXISTS (
        SELECT 1 FROM run_plan_objects
        WHERE run_id=$1 AND kind='manifest'
            AND object_key=$3 AND reference=$4 AND acknowledged
    ) AS manifest_acknowledged
"#;

pub(super) const PUBLISH_PLAN: &str = r#"
    UPDATE run_plans
    SET status='ready',manifest=$2,updated_at=clock_timestamp()
    WHERE run_id=$1
"#;

pub(super) const RELEASE_PUBLISHED_RUN: &str = r#"
    UPDATE runs
    SET coordinator_id=NULL,coordinator_leased_until=NULL,coordinator_heartbeat_at=NULL,error_message=NULL,config_snapshot=config_snapshot || jsonb_build_object('run_plan',$2::jsonb,'plan_status','ready',
        'settlement_plan',(SELECT jsonb_build_object('version',1,'shards',jsonb_object_agg(run_shard::text,jsonb_build_object('hash',planned_chunk_hash,'case_count',case_count,'chunk_count',chunk_count))) FROM run_plan_shards WHERE run_id=$1)),updated_at=clock_timestamp()
    WHERE id=$1
"#;
