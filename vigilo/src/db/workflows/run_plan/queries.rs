//! Bounded SQL operations for run-plan ownership and publication.

pub(super) const SELECT_PLAN: &str = r#"
    SELECT run_id,dataset_upload_id,status,storage_binding_hash,limits,execution_plan,assignments,chunk_cursor,manifest
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
    INSERT INTO run_plans(run_id,dataset_upload_id,storage_binding_hash,limits,execution_plan,assignments)
    VALUES($1,$2,$3,$4,$5,$6)
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

pub(super) const INSERT_OBJECT: &str = r#"
    INSERT INTO run_plan_objects(run_id,object_key,kind,level,range_start,range_end,reference)
    VALUES($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT DO NOTHING
"#;

pub(super) const SELECT_OBJECT: &str = r#"
    SELECT object_key,kind,level,range_start,range_end,reference,acknowledged
    FROM run_plan_objects
    WHERE run_id=$1
    AND object_key=$2
"#;

pub(super) const LOCK_ACKNOWLEDGEMENT: &str = r#"
    SELECT acknowledged
    FROM run_plan_objects
    WHERE run_id=$1
    AND object_key=$2
    AND reference=$3
    FOR UPDATE
"#;

pub(super) const ADVANCE_CHUNKS: &str = r#"
    UPDATE run_plans
    SET chunk_cursor=$3
    WHERE run_id=$1
    AND chunk_cursor=$2
"#;

pub(super) const ACKNOWLEDGE_OBJECT: &str = r#"
    UPDATE run_plan_objects
    SET acknowledged=true
    WHERE run_id=$1
    AND object_key=$2
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

pub(super) const HAS_ACKNOWLEDGED_REFERENCES: &str = r#"
    SELECT NOT EXISTS (
        SELECT 1
        FROM jsonb_to_recordset($2::jsonb) AS expected(object_key text, reference jsonb)
        LEFT JOIN run_plan_objects actual
            ON actual.run_id=$1 AND actual.object_key=expected.object_key
            AND actual.reference=expected.reference AND actual.acknowledged
        WHERE actual.object_key IS NULL
    )
"#;

pub(super) const HAS_UNACKNOWLEDGED_OBJECTS: &str = r#"
    SELECT EXISTS(SELECT 1
    FROM run_plan_objects
    WHERE run_id=$1
    AND NOT acknowledged)
"#;

pub(super) const HAS_MANIFEST: &str = r#"
    SELECT EXISTS(SELECT 1
    FROM run_plan_objects
    WHERE run_id=$1
    AND kind='manifest'
    AND object_key=$2
    AND reference=$3
    AND acknowledged)
"#;

pub(super) const PUBLISH_PLAN: &str = r#"
    UPDATE run_plans
    SET status='ready',manifest=$2,updated_at=clock_timestamp()
    WHERE run_id=$1
"#;

pub(super) const RELEASE_PUBLISHED_RUN: &str = r#"
    UPDATE runs
    SET coordinator_id=NULL,coordinator_leased_until=NULL,coordinator_heartbeat_at=NULL,error_message=NULL,config_snapshot=config_snapshot || jsonb_build_object('run_plan',$2::jsonb,'plan_status','ready'),updated_at=clock_timestamp()
    WHERE id=$1
"#;

pub(super) const SET_CREATION_KIND: &str = r#"
    UPDATE runs SET creation_kind='blob-plan' WHERE id=$1
"#;

pub(super) const INSERT_DATASET_VERSION: &str = r#"
    INSERT INTO dataset_versions(dataset_version_id,dataset_id,dataset_version)
    VALUES($1,$2,$3)
    ON CONFLICT DO NOTHING
"#;

pub(super) const VERIFY_DATASET_VERSION: &str = r#"
    SELECT dataset_id=$2 AND dataset_version=$3
    FROM dataset_versions WHERE dataset_version_id=$1
"#;
