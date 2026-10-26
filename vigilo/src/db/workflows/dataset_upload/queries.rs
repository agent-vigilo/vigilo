//! Parameterized SQL for upload authority, bounded object progress and publication.
//! Every statement targets one upload or an explicitly limited maintenance page.

pub(super) const SELECT_UPLOAD: &str = r#"
    SELECT *
    FROM dataset_uploads
    WHERE id=$1
"#;

pub(super) const SELECT_CATALOG: &str = r#"
    SELECT id,
    manifest
    FROM dataset_catalog
    WHERE id=$1
"#;

pub(super) const FIND_REUSABLE: &str = r#"
    SELECT id,
    manifest
    FROM dataset_catalog
    WHERE storage_alias=$1
    AND binding_hash=$2
    AND dataset_hash=$3
    AND limits=$4
    ORDER BY created_at,
    id
    LIMIT 1
"#;

pub(super) const INSERT_UPLOAD: &str = r#"
    INSERT INTO dataset_uploads(id,
    storage_alias,
    binding_hash,
    identity,
    limits)
    VALUES($1,
    $2,
    $3,
    $4,
    $5)
    ON CONFLICT DO NOTHING
"#;

pub(super) const CLAIM_UPLOAD: &str = r#"
    UPDATE dataset_uploads
    SET lease_owner=$2,
    leased_until=clock_timestamp()+interval '300 seconds',
    generation=generation+1
    WHERE id=$1
    AND (leased_until IS NULL
    OR leased_until<=clock_timestamp())
    AND (($3
    AND status IN ('aborted',
    'expired')
    AND cleaned_at IS NULL
    AND cleanup_after<=clock_timestamp())
    OR (NOT $3
    AND status IN ('staging',
    'finalizing')
    AND expires_at>clock_timestamp()))
    RETURNING generation
"#;

pub(super) const LOCK_LIVE_UPLOAD: &str = r#"
    SELECT *
    FROM dataset_uploads
    WHERE id=$1
    AND lease_owner=$2
    AND generation=$3
    AND leased_until>clock_timestamp()
    AND (($4
    AND status IN ('aborted',
    'expired')
    AND cleanup_after<=clock_timestamp())
    OR (NOT $4
    AND status IN ('staging',
    'finalizing')
    AND expires_at>clock_timestamp()))
    FOR UPDATE
"#;

pub(super) const YIELD_UPLOAD: &str = r#"
    UPDATE dataset_uploads
    SET lease_owner=NULL,
    leased_until=NULL
    WHERE id=$1
    AND lease_owner=$2
    AND generation=$3
"#;

pub(super) const REGISTER_OBJECT: &str = r#"
    INSERT INTO dataset_upload_objects(upload_id,
    object_key,
    kind,
    level,
    range_start,
    range_end,
    reference)
    VALUES($1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7)
    ON CONFLICT DO NOTHING
"#;

pub(super) const SELECT_OBJECT_SLOT: &str = r#"
    SELECT *
    FROM dataset_upload_objects
    WHERE upload_id=$1
    AND (object_key=$2
    OR (kind=$3
    AND level=$4
    AND range_start=$5))
"#;

pub(super) const LOCK_OBJECT: &str = r#"
    SELECT *
    FROM dataset_upload_objects
    WHERE upload_id=$1
    AND object_key=$2
    FOR UPDATE
"#;

pub(super) const ADVANCE_PROGRESS: &str = r#"
    UPDATE dataset_uploads
    SET case_cursor=$2,
    batch_count=batch_count+1
    WHERE id=$1
"#;

pub(super) const ACKNOWLEDGE_OBJECT: &str = r#"
    UPDATE dataset_upload_objects
    SET acknowledged=true,
    cleaned=(kind='probe')
    WHERE upload_id=$1
    AND object_key=$2
"#;

pub(super) const SELECT_OBJECT_PAGE: &str = r#"
    SELECT *
    FROM dataset_upload_objects
    WHERE upload_id=$1
    AND kind=$2
    AND level=$3
    AND acknowledged
    AND range_start>$4
    ORDER BY range_start
    LIMIT 256
"#;

pub(super) const SELECT_CLEANUP_PAGE: &str = r#"
    SELECT *
    FROM dataset_upload_objects
    WHERE upload_id=$1
    AND NOT cleaned
    AND (NOT $2
    OR kind='probe')
    ORDER BY object_key
    LIMIT $3
"#;

pub(super) const MARK_OBJECT_CLEANED: &str = r#"
    UPDATE dataset_upload_objects
    SET cleaned=true,
    acknowledged=CASE WHEN kind='probe'
    THEN true
    ELSE acknowledged
    END
    WHERE upload_id=$1
    AND object_key=$2
"#;

pub(super) const BEGIN_FINALIZATION: &str = r#"
    UPDATE dataset_uploads
    SET status='finalizing'
    WHERE id=$1
"#;

pub(super) const CHECK_PUBLICATION_READY: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM dataset_upload_objects
        WHERE upload_id=$1 AND kind='manifest' AND reference=$2 AND acknowledged
    ) AND EXISTS (
        SELECT 1 FROM dataset_upload_objects
        WHERE upload_id=$1 AND kind='source' AND reference=$3 AND acknowledged
    ) AND EXISTS (
        SELECT 1 FROM dataset_upload_objects
        WHERE upload_id=$1 AND kind='index' AND reference=$4 AND acknowledged
    ) AND NOT EXISTS (
        SELECT 1 FROM dataset_upload_objects WHERE upload_id=$1 AND NOT acknowledged
    )
"#;

pub(super) const INSERT_CATALOG: &str = r#"
    INSERT INTO dataset_catalog(id,
    storage_alias,
    binding_hash,
    dataset_hash,
    dataset_version_id,
    limits,
    manifest)
    VALUES($1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7)
"#;

pub(super) const FINALIZE_UPLOAD: &str = r#"
    UPDATE dataset_uploads
    SET status='finalized', lease_owner=NULL, leased_until=NULL
    WHERE id=$1 AND lease_owner=$2 AND generation=$3
      AND leased_until>clock_timestamp() AND expires_at>clock_timestamp()
      AND status='finalizing'
"#;

pub(super) const ABORT_UPLOAD: &str = r#"
    UPDATE dataset_uploads
    SET status='aborted',
    cleanup_after=GREATEST(COALESCE(leased_until,
    clock_timestamp()),
    clock_timestamp())+interval '120 seconds',
    lease_owner=NULL,
    leased_until=NULL,
    generation=generation+1
    WHERE id=$1
    AND status IN ('staging',
    'finalizing')
"#;

pub(super) const EXPIRE_UPLOADS: &str = r#"
    WITH candidates AS (SELECT id
    FROM dataset_uploads
    WHERE status IN ('staging',
    'finalizing')
    AND expires_at<=clock_timestamp()
    ORDER BY expires_at,
    id
    LIMIT 64
    FOR UPDATE SKIP LOCKED) UPDATE dataset_uploads u
    SET status='expired',
    cleanup_after=GREATEST(COALESCE(leased_until,
    clock_timestamp()),
    clock_timestamp())+interval '120 seconds',
    lease_owner=NULL,
    leased_until=NULL,
    generation=generation+1
    FROM candidates c
    WHERE u.id=c.id
    RETURNING u.id
"#;

pub(super) const CHECK_CLEANUP_PENDING: &str = r#"
    SELECT EXISTS(SELECT 1
    FROM dataset_upload_objects
    WHERE upload_id=$1
    AND NOT cleaned)
"#;

pub(super) const FINISH_CLEANUP: &str = r#"
    UPDATE dataset_uploads
    SET cleaned_at=clock_timestamp(), lease_owner=NULL, leased_until=NULL
    WHERE id=$1
"#;

pub(super) const RENEW_LIVE_UPLOAD: &str = r#"
    UPDATE dataset_uploads
    SET leased_until=clock_timestamp()+interval '300 seconds',
    expires_at=CASE WHEN status IN ('staging','finalizing') THEN clock_timestamp()+interval '7 days' ELSE expires_at END
    WHERE id=$1
    AND lease_owner=$2
    AND generation=$3
    AND leased_until>clock_timestamp()
    AND (($4
    AND status IN ('aborted',
    'expired')
    AND cleanup_after<=clock_timestamp())
    OR (NOT $4
    AND status IN ('staging',
    'finalizing')
    AND expires_at>clock_timestamp()))
"#;
