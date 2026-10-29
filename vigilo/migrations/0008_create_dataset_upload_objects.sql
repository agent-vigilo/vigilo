CREATE TABLE dataset_upload_objects (
    upload_id UUID NOT NULL REFERENCES dataset_uploads(id),
    object_key TEXT NOT NULL CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
    kind TEXT NOT NULL CHECK (kind IN ('batch','index','source','manifest','probe')),
    level SMALLINT NOT NULL DEFAULT 0 CHECK (level BETWEEN 0 AND 7),
    range_start INTEGER CHECK (range_start >= 0),
    range_end INTEGER,
    reference JSONB NOT NULL CHECK (octet_length(reference::text) <= 8192),
    acknowledged BOOLEAN NOT NULL DEFAULT false,
    cleaned BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (upload_id, object_key),
    UNIQUE (upload_id, kind, level, range_start),
    CHECK ((range_start IS NULL AND range_end IS NULL AND kind = 'probe') OR
           (range_start IS NOT NULL AND range_end > range_start AND kind <> 'probe'))
);

CREATE INDEX dataset_upload_pending_cleanup ON dataset_upload_objects (upload_id, object_key)
    WHERE NOT cleaned;

COMMENT ON TABLE dataset_upload_objects IS 'Exact owned attempt keys recorded before remote I/O; progress is acknowledged after byte and semantic verification.';
