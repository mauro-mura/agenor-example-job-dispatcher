-- Job table of the dispatcher. Written only by the manager, only through JobStore.
CREATE TABLE jobs (
    job_id       text        PRIMARY KEY,
    kind         text        NOT NULL,
    input_ref    text        NOT NULL,
    size_bytes   bigint      NOT NULL,
    state        text        NOT NULL,
    -- attempt, deadline_at and excluded: the deadline and the retry history survive a restart
    attempt      int         NOT NULL DEFAULT 0,
    assignee     text,
    deadline_at  timestamptz,
    excluded     text[]      NOT NULL DEFAULT '{}',
    output_ref   text,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_state_valid
        CHECK (state IN ('SUBMITTED', 'ASSIGNED', 'DONE', 'FAILED')),
    CONSTRAINT jobs_assigned_has_owner
        CHECK (state <> 'ASSIGNED' OR (assignee IS NOT NULL AND deadline_at IS NOT NULL))
);

CREATE INDEX jobs_due      ON jobs (state, deadline_at);
CREATE INDEX jobs_awaiting ON jobs (state, created_at);
