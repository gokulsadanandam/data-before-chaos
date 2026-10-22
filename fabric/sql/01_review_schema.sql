/* ---------------------------------------------------------------------------------------
   Data before Chaos - steward review store (Fabric SQL database)

   Why a SQL database: steward decisions are small, concurrent, transactional writes
   (one click = one row) with a lock so two stewards cannot decide the same pair. The
   database mirrors into OneLake automatically, so Spark reads decisions through a
   Lakehouse shortcut (rvw_decision_log) without any export job.

   Run once per environment, in this order: 01 -> 02 -> 03 -> 04. Idempotent.
   Mirroring needs a primary key on each mirrored table; review_queue_stage is a transient
   load table and does not need to be mirrored.
--------------------------------------------------------------------------------------- */

IF SCHEMA_ID(N'review') IS NULL EXEC (N'CREATE SCHEMA review');
GO

/* Who may decide. Role is checked by the user data function on every call. */
IF OBJECT_ID(N'review.steward', N'U') IS NULL
CREATE TABLE review.steward
(
    upn           NVARCHAR(256) NOT NULL CONSTRAINT pk_steward PRIMARY KEY,  -- Entra user principal name
    display_name  NVARCHAR(200) NOT NULL,
    role          NVARCHAR(20)  NOT NULL CONSTRAINT ck_steward_role CHECK (role IN (N'steward', N'lead', N'viewer')),
    is_active     BIT           NOT NULL CONSTRAINT df_steward_active DEFAULT (1),
    created_at    DATETIME2(3)  NOT NULL CONSTRAINT df_steward_created DEFAULT (SYSUTCDATETIME())
);
GO

/* Landing table for the pipeline Copy activity (Lakehouse outbox -> here). Truncated by the MERGE. */
IF OBJECT_ID(N'review.review_queue_stage', N'U') IS NULL
CREATE TABLE review.review_queue_stage
(
    pair_id              NVARCHAR(64)  NOT NULL,
    review_type          NVARCHAR(20)  NOT NULL,
    left_id              NVARCHAR(200) NOT NULL,
    right_id             NVARCHAR(200) NOT NULL,
    left_source          NVARCHAR(50)  NULL,
    left_name            NVARCHAR(200) NULL,
    left_email_masked    NVARCHAR(200) NULL,
    left_phone_masked    NVARCHAR(50)  NULL,
    left_city            NVARCHAR(100) NULL,
    left_postcode        NVARCHAR(20)  NULL,
    right_source         NVARCHAR(50)  NULL,
    right_name           NVARCHAR(200) NULL,
    right_email_masked   NVARCHAR(200) NULL,
    right_phone_masked   NVARCHAR(50)  NULL,
    right_city           NVARCHAR(100) NULL,
    right_postcode       NVARCHAR(20)  NULL,
    confidence           DECIMAL(5, 3) NULL,
    band                 NVARCHAR(10)  NULL,
    explanation          NVARCHAR(1000) NULL,
    evidence             NVARCHAR(1000) NULL,
    priority             INT           NULL,
    created_at           DATETIME2(3)  NULL,
    run_id               NVARCHAR(64)  NULL
);
GO

/* The live queue stewards work from (Power BI review page reads review.v_open_queue). */
IF OBJECT_ID(N'review.review_queue', N'U') IS NULL
CREATE TABLE review.review_queue
(
    pair_id              NVARCHAR(64)  NOT NULL CONSTRAINT pk_review_queue PRIMARY KEY,
    review_type          NVARCHAR(20)  NOT NULL CONSTRAINT ck_rq_type CHECK (review_type IN (N'MATCH_REVIEW', N'SPOT_CHECK')),
    left_id              NVARCHAR(200) NOT NULL,
    right_id             NVARCHAR(200) NOT NULL,
    left_source          NVARCHAR(50)  NULL,
    left_name            NVARCHAR(200) NULL,
    left_email_masked    NVARCHAR(200) NULL,
    left_phone_masked    NVARCHAR(50)  NULL,
    left_city            NVARCHAR(100) NULL,
    left_postcode        NVARCHAR(20)  NULL,
    right_source         NVARCHAR(50)  NULL,
    right_name           NVARCHAR(200) NULL,
    right_email_masked   NVARCHAR(200) NULL,
    right_phone_masked   NVARCHAR(50)  NULL,
    right_city           NVARCHAR(100) NULL,
    right_postcode       NVARCHAR(20)  NULL,
    confidence           DECIMAL(5, 3) NULL,
    band                 NVARCHAR(10)  NULL,
    explanation          NVARCHAR(1000) NULL,
    evidence             NVARCHAR(1000) NULL,
    priority             INT           NOT NULL CONSTRAINT df_rq_priority DEFAULT (0),
    status               NVARCHAR(20)  NOT NULL CONSTRAINT df_rq_status DEFAULT (N'PENDING')
                         CONSTRAINT ck_rq_status CHECK (status IN (N'PENDING', N'DECIDED', N'HELD', N'SUPERSEDED')),
    decided_by           NVARCHAR(256) NULL,
    decided_at           DATETIME2(3)  NULL,
    first_seen_run_id    NVARCHAR(64)  NULL,
    last_seen_run_id     NVARCHAR(64)  NULL,
    created_at           DATETIME2(3)  NOT NULL CONSTRAINT df_rq_created DEFAULT (SYSUTCDATETIME()),
    updated_at           DATETIME2(3)  NOT NULL CONSTRAINT df_rq_updated DEFAULT (SYSUTCDATETIME())
    -- No ROWVERSION column: it cannot be mirrored to OneLake. Concurrency is handled by the
    -- conditional UPDATE ... WHERE status = 'PENDING' in the user data function.
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_review_queue_status_priority')
    CREATE INDEX ix_review_queue_status_priority ON review.review_queue (status, priority DESC) INCLUDE (review_type, band);
GO

/* Append-only decision log. Mirrored to OneLake; Spark reads it as rvw_decision_log.
   Column names match goldenrecord.review.DECISION_LOG_COLUMNS. */
IF OBJECT_ID(N'review.decision_log', N'U') IS NULL
CREATE TABLE review.decision_log
(
    decision_id     BIGINT IDENTITY(1, 1) NOT NULL CONSTRAINT pk_decision_log PRIMARY KEY,
    pair_id         NVARCHAR(64)   NOT NULL,
    decision        NVARCHAR(10)   NOT NULL CONSTRAINT ck_dl_decision CHECK (decision IN (N'APPROVE', N'REJECT', N'HOLD')),
    reason          NVARCHAR(500)  NULL,
    decided_by      NVARCHAR(256)  NOT NULL,
    decided_by_oid  UNIQUEIDENTIFIER NULL,
    decided_at      DATETIME2(3)   NOT NULL CONSTRAINT df_dl_decided DEFAULT (SYSUTCDATETIME()),
    invocation_id   NVARCHAR(64)   NULL,       -- user data function invocation, for tracing
    channel         NVARCHAR(30)   NOT NULL CONSTRAINT df_dl_channel DEFAULT (N'powerbi')
);
GO

/* Approval audit (who approved what, from which queue state). One row per decision. */
IF OBJECT_ID(N'review.approval', N'U') IS NULL
CREATE TABLE review.approval
(
    approval_id      BIGINT IDENTITY(1, 1) NOT NULL CONSTRAINT pk_approval PRIMARY KEY,
    decision_id      BIGINT        NOT NULL CONSTRAINT fk_approval_decision REFERENCES review.decision_log (decision_id),
    pair_id          NVARCHAR(64)  NOT NULL,
    previous_status  NVARCHAR(20)  NOT NULL,
    new_status       NVARCHAR(20)  NOT NULL,
    approver_upn     NVARCHAR(256) NOT NULL,
    approver_role    NVARCHAR(20)  NOT NULL,
    confidence       DECIMAL(5, 3) NULL,
    band             NVARCHAR(10)  NULL,
    approved_at      DATETIME2(3)  NOT NULL CONSTRAINT df_ap_at DEFAULT (SYSUTCDATETIME())
);
GO
