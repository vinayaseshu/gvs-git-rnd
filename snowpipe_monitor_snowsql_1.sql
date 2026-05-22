-- ============================================================
-- SNOWPIPE MONITORING — SnowSQL (Snowflake Scripting) Version
-- Language: SNOWFLAKE SCRIPTING (SQL procedural)
-- Replaces the JavaScript LANGUAGE JAVASCRIPT procedure
-- ============================================================


-- ============================================================
-- STEP 1: Create Email Notification Integration
-- (Run once by ACCOUNTADMIN)
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS snowpipe_email_integration
    TYPE    = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = (
        'your_email@company.com',
        'oncall_team@company.com'
    );

GRANT USAGE ON INTEGRATION snowpipe_email_integration TO ROLE SYSADMIN;


-- ============================================================
-- STEP 2: Schema & Warehouse
-- ============================================================
USE ROLE SYSADMIN;
USE DATABASE YOUR_DATABASE;   -- <-- Replace

CREATE SCHEMA IF NOT EXISTS MONITORING;
USE SCHEMA MONITORING;

CREATE WAREHOUSE IF NOT EXISTS MONITORING_WH
    WAREHOUSE_SIZE   = XSMALL
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;


-- ============================================================
-- STEP 3: Health Log Table
-- ============================================================
CREATE TABLE IF NOT EXISTS MONITORING.SNOWPIPE_HEALTH_LOG (
    log_id                    NUMBER AUTOINCREMENT PRIMARY KEY,
    check_time                TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    pipe_name                 VARCHAR,
    database_name             VARCHAR,
    schema_name               VARCHAR,
    full_pipe_name            VARCHAR,
    pipe_state                VARCHAR,        -- RUNNING | PAUSED | STOPPED | FAILING
    last_ingested_at          TIMESTAMP_LTZ,
    minutes_since_last_ingest NUMBER(18, 2),
    pending_file_count        NUMBER,
    error_message             VARCHAR,
    alert_sent                BOOLEAN DEFAULT FALSE
);


-- ============================================================
-- STEP 4: CHECK_ALL_SNOWPIPES — Pure Snowflake Scripting
-- ============================================================
CREATE OR REPLACE PROCEDURE MONITORING.CHECK_ALL_SNOWPIPES(
    STALE_THRESHOLD_MINUTES FLOAT,    -- alert if no ingest for this many minutes
    EMAIL_INTEGRATION       VARCHAR,  -- notification integration name
    ALERT_RECIPIENTS        VARCHAR   -- comma-separated email list
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Cursor over every pipe the current role can see
    pipe_cur CURSOR FOR
        SELECT PIPE_NAME,
               PIPE_CATALOG  AS DB_NAME,
               PIPE_SCHEMA   AS SCH_NAME
        FROM   INFORMATION_SCHEMA.PIPES
        ORDER  BY PIPE_CATALOG, PIPE_SCHEMA, PIPE_NAME;

    -- Working variables per pipe
    v_pipe_name       VARCHAR;
    v_db_name         VARCHAR;
    v_sch_name        VARCHAR;
    v_full_pipe       VARCHAR;

    -- Fields parsed from SYSTEM$PIPE_STATUS JSON
    v_pipe_status_raw VARIANT;
    v_pipe_state      VARCHAR  DEFAULT 'UNKNOWN';
    v_last_ingest     TIMESTAMP_LTZ;
    v_stale_mins      NUMBER(18,2);
    v_pending         NUMBER   DEFAULT 0;
    v_error_msg       VARCHAR  DEFAULT NULL;
    v_has_issue       BOOLEAN  DEFAULT FALSE;

    -- Counters & email body builders
    v_checked         INTEGER  DEFAULT 0;
    v_issue_count     INTEGER  DEFAULT 0;
    v_issue_rows      VARCHAR  DEFAULT '';
    v_email_subject   VARCHAR;
    v_email_body      VARCHAR;
    v_result          VARCHAR;

BEGIN

    -- ── Loop over every pipe ──────────────────────────────────────────────────
    FOR pipe_rec IN pipe_cur DO

        v_pipe_name  := pipe_rec.PIPE_NAME;
        v_db_name    := pipe_rec.DB_NAME;
        v_sch_name   := pipe_rec.SCH_NAME;
        v_full_pipe  := v_db_name || '.' || v_sch_name || '.' || v_pipe_name;
        v_checked    := v_checked + 1;

        -- Reset per-pipe state
        v_pipe_state  := 'UNKNOWN';
        v_last_ingest := NULL;
        v_stale_mins  := NULL;
        v_pending     := 0;
        v_error_msg   := NULL;
        v_has_issue   := FALSE;

        -- ── Parse SYSTEM$PIPE_STATUS ──────────────────────────────────────────
        BEGIN
            SELECT PARSE_JSON(SYSTEM$PIPE_STATUS(:v_full_pipe))
            INTO   :v_pipe_status_raw;

            v_pipe_state := COALESCE(v_pipe_status_raw:executionState::VARCHAR,  'UNKNOWN');
            v_pending    := COALESCE(v_pipe_status_raw:pendingFileCount::NUMBER,  0);

            -- Last ingestion timestamp (may be absent for brand-new pipes)
            IF (v_pipe_status_raw:lastIngestedTimestamp IS NOT NULL) THEN
                v_last_ingest := v_pipe_status_raw:lastIngestedTimestamp::TIMESTAMP_LTZ;
                v_stale_mins  := DATEDIFF('second',
                                          v_last_ingest,
                                          CURRENT_TIMESTAMP()) / 60.0;
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_pipe_state := 'ERROR_READING_STATUS';
                v_error_msg  := SQLERRM;
                v_has_issue  := TRUE;
        END;

        -- ── Evaluate alert conditions ─────────────────────────────────────────
        IF (NOT v_has_issue) THEN

            -- Condition A: pipe not running
            IF (v_pipe_state <> 'RUNNING') THEN
                v_has_issue := TRUE;
                v_error_msg := 'Pipe state is ' || v_pipe_state || ' (expected RUNNING).';
            END IF;

            -- Condition B: never ingested
            IF (v_last_ingest IS NULL) THEN
                v_has_issue := TRUE;
                v_error_msg := COALESCE(v_error_msg || ' | ', '')
                               || 'No ingestion timestamp — pipe may never have run.';

            -- Condition C: last ingest is stale
            ELSEIF (v_stale_mins > STALE_THRESHOLD_MINUTES) THEN
                v_has_issue := TRUE;
                v_error_msg := COALESCE(v_error_msg || ' | ', '')
                               || 'Last ingestion was '
                               || TO_VARCHAR(ROUND(v_stale_mins, 1))
                               || ' min ago (threshold: '
                               || TO_VARCHAR(STALE_THRESHOLD_MINUTES)
                               || ' min).';
            END IF;

        END IF;

        -- ── Insert into health log ────────────────────────────────────────────
        INSERT INTO MONITORING.SNOWPIPE_HEALTH_LOG (
            pipe_name, database_name, schema_name, full_pipe_name,
            pipe_state, last_ingested_at, minutes_since_last_ingest,
            pending_file_count, error_message, alert_sent
        ) VALUES (
            :v_pipe_name, :v_db_name, :v_sch_name, :v_full_pipe,
            :v_pipe_state, :v_last_ingest, :v_stale_mins,
            :v_pending, :v_error_msg, :v_has_issue
        );

        -- ── Accumulate issue rows for the email ───────────────────────────────
        IF (v_has_issue) THEN
            v_issue_count := v_issue_count + 1;
            v_issue_rows  := v_issue_rows
                || CHR(10) || '• ' || v_full_pipe
                || CHR(10) || '    State        : ' || v_pipe_state
                || CHR(10) || '    Last Ingest  : ' || COALESCE(TO_VARCHAR(v_last_ingest), 'N/A')
                || CHR(10) || '    Stale (min)  : ' || COALESCE(TO_VARCHAR(ROUND(v_stale_mins, 1)), 'N/A')
                || CHR(10) || '    Pending Files: ' || TO_VARCHAR(v_pending)
                || CHR(10) || '    Detail       : ' || COALESCE(v_error_msg, '')
                || CHR(10);
        END IF;

    END FOR;

    -- ── Send consolidated alert email if there are issues ────────────────────
    IF (v_issue_count > 0) THEN

        v_email_subject :=
            '[ALERT] Snowpipe Issues Detected ('
            || TO_VARCHAR(v_issue_count) || ' pipe(s)) — '
            || TO_VARCHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYY-MM-DD HH24:MI')
            || ' UTC';

        v_email_body :=
            'Snowpipe Monitoring Alert'                                    || CHR(10)
            || '========================='                                 || CHR(10)
            || 'Check Time          : ' || TO_VARCHAR(CURRENT_TIMESTAMP()) || CHR(10)
            || 'Total Pipes Checked : ' || TO_VARCHAR(v_checked)           || CHR(10)
            || 'Pipes With Issues   : ' || TO_VARCHAR(v_issue_count)       || CHR(10)
            || CHR(10)
            || 'AFFECTED PIPES'                                            || CHR(10)
            || '--------------'                                            || CHR(10)
            || v_issue_rows
            || CHR(10)
            || 'Investigate via:'                                          || CHR(10)
            || '  SELECT * FROM MONITORING.SNOWPIPE_HEALTH_LOG'            || CHR(10)
            || '  WHERE alert_sent = TRUE ORDER BY check_time DESC;'       || CHR(10)
            || CHR(10)
            || '— Snowflake Monitoring Task';

        CALL SYSTEM$SEND_EMAIL(
            :EMAIL_INTEGRATION,
            :ALERT_RECIPIENTS,
            :v_email_subject,
            :v_email_body
        );

        v_result := 'ALERT SENT: '
                    || TO_VARCHAR(v_issue_count) || ' pipe(s) with issues out of '
                    || TO_VARCHAR(v_checked)     || ' checked.';
    ELSE
        v_result := 'OK: All ' || TO_VARCHAR(v_checked) || ' Snowpipe(s) are healthy.';
    END IF;

    RETURN v_result;

END;
$$;


-- ============================================================
-- STEP 5: Monitoring Task (unchanged — calls the same proc)
-- ============================================================
CREATE OR REPLACE TASK MONITORING.SNOWPIPE_MONITOR_TASK
    WAREHOUSE = MONITORING_WH
    SCHEDULE  = '15 MINUTE'
    COMMENT   = 'Monitors all Snowpipes; alerts on failure or stale ingestion (SnowSQL proc)'
AS
    CALL MONITORING.CHECK_ALL_SNOWPIPES(
        60,                                           -- stale_threshold_minutes
        'snowpipe_email_integration',                 -- notification integration
        'your_email@company.com,oncall@company.com'   -- recipients
    );

ALTER TASK MONITORING.SNOWPIPE_MONITOR_TASK RESUME;


-- ============================================================
-- STEP 6: Test & Verify
-- ============================================================

-- Manual test run
EXECUTE TASK MONITORING.SNOWPIPE_MONITOR_TASK;

-- Or call the procedure directly for instant feedback
CALL MONITORING.CHECK_ALL_SNOWPIPES(
    60,
    'snowpipe_email_integration',
    'your_email@company.com'
);

-- Task run history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'SNOWPIPE_MONITOR_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- Health log — all recent checks
SELECT *
FROM MONITORING.SNOWPIPE_HEALTH_LOG
ORDER BY check_time DESC
LIMIT 50;

-- Health log — only alerted rows
SELECT *
FROM MONITORING.SNOWPIPE_HEALTH_LOG
WHERE alert_sent = TRUE
ORDER BY check_time DESC;
