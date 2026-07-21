-- BNS mortgage 12-month PD default observation windows (nzuser.twelve_mon_def_window).
-- One row per (MORTGAGE_NO, PROCESS_DATE); PROCESS_DATE is the obs-window start
-- month-end (SAS mth_end_dt&mm). Assembled from the MOR window features
-- (DEFAULT_DATE / DEFAULT_BAL / DEFAULT_IND). Only the columns custuniv_01 reads.
CREATE TABLE IF NOT EXISTS emulated.TWELVE_MON_DEF_WINDOW (
    OBSN_DT DATE NOT NULL,
    STREAM VARCHAR NOT NULL,

    MORTGAGE_NO BIGINT NOT NULL,
    PROCESS_DATE DATE NOT NULL,
    DEFAULT_DATE DATE,
    DEFAULT_BAL DOUBLE,
    DEFAULT_IND INTEGER,

    INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL,
    UPDT_PROCESS_TMSTMP TIMESTAMP
);

ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW
SET PARTITIONED BY (OBSN_DT, STREAM);

-- Reconcile already-deployed tables to the trimmed column set (only what
-- custuniv_01 reads). Idempotent: fresh tables no-op, pre-trim tables drop the
-- retired columns.
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW DROP COLUMN IF EXISTS WINDOW_END_DT;
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW DROP COLUMN IF EXISTS STATUS1;
-- MTH_TM_ID / OBSVTN_MTH_TM_ID retired: custuniv joins on MORTGAGE_NO + PROCESS_DATE.
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW DROP COLUMN IF EXISTS MTH_TM_ID;
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW DROP COLUMN IF EXISTS OBSVTN_MTH_TM_ID;
