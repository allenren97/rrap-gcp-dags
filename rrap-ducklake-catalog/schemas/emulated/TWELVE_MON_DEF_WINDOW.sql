-- BNS mortgage 12-month PD default observation windows (nzuser.twelve_mon_def_window).
-- One row per (MORTGAGE_NO, OBSVTN_MTH_TM_ID). OBSVTN_MTH_TM_ID identifies the obs-window
-- start month; PROCESS_DATE is that obs-window start month-end (SAS mth_end_dt&mm).
-- Assembled from the MOR window features (DEFAULT_DATE / DEFAULT_BAL / DEFAULT_IND).
CREATE TABLE IF NOT EXISTS emulated.TWELVE_MON_DEF_WINDOW (
    OBSN_DT DATE NOT NULL,
    STREAM VARCHAR NOT NULL,
    MTH_TM_ID INTEGER NOT NULL,

    MORTGAGE_NO BIGINT NOT NULL,
    OBSVTN_MTH_TM_ID INTEGER NOT NULL,
    PROCESS_DATE DATE NOT NULL,
    DEFAULT_DATE DATE,
    DEFAULT_BAL DOUBLE,
    DEFAULT_IND INTEGER,

    INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL,
    UPDT_PROCESS_TMSTMP TIMESTAMP
);

ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW
SET PARTITIONED BY (OBSN_DT, STREAM);

-- Reconcile already-deployed tables to the lean feature-join schema.
-- Idempotent: fresh tables no-op, pre-refactor tables drop the retired columns
-- and add the new obs-window key.
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW ADD COLUMN IF NOT EXISTS OBSVTN_MTH_TM_ID INTEGER;
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW DROP COLUMN IF EXISTS WINDOW_END_DT;
ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW DROP COLUMN IF EXISTS STATUS1;
