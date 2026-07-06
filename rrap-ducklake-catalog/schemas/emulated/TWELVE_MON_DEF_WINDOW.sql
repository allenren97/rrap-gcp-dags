-- BNS mortgage 12-month PD default observation windows (nzuser.twelve_mon_def_window).
-- One row per (MORTGAGE_NO, WINDOW_END_DT). WINDOW_END_DT identifies the window
-- (obs-window start + 12 months, month-end). PROCESS_DATE matches the SAS: the
-- process_date of the last observation in the window (highest filled slot).
CREATE TABLE IF NOT EXISTS emulated.TWELVE_MON_DEF_WINDOW (
    OBSN_DT DATE NOT NULL,
    STREAM VARCHAR NOT NULL,
    MTH_TM_ID INTEGER NOT NULL,

    MORTGAGE_NO BIGINT NOT NULL,
    PROCESS_DATE DATE NOT NULL,
    WINDOW_END_DT DATE,
    STATUS1 VARCHAR,
    DEFAULT_DATE DATE,
    DEFAULT_BAL DOUBLE,
    DEFAULT_IND INTEGER,

    INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL,
    UPDT_PROCESS_TMSTMP TIMESTAMP
);

ALTER TABLE emulated.TWELVE_MON_DEF_WINDOW
SET PARTITIONED BY (OBSN_DT, STREAM);
