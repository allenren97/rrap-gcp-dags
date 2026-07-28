-- Output of J_CBS_0010_CUSTUNIV_01 (custuniv_01.py -> cbs.CIS_DATA_POP_02).
--
-- Columns / order / types mirror the PRODUCTION CIS_DATA_POP_02 table (SYSCOLUMNS),
-- which uses the SAS POP_02 names. OBSN_DT / STREAM are prepended as the emulated
-- pipeline's partition + delete/replace keys (not present on-prem). Loaded with
-- INSERT ... BY NAME, so names must match custuniv_01.export_result; DuckDB is
-- case-insensitive on identifiers. VARCHAR lengths from prod are dropped (DuckDB
-- VARCHAR is unbounded). Prod DECIMAL precision is preserved; scale is 3 for money
-- balances and 0 for id/count columns.
--
-- The first 14 columns (CIFKEY..FILE_DATE) are the a.* passthrough from
-- ingestion.CIS_DATA_NEW2 -- that table must expose exactly those columns/names.
CREATE TABLE IF NOT EXISTS cbs.CIS_DATA_POP_02 (
    -- Emulated-pipeline partition keys (delete/replace key in custuniv_01.duckdb_delete)
    OBSN_DT DATE NOT NULL,
    STREAM VARCHAR NOT NULL,

    -- Base CIS population (ingestion.CIS_DATA_NEW2 a.*) -- prod cols 1-14
    CIFKEY VARCHAR,
    PRODUCT VARCHAR,
    ACCOUNT VARCHAR,
    CID VARCHAR,
    PRIMARY_FLAG VARCHAR,
    RELATION_CODE VARCHAR,
    CUST_TYPE VARCHAR,
    EMPLOYEE_IND VARCHAR,
    MORT_NO VARCHAR,
    CAB VARCHAR,
    LOAN_NO VARCHAR,
    FILE_YR_MTH VARCHAR,
    LOAD_DATE_TM TIMESTAMP,
    FILE_DATE DATE,

    -- Gather-derived keys
    MTH_TM_ID DECIMAL(11, 0),
    PROCESS_DATE DATE,
    BASEL_ACCT_ID DECIMAL(20, 0),

    -- Kept per-source columns (SPL / MOR / REV)
    PRD_ID_SPL VARCHAR,
    COMM_FLG_SPL VARCHAR,
    OS_BAL_AMT_SPL DECIMAL(18, 3),
    RECD_STAT_CD_SPL VARCHAR,
    LRA_STATUS_MOR VARCHAR,
    PAID_OFF_DATE_MOR TIMESTAMP,
    CURRENT_BAL_MOR DECIMAL(21, 3),
    TOTAL_SUSPENSE_MOR DECIMAL(19, 3),
    COMM_TP_CD_MOR VARCHAR,
    OS_BAL_AMT_MOR DECIMAL(18, 3),
    FRCLSR_F_MOR VARCHAR,
    PD_OFF_F_MOR VARCHAR,
    FUND_CD_MOR VARCHAR,
    MTH_IN_ARRS_CNT_MOR INTEGER,
    LIFE_INSUR_CD_MOR VARCHAR,
    TRNST_NUM_REV VARCHAR,
    SOURCE_CD VARCHAR,
    BLOCK_RECL_CD VARCHAR,
    ACCT_STAT_CD VARCHAR,
    CR_LMT_AMT DECIMAL(18, 3),
    TOT_NEW_BAL_AMT DECIMAL(18, 3),
    NON_ACCRL_DT DATE,
    WRITE_OFF_DT DATE,
    ACCT_CLS_RSN_CD VARCHAR,
    PRD_CD_REV VARCHAR,

    -- Data-step derivations (SAS CIS_DATA_POP_02)
    BLOCKED DECIMAL(11, 0),
    DECEASED DECIMAL(11, 0),
    STOLEN DECIMAL(11, 0),
    LEND_PRODS DECIMAL(11, 0),
    CID_NUM DOUBLE,
    ACCT_NUMERIC DOUBLE,
    MOR_IND DOUBLE,
    SPL_IND DOUBLE,
    REV_IND DOUBLE,
    SSL_IND DOUBLE,
    LEND_PRODS_CUR DOUBLE,
    LEND_PRODS_CLO DOUBLE,
    LEND_PRODS_BNK DOUBLE,
    LEND_PRODS_DEF DOUBLE,
    LEND_PRODS_CHG DOUBLE,
    LEND_PRODS_WO DOUBLE,
    LEND_PRODS_COMM DOUBLE,
    LEND_PRODS_COMM_CUR DOUBLE,
    LEND_PRODS_COMM_CLO DOUBLE,
    LEND_PRODS_COMM_DEF DOUBLE,
    LEND_PRODS_COMM_CHG DOUBLE,
    LEND_PRODS_COMM_WO DOUBLE,
    CHECK_IND DOUBLE,
    PRIVATE_BANK_IND DOUBLE,
    DEFAULT_IND DOUBLE,
    MODEL_EXCL VARCHAR,
    PROD_TREAT VARCHAR,
    PIT_STAT VARCHAR,
    DAYS_DLQ DOUBLE,
    CORP_COMM_EXCL VARCHAR,
    STAFF_EXCL VARCHAR,
    DEFAULT_DATE DATE,
    DEFAULT_BAL DOUBLE
);

ALTER TABLE cbs.CIS_DATA_POP_02
SET PARTITIONED BY (OBSN_DT, STREAM);
