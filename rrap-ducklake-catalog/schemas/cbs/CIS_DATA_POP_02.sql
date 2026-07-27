-- Output of J_CBS_0010_CUSTUNIV_01 (custuniv_01.py -> cbs.CIS_DATA_POP_02).
--
-- Column set = the SAS CIS_DATA_POP_02 data step (POP_01 gather MINUS its DROP= list,
-- PLUS the data-step derivations), stamped with OBSN_DT / STREAM for the emulated
-- (partitioned) pipeline. Types follow what custuniv_01.export_result actually emits.
-- Loaded with INSERT ... BY NAME, so names must match the SELECT; DuckDB is
-- case-insensitive on identifiers.
--
-- CAVEAT: the "base CIS population" block below is the a.* passthrough from
-- ingestion.CIS_DATA_NEW2, which is NOT yet in the catalog. The names/types here are
-- reconstructed from the on-prem DDL (tsz_cbs_cis_data_pop.hql) and the SAS usage; they
-- MUST be reconciled once CIS_DATA_NEW2 is cataloged (BY NAME will fail if that table
-- carries a column absent here). file_date is DATE because custuniv joins it to
-- TM_DIM.TM_LVL_ST_DT (a DATE) without a cast.
CREATE TABLE IF NOT EXISTS cbs.CIS_DATA_POP_02 (
    -- Emulated-pipeline partition keys (delete/replace key in custuniv_01.duckdb_delete)
    OBSN_DT DATE NOT NULL,
    STREAM VARCHAR NOT NULL,

    -- Base CIS population (ingestion.CIS_DATA_NEW2 a.*) -- reconcile with that source
    OP_FILE_NAME VARCHAR,
    CIF_KEY VARCHAR,
    DATE_TYPE VARCHAR,
    PRODUCT VARCHAR,                 -- a.product (on-prem: product_type)
    ACCOUNT VARCHAR,                 -- a.account (on-prem: account_num)
    CID VARCHAR,                     -- a.cid     (on-prem: cust_cid)
    PRIMARY_CUST_FLAG VARCHAR,
    RELATION_CODE VARCHAR,
    EMPLOYEE_IND VARCHAR,
    MORT_NUM VARCHAR,
    CAB VARCHAR,
    LOAN_NUM VARCHAR,
    FILE_YR_MTH VARCHAR,
    LOAD_DATE_TM VARCHAR,
    FILE_DATE DATE,

    -- Gather-derived keys
    MTH_TM_ID INTEGER,               -- a1.TM_ID (reporting month M)
    PROCESS_DATE DATE,               -- a1.TM_LVL_END_DT
    BASEL_ACCT_ID BIGINT,            -- b.BASEL_ACCT_ID (LEFT JOIN -> nullable)

    -- Kept per-source columns (SPL / MOR / REV) that survive the DROP= list
    PRD_ID_SPL VARCHAR,
    COMM_FLG_SPL VARCHAR,
    OS_BAL_AMT_SPL DECIMAL(17, 3),
    RECD_STAT_CD_SPL INTEGER,
    LRA_STATUS_MOR VARCHAR,
    PAID_OFF_DATE_MOR DATE,
    CURRENT_BAL_MOR DECIMAL(17, 3),
    TOTAL_SUSPENSE_MOR DECIMAL(17, 3),
    COMM_TP_CD_MOR VARCHAR,
    OS_BAL_AMT_MOR DECIMAL(17, 3),
    FRCLSR_F_MOR VARCHAR,
    PD_OFF_F_MOR VARCHAR,
    FUND_CD_MOR VARCHAR,
    MTH_IN_ARRS_CNT_MOR INTEGER,
    LIFE_INSUR_CD_MOR VARCHAR,
    TRNST_NUM_REV VARCHAR,
    SOURCE_CD VARCHAR,
    BLOCK_RECL_CD VARCHAR,
    ACCT_STAT_CD VARCHAR,
    CR_LMT_AMT DECIMAL(17, 3),
    TOT_NEW_BAL_AMT DECIMAL(17, 3),
    NON_ACCRL_DT DATE,
    WRITE_OFF_DT DATE,
    ACCT_CLS_RSN_CD VARCHAR,
    PRD_CD_REV VARCHAR,

    -- Data-step derivations (SAS CIS_DATA_POP_02)
    BLOCKED INTEGER,                 -- on-prem: blocked_ind
    DECEASED INTEGER,                -- on-prem: deceased_ind
    STOLEN INTEGER,                  -- on-prem: stolen_ind
    LEND_PRODS INTEGER,              -- on-prem: num_of_lend_prods
    CID_NUM BIGINT,                  -- input(cid,15.)
    ACCT_NUMERIC BIGINT,             -- input(account,13.)
    MOR_IND INTEGER,
    SPL_IND INTEGER,
    REV_IND INTEGER,
    SSL_IND INTEGER,
    LEND_PRODS_CUR INTEGER,
    LEND_PRODS_CLO INTEGER,          -- on-prem: lend_prods_clsd
    LEND_PRODS_BNK INTEGER,          -- on-prem: lend_prods_bnkrpt
    LEND_PRODS_DEF INTEGER,
    LEND_PRODS_CHG INTEGER,          -- on-prem: lend_prods_chrg_off
    LEND_PRODS_WO INTEGER,           -- on-prem: lend_prods_wrt_off
    LEND_PRODS_COMM_CUR INTEGER,
    LEND_PRODS_COMM_CLO INTEGER,
    LEND_PRODS_COMM_DEF INTEGER,
    LEND_PRODS_COMM_CHG INTEGER,
    LEND_PRODS_COMM_WO INTEGER,
    CHECK_IND INTEGER,
    PRIVATE_BANK_IND INTEGER,
    CORP_COMM_EXCL VARCHAR,
    STAFF_EXCL VARCHAR,
    DEFAULT_DATE DATE,
    DEFAULT_BAL DECIMAL(17, 3),
    DEFAULT_IND INTEGER,
    MODEL_EXCL VARCHAR,
    PROD_TREAT VARCHAR,
    PIT_STAT VARCHAR,
    DAYS_DLQ INTEGER                 -- on-prem: days_dlqnt
    -- NOTE: SAS keeps lend_prods_comm (always 0); custuniv_01.py does not emit it yet.
    -- Add `0 AS lend_prods_COMM` to export_result and a column here for full parity.
);

ALTER TABLE cbs.CIS_DATA_POP_02
SET PARTITIONED BY (OBSN_DT, STREAM);
