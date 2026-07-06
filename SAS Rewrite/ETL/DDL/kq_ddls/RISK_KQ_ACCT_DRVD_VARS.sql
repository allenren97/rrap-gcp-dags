Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_KQ_ACCT_DRVD_VARS;
CREATE EXTERNAL TABLE RISK_KQ_ACCT_DRVD_VARS(
    ACCT_NUM                         VARCHAR(30),
    INSRT_PROCESS_TMSTMP             TIMESTAMP,
    OP_FIELD                         VARCHAR(1000),
    PRIM_CUST_CID                    VARCHAR(40),
    PREV_CHRG_OFF_CD                 VARCHAR(1),
    PREV_BLOCK_RECL_CD               VARCHAR(2),
    PREV_TOT_NEW_BAL_AMT             DECIMAL(18, 2)    COMMENT 'Previous Total New Balance Amount',
    PREV_TOT_UNPAID_FNCL_CHRG_AMT    DECIMAL(18, 2),
    RS_FLAG                          VARCHAR(1),
    STEP_CD                          VARCHAR(1),
    HELOC_FLAG                       VARCHAR(1)        COMMENT 'Home Equity Line Of Credit Flag',
    PIT_STAT_VER_2_CD                VARCHAR(10)       COMMENT 'Point In Time Status Version 2 Code',
    CONSMR_PROD_TREATMNT_CD          VARCHAR(10)       COMMENT 'Consumer Product Treatment Code',
    CONSMR_SCORECRD_EXCLSN_FLAG      VARCHAR(1)        COMMENT 'Consumer Scorecard Exclusion Flag',
    PRCHS_INTR_CHRGD_AMT             DECIMAL(18, 2)    COMMENT 'Interest Charged on Purchase Amount',
    PRCHS_CNT                        SMALLINT          COMMENT 'Count of Purchases made during monthly cycle
',
    CSH_ADVNC_INTR_CHRGD_AMT         DECIMAL(18, 2)    COMMENT 'Interest Chaged Amount on Cash Advances',
    TOTAL_INT_CHRGD_AMT              DECIMAL(18, 2)    COMMENT 'Total Interest Charged Amount'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (ACCT_NUM) SORTED BY (ACCT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_KQ_ACCT_DRVD_VARS'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

