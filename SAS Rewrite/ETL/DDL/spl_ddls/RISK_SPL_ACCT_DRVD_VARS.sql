Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_SPL_ACCT_DRVD_VARS;
CREATE EXTERNAL TABLE RISK_SPL_ACCT_DRVD_VARS(
    ACCT_NUM                  VARCHAR(30),
    INRT_PROCESS_TIMESTAMP    TIMESTAMP,
    OP_FIELD                  VARCHAR(1000),
    PRIM_CUST_CID             VARCHAR(40),
    PROD_ID                   VARCHAR(10),
    PIT_STATUS                VARCHAR(10)      COMMENT 'Point In Time Status',
    PROD_CD                   VARCHAR(100),
    PROD_TYPE_CD              VARCHAR(100),
    SUB_PROD_CD               VARCHAR(100),
    SUB_PORTFL_CD             VARCHAR(100)
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (ACCT_NUM) SORTED BY (ACCT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SPL_ACCT_DRVD_VARS'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

