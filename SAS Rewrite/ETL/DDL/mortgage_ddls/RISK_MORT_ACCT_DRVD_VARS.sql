Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_MORT_ACCT_DRVD_VARS;
CREATE EXTERNAL TABLE RISK_MORT_ACCT_DRVD_VARS(
    MORT_NUM                   BIGINT,
    INSRT_PROCESS_TMSTMP       TIMESTAMP,
    OP_FIELD                   VARCHAR(1000),
    PRIM_CUST_CID              VARCHAR(40),
    LAST_BUS_DAY               DATE,
    DLQNT_MTH_CNT              INT,
    LAND_REGS_ACT_STAT_FLAG    VARCHAR(1),
    DLQNT_DAY_CNT              INT,
    PIT_STAT_VER_1_CD          VARCHAR(10),
    CONSMR_PROD_TREATMNT_CD    VARCHAR(10),
    COMM_TYPE_CD               VARCHAR(12),
    OS_BAL_AMT                 DECIMAL(18, 3),
    PRPY_AMT                   DECIMAL(18, 2)
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (MORT_NUM) SORTED BY (MORT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_MORT_ACCT_DRVD_VARS'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

