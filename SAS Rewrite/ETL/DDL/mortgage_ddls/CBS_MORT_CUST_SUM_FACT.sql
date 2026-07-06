Use crz_cust_scorecard;
DROP TABLE IF EXISTS CBS_MORT_CUST_SUM_FACT;
CREATE EXTERNAL TABLE CBS_MORT_CUST_SUM_FACT(
    CUST_CID                     VARCHAR(40),
    INSRT_PROCESS_TMSTMP         TIMESTAMP,
    OP_FIELD                     VARCHAR(1000),
    SUM_PRPTY_AMT                DECIMAL(18, 2),
    AVG_AMORT                    DOUBLE,
    SUM_DEF_BAL                  DECIMAL(18, 2),
    NUM_MORT                     INT,
    NUM_MORT_CRNT                INT,
    NUM_MORT_DEF                 INT,
    SUM_CRNT_BAL                 DECIMAL(18, 2),
    WORST_MORT_DLQNT_DAYS        INT,
    NUM_MORT_DLQNT               INT,
    MAX_TIME_ON_BOOKS            INT,
    MIN_TIME_TO_TERM_MAT         INT,
    MIN_TIME_SINCE_RCNT_RNEWL    INT,
    SUM_INTR_DUE_AMT             DECIMAL(18, 2),
    AVG_PRPTY_VAL                DOUBLE,
    SUM_INTR_ACCR_AMT            DECIMAL(18, 5),
    MIN_TIME_SINCE_UNPD          INT
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'', Weekly, Adhoc etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_MORT_CUST_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;