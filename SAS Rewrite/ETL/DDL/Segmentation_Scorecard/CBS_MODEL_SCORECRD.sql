use crz_cust_scorecard;
drop table if exists CBS_MODEL_SCORECARD;
drop table if exists CBS_MODEL_SCORECRD;
CREATE EXTERNAL TABLE CBS_MODEL_SCORECRD(
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    VER                     INT              COMMENT 'Model version. This reflects the version of the scorecarding/bin lookup table',
    CUST_CID                VARCHAR(40)      COMMENT 'Customer CID',
    SEG_NUM                 INT              COMMENT 'CBS Model Segment Number',
    SC_VAR                  VARCHAR(100)     COMMENT 'Scorecard Variable',
    SCORE                   INT              COMMENT 'Score calculated based on the model logic provided'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Adhoc etc')
CLUSTERED BY (CUST_CID, SC_VAR) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_MODEL_SCORECRD'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

