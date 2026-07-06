use crz_cust_scorecard;
drop table if exists CBS_MODEL_SCORECRD_VAR_SEG7;
CREATE EXTERNAL TABLE CBS_MODEL_SCORECRD_VAR_SEG7(
    CUST_CID                VARCHAR(40),
    OP_FIELD                VARCHAR(1000),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    SEG_NUM                 INT               COMMENT 'CBS Model Segment Number. Defaulted to 7',
    SC_VAR                  VARCHAR(100)      COMMENT 'Scorecard Variable',
    SC_VAR_VAL              DECIMAL(17, 3)    COMMENT 'Value calculated for each scorecard variable'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Adhoc etc')
CLUSTERED BY (CUST_CID, SC_VAR) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_MODEL_SCORECRD_VAR_SEG7'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

