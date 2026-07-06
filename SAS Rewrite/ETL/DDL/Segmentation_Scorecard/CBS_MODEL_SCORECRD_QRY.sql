use crz_cust_scorecard;
drop table if exists CBS_MODEL_SCORECRD_QRY;
CREATE EXTERNAL TABLE CBS_MODEL_SCORECRD_QRY(
    VER                     INT               COMMENT 'Model version. This reflects the version of the scorecarding/bin lookup table',
    SEG_NUM                 INT               COMMENT 'Segment Number',
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    SEG_TABLE               VARCHAR(50)       COMMENT 'Segmentation Table where the scorecard variable value is stored',
    SC_VAR                  VARCHAR(100)      COMMENT 'Name of Scorecard  Variable',
    SC_QRY                  VARCHAR(65535)    COMMENT 'Query constructed for each Scorecard Variable to determine the Variable Score'
)
PARTITIONED BY( EFF_DT DATE COMMENT 'Effective Date', DATE_TYPE VARCHAR(20))
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_MODEL_SCORECRD_QRY'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;