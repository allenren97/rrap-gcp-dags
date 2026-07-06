USE crz_cust_scorecard;
DROP TABLE IF EXISTS CBS_MODEL_SCORECRD_LKP;
CREATE EXTERNAL TABLE CBS_MODEL_SCORECRD_LKP(
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIElD                VARCHAR(1000),
    SEG_NUM                 SMALLINT         COMMENT 'Segment Number',
    SC_VAR                  VARCHAR(100)     COMMENT 'Scorecard Variable',
    BIN_COND                VARCHAR(1000)    COMMENT 'Binning Condition',
    SCORE                   SMALLINT         COMMENT 'Score corresponding to the scorecard variable and bin condition',
    SEQ_NUM                 SMALLINT         COMMENT 'Sequence Number',
    VER                     INT              COMMENT 'Incremented everytime a new Version of Scorecard Lookup file, provided by Business, is ingested'
)
PARTITIONED BY( EFF_DT DATE)
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_MODEL_SCORECRD_LKP'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

