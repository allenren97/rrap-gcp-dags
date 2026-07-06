Use CRZ_CUST_SCORECARD;
DROP TABLE IF EXISTS CBS_JOB_INFO;
CREATE TABLE CBS_JOB_INFO(
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    JOB_ID                  INT,
    JOB_NM                  VARCHAR(100),
    TARGET_SCHEMA_NM        VARCHAR(100),
    TARGET_TBL_NM           VARCHAR(100),
    TARGET_COL_NM           VARCHAR(100),
    TARGET_SQL_CRTRIA       VARCHAR(1000),
    SRC_SCHEMA_NM           VARCHAR(100),
    SRC_TBL_NM              VARCHAR(100),
    SRC_COL_NM              VARCHAR(100),
    SRC_SQL_CRTRIA          VARCHAR(1000),
    DATA_LOAD_TYPE          VARCHAR(50),
    CLNDR_TYPE              VARCHAR(10),
    ACTIVE_F                VARCHAR(1)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_JOB_INFO'
;

