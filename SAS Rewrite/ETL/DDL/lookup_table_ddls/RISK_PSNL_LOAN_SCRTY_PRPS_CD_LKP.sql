Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_PSNL_LOAN_SCRTY_PRPS_CD_LKP;
CREATE EXTERNAL TABLE RISK_PSNL_LOAN_SCRTY_PRPS_CD_LKP(
    PRPS_CD                 SMALLINT,
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    SCRTY_CD                SMALLINT,
    SCRTY_DESC              VARCHAR(30),
    TP                      VARCHAR(10),
    PROD                    VARCHAR(60),
    SUB_PROD                VARCHAR(60),
    STEP_FLAG               VARCHAR(1),
    BASEL_PROD              VARCHAR(60),
    BASEL_SUB_PROD          VARCHAR(60),
    BASEL_PROD_ABBR         VARCHAR(60),
    PROD_ID                 VARCHAR(10)
)
PARTITIONED BY( EFF_DT DATE)
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_PSNL_LOAN_SCRTY_PRPS_CD_LKP'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

