Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_TXN_ACTIVE_MAPPNG;
CREATE EXTERNAL TABLE RISK_TXN_ACTIVE_MAPPNG(
    INSRT_PROCESS_TMSTMP          TIMESTAMP,
    OP_FIELD                      VARCHAR(1000),
    DLVY_CD                       VARCHAR(50),
    DLVY_DESC                     VARCHAR(512),
    TXN_TYPE_CD                   VARCHAR(3),
    TXN_GRP_CD                    VARCHAR(1)
)
PARTITIONED BY( EFF_DT DATE)
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_TXN_ACTIVE_MAPPNG'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

