Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_ACTIVE_DLVY_ORIGIN_MAPPNG;
CREATE EXTERNAL TABLE RISK_ACTIVE_DLVY_ORIGIN_MAPPNG(
    INSRT_PROCESS_TMSTMP          TIMESTAMP,
    OP_FIELD                      VARCHAR(1000),
    HOST_ORIGIN_CD                VARCHAR(1),
    TLLR_LOW                      INT,
    TLLR_HIGH                     INT,
    TXN_CD_LOW                    VARCHAR(10),
    TXN_CD_HIGH			  VARCHAR(10),
    CHNL_ACTIVE_ORIGIN            VARCHAR(5)
)
PARTITIONED BY( EFF_DT DATE)
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_ACTIVE_DLVY_ORIGIN_MAPPNG'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;