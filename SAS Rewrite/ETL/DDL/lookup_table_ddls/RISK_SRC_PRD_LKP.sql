use crz_cust_scorecard;
drop table if exists RISK_SRC_PRD_LKP;
CREATE EXTERNAL TABLE RISK_SRC_PRD_LKP(
    INSRT_PROCESS_TMSTMP       TIMESTAMP,
    OP_FIELD                   VARCHAR(1000),
    PRD_SYS_CD                 VARCHAR(10),
    SRC_PRD_CD                 VARCHAR(10),
    SRC_SUB_PRD_CD             VARCHAR(10),
    BASEL_PRD_CD               VARCHAR(10),
    BASEL_PRD_DESC             VARCHAR(100),
    PRD_DESC                   VARCHAR(100),
    LTV_TP_CD                  VARCHAR(10),
    SML_BUS_F                  VARCHAR(1),
    CONSM_SCORECRD_EXCLSN_F    VARCHAR(1),
    CONSM_PRD_TREATMNT_CD      VARCHAR(10)
)
PARTITIONED BY( EFF_DT DATE)
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SRC_PRD_LKP'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

