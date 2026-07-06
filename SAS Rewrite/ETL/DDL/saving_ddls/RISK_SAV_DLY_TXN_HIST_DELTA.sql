use crz_cust_scorecard;
drop table if exists RISK_SAV_DLY_TXN_HIST_DELTA;
CREATE EXTERNAL TABLE RISK_SAV_DLY_TXN_HIST_DELTA(
    ACCT_NUM                VARCHAR(80),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    WS_BANK                 VARCHAR(3),
    WS_TELLER_BRANCH        INT,
    WS_TELLER_NUM           INT,
    WS_SENDER_ID            VARCHAR(2),
    WS_ORIGINATOR           VARCHAR(2),
    WS_FOR                  VARCHAR(2),
    SAV_TXN_AMT             DECIMAL(18, 2),
    WS_POSTED_DATE          DATE,
    WS_ENTERED_DATE         DATE,
    WS_ENTERED_TIME         TIMESTAMP,
    WS_TXN_CD               INT               COMMENT 'Transaction Code',
    WS_MNEMONIC_TXT         VARCHAR(3),
    WS_SIGN                 VARCHAR(1),
    WS_COLT_MNEMONIC_CD     INT,
    WS_RETRACTED_IND        VARCHAR(1),
    WS_NARRATIVE_CODE       INT,
    WS_APPL_AREA_FLAG       VARCHAR(1),
    WS_CHQ_NUMBER           INT,
    WS_ALT_ACCT_NO          INT,
    WS_CAU_IND              VARCHAR(1),
    WS_PINNED_IND           VARCHAR(1),
    WS_REPORTED_IND         VARCHAR(1),
    WS_UTID_SOURCE          VARCHAR(2),
    WS_UTID_STCK            VARCHAR(8),
    WS_UTID_SEQ_NO          VARCHAR(8),
    WS_APPL_AREA            VARCHAR(200),
    EXPIRYDATE              DATE
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'', ''Monthly'', ''Weekly'', ''Adhoc'' etc')
CLUSTERED BY (ACCT_NUM) SORTED BY (ACCT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SAV_DLY_TXN_HIST_DELTA'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

