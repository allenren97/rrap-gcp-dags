use crz_cust_scorecard;
drop table if exists RISK_CR_BUREAU_DELI_MTH_SNAPSHOT;
CREATE EXTERNAL TABLE RISK_CR_BUREAU_DELI_MTH_SNAPSHOT(
    INSRT_PROCESS_TMSTMP                    TIMESTAMP,
    OP_FIELD                                VARCHAR(1000),
    CUST_CID                                VARCHAR(40),
    MTH_SINCE_OLDST_TRADE_OPND_CNT          DECIMAL(3, 0),
    MTH_SINCE_LAST_30_DAY_DLQNT_CNT         DECIMAL(3, 0),
    MTH_SINCE_LAST_60_DAY_DLQNT_CNT         DECIMAL(3, 0),
    COLCTN_CNT                              DECIMAL(2, 0),
    TOT_BAL_TP_BANKCARD_AMT                 DECIMAL(7, 0),
    TRADE_NEVER_DLQNT_PC                    DECIMAL(5, 2),
    TOT_PD_AMT                              DECIMAL(7, 0),
    TOT_UTLTN_AMT                           DECIMAL(5, 0),
    HIGHST_ACTV_UTLTN                       DECIMAL(5, 0),
    TOT_AVL_CR_NOT_UTILIZED_AMT             DECIMAL(7, 0),
    TOT_UTLTN_BNK_REVLVNG_CRD_AMT           DECIMAL(5, 0),
    MTH_SINCE_MOST_RECNT_DLQNT_CNT          DECIMAL(3, 0),
    MAX_REVLVNG_CR_CRNT_UTLTN_AMT           DECIMAL(5, 0),
    INQRY_CNT                               DECIMAL(2, 0),
    INQRY_PAST_6_MTH_CNT                    DECIMAL(2, 0),
    OCC_60_DAY_PD_WITHIN_PAST_12_MTH_CNT    DECIMAL(3, 0),
    TM_30_DAY_PD_LAST_12_MTH_CNT            DECIMAL(3, 0),
    TRADE_90_DPD_LAST_24_MTH_CNT            DECIMAL(3, 0),
    OLDST_OPN_TRADE_AGE_LINE_MTH_CNT        DECIMAL(5, 0)
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20))
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_CR_BUREAU_DELI_MTH_SNAPSHOT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

