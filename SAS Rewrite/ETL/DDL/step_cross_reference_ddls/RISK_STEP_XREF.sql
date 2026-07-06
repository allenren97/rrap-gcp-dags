Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_STEP_XREF;
CREATE EXTERNAL TABLE RISK_STEP_XREF(
    ACCOUNT_NO              VARCHAR(30),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    BOR_SRC_SYS_CD          VARCHAR(30)      COMMENT 'BOR Source System Code',
    NON_BOR_PRD_CD          VARCHAR(30)      COMMENT 'Non-BOR Product Code',
    BOR_PRD_SRC_SYS_CD      VARCHAR(30)      COMMENT 'BOR Product Source System Code',
    BOR_EFF_FROM_DT         DATE             COMMENT 'BOR Effective Date',
    BOR_EFF_TO_DT           DATE             COMMENT 'BOR Effective To Date',
    CRNT_F                  VARCHAR(1)       COMMENT 'Current Flag',
    CAB_TRANSIT             VARCHAR(30)      COMMENT 'CAB Transit Number',
    PRODUCT_CDE             VARCHAR(30)      COMMENT 'Product Code',
    AGREEMENT_NO            DOUBLE           COMMENT 'Agreement Number',
    SOURCE_SYS_CD           VARCHAR(30)      COMMENT 'Source System Code'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''''Daily'''',''''Monthly'''' etc')
CLUSTERED BY (ACCOUNT_NO) SORTED BY (ACCOUNT_NO ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_STEP_XREF'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

