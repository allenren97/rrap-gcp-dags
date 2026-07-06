Use crz_cust_scorecard;
DROP TABLE IF EXISTS RISK_SPL_ACCT_SNAPSHOT;
CREATE EXTERNAL TABLE RISK_SPL_ACCT_SNAPSHOT(
    ACCT_NUM                     VARCHAR(30),
    INSRT_PROCESS_TMSTMP         TIMESTAMP,
    OP_FIELD                     VARCHAR(1000),
    STEP_PLN_AGRMNT_NUM          INT,
    TRNST_NUM                    INT,
    LOAN_NUM                     VARCHAR(30),
    RT_CD                        VARCHAR(10),
    RECD_STAT_CD                 VARCHAR(10),
    RECD_STAT_DT                 DATE,
    CUST_RSDNC_CD                VARCHAR(10),
    TYPE_SRC_CD                  VARCHAR(10),
    LOAN_PRPS_CD                 VARCHAR(10),
    SCRTY_CD                     VARCHAR(10),
    PROMISSORS_CNT               SMALLINT,
    GUARNTY_CNT                  SMALLINT,
    COMM_LOAN_CD                 VARCHAR(10),
    NOTE_DT                      DATE,
    FRST_RGL_PYMNT_DT            DATE,
    LAST_RGL_PYMT_DT             DATE,
    ORIG_LOAN_AMT                DECIMAL(18, 2),
    ADD_ON_BAL_AMT               DECIMAL(18, 2),
    ADD_ON_INTR_AMT              DECIMAL(18, 3),
    DAYS_OVERDUE                 INT,
    ACCR_INTR_AMT                DECIMAL(18, 3),
    EARLY_MAR_DT                 DATE,
    LAST_PYMT_DT                 DATE,
    PRINCIPAL_BAL_AMT            DECIMAL(18, 2),
    MOTOR_VEHCL_VAL              DECIMAL(18, 2),
    SCRTY_HOUSHLD_CR_SCORE       INT,
    SCRTY_OTH_VAL                DECIMAL(18, 2),
    PLS_CR_SCORE_OVRD_CD         INT,
    BR_LOCTN_TRNST               VARCHAR(5),
    EARNED_MTH_INTR_AMT          DECIMAL(18, 3),
    ORIG_NOT_DT                  DATE,
    CHRG_OFF_DT                  DATE,
    CHRG_OFF_AMT                 DECIMAL(18, 2),
    SECURITIZATION_CD            VARCHAR(10),
    LOAN_TERM                    INT,
    EARLY_MAT_TERM               INT,
    EARLY_MAT_STAT_CD            VARCHAR(10),
    RGL_PYMT_AMT                 DECIMAL(18, 2),
    PRE_AUTH_DR_PYMNT_FREQ_CD    VARCHAR(10),
    INTR_RT                      DOUBLE,
    PRIM_CUST_CID                VARCHAR(40),
    CIF_COMPANY_ID               INT,
    CIF_CUST_ID                  VARCHAR(10),
    GL_TRNST_NUM                 VARCHAR(5),
    CRNCY_CD                     VARCHAR(3),
    CIF_CUST_ID_TIE_BRKR         INT,
    BOOKED_AMT                   DECIMAL(18, 2),
    GL_ACCT_NUM                  VARCHAR(10),
    SUBVENTED_IND                VARCHAR(1)
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (ACCT_NUM) SORTED BY (ACCT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SPL_ACCT_SNAPSHOT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

