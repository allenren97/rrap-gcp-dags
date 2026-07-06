use crz_cust_scorecard;
drop table if exists RISK_SAV_ACCT_SNAPSHOT;
CREATE EXTERNAL TABLE RISK_SAV_ACCT_SNAPSHOT(
    ACCT_NUM                       VARCHAR(80),
    INSRT_PROCESS_TMSTMP           TIMESTAMP,
    OP_FIELD                       VARCHAR(1000),
    ACCT_STATUS_CD                 VARCHAR(1),
    NSF_HIST_PERIOD_1_CNT          INT,
    NSF_HIST_PERIOD_2_CNT          INT,
    NSF_HIST_PERIOD_3_CNT          INT,
    NSF_HIST_PERIOD_4_CNT          INT,
    NSF_HIST_PERIOD_5_CNT          INT,
    AGG_CREDIT_YTD_BAL_AMT         DECIMAL(18, 2),
    AGG_DEBIT_YTD_BAL_AMT          DECIMAL(18, 2),
    OS_BAL_COA_AMT                 DECIMAL(18, 2)    COMMENT 'Outstanding Balance Amount ',
    ACCT_BAL_AMT                   DECIMAL(18, 2)    COMMENT 'Account Balance Amount',
    ACCT_TYPE_CD                   VARCHAR(2)        COMMENT 'Account Type Code',
    FRZN_ACCT_IND                  VARCHAR(1)        COMMENT 'Frozen Account Indicator',
    ACCT_OPEN_DT                   DATE              COMMENT 'Account Opened Date',
    LAST_TFR_DT                    DATE              COMMENT 'Last Transfer Date',
    LAST_ACTIVE_DT                 DATE,
    LAST_STMT_PSSBK_DT             DATE,
    OD_LMT_ADD_DEL_DT              DATE              COMMENT 'Overdraft Limit Add Delete Date',
    OD_LMT_AMT                     DECIMAL(18, 2)    COMMENT 'Overdraft Limit Amount',
    STEP_FLAG                      VARCHAR(1),
    PSSBK_BAL_AMT                  DECIMAL(18, 2),
    DLQNT_1_29_DAY_CNT             INT               COMMENT 'Delinquent 1 to 29 Days Count',
    DLQNT_30_59_DAY_CNT            INT               COMMENT 'Delinquent 30 to 59 Days Count',
    DLQNT_60_UP_DAY_CNT            INT               COMMENT 'Delinquent 60 Up Days Count',
    LAST_DEPOSIT_DT                DATE              COMMENT 'Last Deposit Date',
    LAST_DEPOSIT_AMT               DECIMAL(18, 2)    COMMENT 'Last Deposit Amount',
    LAST_STMT_DT                   DATE              COMMENT 'Last Statement Date',
    DLQNT_MTH_CNT                  INT               COMMENT 'Delinquent Month Count',
    ACCT_BAL_LAST_CYCL_AMT         DECIMAL(18, 2)    COMMENT ' Account Balance Last Cycle Amount ',
    AGG_CREDIT_BAL_CRNT_MTH_AMT    DECIMAL(18, 2)    COMMENT 'Aggregated Debit Balance Current Month Amount',
    AGG_CREDIT_DAY_CRNT_MTH_CNT    INT               COMMENT 'Aggregated Credit Days Current Month Count',
    AGG_DEBIT_BAL_CRNT_MTH_AMT     DECIMAL(18, 2),
    AGG_DEBIT_DAY_CRNT_MTH_CNT     INT               COMMENT 'Aggregated Debit Days Current Month Count',
    MIN_DEPOSIT_PST_DUE_AMT        DECIMAL(18, 2)    COMMENT 'Minimum Deposit Past Due Amount ',
    MIN_DEPOSIT_REQUIRED_AMT       DECIMAL(18, 2)    COMMENT 'Minimum Deposit Required Amount ',
    RESIDENCE_CD                   VARCHAR(1),
    INTRNL_PAC_FLAG                VARCHAR(1)        COMMENT 'Internal Pre Authorized Credit Flag',
    INTRNL_PAD_FLAG                VARCHAR(1)        COMMENT 'Internal Pre Authorized Debit Flag',
    CURRENCY_CD                    VARCHAR(3),
    OD_OCCURRED_DT                 DATE              COMMENT 'Overdraft Occurred Date',
    GL_ACCT_NUM                    VARCHAR(7)        COMMENT 'General Ledger Account Number'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (ACCT_NUM) SORTED BY (ACCT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SAV_ACCT_SNAPSHOT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

