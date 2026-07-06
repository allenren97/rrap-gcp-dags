USE crz_cust_scorecard;
DROP TABLE IF EXISTS CBS_SPL_CUST_SUM_FACT;
CREATE EXTERNAL TABLE CBS_SPL_CUST_SUM_FACT(
    CUST_CID                        VARCHAR(40),
    INSRT_PROCESS_TMSTMP            TIMESTAMP,
    OP_FIELD                        VARCHAR(1000),
    NUM_OF_SPL_ACCT                 SMALLINT          COMMENT 'Total number of SPL accounts for a customer',
    NUM_OF_CRNT                     SMALLINT          COMMENT 'Number of SPL products for the customer that are current',
    NUM_OF_DEF                      SMALLINT          COMMENT 'Numeber of SPL products for a customer that have defaulted',
    NUM_OF_CHG                      SMALLINT          COMMENT 'Number of SPL products for a customer that are charged off',
    SUM_CRNT_BAL                    DECIMAL(18, 2)    COMMENT 'Current total balance on all SPL products for a customer',
    SUM_DEF_BAL                     DECIMAL(18, 2)    COMMENT 'Total balance of all the SPL products for a customer that have defaulted',
    SUM_CHG_BAL                     DECIMAL(18, 2)    COMMENT 'Total balance on SPL products that are charged off for a customer',
    SPL_WORST_DLQNT_DAYS            INT               COMMENT 'Maximu mumber of days a customer has been delinquent',
    NUM_OF_DLQNT_ACCT               SMALLINT          COMMENT 'Total number of SPL accounts for a customer that are delinquent',
    GRNTY_CNT                       SMALLINT,
    MAX_TIME_ON_BOOKS               INT               COMMENT 'Maximum time an SPL account has been associated to a customer',
    MAX_TIME_SINCE_LST_ORIG_PYMT    INT               COMMENT 'Time since last original paymant on SPL',
    SUM_ORIG_LOAN_AMT               DECIMAL(18, 2)    COMMENT 'Original Loan Amount on an SPL account',
    SUM_ACCR_INTR                   DECIMAL(18, 3),
    MIN_CLOSEST_MAT                 INT               COMMENT 'Term for the SPL with the closest maturity date',
    MAX_TIME_SNC_LAST_PYMT          INT               COMMENT 'Time since last payment made on a SPL loan',
    AVG_MOTR_VHCL_VAL               DOUBLE            COMMENT 'Average of all the Motor Vehicle loans'' value for a customer',
    AVG_LOAN_VAL_OTH                DOUBLE,
    SUM_EARNED_MTHLY_INTR           DECIMAL(18, 3),
    SUM_CHRG_OFF_AMT                DECIMAL(18, 2)    COMMENT 'Total charged off amount on SPL loan for a customer',
    AVG_LOAN_TERM                   DOUBLE            COMMENT 'Average of all SPL loan terms for a customer',
    MIN_LOAN_TERM                   INT               COMMENT 'Minimum Loan Term from all the SPL loans for a customer',
    MAX_LOAN_TERM                   INT               COMMENT 'Maximum term from all the SPL loans for a customer',
    AVG_EARLY_MAT_TERM              DOUBLE            COMMENT 'Average Early Maturity term',
    MIN_EARLY_MAT_TERM              INT               COMMENT 'Minimum Early Maturity Term from all SPL loans',
    MAX_EARLY_MAT_TERM              INT               COMMENT 'Maximum early maturity term from all SPL loans for a customer',
    AVG_REG_PYMT_AMT                DOUBLE            COMMENT 'Average of regular payment amout from all SPL products for a customer',
    AVG_INTR_RT                     DOUBLE            COMMENT 'Average interest value of all the interest charged on all SPL (Current) products for a customer ',
    SUM_BOOKED_AMT                  DECIMAL(18, 2),
    SUBVENTED_IND                   VARCHAR(1),
    DIRECT_IND                      VARCHAR(1),
    NUM_OF_NON_AUTO_LOANS           INT               COMMENT 'Count of all the SPL loans that are not auto loans',
    NUM_OF_AUTO_LOANS               INT               COMMENT 'Count of all the Auto loans for a customer'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' , ''Weekly'', ''Adhoc'' etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_SPL_CUST_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

