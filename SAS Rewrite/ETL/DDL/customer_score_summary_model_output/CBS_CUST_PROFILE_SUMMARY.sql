use crz_cust_scorecard;
drop table if exists CBS_CUST_PROFILE_SUMMARY;
CREATE EXTERNAL TABLE CBS_CUST_PROFILE_SUMMARY(
    CUST_CID                            VARCHAR(40)       COMMENT 'Customer ID',
    INSRT_PROCESS_TMSTMP                TIMESTAMP         COMMENT 'Insert Process Timestamp',
    OP_FIELD                            VARCHAR(1000)     COMMENT 'Operational Field',
    NUM_PROD                            INT               COMMENT 'Number of Products',
    NUM_MOR                             INT               COMMENT 'Number of Mortgages',
    NUM_SPL                             INT               COMMENT 'Number of SPL',
    NUM_REV                             INT               COMMENT 'Number of Revolving Products',
    NUM_SSL                             INT               COMMENT 'Number of SSL (e.g. Student Loan)',
    WORST_DLQNT_DAYS                    INT               COMMENT 'Worst Delinquent Days',
    MARITAL_STATUS                      VARCHAR(20)       COMMENT 'Marital Status',
    CUST_TYPE                           VARCHAR(30)       COMMENT 'Customer Type',
    CUST_STAT                           VARCHAR(20)       COMMENT 'Customer Status',
    DECEASED_IND                        VARCHAR(1)        COMMENT 'Deceased Indicator',
    RETAIL_IND                          VARCHAR(1)        COMMENT 'Retail Indicator',
    AGE                                 INT               COMMENT 'Customer''s Age',
    TIME_ON_BOOKS                       DOUBLE            COMMENT 'Time on Books',
    DEP_ONLY_FLG                        VARCHAR(1)        COMMENT 'Deposit Only Flag',
    NOACCT_EXCL                         VARCHAR(1)        COMMENT 'No Account Exclusion Flag',
    UNDER_AGE_EXCL                      VARCHAR(1)        COMMENT 'Under Age Exclusion Flag',
    CUST_PIT_STAT                       VARCHAR(10)       COMMENT 'Customer PIT Status',
    WORST_DLQNT_DAYS_CUST               INT               COMMENT 'Customer Worst Delinquent Days',
    WORST_DLQNT_DAYS_CUST_KQ            INT               COMMENT 'KQ Customer Worst Delinquent Days',
    WORST_DLQNT_DAYS_CUST_MOR           INT               COMMENT 'Mortgage Customer Worst Delinquent Days',
    WORST_DLQNT_DAYS_CUST_SPL           INT               COMMENT 'SPL Customer Worst Delinquent Days',
    NUM_OF_ACCTS_DLQNT                  INT               COMMENT 'Number of Delinquent Accounts',
    CUST_DEFAULT_DATE                   DATE              COMMENT 'Customer Default Date',
    CUST_DEFAULT_IND                    VARCHAR(10)       COMMENT 'Customer Default Indicator',
    CORP_COMM_EXCL                      INT               COMMENT 'Corporate Commercial Exlusion Flag',
    STAFF_EXCL                          VARCHAR(1)        COMMENT 'Staff Exclusion Flag',
    CC_NUM_HELOC                        INT               COMMENT 'Credit Cards Number of HELOC',
    LOC_NUM_HELOC                       INT               COMMENT 'LOC Number of HELOC',
    HELOC_IND                           VARCHAR(1)        COMMENT 'Heloc Indicator',
    MTH_SINCE_OLDST_TRADE_OPND_CNT      DECIMAL(3, 0)     COMMENT 'Month Since Oldest Trade Opened Count',
    OLDST_OPN_TRADE_AGE_LINE_MTH_CNT    DECIMAL(5, 0)     COMMENT 'Oldest Opened Trade Age Line Month Count',
    TRADE_NEVER_DLQNT_PC                DECIMAL(5, 2)     COMMENT 'Trade Never Delinquent Percentage',
    TOT_BAL_INVST_ACCTAVG12M            DECIMAL(17, 3)    COMMENT '12-month average of Total Investments Balance',
    TOT_SAV_INV_BAL_AMTAVG12M           DECIMAL(17, 3)    COMMENT '12-month average of Total Savings and Investments Balance',
    CC_HELOC_IND                        VARCHAR(1)        COMMENT 'Credit Card Heloc Indicator',
    LOC_HELOC_IND                       VARCHAR(1)        COMMENT 'LOC Heloc Indicator',
    MOR_IND                             VARCHAR(1)        COMMENT 'Mortgage Indicator',
    CARD_IND                            VARCHAR(1)        COMMENT 'Card Indicator',
    LOC_IND                             VARCHAR(1)        COMMENT 'Loc Indicator',
    SEG_NM                              VARCHAR(50)       COMMENT 'Segment Name',
    NUM_CARDS                           INT               COMMENT 'Number of Credit Cards',
    NUM_CUR_CARDS                       INT               COMMENT 'Number of Current Credit Cards',
    NUM_DEF_CARDS                       INT               COMMENT 'Number of Default Cards',
    NUM_LOC                             INT               COMMENT 'Number of LOC',
    NUM_CUR_LOC                         INT               COMMENT 'Number of Current LOC',
    NUM_DEF_LOC                         INT               COMMENT 'Number of Default LOC'
)
PARTITIONED BY( EFF_DT DATE COMMENT 'Effective Date', DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Adhoc etc', SEG_NUM INT COMMENT 'Segment Number')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_CUST_PROFILE_SUMMARY'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

