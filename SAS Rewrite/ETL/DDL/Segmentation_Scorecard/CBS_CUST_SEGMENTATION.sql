use crz_cust_scorecard;
drop table if exists CBS_CUST_SEGMENTATION;
CREATE EXTERNAL TABLE CBS_CUST_SEGMENTATION(
    CUST_CID                          VARCHAR(40)      COMMENT 'Customer CID',
    INSRT_PROCESS_TMSTMP              TIMESTAMP        COMMENT 'Row Insert timestamp',
    OP_FIELD                          VARCHAR(1000)    COMMENT 'Operational Field',
    CUST_TYPE                         VARCHAR(30)      COMMENT 'Customer Type - Sourced from Customer Universe',
    CUST_STATUS                       VARCHAR(20)      COMMENT 'Customer Status - From Customer Universe',
    NOACCT_EXCL                       VARCHAR(1)       COMMENT 'No Account Exclusion flag - Y/N',
    DEP_ONLY_FLAG                     VARCHAR(1)       COMMENT 'Deposit Only Flag - 1/0 indicator',
    WORST_DLQ_DAYS                    INT              COMMENT 'Max Number of Days of Delinquency',
    DELQ_CAT                          VARCHAR(35)      COMMENT 'Category based on the # of worst delinquency days.   Valid values: Non-Delinquent, Delinquent - Cycle I, Delinquent - Cycle II, Delinquent - Cycle III, Default',
    TIME_ON_BOOKS                     DOUBLE           COMMENT 'Time on Books',
    NEW_CUST_IND                      VARCHAR(1)       COMMENT 'New Customer Indicator',
    AVG_TOT_SAV_INV_BAL_12M           DOUBLE           COMMENT '$ value from BB and IP, averaged over 12 months',
    HIGH_VAL_CUST_IND                 VARCHAR(1)       COMMENT 'High Value Customer Indicator',
    LOC_HELOC_IND                     VARCHAR(1)       COMMENT 'Customer has LOC/HELOC Acct Y/N Indicator',
    CC_HELOC_IND                      VARCHAR(1)       COMMENT 'Customer has Credit Card HELOC Acct - Y/N Indicator',
    HELOC_IND                         VARCHAR(1)       COMMENT 'HELOC Indicator',
    MORT_IND                          VARCHAR(1)       COMMENT 'Mortgage Indicator - Y/N Value',
    SPL_IND                           VARCHAR(1)       COMMENT 'Scotia Plan Loan Indicator - Y/N Value',
    MTH_SINCE_OLDST_TRADE_OPND_CNT    INT              COMMENT 'Month Since Oldest Trade Opened Count',
    TRADE_NEVER_DLQNT_PC              DECIMAL(5, 2)    COMMENT 'Trade Never Delinquent Percentage - % value from monthly Trans Union',
    SEG_NM                            VARCHAR(50)      COMMENT 'Segment Name',
    SEG_DESC                          VARCHAR(1000)    COMMENT 'Segment Description'
)
PARTITIONED BY( EFF_DT DATE COMMENT 'Effective Date', DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Adhoc etc', SEG_NUM INT COMMENT 'Segment Number - Valid List of Values:  1,2, 3, 4, 5, 6, 7, 8, 9, 10,11')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_CUST_SEGMENTATION'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;