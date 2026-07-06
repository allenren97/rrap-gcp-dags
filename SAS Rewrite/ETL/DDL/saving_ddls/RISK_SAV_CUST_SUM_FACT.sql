use crz_cust_scorecard;
drop table if exists RISK_SAV_CUST_SUM_FACT;
CREATE EXTERNAL TABLE RISK_SAV_CUST_SUM_FACT(
    CUST_CID                VARCHAR(40),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    NUM_OF_SAV_ACCT_PRIM    INT               COMMENT 'Number of Savings Accounts as Primary Customer',
    SUM_SAV_BAL_PRIM_AMT    DECIMAL(18, 2)    COMMENT 'Total Savings Balance on all the Savings Accounts for a customer',
    NUM_OF_SAV_ACCT_SEC     INT               COMMENT 'Number of Savings Accounts as a Secondary Customer',
    SUM_SAV_BAL_SEC_AMT     DECIMAL(18, 2)    COMMENT 'Total Balance in all Savings Accounts as Secondary Customer ',
    NUM_OF_SAV_ACCT         INT               COMMENT 'Total Number of all Savings Accounts for a customer (Primary + Secondary)',
    SUM_SAV_BAL_AMT         DECIMAL(18, 2)    COMMENT 'Total Balance of all Savings Accounts for a customer'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SAV_CUST_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

