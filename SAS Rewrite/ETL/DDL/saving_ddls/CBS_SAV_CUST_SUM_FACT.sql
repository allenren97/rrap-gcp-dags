use crz_cust_scorecard;
drop table if exists CBS_SAV_CUST_SUM_FACT;
CREATE EXTERNAL TABLE CBS_SAV_CUST_SUM_FACT(
    CUST_CID                VARCHAR(40),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    NUM_SAV_ACCT_PRIM       INT               COMMENT 'Total Number of Savings Accounts as Primary Customer',
    SUM_SAV_BAL_PRIM_AMT    DECIMAL(18, 2)    COMMENT 'Total Balance on Savings/Chk Accounts as a Primary Customer',
    NUM_SAV_ACCT_SEC        INT               COMMENT 'Total number of Savings/Chq accts as Secondary Customer',
    SUM_SAV_BAL_SEC_AMT     DECIMAL(18, 2)    COMMENT 'Total balance on all Savings Accounts as Secondary Customer',
    NUM_OF_SAV_ACCT         INT               COMMENT 'Total Number of Savings/Chk Accounts for a Customer (Primary + Secondary)',
    SUM_SAV_BAL_AMT         DECIMAL(18, 2)    COMMENT 'Total of all Savings/Chk balance for a Customer (Primary + Savings)',
    NUM_OF_DEPOSITS         INT               COMMENT 'Total Number of Deposits Made on all Accounts for a Customer',
    SUM_DEP_AMT             DECIMAL(18, 2)    COMMENT 'Sum of All the Deposit Transaction Amounts',
    NUM_OF_PYMT             INT               COMMENT 'Total Number of Payments made on all Accounts for a Customer',
    SUM_PYMT_AMT            DECIMAL(18, 2)    COMMENT 'Sum of al the Payment Amounts on all Savings/Chk Accounts for a Customer',
    NUM_OF_WITHDRAWAL       INT               COMMENT 'Total number of  withdrawals made on all Accounts for a Cusotmer',
    SUM_WITHDRAWAL_AMT      DECIMAL(18, 2)    COMMENT 'Sum of all the Withdrawals made on all Accounts for a Customer',
    NUM_OF_NSF              INT               COMMENT 'Total number of NSF (Not Sufficient Funds) transactions made on all Accounts for a Customer',
    SUM_NSF_AMT             DECIMAL(18, 2)    COMMENT 'Sum of NSF (Not Sufficient Funds) transactions amounts on all Accounts for a Customer',
    NUM_OF_ODP              INT               COMMENT 'Total number of ODP (Overdraft Protection) transactions on all Accounts for a Customer',
    SUM_ODP_AMT             DECIMAL(18, 2)    COMMENT 'Sum of all the ODP (Overdraft Protection ) transaction amounts on all Accounts for a Customer'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_SAV_CUST_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

