use crz_cust_scorecard;
drop table if exists RISK_SAV_ACCT_TXN_SUM_FACT;
CREATE EXTERNAL TABLE RISK_SAV_ACCT_TXN_SUM_FACT(
    ACCT_NUM                VARCHAR(80),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    NUM_OF_DEPOSITS         INT               COMMENT 'Number of Deposits',
    SUM_DEPOSIT_AMT         DECIMAL(18, 2)    COMMENT 'Sum of Deposit Amount',
    NUM_OF_PAYMENTS         INT               COMMENT 'Number of Payments',
    SUM_PAYMENT_AMT         DECIMAL(18, 2)    COMMENT 'Sum of Payment Amounts',
    NUM_OF_WITHDRAWALS      INT               COMMENT 'Number of Withdrawals',
    SUM_WITHDRAWAL_AMT      DECIMAL(18, 2)    COMMENT 'Sum of Withdrawals amount',
    NUM_OF_NSF_TXN          INT               COMMENT 'Number of Non Sufficient Funds (NSF) Amount',
    SUM_NSF_AMT             DECIMAL(18, 2)    COMMENT 'Sum of Non Sufficient Amount',
    NUM_OF_ODP_TXN          INT               COMMENT 'Total Number Of  Transactions where Overdraft Protection was used',
    SUM_ODP_AMT             DECIMAL(18, 2)    COMMENT 'Sum ofl Overdraft Protection Amount',
    NUM_OF_OTH_TXN          INT               COMMENT 'Number of Other Transactions',
    SUM_OF_OTH_TXN_AMT      DECIMAL(18, 2)    COMMENT 'Sum of Other Transactions Amount'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc')
CLUSTERED BY (ACCT_NUM) SORTED BY (ACCT_NUM ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/RISK_SAV_ACCT_TXN_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

