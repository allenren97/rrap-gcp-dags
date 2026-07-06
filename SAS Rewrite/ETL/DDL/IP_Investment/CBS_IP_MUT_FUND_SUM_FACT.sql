use crz_cust_scorecard;
drop table if exists CBS_IP_MUT_FUND_SUM_FACT;
CREATE EXTERNAL TABLE CBS_IP_MUT_FUND_SUM_FACT(
    CUST_CID                     VARCHAR(40),
    INSRT_PROCESS_TMSTMP         TIMESTAMP,
    OP_FIELD                     VARCHAR(1000),
    NUM_MUT_FUND_REG_ACCT        INT               COMMENT 'Total number of Registered accounts for a customer under mutual funds',
    SUM_MUT_FUND_REG_AMT         DECIMAL(18, 2)    COMMENT 'Sum of amounts for all Registered accounts for a customer under mutual funds',
    NUM_MUT_FUND_NON_REG_ACCT    INT               COMMENT 'Total number of Non-Registrered accounts under Mutual Funds',
    SUM_MUT_FUND_NON_REG_AMT     DECIMAL(18, 2)    COMMENT 'Sum of amounts for all non-registered accounts under Mutual Funds',
    NUM_MUT_FUND_RRSP_ACCT       INT               COMMENT 'Total number of RRSP accounts under Mutual Funds',
    SUM_MUT_FUND_RRSP_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all RRSP accounts under Mutual Funds',
    NUM_MUT_FUND_RESP_ACCT       INT               COMMENT 'Total number of RESP accounts under Mutual Funds',
    SUM_MUT_FUND_RESP_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all RESP accounts under Mutual Funds',
    NUM_MUT_FUND_TFSA_ACCT       INT               COMMENT 'Total number of  TFSA accounts under Mutual Funds',
    SUM_MUT_FUND_TFSA_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all TFSA accounts under Mutual Funds'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Ad-hoc etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_IP_MUT_FUND_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

