use crz_cust_scorecard;
drop table if exists CBS_IP_CUST_SUM_FACT;
CREATE EXTERNAL TABLE CBS_IP_CUST_SUM_FACT(
    CUST_CID                VARCHAR(40),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    OP_FIELD                VARCHAR(1000),
    TOT_NUM_REG_ACCT        INT               COMMENT 'Total count of Registered accouts for a borrower',
    TOT_BAL_REG_AMT         DECIMAL(18, 2)    COMMENT 'Total anount balance on all Registered accouts for a borrower',
    TOT_NUM_NON_REG_ACCT    INT               COMMENT 'Total count of Non Registered accounts for a borrower',
    TOT_BAL_NON_REG_AMT     DECIMAL(18, 2)    COMMENT 'Total amount balance on all Non Registered accounts for a borrower',
    TOT_NUM_RRSP_ACCT       INT               COMMENT 'Total number of RRSP accounts for a borrower',
    TOT_BAL_RRSP_AMT        DECIMAL(18, 2)    COMMENT 'Total amount balance on all RRSP accounts for a borrower',
    TOT_NUM_RESP_ACCT       INT               COMMENT 'Total number of RESP accounts for a borrower',
    TOT_BAL_RESP_AMT        DECIMAL(18, 2)    COMMENT 'Total amount balance on all RESP accounts for customer',
    TOT_NUM_TFSA_ACCT       INT               COMMENT 'Total number of TFSA accounts for a borrower',
    TOT_BAL_TFSA_AMT        DECIMAL(18, 2)    COMMENT 'Total amount balance on all TFSA accounts for a borrower'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Ad-hoc etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_IP_CUST_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

