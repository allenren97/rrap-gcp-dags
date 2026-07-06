use crz_cust_scorecard;
drop table if exists CBS_IP_TERM_SUM_FACT;
CREATE EXTERNAL TABLE CBS_IP_TERM_SUM_FACT(
    CUST_CID                 VARCHAR(40),
    INSRT_PROCESS_TMSTMP     TIMESTAMP,
    OP_FIELD                 VARCHAR(1000),
    NUM_TERM_REG_ACCT        INT               COMMENT 'Total number of Registered Accounts for Term Investment',
    SUM_TERM_REG_AMT         DECIMAL(18, 2)    COMMENT 'Sum of amounts for all Registered accounts under Term Investment',
    NUM_TERM_NON_REG_ACCT    INT               COMMENT 'Total number of Non-Rregistered accounts under Term Investment',
    SUM_TERM_NON_REG_AMT     DECIMAL(18, 2)    COMMENT 'Sum of amounts for Non-Registered accounts under Term Investment',
    NUM_TERM_RRSP_ACCT       INT               COMMENT 'Total number of RRSP accounts under Term Investment',
    SUM_TERM_RRSP_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for RRSP accounts under Term Investment',
    NUM_TERM_RESP_ACCT       INT               COMMENT 'Total number of RESP accounts under Term Investment',
    SUM_TERM_RESP_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all RESP accounts under Term Investment',
    NUM_TERM_TFSA_ACCT       INT               COMMENT 'Total number of TFSA accounts under Term Investment',
    SUM_TERM_TFSA_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all TFSA accounts under Term Investment'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Ad-hoc etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_IP_TERM_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

