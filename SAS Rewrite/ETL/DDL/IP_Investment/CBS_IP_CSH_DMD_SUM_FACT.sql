use crz_cust_scorecard;
drop table if exists CBS_IP_CSH_DMD_SUM_FACT;
CREATE EXTERNAL TABLE CBS_IP_CSH_DMD_SUM_FACT(
    CUST_CID                    VARCHAR(40),
    INSRT_PROCESS_TMSTMP        TIMESTAMP,
    OP_FIELD                    VARCHAR(1000),
    NUM_CSH_DMD_REG_ACCT        INT               COMMENT 'Total number of registered plan products for cash demand acct',
    SUM_CSH_DMD_REG_AMT         DECIMAL(18, 2)    COMMENT 'Sum of amounts for all Registered accounts under Cash Demand',
    NUM_CSH_DMD_NON_REG_ACCT    INT               COMMENT 'Total number of Non-Regisreted Accounts under Mutual Funds',
    SUM_CSH_DMD_NON_REG_AMT     DECIMAL(18, 2)    COMMENT 'Sum of amounts for all Non-Regisrered accounts under Cash Demand',
    NUM_CSH_DMD_RRSP_ACCT       INT               COMMENT 'Total number of RRSP accounts under Cash Demand',
    SUM_CSH_DMD_RRSP_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all RRSP accounts under Cash Demand',
    NUM_CSH_DMD_RESP_ACCT       INT               COMMENT 'Total number of RESP accounts under Cash Demand',
    SUM_CSH_DMD_RESP_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all RESP accounts under Cash Demanda',
    NUM_CSH_DMD_TFSA_ACCT       INT               COMMENT 'Total number of TFSA accounts under Cash Demand',
    SUM_CSH_DMD_TFSA_AMT        DECIMAL(18, 2)    COMMENT 'Sum of amounts for all TFSA accounts under Cash Demand'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Ad-hoc etc')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_IP_CSH_DMD_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

