use crz_cust_scorecard;
drop table if exists CBS_KQ_CUST_SUM_FACT;
CREATE EXTERNAL TABLE CBS_KQ_CUST_SUM_FACT(
    CUST_CID                            VARCHAR(40),
    INSRT_PROCESS_TMSTMP                TIMESTAMP         COMMENT 'Insert Process Timestamp',
    OP_FIELD                            VARCHAR(1000),
    NUM_OF_ACCTS                        INT               COMMENT 'Total Number of Credit Accounts for a Customer',
    NUM_OF_ACCTS_CRNT                   INT               COMMENT 'Total Number of Credit Accounts that are Current',
    NUM_OF_ACCTS_DEF                    INT               COMMENT 'Number of Defaulted Credit Accounts for a customer',
    NUM_OF_ACCTS_CHRG_OFF               INT               COMMENT 'Number of Credit Accounts that have been Charged Off',
    SUM_BAL_CRNT_AMT                    DECIMAL(18, 2)    COMMENT 'Sum of Balance on Credit Accounts that are Current',
    SUM_BAL_DEF_AMT                     DECIMAL(18, 2)    COMMENT 'Sum of Balance of Defaulted Amounts on Credit Accounts',
    SUM_BAL_CHRG_OFF_AMT                DECIMAL(18, 2)    COMMENT 'Sum of Balance of Charged Off Amount on Credit Accounts',
    SUM_CR_LMT_AMT                      DECIMAL(18, 2)    COMMENT 'Sum of the Credit Limit Amount on all Credit Accounts for a Customer',
    SUM_TOT_NEW_BAL_AMT                 DECIMAL(18, 2)    COMMENT 'Sum of the New Balance Amount for Credit  Accounts',
    WORST_DLQNT_DAYS                    INT               COMMENT 'Maximum number of delinquent days on Credit Accounts',
    NUM_OF_ACCTS_DLQNT                  INT               COMMENT 'Total Number of Credit Accounts that are Delinquent',
    SUM_ORIG_CHRG_OFF_AMT               DECIMAL(18, 2)    COMMENT 'Sum of Original Charge Off Amount',
    SUM_PRCH_BAL_AMT                    DECIMAL(18, 2)    COMMENT 'Sum of Purchase Balance Amounts',
    SUM_1_CYCL_AGO_PRCH_BAL_AMT         DECIMAL(18, 2)    COMMENT 'Sum of Purchase Balance  Amounts 1 Billing cycle ago',
    SUM_2_CYCL_AGO_PRCH_BAL_AMT         DECIMAL(18, 2)    COMMENT 'Sum of Purchase Balance Amount 2 Billing Cycles Ago',
    SUM_CSH_ADVNC_BAL_AMT               DECIMAL(18, 2)    COMMENT 'Sum of Cash Advance Balance on all Credit Accounts',
    SUM_1_CYCL_AGO_CSH_ADVNC_BAL_AMT    DECIMAL(18, 2)    COMMENT 'Sum of Cash Advance Balance Amounts 1 Billing Cycle Ago',
    SUM_2_CYCL_AGO_CSH_ADVNC_BAL_AMT    DECIMAL(18, 2)    COMMENT 'Sum of Cash Advance Balance Amounts  2 Billing Cycles Ago',
    NUM_OF_PRCHS                        INT               COMMENT 'Number of Purchases made on all Credit Acccounts for a Customer',
    SUM_PRCHS_INTR_CHRGD_AMT            DECIMAL(18, 2)    COMMENT 'Sum of Purchase Interest Charged Amount on all Credit Accounts',
    SUM_CSH_ADVNC_INTR_CHRGD_AMT        DECIMAL(18, 2)    COMMENT 'Sum of Cash Advance Interest Charged Amount on all Credit Accounts',
    SUM_TOTAL_INT_CHRGD_AMT             DECIMAL(18, 2)    COMMENT 'Sum of Total Interest Charged on all Credit Accounts for a Customer',
    MAX_SCRTY_VAL_AMT                   DECIMAL(18, 2),
    MAX_REV_IND                         INT,
    NUM_OF_HELOC                        INT               COMMENT 'Total Number of Home Equity Line Of Credit Produts for a Cusotmer',
    UTIL                                DOUBLE
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - ''Daily'',''Monthly'' etc', CR_TYPE VARCHAR(20) COMMENT 'Type of Credit - Line of Credit (LOC) or Credit Card')
CLUSTERED BY (CUST_CID) SORTED BY (CUST_CID ASC) INTO 60 BUCKETS
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_KQ_CUST_SUM_FACT'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;