Use crz_cust_scorecard;
DROP TABLE IF EXISTS CBS_MDM_FLAGS;
CREATE EXTERNAL TABLE CBS_MDM_FLAGS(
    PARTY_ID                VARCHAR(30)      COMMENT 'Unique Customer Identifier',
    OP_FIELD                VARCHAR(1000),
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    PREF_LANG               VARCHAR(20)      COMMENT 'Preferred Language (English, French etc)',
    GENDER_CD               VARCHAR(10)      COMMENT 'Gender Code (Male, Female - Vlaid values `M`,`F`)',
    MARITAL_STATUS          VARCHAR(20)      COMMENT 'Marital Status - Married, Single etc.',
    EMP_TYPE_CD             VARCHAR(10)      COMMENT 'Employment Type Code - Indicates if the customer is Employed or Unemployed etc.',
    OCCUP_CD                VARCHAR(10)      COMMENT 'Occupation Code',
    OCCUP_TYPE_CD           VARCHAR(10)      COMMENT 'Occupation Type Code',
    OCCUP_STAT_CD           VARCHAR(10)      COMMENT 'Occupation Status Code',
    OCCUP_CAT_CD            VARCHAR(10)      COMMENT 'Occupation Category Code',
    TRANSIT_NUM             VARCHAR(10)      COMMENT 'Bank Transit Number',
    SENSITIVITY_CD          VARCHAR(10)      COMMENT 'Sensitivity code',
    DECEASED_IND            VARCHAR(1)       COMMENT 'Deceased Indicator - Indicates if the customer is deceased or alive',
    CUST_STATUS             VARCHAR(20)      COMMENT 'Customer Status - Open, Closed.',
    BNKRPTCY_FLAG           VARCHAR(1)       COMMENT 'Indicates if the customer has file for Bankruptcy in the past',
    UNDER_18_FLAG           VARCHAR(1)       COMMENT 'Indicates if the customer is under the age of 18.',
    CUST_TYPE               VARCHAR(30)      COMMENT 'Customer Type - `Retail`,`Commercial`,`Corporate`,`Small Business`.',
    TIME_ON_BOOKS           DOUBLE           COMMENT 'Months since the customer has been on the accounting books',
    CUST_AGE                INT              COMMENT 'Customer''s Age'
)
PARTITIONED BY( EFF_DT DATE, DATE_TYPE VARCHAR(20) COMMENT 'Shows Date Type for the partition date columm - Daily,Monthly, Weekly, Ad-hoc etc')
STORED AS ORC
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_MDM_FLAGS'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

