-- emulated.CBS_MDM_FLAGS — regenerated from the EDL-R Hive DDL
--   source: crz_cust_scorecard.CBS_MDM_FLAGS
--   (SAS Rewrite/ETL/DDL/customer_universe_ddls/CBS_MDM_FLAGS.sql)
--
-- Deltas from the Hive source (emulated conventions):
--   * EFF_DT, DATE_TYPE      : Hive partition columns -> regular columns here
--   * STREAM, MTH_TM_ID      : added (run/partition context, not in Hive)
--   * OP_FIELD               : dropped (matches SAS proc append drop=op_field)
--   * INSRT_PROCESS_TMSTMP   : moved to the audit block; UPDT_PROCESS_TMSTMP added
--   * VARCHAR(n) sizes       : relaxed to VARCHAR (DuckDB idiom)
-- Partitioned by (EFF_DT, STREAM). One row per (EFF_DT, STREAM, DATE_TYPE, PARTY_ID).
CREATE TABLE IF NOT EXISTS emulated.CBS_MDM_FLAGS (
    EFF_DT DATE NOT NULL,               -- Hive partition date
    STREAM VARCHAR NOT NULL,            -- emulated run stream
    MTH_TM_ID INTEGER NOT NULL,         -- emulated month time id
    DATE_TYPE VARCHAR NOT NULL,         -- Hive partition: Daily/Monthly/Weekly/Ad-hoc

    PARTY_ID VARCHAR NOT NULL,          -- Unique Customer Identifier
    PREF_LANG VARCHAR,                  -- Preferred Language (English, French etc)
    GENDER_CD VARCHAR,                  -- Gender Code (M/F)
    MARITAL_STATUS VARCHAR,             -- Marital Status - Married, Single etc.
    EMP_TYPE_CD VARCHAR,                -- Employment Type Code
    OCCUP_CD VARCHAR,                   -- Occupation Code
    OCCUP_TYPE_CD VARCHAR,              -- Occupation Type Code
    OCCUP_STAT_CD VARCHAR,              -- Occupation Status Code
    OCCUP_CAT_CD VARCHAR,               -- Occupation Category Code
    TRANSIT_NUM VARCHAR,                -- Bank Transit Number
    SENSITIVITY_CD VARCHAR,             -- Sensitivity code
    DECEASED_IND VARCHAR,               -- Deceased Indicator
    CUST_STATUS VARCHAR,                -- Customer Status - Open, Closed
    BNKRPTCY_FLAG VARCHAR,              -- Filed for Bankruptcy in the past
    UNDER_18_FLAG VARCHAR,              -- Customer is under the age of 18
    CUST_TYPE VARCHAR,                  -- Retail/Commercial/Corporate/Small Business
    TIME_ON_BOOKS DOUBLE,               -- Months since customer on the books
    CUST_AGE INTEGER,                   -- Customer's Age

    INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL,
    UPDT_PROCESS_TMSTMP TIMESTAMP
);

ALTER TABLE emulated.CBS_MDM_FLAGS
SET PARTITIONED BY (EFF_DT, STREAM);
