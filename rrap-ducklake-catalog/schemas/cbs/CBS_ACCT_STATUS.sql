-- Output of J_CBS_0030_CUSTUNIV_03 (the NZWRK.STATUS table, :29-37) -> cbs.CBS_ACCT_STATUS.
--
-- custuniv_03 builds acct_list = DISTINCT (lpad(cast(account as bigint),18,'0'), account,
-- product) from CIS_DATA_POP_02 JOIN CUST_BASE_05 where cust_type='Retail', then joins
-- acct_xref + IWF_CUST_ACCT to add ACCT_TYP / ACCT_BASE_KEY / ACCT_LCST (one row per
-- account, rn=1). Six data columns per the SAS SELECT.
--
-- No SYSCOLUMNS screenshot was available for this table, so types are inferred from the
-- SAS and the source DDLs (IWF_CUST_ACCT: ACCT_BASE_KEY BIGINT, ACCT_LCST VARCHAR).
-- ACCT_TYP is from owtact.acct_xref (not cataloged) -> VARCHAR assumed. Verify.
-- OBSN_DT / STREAM prepended as the emulated partition + delete/replace keys.
CREATE TABLE IF NOT EXISTS cbs.CBS_ACCT_STATUS (
    OBSN_DT DATE NOT NULL,
    STREAM VARCHAR NOT NULL,

    ACCT_ID VARCHAR,          -- lpad(cast(account as bigint), 18, '0') -- 18-char zero-padded
    ACCOUNT VARCHAR,          -- CIS_DATA_POP_02.ACCOUNT
    PRODUCT VARCHAR,          -- CIS_DATA_POP_02.PRODUCT
    ACCT_TYP VARCHAR,         -- acct_xref.ACCT_TYP (type inferred)
    ACCT_BASE_KEY BIGINT,     -- IWF_CUST_ACCT.ACCT_BASE_KEY
    ACCT_LCST VARCHAR         -- IWF_CUST_ACCT.ACCT_LCST
);

ALTER TABLE cbs.CBS_ACCT_STATUS
SET PARTITIONED BY (OBSN_DT, STREAM);
