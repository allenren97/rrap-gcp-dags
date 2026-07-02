-- Table creation for downstream asset in GCP database since pd_acct_score currently exists as a column in other files

CREATE TABLE
    IF NOT EXISTS instruments.PD_ACCT_SCORE (
        OBSN_DT DATE NOT NULL,
        BASEL_ACCT_ID BIGINT NOT NULL,
        PD_ACCT_SCORE SMALLINT,
        STREAM VARCHAR
    )
;

ALTER TABLE instruments.PD_ACCT_SCORE 
SET
    PARTITIONED BY (OBSN_DT, STREAM)
;
