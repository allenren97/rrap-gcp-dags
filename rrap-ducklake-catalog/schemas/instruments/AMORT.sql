-- AMORT feature definition
CREATE TABLE
    IF NOT EXISTS instruments.AMORT (
        OBSN_DT DATE NOT NULL,
        BASEL_ACCT_ID BIGINT NOT NULL,
        SRC_SYS_CD VARCHAR,
        AMORT INTEGER,
        STREAM VARCHAR
    )
;

-- Set partitions 
ALTER TABLE instruments.AMORT
SET
    PARTITIONED BY (OBSN_DT, STREAM)
;