CREATE EXTERNAL TABLE tsz_rma.cbs_model_scorecrd_lkp(
    insrt_process_tmstmp     TIMESTAMP,
    op_file_name             VARCHAR(1000),
    seg_num                  SMALLINT,
    scorecrd_var             VARCHAR(100),
    bin_cond                 VARCHAR(100),
    score                    SMALLINT,
    seq_num                  SMALLINT
)
PARTITIONED BY( businesseffectivedate DATE)
STORED AS ORC
LOCATION '/data/lz/int/tsz/rma/cbs_model_scorecrd_lkp'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
;

ALTER TABLE tsz_rma.cbs_model_scorecrd_lkp
ADD IF NOT EXISTS PARTITION (businesseffectivedate='${businesseffectivedate}')
LOCATION '/data/lz/int/tsz/rma/cbs_model_scorecrd_lkp/businesseffectivedate=${businesseffectivedate}';