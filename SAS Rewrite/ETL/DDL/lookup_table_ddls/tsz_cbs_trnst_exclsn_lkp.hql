CREATE EXTERNAL TABLE IF NOT EXISTS tsz_rma.cbs_trnst_exclsn_lkp(
    insrt_process_tmstmp     TIMESTAMP,
    op_file_name             VARCHAR(100),
    excluded_trnst_num       VARCHAR(5)
)
PARTITIONED BY( businesseffectivedate DATE)
STORED AS ORC
LOCATION '/data/lz/int/tsz/rma/cbs_trnst_exclsn_lkp'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
;

ALTER TABLE tsz_rma.cbs_trnst_exclsn_lkp
ADD IF NOT EXISTS PARTITION (businesseffectivedate='${businesseffectivedate}')
LOCATION '/data/lz/int/tsz/rma/cbs_trnst_exclsn_lkp/businesseffectivedate=${businesseffectivedate}';

