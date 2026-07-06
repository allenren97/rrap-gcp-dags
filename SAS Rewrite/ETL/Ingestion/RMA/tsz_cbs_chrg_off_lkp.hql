CREATE EXTERNAL TABLE tsz_rma.cbs_chrg_off_lkp(
    insrt_process_tmstmp     TIMESTAMP,
    op_file_name             VARCHAR(1000),
    chrg_off_cd              VARCHAR(10),
    chrg_off_stat_flag       VARCHAR(1),
    chrg_off_stat_desc       VARCHAR(50),
    accrl_stat_flag          VARCHAR(1),
    accrl_stat_desc          VARCHAR(50)
)
PARTITIONED BY( businesseffectivedate DATE)
STORED AS ORC
LOCATION '/data/lz/int/tsz/rma/cbs_chrg_off_lkp'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
;

ALTER TABLE tsz_rma.cbs_chrg_off_lkp
ADD IF NOT EXISTS PARTITION (businesseffectivedate='${businesseffectivedate}')
LOCATION '/data/lz/int/tsz/rma/cbs_chrg_off_lkp/businesseffectivedate=${businesseffectivedate}';

