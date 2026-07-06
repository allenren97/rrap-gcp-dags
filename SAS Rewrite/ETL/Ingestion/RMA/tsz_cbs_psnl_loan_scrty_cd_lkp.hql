CREATE EXTERNAL TABLE tsz_rma.cbs_psnl_loan_scrty_cd_lkp(
    insrt_process_tmstmp     TIMESTAMP,
    op_file_name             VARCHAR(1000),
    scrty_cd                 SMALLINT,
    scrty_desc               VARCHAR(30),
    type                     VARCHAR(10),
    prod                     VARCHAR(60),
    sub_prod                 VARCHAR(60),
    step_flag                VARCHAR(1),
    basel_prod               VARCHAR(60),
    basel_sub_prod           VARCHAR(60),
    basel_prod_abbr          VARCHAR(60),
    prod_id                  VARCHAR(10)
)
PARTITIONED BY( businesseffectivedate DATE)
STORED AS ORC
LOCATION '/data/lz/int/tsz/rma/cbs_psnl_loan_scrty_cd_lkp'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
;

ALTER TABLE tsz_rma.cbs_psnl_loan_scrty_cd_lkp
ADD IF NOT EXISTS PARTITION (businesseffectivedate='${businesseffectivedate}')
LOCATION '/data/lz/int/tsz/rma/cbs_psnl_loan_scrty_cd_lkp/businesseffectivedate=${businesseffectivedate}';