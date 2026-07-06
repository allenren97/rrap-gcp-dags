USE crz_cust_scorecard;
DROP TABLE cbs_data_quality_check_log;
CREATE EXTERNAL TABLE cbs_data_quality_check_log(
    insrt_process_tmstmp    TIMESTAMP,
    op_field                VARCHAR(1000),
    proc_dt                 DATE,
    qry_id                  INT,
    subj_area_nm            VARCHAR(60),
    src_cd                  VARCHAR(20),
    vldtn_type              VARCHAR(60),
    src_tbl_nm              VARCHAR(100),
    target_tbl_nm           VARCHAR(100),
    step_num                SMALLINT,
    variance_lmt            DECIMAL(4, 2),
    severity_lvl            SMALLINT,
    crnt_dt                 DATE,
    crnt_val                DECIMAL(23, 8),
    basln_dt                DATE,
    basln_val               DECIMAL(23, 8),
    variance_val            DECIMAL(5, 2),
    btch_id                 BIGINT,
    sql_text                VARCHAR(2000),
    rslt_text               VARCHAR(2000)
)
PARTITIONED BY (job_nm VARCHAR(100),date_type VARCHAR(20))
STORED AS ORC
LOCATION 'hdfs://hd0/data/crz/bbcx/crz_cust_scorecard.db/cbs_data_quality_check_log'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

DROP TABLE tmp_cbs_data_quality_check_log;
CREATE TABLE tmp_cbs_data_quality_check_log(
    qry_id                  INT,
    msg_type        INT,
    crnt_dt                 DATE,
    crnt_val                DECIMAL(23, 8),
    basln_dt                DATE,
    basln_val               DECIMAL(23, 8),
    variance_val            DECIMAL(5, 2),
    rslt_text               string
)
PARTITIONED BY (job_nm string)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t';
;


