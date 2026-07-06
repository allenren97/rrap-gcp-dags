USE crz_cust_scorecard;
DROP TABLE IF EXISTS cbs_data_quality_check_info;
CREATE EXTERNAL TABLE cbs_data_quality_check_info(
    insrt_process_tmstmp    TIMESTAMP,
    op_field                VARCHAR(1000),
    qry_id                  INT,
    subj_area_nm            VARCHAR(60),
    src_cd                  VARCHAR(20),
    vldtn_type              VARCHAR(60),
    src_tbl_nm              VARCHAR(100),
    target_tbl_nm           VARCHAR(100),
    job_nm                  VARCHAR(100),
    step_num                SMALLINT,
    variance_lmt            DECIMAL(5, 2),
    parm_nm                 VARCHAR(100),
    severity_lvl            SMALLINT,
    active_f                VARCHAR(1),
    sql_text                VARCHAR(5000),
    date_type               VARCHAR(20)
)
STORED AS ORC
LOCATION 'hdfs://hd0/data/crz/bbcx/crz_cust_scorecard.db/cbs_data_quality_check_info'
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
;

DROP TABLE tmp_risk_data_qry_txt;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp_risk_data_qry_txt (
qry_id                  INT,
subj_area_nm            VARCHAR(60),
src_cd                  VARCHAR(20),
vldtn_type              VARCHAR(60),
src_tbl_nm              VARCHAR(100),
target_tbl_nm           VARCHAR(100),
job_nm           VARCHAR(100),
step_num                SMALLINT,
variance_lmt            DECIMAL(4, 2),
parm_nm                 VARCHAR(100),
severity_lvl            SMALLINT,
active_f                VARCHAR(1),
sql_text                VARCHAR(5000),
sql_text_orig    VARCHAR(5000),
DQ_Case          VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~'
STORED AS TEXTFILE;

load data inpath '/tmp/CBS_DATA_QUALITY_CHECK_INFO.csv' overwrite into table tmp_risk_data_qry_txt;

insert overwrite table cbs_data_quality_check_info
select
  from_unixtime(unix_timestamp())
  , concat('risk_data_qry', '~',  from_unixtime(unix_timestamp()) )
  , qry_id
  , subj_area_nm
  , src_cd
  , vldtn_type
  , src_tbl_nm
  , target_tbl_nm
  , job_nm
  , step_num
  , variance_lmt
  , parm_nm
  , severity_lvl
  , active_f
  , regexp_replace(sql_text , 'zyx', "\n") sql_text
  ,'m'
from tmp_risk_data_qry_txt
where qry_id is not null;

