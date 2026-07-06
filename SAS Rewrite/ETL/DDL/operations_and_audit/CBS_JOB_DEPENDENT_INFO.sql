USE CRZ_CUST_SCORECARD;
DROP TABLE IF EXISTS CBS_JOB_DEPENDENT_INFO;
CREATE TABLE CBS_JOB_DEPENDENT_INFO(
    INSRT_PROCESS_TMSTMP    TIMESTAMP,
    JOB_ID                  INT,
    JOB_NM                  VARCHAR(100),
    PARENT_JOB_ID           INT,
    PARENT_JOB_NM           VARCHAR(100)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_JOB_DEPENDENT_INFO'
; 



DROP TABLE IF EXISTS CBS_JOB_DEPENDENT_INFO_TEMP;
CREATE TABLE CBS_JOB_DEPENDENT_INFO_TEMP(
    JOB_NM                  VARCHAR(100),
    PARENT_JOB_NM           VARCHAR(100)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/crz/bbcx/crz_cust_scorecard.db/CBS_JOB_DEPENDENT_INFO_TEMP'
TBLPROPERTIES("skip.header.line.count"="1");
; 


LOAD DATA  INPATH '/tmp/CBS_JOB_DEPENDENT_INFO.csv' overwrite  into table CBS_JOB_DEPENDENT_INFO_TEMP;


INSERT OVERWRITE TABLE cbs_job_dependent_info 
SELECT current_timestamp(), b.job_id, a.job_nm, c.job_id, a.parent_job_nm 
FROM cbs_job_dependent_info_temp a 
INNER JOIN  cbs_job_info b on a.job_nm = b.job_nm 
INNER JOIN  cbs_job_info c on c.job_nm = a.parent_job_nm;

