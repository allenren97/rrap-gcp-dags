/*********************************************************************************************************
2024/03/04 RRMSS-1947 - Modify source & logic to report 0599 with breakdown; change to BCAR 50 schedules
***********************************************************************************************************/

options errorabend;

%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%let datetime_start = %sysfunc(TIME()) ;
%put >>> START TIME: %sysfunc(datetime(),datetime14.);

%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */

%global mth_tm_id;
%global tm_lvl_st_dt;
%global tm_lvl_end_dt;
%global dtime;

/** To Clear Work Library **/
PROC DATASETS LIB = WORK KILL NOLIST;
RUN; QUIT;

proc sql noprint;
select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime 
format datetime25.
into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime
from EDRTLRT.TM_DIM
where tm_id=&mth_tm_id.;
quit;

%PUT &MTH_TM_ID;
%PUT &tm_lvl_st_dt;
%PUT &tm_lvl_end_dt;
%PUT &dtime;


/*  Record Type 010 */
PROC SQL;
CONNECT USING EDRRAPT AS DBCON;
CREATE TABLE ATTESTTN_FACT_010 AS SELECT * FROM CONNECTION TO DBCON (
     SELECT MTH_TM_ID
            ,'010' AS NCR_BD_RECD_KEY_VAL
            ,EXPSR_CL_KEY_VAL
            ,RTRIM(LISTAGG(BCAR_SCHED_NUM_50, '+') WITHIN GROUP (ORDER BY BCAR_SCHED_NUM_50)) AS ALL_BCAR_SCHED_NUM_50
            ,ROUND(SUM(ADJUSTED_OS_BAL_AMT)/1000,0) AS ADJUSTED_OS_BAL_AMT
     FROM (
          SELECT MTH_TM_ID
                 ,CASE WHEN BCAR_SCHED_NUM_50 = '50.210' THEN '0599'||'-'||NCR_PRNT_KEY_VAL ELSE NCR_PRNT_KEY_VAL END AS EXPSR_CL_KEY_VAL
                 ,BCAR_SCHED_NUM_50
                 ,SUM(CASE WHEN COALESCE(ADJUSTED_OS_BAL_AMT, 0) <0 THEN 0 ELSE COALESCE(ADJUSTED_OS_BAL_AMT, 0) END) AS ADJUSTED_OS_BAL_AMT
          FROM &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT 
          LEFT JOIN &DBSCHEMA..BASEL_NCR_HIERARCHY_LKP 
          ON NCR_PRNT_KEY_VAL in ('0502', '0503','0505', '0506', '0507') AND NCR_KEY_VAL = NCR_EXPSR_CL_KEY_VAL 
          WHERE MTH_TM_ID = &MTH_TM_ID. AND CCAR_F = 1 
          GROUP BY MTH_TM_ID, NCR_PRNT_KEY_VAL, BCAR_SCHED_NUM_50
          ) 
     GROUP BY MTH_TM_ID, EXPSR_CL_KEY_VAL; 
);
QUIT;

PROC SQL;
CREATE TABLE BASEL_NCR_BCAR_ATTESTTN_FACT_010 AS 
SELECT 
	MTH_TM_ID,
	NCR_BD_RECD_KEY_VAL,
	EXPSR_CL_KEY_VAL,
	ALL_BCAR_SCHED_NUM_50 AS BCAR_SCHED_NUM length=255,
	ADJUSTED_OS_BAL_AMT,
	. as ESTD_EAD_AMT,
	. as IRB_EXPCTD_LOSS_AMT,
	&SESSIONTIME AS INSRT_PROCESS_TMSTMP format datetime25.,
	&SESSIONTIME AS UPDT_PROCESS_TMSTMP format datetime25.
FROM ATTESTTN_FACT_010 
ORDER BY EXPSR_CL_KEY_VAL;
QUIT;


/*  Record Type 045 */
PROC SQL;
CONNECT USING EDRRAPT AS DBCON;
CREATE TABLE ATTESTTN_FACT_045 AS SELECT * FROM CONNECTION TO DBCON (
   SELECT MTH_TM_ID
          ,'045' AS NCR_BD_RECD_KEY_VAL
          ,CASE WHEN NCR_EXPSR_CL_KEY_VAL = '0599' THEN '0599'||'-'||PRIOR_NCR_EXPSR_CL_KEY_VAL ELSE NCR_EXPSR_CL_KEY_VAL END AS EXPSR_CL_KEY_VAL
/*		    ,ALL_BCAR_SCHED_NUM_50*/
          ,ROUND(ESTD_EAD_AMT/1000,0) AS ESTD_EAD_AMT
          FROM &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT 
/*          LEFT JOIN (*/
/*                     SELECT NCR_PRNT_KEY_VAL, LISTAGG(BCAR_SCHED_NUM_50 , '+') WITHIN GROUP (ORDER BY BCAR_SCHED_NUM_50) AS ALL_BCAR_SCHED_NUM_50*/
/*                     FROM (*/
/*                           SELECT DISTINCT MTH_TM_ID */
/*                                           ,CASE WHEN BCAR_SCHED_NUM_50 = '50.210' THEN '0599'||'-'||NCR_PRNT_KEY_VAL ELSE NCR_PRNT_KEY_VAL END AS NCR_PRNT_KEY_VAL */
/*                                           ,BCAR_SCHED_NUM_50 */
/*                           FROM &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT */
/*                           LEFT JOIN &DBSCHEMA..BASEL_NCR_HIERARCHY_LKP */
/*                           ON NCR_PRNT_KEY_VAL in ('0502', '0503','0505', '0506', '0507') AND NCR_KEY_VAL = NCR_EXPSR_CL_KEY_VAL */
/*                           WHERE MTH_TM_ID = &MTH_TM_ID. AND CCAR_F = 1 */
/*                           ) */
/*                     GROUP BY NCR_PRNT_KEY_VAL*/
/*                    ) ON CASE WHEN NCR_EXPSR_CL_KEY_VAL='0599' THEN '0599'||'-'||PRIOR_NCR_EXPSR_CL_KEY_VAL ELSE NCR_EXPSR_CL_KEY_VAL END = NCR_PRNT_KEY_VAL */
          WHERE MTH_TM_ID = &MTH_TM_ID. AND NCR_EXPSR_CL_KEY_VAL IN ('0502', '0503', '0505', '0506', '0507', '0599'); 
);
QUIT;

/* join with above 010 results to leverage its BCAR 50 composition instead of re-querying */
PROC SQL;
CREATE TABLE BASEL_NCR_BCAR_ATTESTTN_FACT_045 AS 
SELECT 
	a.MTH_TM_ID,
	a.NCR_BD_RECD_KEY_VAL,
	a.EXPSR_CL_KEY_VAL,
	b.ALL_BCAR_SCHED_NUM_50 as BCAR_SCHED_NUM length=255,
	. as ADJUSTED_OS_BAL_AMT,
	a.ESTD_EAD_AMT,
	. as IRB_EXPCTD_LOSS_AMT,
	&SESSIONTIME AS INSRT_PROCESS_TMSTMP format datetime25.,
	&SESSIONTIME AS UPDT_PROCESS_TMSTMP format datetime25.
FROM ATTESTTN_FACT_045 a 
LEFT JOIN ATTESTTN_FACT_010 b 
ON a.EXPSR_CL_KEY_VAL = b.EXPSR_CL_KEY_VAL
ORDER BY EXPSR_CL_KEY_VAL;
QUIT;


/*  Record Type 047 */
PROC SQL;
CONNECT USING EDRRAPT AS DBCON;
CREATE TABLE ATTESTTN_FACT_047 AS SELECT * FROM CONNECTION TO DBCON (
   SELECT MTH_TM_ID
          ,'047' AS NCR_BD_RECD_KEY_VAL
          ,CASE WHEN NCR_EXPSR_CL_KEY_VAL = '0599' THEN '0599'||'-'||PRIOR_NCR_EXPSR_CL_KEY_VAL ELSE NCR_EXPSR_CL_KEY_VAL END AS EXPSR_CL_KEY_VAL
/*		  ,ALL_BCAR_SCHED_NUM_50*/
          ,ROUND(IRB_EXPCTD_LOSS_AMT/1000,0) AS IRB_EXPCTD_LOSS_AMT
          FROM &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT 
/*          LEFT JOIN (*/
/*                 SELECT NCR_PRNT_KEY_VAL, LISTAGG(BCAR_SCHED_NUM_50 , '+') WITHIN GROUP (ORDER BY BCAR_SCHED_NUM_50) AS ALL_BCAR_SCHED_NUM_50*/
/*                 FROM (*/
/*                       SELECT DISTINCT MTH_TM_ID */
/*                                       ,CASE WHEN BCAR_SCHED_NUM_50 = '50.210' THEN '0599'||'-'||NCR_PRNT_KEY_VAL ELSE NCR_PRNT_KEY_VAL END AS NCR_PRNT_KEY_VAL */
/*                                       ,BCAR_SCHED_NUM_50 */
/*                       FROM &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT */
/*                       LEFT JOIN &DBSCHEMA..BASEL_NCR_HIERARCHY_LKP */
/*                       ON NCR_PRNT_KEY_VAL in ('0502', '0503','0505', '0506', '0507') AND NCR_KEY_VAL = NCR_EXPSR_CL_KEY_VAL */
/*                       WHERE MTH_TM_ID = &MTH_TM_ID. AND CCAR_F = 1 */
/*                       ) */
/*                 GROUP BY NCR_PRNT_KEY_VAL*/
/*                ) ON CASE WHEN NCR_EXPSR_CL_KEY_VAL='0599' THEN '0599'||'-'||PRIOR_NCR_EXPSR_CL_KEY_VAL ELSE NCR_EXPSR_CL_KEY_VAL END = NCR_PRNT_KEY_VAL */
          WHERE MTH_TM_ID = &MTH_TM_ID. AND NCR_EXPSR_CL_KEY_VAL IN ('0502', '0503', '0505', '0506', '0507', '0599'); 
);
QUIT;

/* join with above 010 results to leverage its BCAR 50 composition instead of re-querying */
PROC SQL;
CREATE TABLE BASEL_NCR_BCAR_ATTESTTN_FACT_047 AS SELECT 
	a.MTH_TM_ID,
	a.NCR_BD_RECD_KEY_VAL,
	a.EXPSR_CL_KEY_VAL,
	b.ALL_BCAR_SCHED_NUM_50 as BCAR_SCHED_NUM length=255,
	. as ADJUSTED_OS_BAL_AMT,
	. as ESTD_EAD_AMT,
	a.IRB_EXPCTD_LOSS_AMT,
	&SESSIONTIME AS INSRT_PROCESS_TMSTMP format datetime25.,
	&SESSIONTIME AS UPDT_PROCESS_TMSTMP format datetime25.
FROM ATTESTTN_FACT_047 a 
LEFT JOIN ATTESTTN_FACT_010 b 
ON a.EXPSR_CL_KEY_VAL = b.EXPSR_CL_KEY_VAL
ORDER BY EXPSR_CL_KEY_VAL;
QUIT;


/*  combine Record Type 010, 045, 047 */
PROC SQL;
CREATE TABLE BASEL_NCR_BCAR_ATTESTTN_FACT_ALL AS
SELECT * FROM BASEL_NCR_BCAR_ATTESTTN_FACT_010
UNION ALL
SELECT * FROM BASEL_NCR_BCAR_ATTESTTN_FACT_045
UNION ALL
SELECT * FROM BASEL_NCR_BCAR_ATTESTTN_FACT_047
;
QUIT;


PROC SQL;
DELETE FROM &DB..BASEL_NCR_BCAR_ATTESTTN_FACT WHERE MTH_TM_ID=&MTH_TM_ID.;
QUIT;

PROC APPEND BASE=&DB..BASEL_NCR_BCAR_ATTESTTN_FACT (BULKLOAD=YES BL_METHOD=CLILOAD)
DATA=BASEL_NCR_BCAR_ATTESTTN_FACT_ALL; 
RUN;
%put &SYSDSN.;
%put &SQLOBS.;

/**  Step end User Written **/
%put etls_endTime = %sysfunc(datetime(),datetime.);


%put >>> END TIME: %sysfunc(datetime(),datetime14.);
%put >>> Benchmarking PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.)) (mm:ss) ;
