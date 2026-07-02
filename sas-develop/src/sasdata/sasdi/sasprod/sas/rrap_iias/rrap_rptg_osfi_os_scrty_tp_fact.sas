
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%let datetime_start = %sysfunc(TIME()) ;
%put >>> START TIME: %sysfunc(datetime(),datetime14.);

%PUT >> Processing_Month_Time_ID = &Processing_Month_Time_ID.;


proc sql;
DELETE FROM &DB..BASEL_RPTG_OSFI_OS_SCRTY_TP_FACT
WHERE MTH_TM_ID = &MTH_TM_ID.;
quit;

%global mth_tm_id;
%global tm_lvl_st_dt;
%global tm_lvl_end_dt;
%global dtime;

proc datasets lib=work kill NOLIST;
run;quit;

proc sql noprint;
select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime 
format datetime25.
into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime
from  EDRTLRT.TM_DIM
/*where tm_id=(select max(mth_tm_id) from edrrapt.BASEL_NCR_BUS_AGGRTD_FACT); 
KEEP FOR PRODUCTION*/
where tm_id= &MTH_TM_ID.; * FOR TEST ONLY End of April test date 
;
quit;
/*** RRAP_STG_SUM_REPORTING_OSFI_OUTSTANDING_SECURITY_TYPE_FACT_BSTM****/
PROC SQL THREADS;
create table WORK.REPORT_OSFI_OS_SEC_TP_FACT_BSTM as
select 
MTH_TM_ID AS MONTH_TIME_ID
,SRC_SYS_CD LENGTH=50 AS SOURCE_SYSTEM_CODE
,SCRTY_TP_DESC LENGTH=50 AS SECURITY_TYPE_DESCRIPTION
,BASEL_PRD_ABR LENGTH=50 AS BASEL_PRODUCT_ABBREVIATION
,SUM(OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/) FORMAT=17.3 
 AS OUTSTANDING_BALANCE_AMOUNT
FROM &LIB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
WHERE
CONSM_PRD_TREATMNT_CD = 'A' AND
SML_BUS_F = 'N' AND
PIT_STAT_CD IN ('CUR', 'DEF') AND
TRNST_EXCLSN_F = 'N' AND
MTH_TM_ID = &Processing_Month_Time_ID AND
SRC_SYS_CD in ('KS','SPL')
GROUP BY
MTH_TM_ID
,SRC_SYS_CD
,SCRTY_TP_DESC
,BASEL_PRD_ABR
;
QUIT;

PROC SQL THREADS;
DELETE FROM &DB..BASEL_RPTG_OSFI_OS_SCRTY_TP_FACT
WHERE MTH_TM_ID = &MTH_TM_ID.
;
INSERT INTO &DB..BASEL_RPTG_OSFI_OS_SCRTY_TP_FACT (BULKLOAD=YES BL_METHOD=CLILOAD)
SELECT *,datetime() format=datetime25.,datetime() format=datetime25.
FROM REPORT_OSFI_OS_SEC_TP_FACT_BSTM
;
quit;
/*---- End of User Written Code  ----*/ 

%put >>> END TIME: %sysfunc(datetime(),datetime14.);
%put >>> Benchmarking PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.)) (mm:ss) ;

