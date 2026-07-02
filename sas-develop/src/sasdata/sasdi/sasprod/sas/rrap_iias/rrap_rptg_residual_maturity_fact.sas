 
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);


%let datetime_start = %sysfunc(TIME()) ;
%put >>> START TIME: %sysfunc(datetime(),datetime14.);

%PUT >> Processing_Month_Time_ID = &Processing_Month_Time_ID.;
%global mth_tm_id;
%global tm_lvl_st_dt;
%global tm_lvl_end_dt;
%global dtime;


proc sql;
DELETE FROM &DB..BASEL_RPTG_RESIDUAL_MAT_FACT
WHERE MTH_TM_ID = &MTH_TM_ID.
;
quit;

proc datasets lib=work kill NOLIST;
run;quit;

proc sql noprint;
select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime 
format datetime25.
into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime
from  EDRTLRT.TM_DIM
/*where tm_id=(select max(mth_tm_id) from edrrapt.BASEL_NCR_BUS_AGGRTD_FACT); 
KEEP FOR PRODUCTION*/
where tm_id= &Processing_Month_Time_ID.; * FOR TEST ONLY End of April test date 
;
quit;

/*Updated SPL Logic by Khalid dated 05-SEP-2014 according to BSTM v_0_2*/
 
PROC SQL THREADS;
create table &LIB..REPORT_RESIDUAL_MAT_FACT as
select MTH_TM_ID AS MONTH_TIME_ID,SRC_SYS_CD, 
(case when SRC_SYS_CD='KS' then . else 
	SUM(case when SRC_SYS_CD in ('TNG-MOR','MOR') and RESIDUAL_MAT < 12 then ADJUSTED_OS_BAL_AMT 
   		when SRC_SYS_CD='SPL' and RESIDUAL_MAT < 12 then ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/ 
   end)
end) LENGTH=8 FORMAT=20.8 as ADJ_LESS_1_YR_MAT_OS_BAL_AMT, 
(case when SRC_SYS_CD='KS' then . else 
 SUM(case when SRC_SYS_CD in ('TNG-MOR','MOR') and RESIDUAL_MAT >= 12 and RESIDUAL_MAT < 60 then ADJUSTED_OS_BAL_AMT
   when SRC_SYS_CD='SPL' and RESIDUAL_MAT >= 12 and RESIDUAL_MAT < 60 then ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/ 
   end)
end) LENGTH=8 FORMAT=20.8 as ADJ_1_TO_5_YR_MAT_OS_BAL_AMT, 
SUM(case when SRC_SYS_CD='KS' then ADJUSTED_OS_BAL_AMT
when SRC_SYS_CD in ('TNG-MOR','MOR') and RESIDUAL_MAT eq . then ADJUSTED_OS_BAL_AMT
when SRC_SYS_CD='SPL' and RESIDUAL_MAT eq . then ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/ 
 end)
 LENGTH=8 FORMAT=20.8 as ADJ_UNSPECIFIED_MAT_OS_BAL_AMT,
(case when SRC_SYS_CD='KS' then . else 
 SUM(case when SRC_SYS_CD in ('TNG-MOR','MOR') and RESIDUAL_MAT >= 60 and RESIDUAL_MAT < 120 then ADJUSTED_OS_BAL_AMT
   when SRC_SYS_CD='SPL' and RESIDUAL_MAT >= 60 and RESIDUAL_MAT < 120 then ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/ 
   end)
end) LENGTH=8 FORMAT=20.8 as ADJ_5_TO_10_YR_MAT_OS_BAL_AMT,
(case when SRC_SYS_CD='KS' then . else 
 SUM(case when SRC_SYS_CD in ('TNG-MOR','MOR') and RESIDUAL_MAT >= 120 and RESIDUAL_MAT < 240 then ADJUSTED_OS_BAL_AMT
   when SRC_SYS_CD='SPL' and RESIDUAL_MAT >= 120 and RESIDUAL_MAT < 240 then ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/ 
   end)
end) LENGTH=8 FORMAT=20.8 as ADJ_10_TO_20_YR_MAT_OS_BAL_AMT,
(case when SRC_SYS_CD='KS' then . else 
 SUM(case when SRC_SYS_CD in ('TNG-MOR','MOR') and RESIDUAL_MAT >= 240 then ADJUSTED_OS_BAL_AMT
   when SRC_SYS_CD='SPL' and RESIDUAL_MAT >= 240 then ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/ 
   end)
end) LENGTH=8 FORMAT=20.8 as ADJ_OVER_20_YR_MAT_OS_BAL_AMT
/*from EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT*/
/*where MTH_TM_ID=15556 and PIT_STAT_CD in ('CUR','DEF') and SML_BUS_F='N' and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N'*/

FROM 
&LIB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
WHERE
CONSM_PRD_TREATMNT_CD = 'A' AND
SML_BUS_F = 'N' AND
PIT_STAT_CD IN ('CUR', 'DEF') AND
TRNST_EXCLSN_F = 'N' AND
MTH_TM_ID = &Processing_Month_Time_ID. 
GROUP BY
MTH_TM_ID, SRC_SYS_CD;
quit;


PROC SQL ;

INSERT INTO &DB..BASEL_RPTG_RESIDUAL_MAT_FACT (BULKLOAD=YES BL_METHOD=CLILOAD)
SELECT * ,DATETIME() FORMAT DATETIME25.,DATETIME() FORMAT=DATETIME25.
FROM &LIB..REPORT_RESIDUAL_MAT_FACT
;
QUIT;
/*---- End of User Written Code  ----*/ 
%PUT etls_endTime= %sysfunc(datetime(),datetime.);

%put >>> END TIME: %sysfunc(datetime(),datetime14.);
%put >>> Benchmarking PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.)) (mm:ss) ;

