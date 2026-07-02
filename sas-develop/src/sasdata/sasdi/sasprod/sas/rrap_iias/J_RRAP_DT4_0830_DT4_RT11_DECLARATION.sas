***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0810_DT4_RT11_DECLARATION.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT11_DECLARATION
*  
*  Purpose: 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2022-01-07: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_RT11_DECLARATION where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
	create table RT11 as 
	select distinct 
		 a.process_mth_tm_id as MTH_TM_ID
		,a.DT4_RISK_RT_KEY_VAL as RRS
		,a.DT4_LGD_SEG_KEY_VAL as LGD_SEG
		,catx('-',a.DT4_LGD_SEG_KEY_VAL,b.DT4_SEG_DESC) as LGD_NAME
		,datetime() format=datetime25.6 as INSRT_PROCESS_TMSTMP
		,datetime() format=datetime25.6 as UPDT_PROCESS_TMSTMP
	from NZRRAP.DT4_RT30_FINAL_RPTG_VARS a, NZRRAP.DT4_SEG_DIM b
	WHERE a.process_mth_tm_id = &mth_tm_id. and a.DT4_LGD_SEG_KEY_VAL = b.DT4_SEG_KEY_VAL and b.model_type in ('LGD-D','LGD-ND') and &yrmth. between input(b.EFF_FROM_YR_MTH,6.) and input(b.EFF_TO_YR_MTH,6.)
	ORDER BY 1,2;
quit;


proc append base=NZRRAP.DT4_RT11_DECLARATION data=RT11 force; run;

