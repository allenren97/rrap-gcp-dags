***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0810_DT4_RT05_DECLARATION.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT05_DECLARATION
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
execute(delete from &RRAP_DB..DT4_RT10_DECLARATION where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;


proc sql;
	create table RT10 as 
	select distinct 
		 a.process_mth_tm_id as MTH_TM_ID
		,a.DT4_RISK_RT_KEY_VAL as RRS
		,a.DT4_PD_SEG_KEY_VAL as PD_SEG
		,catx('-',a.DT4_PD_SEG_KEY_VAL,b.DT4_SEG_DESC) as PD_Segment_Name
		,datetime() format=datetime25.6 as INSRT_PROCESS_TMSTMP
		,datetime() format=datetime25.6 as UPDT_PROCESS_TMSTMP
	from NZRRAP.DT4_RT20_FINAL_RPTG_VARS a, NZRRAP.DT4_SEG_DIM b
	WHERE a.process_mth_tm_id = &mth_tm_id. and a.DT4_PD_SEG_KEY_VAL = b.DT4_SEG_KEY_VAL and b.model_type='PD' and &yrmth. between input(b.EFF_FROM_YR_MTH,6.) and input(b.EFF_TO_YR_MTH,6.)
	ORDER BY 1,2;
quit;

proc append base=NZRRAP.DT4_RT10_DECLARATION data=RT10 force; run;

