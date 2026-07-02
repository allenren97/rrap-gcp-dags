***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0420_DT4_PD12_FINAL_RPTG_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_PD12_FINAL_RPTG_VARS
*  
*  Purpose: Derive PD12 Realized variables to be used in final aggregation
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-10-29: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();



proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_PD12_FINAL_RPTG_VARS where process_mth_tm_id=&mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(insert into &RRAP_DB..DT4_PD12_FINAL_RPTG_VARS
SELECT 
a.PROCESS_MTH_TM_ID, a.OBS_MTH_TM_ID, a.DT4_EXPSR_CL_KEY_VAL, b.NCR_EXPSR_CL_DESC as DT4_EXPSR_CL_DESC
/*,sum(a.MODEL_DFT_F*(a.drawn+a.undrawn)) AS PREDICTED_EAD_DEFLTRS, sum(a.drawn+a.undrawn) AS PREDICTED_EAD_PORT*/
	,((sum(a.MODEL_DFT_F*(a.drawn+a.undrawn)))/(sum(a.drawn+a.undrawn))) AS OBSERVED_PD_12M
	,now() as INSRT_PROCESS_TMSTMP
	,now() as UPDT_PROCESS_TMSTMP
	
	FROM &RRAP_DB..DT4_PD12_RLZ_DRVD_VARS a 
	LEFT JOIN &RRAP_DB..RPTG_EXPSR_CL_DIM b
		on a.DT4_EXPSR_CL_KEY_VAL = b.NCR_EXPSR_CL_KEY_VAL AND &yrmth. BETWEEN cast(b.EFF_FROM_YR_MTH AS integer) AND cast(b.EFF_TO_YR_MTH AS integer)
	WHERE a.PROCESS_MTH_TM_ID = &mth_tm_id. AND a.OBS_MTH_TM_ID = (&mth_tm_id. -40*12) AND a.OBS_PIT_STAT_CD = 'CUR'
	GROUP BY a.PROCESS_MTH_TM_ID, a.OBS_MTH_TM_ID, a.DT4_EXPSR_CL_KEY_VAL, b.NCR_EXPSR_CL_DESC
	ORDER BY PROCESS_MTH_TM_ID, OBS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL
	
) by nzcon;
execute(commit;) by nzcon;
quit;
