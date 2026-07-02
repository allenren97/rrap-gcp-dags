***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0430_DT4_RT18_FINAL_RPTG_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_FINAL_RPTG_VARS
*  
*  Purpose: Derive Expected Results variables at the account level 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*   2022-01-13: Hadi Dimashkieh - Change WEIGHTED_PD_AFTERCRM to WEIGHTED_PD_BEFORECRM
*	2023-03-26: Hadi Dimashkieh - RRMSS-2030 - remove 1.06 scalar factor
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();



proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_RT18_FINAL_RPTG_VARS where mth_tm_id=&mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(insert into &RRAP_DB..DT4_RT18_FINAL_RPTG_VARS
SELECT 
	 b.mth_tm_id, b.DT4_EXPSR_CL_KEY_VAL, a.NCR_EXPSR_CL_DESC AS DT4_EXPSR_CL_DESC
	,b.WEIGHTED_PD_BEFORECRM AS PRODUCTION_PD
	,c.OBSERVED_PD
	,b.WEIGHTED_LGD_BEFORECRM AS PRODUCTION_LGD
	,c.OBSERVED_LGD
	,b.WEIGHTED_EAD_AFTERCRM AS PRODUCTION_EAD
	,c.OBSERVED_EAD
	,b.EAD_AFTERCRM_NODEF/1000000 AS EAD_MM_NON_DEFAULTED_ACCOUNTS
	,b.EAD_AFTERCRM_DEF/1000000 AS EAD_MM_DEFAULTED_ACCOUNTS
	,b.RWA/1000000 AS RWA_MM
	,now() as INSRT_PROCESS_TMSTMP
	,now() as UPDT_PROCESS_TMSTMP
FROM 
&RRAP_DB..DT4_RT18_EST_VARS b 
	LEFT JOIN &RRAP_DB..DT4_RT18_RLZ_VARS c 
			ON b.DT4_EXPSR_CL_KEY_VAL = c.DT4_EXPSR_CL_KEY_VAL AND c.PROCESS_MTH_TM_ID = b.mth_tm_id
			
	LEFT JOIN &RRAP_DB..RPTG_EXPSR_CL_DIM a
			ON a.NCR_EXPSR_CL_KEY_VAL = b.DT4_EXPSR_CL_KEY_VAL AND &YRMTH. BETWEEN cast(a.EFF_FROM_YR_MTH AS integer) AND cast(a.EFF_TO_YR_MTH AS integer)
WHERE b.mth_tm_id = &mth_tm_id.
ORDER BY 1,2
	
) by nzcon;
execute(commit;) by nzcon;
quit;

