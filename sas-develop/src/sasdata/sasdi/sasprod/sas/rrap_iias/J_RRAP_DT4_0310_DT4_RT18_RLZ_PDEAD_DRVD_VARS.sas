***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0310_DT4_RT18_RLZ_PDEAD_DRVD_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_RLZ_PDEAD_DRVD_VARS
*  
*  Purpose: Derive RT18 Realized variables to be used in final aggregation
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*   2021-11-10: Hadi Dimashkieh - Rewrite to leverage rrap_defaulter_model macro
*	2023-04-01: Hadi Dimashkieh - RRMSS-2047 - Change account base filter criteria DT4_PD_MODEL_INCL_F = 1
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();

%rrap_defaulter_model(
		 IIAS_LIBREF = NZUSER
		,_DEFAULTER_TABLE = &RRAP_WRK..DT4_RT18_RLZ_PDEAD_OBS_WINDOW
		,_DATA_PREP_TABLE = &RRAP_WRK..DT4_RLZ_DATA_PREP
		,WINDOW_START = (&mth_tm_id.-2*40-40)
		,WINDOW_END = &mth_tm_id.
);




proc sql;
connect using NZRRAP as nzcon;
execute(DELETE from &RRAP_DB..DT4_RT18_RLZ_PDEAD_DRVD_VARS WHERE process_mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using NZRRAP as nzcon;
execute(INSERT INTO &RRAP_DB..DT4_RT18_RLZ_PDEAD_DRVD_VARS
SELECT 
	 a.SRC_SYS_CD
	,a.basel_acct_id
	,a.mort_no
	,&mth_tm_id. AS process_mth_tm_id
	,a.mth_tm_id AS OBS_mth_tm_id
	,a.PIT_STATUS_CD AS OBS_pit_stat_cd
	,a.DT4_RISK_RT_KEY_VAL 
	,a.DT4_EXPSR_CL_KEY_VAL
	,a.drawn as DRAWN
	,a.undrawn as UNDRAWN
	,coalesce(cte1.MODEL_DFT_F,0) as MODEL_DFT_F	
	,a.EAD_INCL
	,a.EAD_OBS_AUTHORIZED_AMT
	,cte1.LAST_NEW_DEFAULT_DATE
	,cte1.LAST_NEW_DEFAULT_OS_BAL_AMT
	,now() as INSRT_PROCESS_TMSTMP
	,now() as UPDT_PROCESS_TMSTMP

FROM 
	&DATA_PREP_TABLE. a	
	LEFT JOIN &DEFAULTER_TABLE. cte1
		ON cte1.basel_acct_id = a.basel_acct_id		

WHERE	
	a.MTH_TM_ID = (&mth_tm_id.-3*40)
	and a.PIT_STATUS_CD = 'CUR' 
	AND a.CONSM_PRD_TREATMNT_CD = 'A'
		AND a.SML_BUS_F <> 'Y'   
		AND a.TRNST_EXCLSN_F <> 'Y'
		and a.DT4_PD_MODEL_INCL_F = 1
	
;) by nzcon;
execute(commit;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT18_RLZ_PDEAD_DRVD_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;

