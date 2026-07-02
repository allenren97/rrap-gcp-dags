***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0310_DT4_RT18_RLZ_LGD_DRVD_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_RLZ_LGD_DRVD_VARS
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
*	2024-08-28: Ganesh Patro - RRMSS-3056 - Integrate RLZ_LGDC_CAP_125_RTO calculated in GCP to DT4 JOB.
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();

%rrap_defaulter_model(
		 IIAS_LIBREF = NZUSER
		,_DEFAULTER_TABLE = &RRAP_WRK..DT4_RT18_RLZ_LGD_OBS_WINDOW
		,_DATA_PREP_TABLE = &RRAP_WRK..DT4_RLZ_DATA_PREP
		,WINDOW_START = (&mth_tm_id.-12*2*40-3*40)
		,WINDOW_END = (&mth_tm_id.-12*2*40)
);
proc sql;
connect using NZRRAP as nzcon;
execute(DELETE from &RRAP_DB..DT4_RT18_RLZ_LGD_DRVD_VARS WHERE process_mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
/*RRMSS-3631 - Ganesh Patro : Section added part of LGD/DT4 change which makes 
								GCP LGD_RLZ_FINAL as driving table for KS, SPL, MOR*/
connect using NZRRAP as nzcon;
execute(INSERT INTO &RRAP_DB..DT4_RT18_RLZ_LGD_DRVD_VARS 
SELECT 
	 RLZ.SRC_SYS_CD
	,RLZ.basel_acct_id
	,a.mort_no
	,t.TM_ID AS process_mth_tm_id
	,OBS.TM_ID AS OBS_MTH_TM_ID
	,a.PIT_STATUS_CD AS OBS_pit_stat_cd
	,a.DT4_RISK_RT_KEY_VAL 
	,a.DT4_EXPSR_CL_KEY_VAL 
	,1 as MODEL_DFT_F
	,DFT_MTH.TM_ID AS LAST_NEW_DEFAULT_DATE
	,RLZ.LAST_NEW_DEFAULT_BAL AS LAST_NEW_DEFAULT_OS_BAL_AMT	
	,RLZ.RLZ_LGDC_CAP_125_RTO as rlz_lgd_rto
	,now() as INSRT_PROCESS_TMSTMP
	,now() as UPDT_PROCESS_TMSTMP

FROM 
	&RRAP_LGD..LGD_RLZ_FINAL RLZ
	LEFT JOIN &RRAP_DB..TM_DIM t
		ON RLZ.PROCESS_MTH = t.TM_LVL_ST_DT and t.tm_lvl='Month'
	LEFT JOIN &RRAP_DB..TM_DIM OBS
	ON RLZ.OBSVTN_MTH = OBS.TM_LVL_ST_DT and OBS.tm_lvl='Month'
	LEFT JOIN &RRAP_DB..TM_DIM DFT_MTH
	ON RLZ.LAST_NEW_DFT_MTH = DFT_MTH.TM_LVL_ST_DT and DFT_MTH.tm_lvl='Month'				
	LEFT JOIN &DATA_PREP_TABLE. a
		ON a.mth_tm_id = OBS.TM_ID
		AND RLZ.basel_acct_id = a.basel_acct_id 
WHERE t.TM_ID= &mth_tm_id.
	AND RLZ.LGD_NON_DEFAULTER_F = 'Y'
	AND RLZ.RLZ_LGDC_CAP_125_RTO IS NOT NULL	
	AND RLZ.LAST_NEW_DFT_MTH BETWEEN  ADD_MONTHS(RLZ.OBSVTN_MTH, 10) AND ADD_MONTHS(RLZ.OBSVTN_MTH, 12)
	AND a.CONSM_PRD_TREATMNT_CD = 'A'
	AND a.SML_BUS_F <> 'Y'   
	AND a.TRNST_EXCLSN_F <> 'Y'
	and a.DT4_PD_MODEL_INCL_F = 1
		;) by nzcon;
execute(commit;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT18_RLZ_LGD_DRVD_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;

/*RRMSS-3631: Ganesh Patro: BELOW IS LEGACY CODE WHEREIN ONLY MAINTAINING TNG SECTION */
proc sql;
connect using NZRRAP as nzcon;
execute(INSERT INTO &RRAP_DB..DT4_RT18_RLZ_LGD_DRVD_VARS 
SELECT 
	 a.SRC_SYS_CD
	,a.basel_acct_id
	,a.mort_no
	,&mth_tm_id. AS process_mth_tm_id
	,a.mth_tm_id AS OBS_MTH_TM_ID
	,a.PIT_STATUS_CD AS OBS_pit_stat_cd
	,a.DT4_RISK_RT_KEY_VAL 
	,a.DT4_EXPSR_CL_KEY_VAL 
	,coalesce(cte1.MODEL_DFT_F,0) as MODEL_DFT_F
	,cte1.LAST_NEW_DEFAULT_DATE
	,cte1.LAST_NEW_DEFAULT_OS_BAL_AMT	
	,RLZ_TNG.LGD_COSTS_CAP_150 as rlz_lgd_rto
	,now() as INSRT_PROCESS_TMSTMP
	,now() as UPDT_PROCESS_TMSTMP

FROM 
	&DATA_PREP_TABLE. a	
	LEFT JOIN &DEFAULTER_TABLE. cte1
		ON cte1.basel_acct_id = a.basel_acct_id		
	LEFT JOIN &RRAP_DB..TM_DIM t
		ON a.mth_tm_id = t.tm_id and t.tm_lvl='Month'	
	LEFT JOIN &MOR_DB..TNG_LGD_ND_REALIZED_FINAL RLZ_TNG
			on RLZ_TNG.ACCOUNT_ID = a.MORT_NO
			and t.tm_lvl_end_dt = RLZ_TNG.month_end_dt
WHERE	
	a.MTH_TM_ID = (&mth_tm_id.-12*3*40) 
	and a.SRC_SYS_CD = 'TNG-MOR'
	AND a.PIT_STATUS_CD = 'CUR'
	AND a.CONSM_PRD_TREATMNT_CD = 'A'
		AND a.SML_BUS_F <> 'Y'   
		AND a.TRNST_EXCLSN_F <> 'Y'
		and a.DT4_PD_MODEL_INCL_F = 1
		;) by nzcon;
execute(commit;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT18_RLZ_LGD_DRVD_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;

