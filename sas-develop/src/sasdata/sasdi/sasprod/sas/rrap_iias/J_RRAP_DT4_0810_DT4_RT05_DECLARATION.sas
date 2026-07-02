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
execute(delete from &RRAP_DB..DT4_RT05_DECLARATION where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;


proc sql;
connect using nzrrap as nzcon;
execute(
insert into &RRAP_DB..DT4_RT05_DECLARATION
	SELECT 
		&mth_tm_id. AS mth_tm_id
		,a.DT4_RISK_RT_KEY_VAL as RRS
		,CONCAT(CONCAT(a.DT4_RISK_RT_KEY_VAL, '-'), b.NCR_RISK_RT_DESC) as RRS_NAME
		,now() AS INSRT_PROCESS_TMSTMP
		,now() AS UPDT_PROCESS_TMSTMP

	FROM 

	(SELECT DISTINCT DT4_RISK_RT_KEY_VAL FROM &RRAP_DB..DT4_RT20_FINAL_RPTG_VARS WHERE process_mth_tm_id = &mth_tm_id.
	UNION  
	 SELECT DISTINCT DT4_RISK_RT_KEY_VAL FROM &RRAP_DB..DT4_RT30_FINAL_RPTG_VARS WHERE process_mth_tm_id = &mth_tm_id.
	UNION  
	 SELECT DISTINCT DT4_RISK_RT_KEY_VAL FROM &RRAP_DB..DT4_RT40_FINAL_RPTG_VARS WHERE process_mth_tm_id = &mth_tm_id.) a 

		INNER JOIN 

	&RRAP_DB..RPTG_RISK_RT_SYS_DIM B 
	ON a.DT4_RISK_RT_KEY_VAL = b.NCR_RISK_RT_KEY_VAL AND &yrmth. BETWEEN cast(b.EFF_FROM_YR_MTH AS integer) AND cast(b.EFF_TO_YR_MTH AS integer)
	ORDER BY 1,2;
	) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT05_DECLARATION on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;

