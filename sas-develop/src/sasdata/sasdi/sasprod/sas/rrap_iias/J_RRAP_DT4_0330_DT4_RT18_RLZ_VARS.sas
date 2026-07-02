***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0320_DT4_RT18_RLZ_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_RLZ_VARS
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
*	2021-02-16: Kalind Patel - DT4 RT18 - Cap Realized EAD and results at 125%
*
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();


proc sql;
connect using nzrrap as nzcon;
create table pd as select * from connection to nzcon(
	SELECT PROCESS_MTH_TM_ID, 
/*	OBS_MTH_TM_ID AS PD_OBS_MTH_TM_ID,  RRMSS-3668*/
	DT4_EXPSR_CL_KEY_VAL, sum(MODEL_DFT_F*(drawn+undrawn)) AS PREDICTED_EAD_DEFLTRS,  sum(drawn+undrawn) AS PREDICTED_EAD_PORT,
	((sum(MODEL_DFT_F*(drawn+undrawn)))/(sum(drawn+undrawn)))*4 AS OBSERVED_PD
	FROM &RRAP_DB..DT4_RT18_RLZ_PDEAD_DRVD_VARS
	WHERE PROCESS_MTH_TM_ID = &mth_tm_id. AND OBS_MTH_TM_ID = (&mth_tm_id.-40*3) AND OBS_PIT_STAT_CD = 'CUR'
	GROUP BY PROCESS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL /* OBS_MTH_TM_ID RRMSS-3668*/
	ORDER BY PROCESS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL /* OBS_MTH_TM_ID RRMSS-3668*/
);
quit;
proc sort data=pd; by PROCESS_MTH_TM_ID DT4_EXPSR_CL_KEY_VAL; run;

proc sql;
connect using nzrrap as nzcon;
create table lgd as select * from connection to nzcon(
	SELECT PROCESS_MTH_TM_ID 
/*	OBS_MTH_TM_ID AS LGD_OBS_MTH_TM_ID RRMSS-3668*/
	, DT4_EXPSR_CL_KEY_VAL 
	,sum(LAST_NEW_DEFAULT_OS_BAL_AMT * RLZ_LGD_RTO) AS sum_RLZEAD_X_LGD_RTO
	,sum(LAST_NEW_DEFAULT_OS_BAL_AMT) AS sum_LGD_PR_3M_LAST_DEFAULT_OS_BAL_AMT
	,sum(LAST_NEW_DEFAULT_OS_BAL_AMT * RLZ_LGD_RTO) / sum(LAST_NEW_DEFAULT_OS_BAL_AMT) AS OBSERVED_LGD
	FROM &RRAP_DB..DT4_RT18_RLZ_LGD_DRVD_VARS /* TESTING WITH  RRMSS-3632*/
	WHERE PROCESS_MTH_TM_ID = &mth_tm_id. 
/*	AND OBS_MTH_TM_ID = (&mth_tm_id.-40*36) 	RRMSS-3632 COMMENTING THIS CODE PER REQUIREMENT*/
	AND MODEL_DFT_F = 1 AND rlz_lgd_rto IS NOT NULL 
	GROUP BY PROCESS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL /* OBS_MTH_TM_ID RRMSS-3668*/
	ORDER BY PROCESS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL /* OBS_MTH_TM_ID RRMSS-3668*/
);
quit;
proc sort data=lgd; by PROCESS_MTH_TM_ID DT4_EXPSR_CL_KEY_VAL; run;


/*2024-02-16: Kalind Patel - DT4 RT18 - Cap Realized EAD and results at 125%*/
proc sql;
connect using nzrrap as nzcon;
create table ead as select * from connection to nzcon(
	SELECT PROCESS_MTH_TM_ID 
/*	OBS_MTH_TM_ID AS EAD_OBS_MTH_TM_ID RRMSS-3668*/
	, DT4_EXPSR_CL_KEY_VAL
	,sum(MIN((LAST_NEW_DEFAULT_OS_BAL_AMT/EAD_OBS_AUTHORIZED_AMT),1.25) * LAST_NEW_DEFAULT_OS_BAL_AMT) AS RLZEAD_DIVBY_AUTH_x_RLZ
	,sum(LAST_NEW_DEFAULT_OS_BAL_AMT) AS RLZEAD
	,sum(MIN((LAST_NEW_DEFAULT_OS_BAL_AMT/EAD_OBS_AUTHORIZED_AMT),1.25) * LAST_NEW_DEFAULT_OS_BAL_AMT) / sum(LAST_NEW_DEFAULT_OS_BAL_AMT) AS OBSERVED_EAD 
	FROM &RRAP_DB..DT4_RT18_RLZ_PDEAD_DRVD_VARS
	WHERE PROCESS_MTH_TM_ID = &mth_tm_id. AND OBS_MTH_TM_ID = (&mth_tm_id.-40*3) AND OBS_PIT_STAT_CD = 'CUR' AND SRC_SYS_CD = 'KS' and EAD_INCL = 1
	GROUP BY PROCESS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL /* OBS_MTH_TM_ID RRMSS-3668*/
	ORDER BY PROCESS_MTH_TM_ID, DT4_EXPSR_CL_KEY_VAL /* OBS_MTH_TM_ID RRMSS-3668*/
);
quit;
proc sort data=ead; by PROCESS_MTH_TM_ID DT4_EXPSR_CL_KEY_VAL; run;

data DT4_RT18_RLZ_VARS;
	retain process_mth_tm_id DT4_EXPSR_CL_KEY_VAL;
	merge pd lgd ead;
	by process_mth_tm_id DT4_EXPSR_CL_KEY_VAL;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
run;

proc sql;
	connect using nzrrap as nzcon;
	execute(delete from &RRAP_DB..DT4_RT18_RLZ_VARS where process_mth_tm_id = &mth_tm_id.) by nzcon;
	execute(commit;) by nzcon;
quit;

proc append base=nzrrap.DT4_RT18_RLZ_VARS data=DT4_RT18_RLZ_VARS force; run;

