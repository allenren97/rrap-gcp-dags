***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0510_DT4_RT20_RLZ_DRVD_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT20_RLZ_DRVD_VARS
*  
*  Purpose: Derive RT18 Realized variables to be used in final aggregation
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-11-15: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();


proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_RT20_RLZ_DRVD_VARS where process_mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;



proc sql;
connect using nzrrap as nzcon;
execute(insert into &RRAP_DB..DT4_RT20_RLZ_DRVD_VARS  
(SELECT a.PROCESS_MTH_TM_ID, a.OBS_MTH_TM_ID, a.SRC_SYS_CD, a.BASEL_ACCT_ID, a.MORT_NO, a.DT4_RISK_RT_KEY_VAL, a.DT4_EXPSR_CL_KEY_VAL
,x.PD_BASEL_SEG_NUM, x.PD_BASEL_SEG_ID, x.PD_BASEL_MODEL_ID, a.MODEL_DFT_F
,p.final_rto AS EST_PD_FINAL_RPTG_RTO
,c.ADJ_FOR_CRM, c.NETEAD_DRAWN AS EAD_After_CRM_Drawn, c.NETEAD_UNDRAWN as EAD_After_CRM_Undrawn
,now() as INSRT_PROCESS_TMSTMP
,now() as UPDT_PROCESS_TMSTMP

FROM &RRAP_DB..DT4_PD12_RLZ_DRVD_VARS a LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS B 
ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND a.OBS_MTH_TM_ID = b.MTH_TM_ID
LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF x
	on a.basel_acct_id = x.basel_acct_id and a.OBS_MTH_TM_ID = x.mth_tm_id AND &YRMTH. BETWEEN cast(x.EFF_FROM_YR_MTH AS integer) AND cast(x.EFF_TO_YR_MTH AS integer)
LEFT JOIN &RRAP_DB..DT4_RT18_EST_ER_VARS c
ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID AND a.PROCESS_MTH_TM_ID = c.MTH_TM_ID
LEFT JOIN &RRAP_DB..BASEL_SEG_RPTG_PARM P 
ON x.PD_BASEL_SEG_ID = p.BASEL_SEG_ID AND x.PD_BASEL_MODEL_ID = p.BASEL_MODEL_ID AND %nrbquote('&MTH_END_DT_NZ.') BETWEEN p.EFF_FROM_DT  AND p.EFF_TO_DT 

WHERE a.OBS_MTH_TM_ID = &mth_tm_id.-12*40 AND x.DT4_PD_MODEL_INCL_F = 1);) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT20_RLZ_DRVD_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;


