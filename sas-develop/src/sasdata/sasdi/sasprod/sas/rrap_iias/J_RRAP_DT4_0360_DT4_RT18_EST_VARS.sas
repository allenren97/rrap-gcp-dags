***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0420_DT4_RT18_EST_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_EST_VARS
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
*   2022-01-13: Hadi Dimashkieh - Add new column Weighted_PD_beforeCRM and drop Weighted_PD_afterCRM
*	2023-03-27: Hadi Dimashkieh - RRMSS-2082. Change EAD_FINAL_RPTG_RTO to EAD_FLRD_RPTG_RTO. Use DLGD_RPTG_RTO
*	2023-10-12: Hadi Dimashkieh - Convert job to monthly
*
***************************************************************************************************************************;


%rrap_dt4_autoexec(FREQ=MONTH);


proc sql;
connect using nzrrap as nzcon;
create table RT18_EST_VARS_01 as select * from connection to nzcon(
WITH RMA_VARS AS (
SELECT basel_acct_id
	,PD_BAND
	,PD_VALUE
	,a.DT4_EXPSR_CL_KEY_VAL
	,b.NCR_EXPSR_CL_DESC AS EXPOSURE_CLASS
	,a.DLGD_RPTG_RTO AS DLGD_Value
	,a.EAD_FLRD_RPTG_RTO AS EAD_F
	,EAD_INCL
	,NETEAD_BEFORECRM_DRAWN /******************NEW!!;*/
	,NETEAD_DRAWN AS EAD_After_CRM_Drawn
	,NETEAD_UNDRAWN AS EAD_After_CRM_Undrawn
	,RWA_DRAWN + RWA_UNDRAWN AS RWA_Total_After_CRM
	,EXPOSURE_DRAWN * RETAIL_RW + RWA_UNDRAWN AS RWA_Total_Before_CRM
	
FROM &RRAP_DB..DT4_RT18_EST_ER_VARS a LEFT JOIN &RRAP_DB..RPTG_EXPSR_CL_DIM b
	ON a.DT4_EXPSR_CL_KEY_VAL = b.NCR_EXPSR_CL_KEY_VAL AND &yrmth BETWEEN cast(b.EFF_FROM_YR_MTH as integer) AND cast(b.EFF_TO_YR_MTH as integer)
WHERE mth_tm_id = &mth_tm_id.
)

SELECT 
a.DT4_EXPSR_CL_KEY_VAL, a.EXPOSURE_CLASS , count(1) AS OBLIGORS
,CASE  
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501','5502') THEN 
		sum(b.PD_Value*(b.NETEAD_BEFORECRM_DRAWN))/ sum(b.NETEAD_BEFORECRM_DRAWN)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506')
		THEN sum(b.PD_Value*(b.NETEAD_BEFORECRM_DRAWN+b.EAD_After_CRM_Undrawn))/ sum(b.NETEAD_BEFORECRM_DRAWN+b.EAD_after_CRM_Undrawn)
	ELSE NULL 
END AS Weighted_PD_beforeCRM


,CASE 
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501','5502') THEN 
		sum(b.DLGD_Value*(b.NETEAD_BEFORECRM_DRAWN))/ sum(b.NETEAD_BEFORECRM_DRAWN) 
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506') THEN 
		sum(b.DLGD_Value*(b.NETEAD_BEFORECRM_DRAWN+b.EAD_After_CRM_Undrawn))/ sum(b.NETEAD_BEFORECRM_DRAWN+b.EAD_After_CRM_Undrawn)
END AS Weighted_LGD_beforeCRM

	
,CASE 
	when a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508') then 
		sum(d.EAD_F*(d.EAD_After_CRM_Drawn+d.EAD_After_CRM_Undrawn))/ sum(d.EAD_After_CRM_Drawn+d.EAD_After_CRM_Undrawn)
	else NULL 
end as Weighted_EAD_afterCRM

,CASE  
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501','5502') THEN sum(b.EAD_After_CRM_Drawn)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506') THEN sum(b.EAD_After_CRM_Drawn+b.EAD_After_CRM_Undrawn)
	ELSE NULL 
END AS EAD_afterCRM_nodef

,CASE 
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501','5502') THEN sum(c.EAD_After_CRM_Drawn)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506') THEN sum(c.EAD_After_CRM_Drawn+c.EAD_After_CRM_Undrawn)
	ELSE NULL 
END AS EAD_afterCRM_def

,CASE 
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501') THEN sum(b.RWA_Total_After_CRM)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5502') THEN sum(b.RWA_Total_Before_CRM)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506') THEN sum(b.RWA_Total_Before_CRM)
	ELSE NULL 
END AS RWA_nodef

,CASE  
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501') THEN sum(c.RWA_Total_After_CRM)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5502') THEN sum(c.RWA_Total_Before_CRM)
	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506') THEN sum(c.RWA_Total_Before_CRM)
	ELSE NULL 
END AS RWA_def

/*,CASE */
/*	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5501') THEN sum(b.RWA_Total_After_CRM)+sum(c.RWA_Total_After_CRM)*/
/*	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5502') THEN sum(b.RWA_Total_Before_CRM)+sum(c.RWA_Total_Before_CRM)*/
/*	WHEN a.DT4_EXPSR_CL_KEY_VAL in ('5503', '5504', '5505', '5508', '5506') THEN sum(c.RWA_Total_Before_CRM)+sum(b.RWA_Total_Before_CRM)*/
/*	ELSE NULL */
/*END AS RWA*/


FROM RMA_VARS a
	left join RMA_VARS b
		on a.basel_acct_id = b.basel_acct_id and b.pd_band <> 26 
	left join RMA_VARS c 
		on a.basel_acct_id = c.basel_acct_id and c.pd_band = 26
	left join RMA_VARS d 
		on a.basel_acct_id = d.basel_acct_id and d.pd_band <> 26 and d.EAD_INCL = 1
		
GROUP BY  a.DT4_EXPSR_CL_KEY_VAL, a.EXPOSURE_CLASS
ORDER BY a.DT4_EXPSR_CL_KEY_VAL, a.EXPOSURE_CLASS;);
quit;

data RT18_EST_VARS_02;
retain mth_tm_id;
	set RT18_EST_VARS_01;
	mth_tm_id = &mth_tm_id.;
	*EAD_afterCRM = sum(EAD_afterCRM_nodef, EAD_afterCRM_def);

	*EAD_afterCRM_nodef=EAD_afterCRM_nodef;
	*EAD_afterCRM_def=EAD_afterCRM_def;
	RWA = sum(RWA_nodef, RWA_def);
	RWA_wScalar = sum(RWA_nodef, RWA_def)*1.06;
	
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
run;

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_RT18_EST_VARS where mth_tm_id=&mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=NZRRAP.DT4_RT18_EST_VARS data=RT18_EST_VARS_02 force; run;

