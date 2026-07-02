***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0610_DT4_RT30_RLZ_DRVD_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT30_RLZ_DRVD_VARS
*  
*  Purpose: Derive RT30 Realized variables to be used in final aggregation
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-11-15: Hadi Dimashkieh - Initial Development
*	2024-10-04: Ganesh Patro - LGD Realized integration from GCP
*   2025-03-26: Juan Pablo Guerrero - RRMSS-3633 - LGD Realized changes
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();


proc sql noprint;
	select td.tm_lvl_end_dt format yymmdd10., tnd.tm_lvl_end_dt format yymmdd10. into :lgdd_date, :lgdnd_date
	from nzrrap.tm_dim t
		left join nzrrap.tm_dim td on (t.tm_id-24*40) = td.tm_id
		left join nzrrap.tm_dim tnd on (t.tm_id-36*40) = tnd.tm_id
	where t.tm_lvl='Month' and t.tm_id = &mth_tm_id.;
quit;
%put &lgdd_date. &lgdnd_date.;

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_RT30_RLZ_DRVD_VARS where process_mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(insert into &RRAP_DB..DT4_RT30_RLZ_DRVD_VARS  

with cte as (
SELECT a.process_mth_tm_id, a.OBS_MTH_TM_ID, a.src_sys_cd, a.basel_acct_id, a.MORT_NUM
,d.DT4_RISK_RT_KEY_VAL, d.DT4_EXPSR_CL_KEY_VAL
,CASE 
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -36*40 THEN 'LGD-ND'
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -24*40 THEN 'LGD-D'
	WHEN a.LGD_NON_DEFAULTER_F = 'Y' THEN 'LGD-ND'
	WHEN a.LGD_DEFAULTER_F = 'Y' THEN 'LGD-D'
	ELSE NULL 
END AS MODEL_TYPE
,CASE 
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -36*40 THEN x.LGD_ND_BASEL_SEG_NUM
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -24*40 THEN x.LGD_D_BASEL_SEG_NUM
	WHEN a.LGD_NON_DEFAULTER_F = 'Y' THEN x.LGD_ND_BASEL_SEG_NUM
	WHEN a.LGD_DEFAULTER_F = 'Y' THEN x.LGD_D_BASEL_SEG_NUM
	ELSE NULL 
END AS LGD_BASEL_SEG_NUM
,CASE 
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -36*40 THEN x.LGD_ND_BASEL_SEG_ID
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -24*40 THEN x.LGD_D_BASEL_SEG_ID
	WHEN a.LGD_NON_DEFAULTER_F = 'Y' THEN x.LGD_ND_BASEL_SEG_ID
	WHEN a.LGD_DEFAULTER_F = 'Y' THEN x.LGD_D_BASEL_SEG_ID
	ELSE NULL 
END AS LGD_BASEL_SEG_ID
,CASE 
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -36*40 THEN x.LGD_ND_BASEL_MODEL_ID
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -24*40 THEN x.LGD_D_BASEL_MODEL_ID
	WHEN a.LGD_BASEL_MODEL_ID IS NOT NULL THEN a.LGD_BASEL_MODEL_ID
	ELSE NULL 
END AS LGD_BASEL_MODEL_ID
,a.LGD_NPV_ALL_COST_FLR_CAP_150_RTO
,c1.MODEL_DFT_F
,c.ADJ_FOR_CRM, c.NETEAD_DRAWN AS EAD_After_CRM_Drawn, c.NETEAD_UNDRAWN AS EAD_After_CRM_UnDrawn


FROM (
SELECT &mth_tm_id. AS PROCESS_MTH_TM_ID, b.mth_tm_id AS OBS_MTH_TM_ID, b.src_sys_cd, b.basel_acct_id, b.MORT_NUM, LGD_NPV_ALL_COST_FLR_CAP_150_RTO
,NULL AS LGD_NON_DEFAULTER_F, NULL AS LGD_DEFAULTER_F, NULL AS LGD_BASEL_MODEL_ID
FROM 
(

SELECT month_end_dt AS TIME_KEY, account_id AS mort_num, LGD_COSTS_CAP_150 as LGD_NPV_ALL_COST_FLR_CAP_150_RTO 
FROM &MOR_DB..TNG_LGD_ND_REALIZED_FINAL WHERE month_end_dt = %nrbquote('&lgdnd_date')
UNION ALL 
SELECT month_end_dt AS TIME_KEY, account_id AS mort_num, LGD_COSTS_CAP_150 as LGD_NPV_ALL_COST_FLR_CAP_150_RTO 
FROM &MOR_DB..TNG_LGDD_SEGMENTATION WHERE month_end_dt = %nrbquote('&lgdd_date') AND lgd IS NOT NULL 
) a 
		LEFT JOIN &RRAP_DB..TM_DIM t ON a.time_key = t.TM_LVL_END_DT AND t.tm_lvl='Month'
		INNER JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS b ON trim(a.MORT_NUM) = trim(b.MORT_NUM) AND t.tm_id = b.MTH_TM_ID 

UNION ALL
	SELECT p_tm.tm_id AS PROCESS_MTH_TM_ID , o_tm.tm_id AS OBS_MTH_TM_ID, a.src_sys_cd, a.BASEL_ACCT_ID, e.MORT_NUM, a.RLZ_LGDC_CAP_125_RTO AS LGD_NPV_ALL_COST_FLR_CAP_150_RTO 
	,a.LGD_NON_DEFAULTER_F, a.LGD_DEFAULTER_F, a.BASEL_MODEL_ID AS LGD_BASEL_MODEL_ID
	FROM &RRAP_LGD..LGD_RLZ_FINAL a
	LEFT JOIN &RRAP_DB..TM_DIM p_tm ON a.PROCESS_MTH = p_tm.TM_LVL_ST_DT AND p_tm.tm_lvl='Month'
	LEFT JOIN &RRAP_DB..TM_DIM o_tm ON a.OBSVTN_MTH = o_tm.TM_LVL_ST_DT AND o_tm.tm_lvl='Month'
	INNER JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS e ON trim(a.basel_acct_id) = trim(e.basel_acct_id) AND o_tm.tm_id = e.MTH_TM_ID
	WHERE a.PROCESS_MTH = %nrbquote('&MTH_ST_DT')
	AND a.SRC_SYS_CD IN ('KS','SPL','MOR')
	AND a.RLZ_LGDC_CAP_125_RTO IS NOT NULL
) a	
	
LEFT JOIN &RRAP_DB..DT4_RT18_EST_ER_VARS c
	ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID AND a.PROCESS_MTH_TM_ID = c.MTH_TM_ID
left join &RRAP_DB..DT4_PD12_RLZ_DRVD_VARS c1
	on a.basel_acct_id = c1.basel_Acct_id and a.PROCESS_MTH_TM_ID = c1.process_MTH_TM_ID
LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS d 
	ON a.basel_acct_id = d.BASEL_ACCT_ID AND a.obs_mth_tm_id = d.MTH_TM_ID
LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF x
	on a.basel_acct_id = x.basel_acct_id and a.OBS_MTH_TM_ID = x.mth_tm_id AND &YRMTH. BETWEEN cast(x.EFF_FROM_YR_MTH AS integer) AND cast(x.EFF_TO_YR_MTH AS integer)


)

SELECT 
		a.PROCESS_MTH_TM_ID, a.OBS_MTH_TM_ID, a.SRC_SYS_CD, a.BASEL_ACCT_ID, a.MORT_NUM
		,a.DT4_RISK_RT_KEY_VAL, a.DT4_EXPSR_CL_KEY_VAL, a.MODEL_TYPE
		,a.LGD_BASEL_SEG_NUM, a.LGD_BASEL_SEG_ID, a.LGD_BASEL_MODEL_ID
		,a.LGD_NPV_ALL_COST_FLR_CAP_150_RTO
		,p.final_rto AS EST_LGD_FINAL_RPTG_RTO
		,a.MODEL_DFT_F
		,a.ADJ_FOR_CRM, a.EAD_AFTER_CRM_DRAWN, a.EAD_AFTER_CRM_UNDRAWN
		,now() as INSRT_PROCESS_TMSTMP ,now() as UPDT_PROCESS_TMSTMP
from cte a
LEFT JOIN &RRAP_DB..BASEL_SEG_RPTG_PARM P 
	ON a.LGD_BASEL_SEG_ID = p.BASEL_SEG_ID AND a.LGD_BASEL_MODEL_ID = p.BASEL_MODEL_ID AND %nrbquote('&mth_end_dt_nz.') BETWEEN p.EFF_FROM_DT  AND p.EFF_TO_DT 
LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF x
	on a.basel_acct_id = x.basel_acct_id and a.OBS_MTH_TM_ID = x.mth_tm_id AND &YRMTH. BETWEEN cast(x.EFF_FROM_YR_MTH AS integer) AND cast(x.EFF_TO_YR_MTH AS integer)
LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL i
	on a.LGD_BASEL_SEG_ID = i.BASEL_SEG_ID AND &YRMTH. BETWEEN cast(i.EFF_FROM_YR_MTH AS integer) AND cast(i.EFF_TO_YR_MTH AS integer)

where CASE 
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -36*40 THEN x.DT4_LGD_ND_MODEL_INCL_F
	WHEN a.src_sys_cd = 'TNG-MOR' AND a.obs_mth_tm_id = &mth_tm_id. -24*40 THEN x.DT4_LGD_D_MODEL_INCL_F
	WHEN a.src_sys_cd in ('KS','SPL','MOR') THEN i.INCL_F
 	ELSE NULL END = 1

;) by nzcon;
execute(COMMIT;) by nzcon;
quit;



/*TANGERINE ADDITIONAL 4 QUARTER ROLLUP*/


proc sql;
connect using NZ_LGD as nzcon;
execute(insert into &RRAP_DB..DT4_RT30_RLZ_DRVD_VARS


SELECT a.process_mth_tm_id, a.OBS_MTH_TM_ID, a.src_sys_cd, a.basel_acct_id, a.MORT_NUM
,d.DT4_RISK_RT_KEY_VAL, d.DT4_EXPSR_CL_KEY_VAL
,a.model AS MODEL_TYPE
,CASE 
	WHEN a.model = 'LGD-ND' THEN x.LGD_ND_BASEL_SEG_NUM
	WHEN a.model = 'LGD-D' THEN x.LGD_D_BASEL_SEG_NUM
 	ELSE NULL 
 END AS LGD_BASEL_SEG_NUM
,CASE 
	WHEN a.model = 'LGD-ND' THEN x.LGD_ND_BASEL_SEG_ID
	WHEN a.model = 'LGD-D' THEN x.LGD_D_BASEL_SEG_ID
 	ELSE NULL 
 END AS LGD_BASEL_SEG_ID
,CASE 
	WHEN a.model = 'LGD-ND' THEN x.LGD_ND_BASEL_MODEL_ID
	WHEN a.model = 'LGD-D' THEN x.LGD_D_BASEL_MODEL_ID
 	ELSE NULL 
 END AS LGD_BASEL_MODEL_ID

 
,a.LGD_NPV_ALL_COST_FLR_CAP_150_RTO
,null as EST_LGD_FINAL_RPTG_RTO
,NULL AS MODEL_DFT_F
,NULL AS ADJ_FOR_CRM, NULL AS EAD_After_CRM_Drawn, NULL AS EAD_After_CRM_UnDrawn
,now() as INSRT_PROCESS_TMSTMP ,now() as UPDT_PROCESS_TMSTMP


FROM (
SELECT &mth_tm_id. AS PROCESS_MTH_TM_ID, b.mth_tm_id AS OBS_MTH_TM_ID, b.src_sys_cd, b.basel_acct_id, b.MORT_NUM,  LGD_NPV_ALL_COST_FLR_CAP_150_RTO,  MODEL
FROM 
(SELECT month_end_dt AS TIME_KEY, account_id AS mort_num, LGD_COSTS_CAP_150 as LGD_NPV_ALL_COST_FLR_CAP_150_RTO , 'LGD-ND' AS model
FROM &MOR_DB..TNG_LGD_ND_REALIZED_FINAL WHERE (month_end_dt BETWEEN LAST_day(add_months(%nrbquote('&lgdnd_date'),-3*3)) AND  LAST_day(add_months(%nrbquote('&lgdnd_date'),-1))) AND month(MONTH_END_DT) IN (1,4,7,10)

UNION ALL 
SELECT month_end_dt AS TIME_KEY, account_id AS mort_num,LGD_COSTS_CAP_150 as LGD_NPV_ALL_COST_FLR_CAP_150_RTO , 'LGD-D' AS model 
FROM &MOR_DB..TNG_LGDD_SEGMENTATION WHERE (month_end_dt between LAST_day(add_months(%nrbquote('&lgdd_date'),-3*3)) AND  LAST_day(add_months(%nrbquote('&lgdd_date'),-1))) AND month(MONTH_END_DT) IN (1,4,7,10) AND lgd IS NOT NULL 
) a 

		LEFT JOIN &RRAP_DB..TM_DIM t ON a.time_key = t.TM_LVL_END_DT AND t.tm_lvl='Month'
		INNER JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS b ON trim(a.MORT_NUM) = trim(b.MORT_NUM) AND t.tm_id = b.MTH_TM_ID 
) a



LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS d 
ON a.basel_acct_id = d.BASEL_ACCT_ID AND a.obs_mth_tm_id = d.MTH_TM_ID 
LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF x
	on a.basel_acct_id = x.basel_acct_id and a.OBS_MTH_TM_ID = x.mth_tm_id AND &YRMTH. BETWEEN cast(x.EFF_FROM_YR_MTH AS integer) AND cast(x.EFF_TO_YR_MTH AS integer)

where CASE 

	WHEN a.model = 'LGD-ND' THEN x.DT4_LGD_ND_MODEL_INCL_F
	WHEN a.model = 'LGD-D' THEN x.DT4_LGD_D_MODEL_INCL_F
 	ELSE NULL END = 1
 	
;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT30_RLZ_DRVD_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit; 
 
 	
 	
 	
 	
