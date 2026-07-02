***************************************************************************************************************************;
%let etls_jobname = J_RRAP_DT4_0620_DT4_RT30_FINAL_RPTG_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;

*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT30_FINAL_RPTG_VARS
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
*	2024-01-11: Kalind Patel - RRMSS-2396 - Changes to DT-4/BRDR - Breach Calculation
*   2025-03-02: Juan Pablo Guerrero - RRMSS-3634 - obs_mth_tm_id derived from DT4_RT30_RLZ_DRVD_VARS
*	2026-02-03: Kalind Patel - RRMSS-3932 - Fix DT4 RT30 Part 1 breach Logic
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
%put "LGD_D_DATE:" &lgdd_date.;
%put "LGD_ND_DATE:" &lgdnd_date.;

proc sql;
	connect using nzrrap as nzcon;
	create table SEGMENT_AGG as select * from connection to nzcon(
		select PART1.*, PART2.expsr_per_model from (
			WITH preagg AS (
		SELECT
			r.process_mth_tm_id
			,
		CASE
			WHEN r.src_sys_cd = 'TNG-MOR' AND substr(d.DT4_SEG_DESC, 1, 6) = 'LGD-ND' THEN &mth_tm_id.-36 * 40
			WHEN r.src_sys_cd = 'TNG-MOR' AND substr(d.DT4_SEG_DESC, 1, 6) = 'LGD-D' THEN &mth_tm_id.-24 * 40
			ELSE r.obs_mth_tm_id
		END 
	AS obs_mth_tm_id 
		,r.DT4_RISK_RT_KEY_VAL 
		,g.NCR_RISK_RT_DESC AS DT4_RISK_RT_DESC 
		,i.model_nm 
		,d.DT4_SEG_KEY_VAL AS DT4_LGD_SEG_KEY_VAL 
		,d.DT4_SEG_DESC AS DT4_LGD_SEG_DESC 
		,r.LGD_BASEL_SEG_NUM 
		,r.LGD_BASEL_SEG_ID 
		,r.LGD_BASEL_MODEL_ID 
		,
	CASE
		WHEN r.model_dft_f = 1 THEN COALESCE(r.EAD_After_CRM_Drawn, 0)
		WHEN r.model_dft_f IS NULL THEN COALESCE(r.EAD_After_CRM_Drawn, 0)
		ELSE 0
	END 
AS EAD_def 
	,
CASE
	WHEN r.model_dft_f = 0 THEN COALESCE(r.EAD_After_CRM_Drawn, 0)+ COALESCE(r.EAD_After_CRM_UnDrawn, 0)
	ELSE 0
END 
AS EAD_nondef
,r.LGD_NPV_ALL_COST_FLR_CAP_150_RTO 
,r.EST_LGD_FINAL_RPTG_RTO
FROM &RRAP_DB..DT4_RT30_RLZ_DRVD_VARS r
LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL i
	ON r.LGD_BASEL_SEG_ID = i.BASEL_SEG_ID AND r.LGD_BASEL_MODEL_ID = i.BASEL_MODEL_ID AND &YRMTH. BETWEEN cast(i.EFF_FROM_YR_MTH AS integer) AND cast(i.EFF_TO_YR_MTH AS integer)
LEFT JOIN &RRAP_DB..DT4_SEG_DIM d 
	ON d.rrap_seg_num = r.LGD_BASEL_SEG_NUM and r.model_type = d.model_type AND &YRMTH. BETWEEN cast(d.EFF_FROM_YR_MTH AS integer) AND cast(d.EFF_TO_YR_MTH AS integer)
LEFT JOIN &RRAP_DB..RPTG_RISK_RT_SYS_DIM g 
	ON r.DT4_RISK_RT_KEY_VAL = g.NCR_RISK_RT_KEY_VAL AND &YRMTH. BETWEEN CAST(g.EFF_FROM_YR_MTH AS integer) AND cast(g.EFF_TO_YR_MTH AS integer)
WHERE r.process_mth_tm_id = &mth_tm_id.
	) 
SELECT process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
	,count(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) as accounts
	,avg(EST_LGD_FINAL_RPTG_RTO) as PREDICTED_LGD
	,avg(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) AS REALIZED_LGD
	,stddev(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) as calc_std
	,sum(EAD_def)/1000000 AS EAD_def
	,sum(EAD_nondef)/1000000 AS EAD_nondef
	,now() as INSRT_PROCESS_TMSTMP ,now() as UPDT_PROCESS_TMSTMP
FROM preagg
	GROUP BY process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
		/*ORDER BY process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID*/
	ORDER BY DT4_LGD_SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL
		) PART1
	LEFT JOIN (
		SELECT SEG.DT4_RISK_RT_KEY_VAL, SEG.DT4_LGD_SEG_KEY_VAL, SEG.ead_seg, MDL.ead_model, (ead_seg/ead_model) AS expsr_per_model FROM 
			(
		SELECT DT4_RISK_RT_KEY_VAL , DT4_LGD_SEG_KEY_VAL,model_nm, count(1) AS count, round(sum(ead)/1,0) AS ead_seg
			FROM (
				SELECT  * FROM  (
				SELECT d.process_mth_tm_id, a.MTH_TM_ID, a.SRC_SYS_CD,a.BASEL_ACCT_ID, a.MORT_NUM,a.DT4_RISK_RT_KEY_VAL,a.DT4_EXPSR_CL_KEY_VAL,a.BCAR_SCHED_NUM, d.DT4_LGD_SEG_KEY_VAL, d.DT4_LGD_SEG_DESC, 
					case 
						when a.pit_Stat_cd = 'CUR' then (c.NETEAD_DRAWN + c.NETEAD_UNDRAWN) 
						else c.NETEAD_DRAWN 
					end 
				as EAD, d.model_nm, d.LGD_BASEL_SEG_ID, a.pit_Stat_cd
					FROM &RRAP_DB..DT4_RPTG_DRVD_VARS a 
						LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF b ON a.MTH_TM_ID = b.mth_tm_id AND a.BASEL_ACCT_ID = b.basel_acct_id AND &YRMTH. BETWEEN b.EFF_FROM_YR_MTH AND b.EFF_TO_YR_MTH
						LEFT JOIN &RRAP_DB..DT4_RT18_EST_ER_VARS c ON a.MTH_TM_ID  = c.MTH_TM_ID AND a.BASEL_ACCT_ID = c.basel_acct_id
						LEFT JOIN  
							(
						select *,substr(DT4_LGD_SEG_DESC,1,5) from (
							WITH segagg AS (
						SELECT
							r.process_mth_tm_id
							,
						CASE
							WHEN r.src_sys_cd = 'TNG-MOR' AND substr(d.DT4_SEG_DESC, 1, 6) = 'LGD-ND' THEN &mth_tm_id.-36 * 40
							WHEN r.src_sys_cd = 'TNG-MOR' AND substr(d.DT4_SEG_DESC, 1, 6) = 'LGD-D' THEN &mth_tm_id.-24 * 40
							ELSE r.obs_mth_tm_id
						END 
					AS obs_mth_tm_id 
						,r.DT4_RISK_RT_KEY_VAL 
						,g.NCR_RISK_RT_DESC AS DT4_RISK_RT_DESC 
						,i.model_nm 
						,d.DT4_SEG_KEY_VAL AS DT4_LGD_SEG_KEY_VAL 
						,d.DT4_SEG_DESC AS DT4_LGD_SEG_DESC 
						,r.LGD_BASEL_SEG_NUM 
						,r.LGD_BASEL_SEG_ID 
						,r.LGD_BASEL_MODEL_ID 
						,
					CASE
						WHEN r.model_dft_f = 1 THEN COALESCE(r.EAD_After_CRM_Drawn, 0)
						WHEN r.model_dft_f IS NULL THEN COALESCE(r.EAD_After_CRM_Drawn, 0)
						ELSE 0
					END 
				AS EAD_def 
					,
				CASE
					WHEN r.model_dft_f = 0 THEN COALESCE(r.EAD_After_CRM_Drawn, 0)+ COALESCE(r.EAD_After_CRM_UnDrawn, 0)
					ELSE 0
				END 
			AS EAD_nondef
				,r.LGD_NPV_ALL_COST_FLR_CAP_150_RTO 
				,r.EST_LGD_FINAL_RPTG_RTO
			FROM &RRAP_DB..DT4_RT30_RLZ_DRVD_VARS r
				LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL i
					ON r.LGD_BASEL_SEG_ID = i.BASEL_SEG_ID AND r.LGD_BASEL_MODEL_ID = i.BASEL_MODEL_ID AND &YRMTH. BETWEEN cast(i.EFF_FROM_YR_MTH AS integer) AND cast(i.EFF_TO_YR_MTH AS integer)
				LEFT JOIN &RRAP_DB..DT4_SEG_DIM d 
					ON d.rrap_seg_num = r.LGD_BASEL_SEG_NUM and r.model_type = d.model_type AND &YRMTH. BETWEEN cast(d.EFF_FROM_YR_MTH AS integer) AND cast(d.EFF_TO_YR_MTH AS integer)
				LEFT JOIN &RRAP_DB..RPTG_RISK_RT_SYS_DIM g 
					ON r.DT4_RISK_RT_KEY_VAL = g.NCR_RISK_RT_KEY_VAL AND &YRMTH. BETWEEN CAST(g.EFF_FROM_YR_MTH AS integer) AND cast(g.EFF_TO_YR_MTH AS integer)
				WHERE r.process_mth_tm_id = &mth_tm_id.
			) 
				SELECT process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
					,count(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) as accounts
					,count(1) AS count
					,avg(EST_LGD_FINAL_RPTG_RTO) as PREDICTED_LGD
					,avg(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) AS REALIZED_LGD
					,stddev(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) as calc_std
					,sum(EAD_def)/1000000 AS EAD_def
					,sum(EAD_nondef)/1000000 AS EAD_nondef
					,now() as INSRT_PROCESS_TMSTMP ,now() as UPDT_PROCESS_TMSTMP
				FROM segagg
					GROUP BY process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
						ORDER BY process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
							)
							) d ON  a.MTH_TM_ID = d.PROCESS_MTH_TM_ID AND a.DT4_RISK_RT_KEY_VAL=d.DT4_RISK_RT_KEY_VAL 
							AND 
						CASE 
							WHEN substr(d.DT4_LGD_SEG_DESC,1,6) = 'LGD-ND' AND a.pit_Stat_cd = 'CUR' THEN b.LGD_ND_BASEL_SEG_ID
							WHEN substr(d.DT4_LGD_SEG_DESC,1,5) = 'LGD-D'  AND a.pit_Stat_cd = 'DEF' THEN b.LGD_D_BASEL_SEG_ID 
						END 	
						= d.LGD_BASEL_SEG_ID
						AND 
					CASE 
						WHEN substr(d.DT4_LGD_SEG_DESC,1,6) = 'LGD-ND' AND a.pit_Stat_cd = 'CUR' THEN b.LGD_ND_BASEL_MODEL_ID
						WHEN substr(d.DT4_LGD_SEG_DESC,1,5) = 'LGD-D' AND a.pit_Stat_cd = 'DEF' THEN b.LGD_D_BASEL_MODEL_ID 
					END 	
					= d.LGD_BASEL_MODEL_ID
				WHERE a.mth_tm_id = &mth_tm_id. )
				where 
					EAD IS NOT NULL 
					AND 
					DT4_RISK_RT_KEY_VAL IS NOT NULL AND DT4_LGD_SEG_KEY_VAL IS NOT NULL 
					)
				GROUP BY DT4_LGD_SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL, MODEL_NM
					ORDER BY DT4_LGD_SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL, MODEL_NM
						) SEG 
					LEFT JOIN (				
						SELECT DT4_RISK_RT_KEY_VAL ,MODEL_NM, count(1) AS count, round(sum(ead)/1,0) AS ead_model
							FROM (
								SELECT  * FROM (
								SELECT d.process_mth_tm_id, a.MTH_TM_ID, a.SRC_SYS_CD,a.BASEL_ACCT_ID, a.MORT_NUM,a.DT4_RISK_RT_KEY_VAL,a.DT4_EXPSR_CL_KEY_VAL,a.BCAR_SCHED_NUM, d.DT4_LGD_SEG_KEY_VAL, d.DT4_LGD_SEG_DESC,
									case 
										when a.pit_Stat_cd = 'CUR' then (c.NETEAD_DRAWN + c.NETEAD_UNDRAWN) 
										else c.NETEAD_DRAWN 
									end 
								as EAD, d.model_nm, d.LGD_BASEL_SEG_ID, a.pit_Stat_cd
									FROM &RRAP_DB..DT4_RPTG_DRVD_VARS a 
										LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF b ON a.MTH_TM_ID = b.mth_tm_id AND a.BASEL_ACCT_ID = b.basel_acct_id AND &YRMTH. BETWEEN b.EFF_FROM_YR_MTH AND b.EFF_TO_YR_MTH
										LEFT JOIN &RRAP_DB..DT4_RT18_EST_ER_VARS c ON a.MTH_TM_ID  = c.MTH_TM_ID AND a.BASEL_ACCT_ID = c.basel_acct_id
										LEFT JOIN 
											(
										select * from (
											WITH mdlagg AS (
										SELECT
											r.process_mth_tm_id
											,
										CASE
											WHEN r.src_sys_cd = 'TNG-MOR' AND substr(d.DT4_SEG_DESC, 1, 6) = 'LGD-ND' THEN &mth_tm_id.-36 * 40
											WHEN r.src_sys_cd = 'TNG-MOR' AND substr(d.DT4_SEG_DESC, 1, 6) = 'LGD-D' THEN &mth_tm_id.-24 * 40
											ELSE r.obs_mth_tm_id
										END 
									AS obs_mth_tm_id 
										,r.DT4_RISK_RT_KEY_VAL 
										,g.NCR_RISK_RT_DESC AS DT4_RISK_RT_DESC 
										,i.model_nm 
										,d.DT4_SEG_KEY_VAL AS DT4_LGD_SEG_KEY_VAL 
										,d.DT4_SEG_DESC AS DT4_LGD_SEG_DESC 
										,r.LGD_BASEL_SEG_NUM 
										,r.LGD_BASEL_SEG_ID 
										,r.LGD_BASEL_MODEL_ID 
										,
									CASE
										WHEN r.model_dft_f = 1 THEN COALESCE(r.EAD_After_CRM_Drawn, 0)
										WHEN r.model_dft_f IS NULL THEN COALESCE(r.EAD_After_CRM_Drawn, 0)
										ELSE 0
									END 
								AS EAD_def 
									,
								CASE
									WHEN r.model_dft_f = 0 THEN COALESCE(r.EAD_After_CRM_Drawn, 0)+ COALESCE(r.EAD_After_CRM_UnDrawn, 0)
									ELSE 0
								END 
							AS EAD_nondef
								,r.LGD_NPV_ALL_COST_FLR_CAP_150_RTO 
								,r.EST_LGD_FINAL_RPTG_RTO
							FROM 

/*	2026-02-03: Kalind Patel - Fix DT4 RT30 Part 1 breach Logic*/							
							(with cte as (
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
UNION ALL
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
	) r



								LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL i
									ON r.LGD_BASEL_SEG_ID = i.BASEL_SEG_ID AND r.LGD_BASEL_MODEL_ID = i.BASEL_MODEL_ID AND &YRMTH. BETWEEN cast(i.EFF_FROM_YR_MTH AS integer) AND cast(i.EFF_TO_YR_MTH AS integer)
								LEFT JOIN &RRAP_DB..DT4_SEG_DIM d 
									ON d.rrap_seg_num = r.LGD_BASEL_SEG_NUM and r.model_type = d.model_type AND &YRMTH. BETWEEN cast(d.EFF_FROM_YR_MTH AS integer) AND cast(d.EFF_TO_YR_MTH AS integer)
								LEFT JOIN &RRAP_DB..RPTG_RISK_RT_SYS_DIM g 
									ON r.DT4_RISK_RT_KEY_VAL = g.NCR_RISK_RT_KEY_VAL AND &YRMTH. BETWEEN CAST(g.EFF_FROM_YR_MTH AS integer) AND cast(g.EFF_TO_YR_MTH AS integer)
								WHERE r.process_mth_tm_id = &mth_tm_id.
											) 
												SELECT process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
													,count(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) as accounts
													,count(1) AS count
													,avg(EST_LGD_FINAL_RPTG_RTO) as PREDICTED_LGD
													,avg(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) AS REALIZED_LGD
													,stddev(LGD_NPV_ALL_COST_FLR_CAP_150_RTO) as calc_std
													,sum(EAD_def)/1000000 AS EAD_def
													,sum(EAD_nondef)/1000000 AS EAD_nondef
													,now() as INSRT_PROCESS_TMSTMP ,now() as UPDT_PROCESS_TMSTMP
												FROM mdlagg
													GROUP BY process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
														ORDER BY process_mth_tm_id, obs_mth_tm_id, DT4_RISK_RT_KEY_VAL, DT4_RISK_RT_DESC,model_nm, DT4_LGD_SEG_KEY_VAL, DT4_LGD_SEG_DESC, LGD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, LGD_BASEL_MODEL_ID
															)
															) d ON  a.MTH_TM_ID = d.PROCESS_MTH_TM_ID AND a.DT4_RISK_RT_KEY_VAL=d.DT4_RISK_RT_KEY_VAL 
															AND 
														CASE 
															WHEN substr(d.DT4_LGD_SEG_DESC,1,6) = 'LGD-ND' AND a.pit_Stat_cd = 'CUR' THEN b.LGD_ND_BASEL_SEG_ID
															WHEN substr(d.DT4_LGD_SEG_DESC,1,5) = 'LGD-D'  AND a.pit_Stat_cd = 'DEF' THEN b.LGD_D_BASEL_SEG_ID 
														END 	
														= d.LGD_BASEL_SEG_ID
														AND 
													CASE 
														WHEN substr(d.DT4_LGD_SEG_DESC,1,6) = 'LGD-ND' AND a.pit_Stat_cd = 'CUR' THEN b.LGD_ND_BASEL_MODEL_ID
														WHEN substr(d.DT4_LGD_SEG_DESC,1,5) = 'LGD-D' AND a.pit_Stat_cd = 'DEF' THEN b.LGD_D_BASEL_MODEL_ID 
													END 	
													= d.LGD_BASEL_MODEL_ID
												WHERE a.mth_tm_id = &mth_tm_id. )
												where 
													EAD IS NOT NULL 
													AND 
													DT4_RISK_RT_KEY_VAL IS NOT NULL AND DT4_LGD_SEG_KEY_VAL IS NOT NULL 
													)
												GROUP BY DT4_RISK_RT_KEY_VAL, MODEL_NM
													ORDER BY DT4_RISK_RT_KEY_VAL, MODEL_NM												
														) MDL
														ON SEG.DT4_RISK_RT_KEY_VAL = MDL.DT4_RISK_RT_KEY_VAL AND SEG.MODEL_NM = MDL.MODEL_NM
													ORDER BY SEG.DT4_RISK_RT_KEY_VAL, DT4_LGD_SEG_KEY_VAL
		) PART2
														ON PART1.DT4_RISK_RT_KEY_VAL = PART2.DT4_RISK_RT_KEY_VAL AND part1.DT4_LGD_SEG_KEY_VAL=part2.DT4_LGD_SEG_KEY_VAL 							
													ORDER BY PART1.DT4_RISK_RT_KEY_VAL,PART1.DT4_LGD_SEG_KEY_VAL, PART1.MODEL_NM
														);
quit;

data portfolio_calibration;
	set SEGMENT_AGG;
	defaulters = accounts;
	tvalue = tinv(0.95,defaulters-1);
	lower_ci = REALIZED_LGD - tvalue * (calc_std/sqrt(defaulters));
	upper_ci = REALIZED_LGD + tvalue * (calc_std/sqrt(defaulters));
	breach= PREDICTED_LGD < lower_ci and EXPSR_PER_MODEL>=0.02;
run;

proc sql;
	connect using nzrrap as nzcon;
	execute(delete from &RRAP_DB..DT4_RT30_FINAL_RPTG_VARS where process_mth_tm_id = &mth_tm_id.;
	) by nzcon;
	execute(commit;
	) by nzcon;
quit;


proc append base=nzrrap.DT4_RT30_FINAL_RPTG_VARS data=portfolio_calibration force;
run;