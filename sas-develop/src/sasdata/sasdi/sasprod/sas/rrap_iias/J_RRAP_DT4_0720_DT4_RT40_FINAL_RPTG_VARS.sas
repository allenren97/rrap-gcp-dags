***************************************************************************************************************************;
%let etls_jobname = J_RRAP_DT4_0720_DT4_RT40_FINAL_RPTG_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;

*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT40_FINAL_RPTG_VARS
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
*   2024-01-11: Kalind Patel - RRMSS-2396 - Changes to DT-4/BRDR - Breach Calculation
*	2025-08-20: Kalind Patel - RRMSS-3789 - Realized EAD: DT4 Reports Update
*
***************************************************************************************************************************;
%rrap_dt4_autoexec();



proc sql;
	connect using nzrrap as nzcon;
	create table SEGMENT_AGG as select * from connection to nzcon(
	SELECT
PARTA.PROCESS_MTH_TM_ID ,PARTA.OBS_MTH_TM_ID ,PARTA.DT4_RISK_RT_KEY_VAL ,PARTA.DT4_RISK_RT_DESC ,PARTA.MODEL_NM ,PARTA.DT4_EAD_SEG_KEY_VAL ,PARTA.DT4_EAD_SEG_DESC ,
PARTA.EAD_BASEL_SEG_NUM ,PARTA.EAD_BASEL_SEG_ID ,PARTA.EAD_BASEL_MODEL_ID ,PARTB.MODEL_DEF_F_TOT_CNT as ACCOUNTS ,PARTA.PREDICTED_EAD ,PARTB.AVG_EAD_FLR_CAP_200_RTO as REALIZED_EAD
,PARTB.calc_std as CALC_STD ,PARTA.EAD_DEF ,PARTA.EAD_NONDEF ,PARTA.EXPSR_PER_MODEL, PARTA.INSRT_PROCESS_TMSTMP, PARTA.UPDT_PROCESS_TMSTMP
 FROM
(
		select PART1.*, PART2.expsr_per_model from (
		SELECT rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC AS DT4_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL AS DT4_EAD_SEG_KEY_VAL, d.DT4_SEG_DESC AS DT4_EAD_SEG_DESC
			, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
			,count(rlz.basel_acct_id) AS accounts
			,avg(rlz.EST_EAD_FINAL_RPTG_RTO) AS predicted_ead
			,avg(rlz.EAD_FLR_CAP_200_RTO) as REALIZED_EAD
			,STDDEV(rlz.EAD_FLR_CAP_200_RTO) as calc_std
			,sum(b.EAD_After_CRM_Drawn)/1000000 AS EAD_def
			,sum(c.EAD_After_CRM_Drawn+c.EAD_After_CRM_UnDrawn)/1000000 AS EAD_nondef
			,now() as INSRT_PROCESS_TMSTMP
			,now() as UPDT_PROCESS_TMSTMP
		FROM &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS rlz /* LEFT JOIN &RRAP_DB..DT4_RT40_EST_DRVD_VARS pred
			ON rlz.EAD_BASEL_SEG_ID = pred.EAD_BASEL_SEG_ID AND rlz.EAD_BASEL_MODEL_ID = pred.EAD_BASEL_MODEL_ID AND rlz.process_mth_tm_id = pred.mth_tm_id */
		LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL seg 
			ON rlz.EAD_BASEL_SEG_ID = seg.BASEL_SEG_ID AND rlz.EAD_BASEL_MODEL_ID = seg.BASEL_MODEL_ID  AND &YRMTH. BETWEEN cast(seg.EFF_FROM_YR_MTH AS integer) AND cast(seg.EFF_TO_YR_MTH AS integer)
		LEFT JOIN &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS b ON rlz.basel_acct_id = b.basel_acct_id AND rlz.process_mth_tm_id = b.process_mth_tm_id  AND b.MODEL_DFT_F = 1
		LEFT JOIN &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS c ON rlz.basel_acct_id = c.basel_acct_id AND rlz.process_mth_tm_id = c.process_mth_tm_id  AND c.MODEL_DFT_F = 0
		LEFT JOIN &RRAP_DB..DT4_SEG_DIM d ON d.rrap_seg_num = rlz.EAD_BASEL_SEG_NUM and d.model_type='EAD' AND &YRMTH. BETWEEN cast(d.EFF_FROM_YR_MTH AS integer) AND cast(d.EFF_TO_YR_MTH AS integer)
		LEFT JOIN &RRAP_DB..RPTG_RISK_RT_SYS_DIM g 
			ON rlz.DT4_RISK_RT_KEY_VAL = g.NCR_RISK_RT_KEY_VAL AND &YRMTH. BETWEEN CAST(g.EFF_FROM_YR_MTH AS integer) AND cast(g.EFF_TO_YR_MTH AS integer)
		WHERE rlz.PROCESS_MTH_TM_ID = &mth_tm_id.
			GROUP BY rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL, d.DT4_SEG_DESC, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
				ORDER BY rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL, d.DT4_SEG_DESC, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
					) PART1
				LEFT JOIN (
					SELECT SEG.DT4_RISK_RT_KEY_VAL, SEG.dt4_ead_seg_key_val, SEG.ead_seg, MDL.ead_model, (ead_seg/ead_model) AS expsr_per_model FROM 
						(
					SELECT DT4_EAD_SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL, count(1) AS count, round(sum(ead)/1,0) AS ead_seg
						FROM (
							SELECT * FROM (
							SELECT a.MTH_TM_ID,a.SRC_SYS_CD,a.BASEL_ACCT_ID, a.MORT_NUM,d.DT4_RISK_RT_KEY_VAL,a.DT4_EXPSR_CL_KEY_VAL,a.BCAR_SCHED_NUM,d.DT4_EAD_SEG_KEY_VAL, d.DT4_EAD_SEG_DESC,
								case 
									when a.pit_Stat_cd = 'CUR' then (c.NETEAD_DRAWN + c.NETEAD_UNDRAWN) 
									else c.NETEAD_DRAWN 
								end 
							as EAD
								FROM EDRTLRP1D.DT4_RPTG_DRVD_VARS a
									LEFT JOIN EDRTLRP1D.DT4_SEGMENT_XREF b ON a.MTH_TM_ID = b.mth_tm_id AND a.BASEL_ACCT_ID = b.basel_acct_id AND &YRMTH. BETWEEN b.EFF_FROM_YR_MTH AND b.EFF_TO_YR_MTH 
									LEFT JOIN EDRTLRP1D.DT4_RT18_EST_ER_VARS c ON a.MTH_TM_ID  = c.MTH_TM_ID AND a.BASEL_ACCT_ID = c.basel_acct_id 
									LEFT JOIN 
										(
									SELECT rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC AS DT4_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL AS DT4_EAD_SEG_KEY_VAL, d.DT4_SEG_DESC AS DT4_EAD_SEG_DESC
										, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
										,count(rlz.basel_acct_id) AS accounts
										,avg(rlz.EST_EAD_FINAL_RPTG_RTO) AS predicted_ead
										,avg(rlz.EAD_FLR_CAP_200_RTO) as REALIZED_EAD
										,STDDEV(rlz.EAD_FLR_CAP_200_RTO) as calc_std
										,sum(b.EAD_After_CRM_Drawn)/1000000 AS EAD_def
										,sum(c.EAD_After_CRM_Drawn+c.EAD_After_CRM_UnDrawn)/1000000 AS EAD_nondef
										,now() as INSRT_PROCESS_TMSTMP
										,now() as UPDT_PROCESS_TMSTMP
									FROM &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS rlz /* LEFT JOIN &RRAP_DB..DT4_RT40_EST_DRVD_VARS pred
										ON rlz.EAD_BASEL_SEG_ID = pred.EAD_BASEL_SEG_ID AND rlz.EAD_BASEL_MODEL_ID = pred.EAD_BASEL_MODEL_ID AND rlz.process_mth_tm_id = pred.mth_tm_id */
									LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL seg 
										ON rlz.EAD_BASEL_SEG_ID = seg.BASEL_SEG_ID AND rlz.EAD_BASEL_MODEL_ID = seg.BASEL_MODEL_ID  AND &YRMTH. BETWEEN cast(seg.EFF_FROM_YR_MTH AS integer) AND cast(seg.EFF_TO_YR_MTH AS integer)
									LEFT JOIN &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS b ON rlz.basel_acct_id = b.basel_acct_id AND rlz.process_mth_tm_id = b.process_mth_tm_id  AND b.MODEL_DFT_F = 1
									LEFT JOIN &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS c ON rlz.basel_acct_id = c.basel_acct_id AND rlz.process_mth_tm_id = c.process_mth_tm_id  AND c.MODEL_DFT_F = 0
									LEFT JOIN &RRAP_DB..DT4_SEG_DIM d ON d.rrap_seg_num = rlz.EAD_BASEL_SEG_NUM and d.model_type='EAD' AND &YRMTH. BETWEEN cast(d.EFF_FROM_YR_MTH AS integer) AND cast(d.EFF_TO_YR_MTH AS integer)
									LEFT JOIN &RRAP_DB..RPTG_RISK_RT_SYS_DIM g 
										ON rlz.DT4_RISK_RT_KEY_VAL = g.NCR_RISK_RT_KEY_VAL AND &YRMTH. BETWEEN CAST(g.EFF_FROM_YR_MTH AS integer) AND cast(g.EFF_TO_YR_MTH AS integer)
									WHERE rlz.PROCESS_MTH_TM_ID = &mth_tm_id.
										GROUP BY rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL, d.DT4_SEG_DESC, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
											ORDER BY rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL, d.DT4_SEG_DESC, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
						)d ON a.MTH_TM_ID = d.PROCESS_MTH_TM_ID  AND b.EAD_BASEL_SEG_ID = d.EAD_BASEL_SEG_ID AND b.EAD_BASEL_MODEL_ID = d.EAD_BASEL_MODEL_ID
							WHERE a.mth_tm_id = &mth_tm_id.
								) WHERE EAD IS NOT NULL AND DT4_RISK_RT_KEY_VAL IS NOT NULL AND DT4_EAD_SEG_KEY_VAL IS NOT NULL
								)
							GROUP BY DT4_EAD_SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL
								ORDER BY DT4_EAD_SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL
									) SEG 
								LEFT JOIN (
									SELECT DT4_RISK_RT_KEY_VAL, count(1) AS count, round(sum(ead)/1,0) AS EAD_MODEL
										FROM (
											SELECT * FROM (
											SELECT a.MTH_TM_ID,a.SRC_SYS_CD,a.BASEL_ACCT_ID, a.MORT_NUM,d.DT4_RISK_RT_KEY_VAL,a.DT4_EXPSR_CL_KEY_VAL,a.BCAR_SCHED_NUM,d.DT4_EAD_SEG_KEY_VAL, d.DT4_EAD_SEG_DESC,
												case 
													when a.pit_Stat_cd = 'CUR' then (c.NETEAD_DRAWN + c.NETEAD_UNDRAWN) 
													else c.NETEAD_DRAWN 
												end 
											as EAD
												FROM EDRTLRP1D.DT4_RPTG_DRVD_VARS a
													LEFT JOIN EDRTLRP1D.DT4_SEGMENT_XREF b ON a.MTH_TM_ID = b.mth_tm_id AND a.BASEL_ACCT_ID = b.basel_acct_id AND &YRMTH. BETWEEN b.EFF_FROM_YR_MTH AND b.EFF_TO_YR_MTH 
													LEFT JOIN EDRTLRP1D.DT4_RT18_EST_ER_VARS c ON a.MTH_TM_ID  = c.MTH_TM_ID AND a.BASEL_ACCT_ID = c.basel_acct_id 
													LEFT JOIN 
														(SELECT rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC AS DT4_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL AS DT4_EAD_SEG_KEY_VAL, d.DT4_SEG_DESC AS DT4_EAD_SEG_DESC
															, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
															,count(rlz.basel_acct_id) AS accounts
															,avg(rlz.EST_EAD_FINAL_RPTG_RTO) AS predicted_ead
															,avg(rlz.EAD_FLR_CAP_200_RTO) as REALIZED_EAD
															,STDDEV(rlz.EAD_FLR_CAP_200_RTO) as calc_std
															,sum(b.EAD_After_CRM_Drawn)/1000000 AS EAD_def
															,sum(c.EAD_After_CRM_Drawn+c.EAD_After_CRM_UnDrawn)/1000000 AS EAD_nondef
															,now() as INSRT_PROCESS_TMSTMP
															,now() as UPDT_PROCESS_TMSTMP
														FROM &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS rlz 
															LEFT JOIN &RRAP_DB..DT4_BASEL_SEG_INCL seg 
																ON rlz.EAD_BASEL_SEG_ID = seg.BASEL_SEG_ID AND rlz.EAD_BASEL_MODEL_ID = seg.BASEL_MODEL_ID  AND &YRMTH. BETWEEN cast(seg.EFF_FROM_YR_MTH AS integer) AND cast(seg.EFF_TO_YR_MTH AS integer)
															LEFT JOIN &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS b ON rlz.basel_acct_id = b.basel_acct_id AND rlz.process_mth_tm_id = b.process_mth_tm_id  AND b.MODEL_DFT_F = 1
															LEFT JOIN &RRAP_DB..DT4_RT40_RLZ_DRVD_VARS c ON rlz.basel_acct_id = c.basel_acct_id AND rlz.process_mth_tm_id = c.process_mth_tm_id  AND c.MODEL_DFT_F = 0
															LEFT JOIN &RRAP_DB..DT4_SEG_DIM d ON d.rrap_seg_num = rlz.EAD_BASEL_SEG_NUM and d.model_type='EAD' AND &YRMTH. BETWEEN cast(d.EFF_FROM_YR_MTH AS integer) AND cast(d.EFF_TO_YR_MTH AS integer)
															LEFT JOIN &RRAP_DB..RPTG_RISK_RT_SYS_DIM g 
																ON rlz.DT4_RISK_RT_KEY_VAL = g.NCR_RISK_RT_KEY_VAL AND &YRMTH. BETWEEN CAST(g.EFF_FROM_YR_MTH AS integer) AND cast(g.EFF_TO_YR_MTH AS integer)
															WHERE rlz.PROCESS_MTH_TM_ID = &mth_tm_id.
																GROUP BY rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL, d.DT4_SEG_DESC, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
																	ORDER BY rlz.PROCESS_MTH_TM_ID, rlz.OBS_MTH_TM_ID, rlz.DT4_RISK_RT_KEY_VAL, g.NCR_RISK_RT_DESC, seg.MODEL_NM, d.DT4_SEG_KEY_VAL, d.DT4_SEG_DESC, rlz.EAD_BASEL_SEG_NUM, rlz.EAD_BASEL_SEG_ID, rlz.EAD_BASEL_MODEL_ID
														)d ON a.MTH_TM_ID = d.PROCESS_MTH_TM_ID  AND b.EAD_BASEL_SEG_ID = d.EAD_BASEL_SEG_ID AND b.EAD_BASEL_MODEL_ID = d.EAD_BASEL_MODEL_ID
															WHERE a.mth_tm_id = &mth_tm_id.
																) WHERE EAD IS NOT NULL AND DT4_RISK_RT_KEY_VAL IS NOT NULL AND DT4_EAD_SEG_KEY_VAL IS NOT NULL
																)
															GROUP BY DT4_RISK_RT_KEY_VAL
																ORDER BY DT4_RISK_RT_KEY_VAL
																	) MDL
																	ON SEG.DT4_RISK_RT_KEY_VAL = MDL.DT4_RISK_RT_KEY_VAL
																ORDER BY SEG.DT4_RISK_RT_KEY_VAL,DT4_EAD_SEG_KEY_VAL
																	) PART2
																	ON PART1.DT4_RISK_RT_KEY_VAL = PART2.DT4_RISK_RT_KEY_VAL AND part1.dt4_ead_seg_key_val=part2.dt4_ead_seg_key_val
																ORDER BY PART1.DT4_RISK_RT_KEY_VAL,PART1.DT4_EAD_SEG_KEY_VAL
																	) PARTA 
																		LEFT JOIN &RRAP_DB..EAD_SEG_QTR_REALZ_VAL PARTB
			ON PARTB.BASEL_SEG_ID = PARTA.EAD_BASEL_SEG_ID AND PARTB.BASEL_MODEL_ID = PARTA.EAD_BASEL_MODEL_ID 
			AND PARTB.QTR_MTH_TM_ID = PARTA.PROCESS_MTH_TM_ID;)
quit;

/*2024-01-11: Kalind Patel - RRMSS-2396 - Changes to DT-4/BRDR - Breach Calculation*/
/* Added EXPSR_PER_MODEL>=0.02 condition in breach as per the requirement */
data portfolio_calibration;
	set SEGMENT_AGG;
	defaulters = accounts;
	tvalue = tinv(0.95,defaulters-1);
	lower_ci = realized_ead - tvalue * (calc_std/sqrt(defaulters));
	upper_ci = realized_ead + tvalue * (calc_std/sqrt(defaulters));
	breach= predicted_ead < lower_ci and EXPSR_PER_MODEL>=0.02;
run;

proc sort data=portfolio_calibration;
	by process_mth_tm_id obs_mth_tm_id dt4_risk_rt_key_val DT4_EAD_SEG_KEY_VAL;
run;

proc sql;
	connect using nzrrap as nzcon;
	execute(delete from &RRAP_DB..DT4_RT40_FINAL_RPTG_VARS where process_mth_tm_id = &mth_tm_id.;
	) by nzcon;
	execute(commit;
	) by nzcon;
quit;

proc append base=nzrrap.DT4_RT40_FINAL_RPTG_VARS data=portfolio_calibration force;
run;