***************************************************************************************************************************;
%let etls_jobname = J_RRAP_DT4_0520_DT4_RT20_FINAL_RPTG_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;

*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT20_FINAL_RPTG_VARS
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
*	2024-01-11: Kalind Patel - RRMSS-2396 - Changes to DT-4/BRDR - Breach Calculation
*
*
***************************************************************************************************************************;
%rrap_dt4_autoexec();

PROC SQL;
	connect using nzrrap as nzcon;
	create table SEGMENT_AGG as select * from connection to nzcon(
		SELECT    part1.*,
			part2.expsr_per_model
		FROM      (
			SELECT    rlz.process_mth_tm_id,
				rlz.obs_mth_tm_id,
				rlz.dt4_risk_rt_key_val,
				g.ncr_risk_rt_desc AS dt4_risk_rt_desc,
				seg.model_nm ,
				d.dt4_seg_key_val AS dt4_pd_seg_key_val,
				d.dt4_seg_desc    AS dt4_pd_seg_desc ,
				rlz.pd_basel_seg_num,
				rlz.pd_basel_seg_id,
				rlz.pd_basel_model_id ,
				Sum(rlz.model_dft_f)                                       AS defaulters ,
				COUNT(rlz.model_dft_f)                                     AS accounts ,
				Avg(rlz.est_pd_final_rptg_rto)                             AS pd_final_rptg_rto ,
				Sum(b.ead_after_crm_drawn)/1000000                         AS ead_def ,
				Sum(c.ead_after_crm_drawn+c.ead_after_crm_undrawn)/1000000 AS ead_nondef ,
				Now()                                                      AS insrt_process_tmstmp ,
				Now()                                                      AS updt_process_tmstmp
			FROM      &rrap_db..dt4_rt20_rlz_drvd_vars rlz
				LEFT JOIN &rrap_db..dt4_basel_seg_incl seg
					ON        rlz.pd_basel_seg_id = seg.basel_seg_id
					AND       rlz.pd_basel_model_id = seg.basel_model_id
					AND       &yrmth. between cast(seg.eff_from_yr_mth AS integer) AND       CAST(seg.eff_to_yr_mth AS integer)
				LEFT JOIN &rrap_db..dt4_rt20_rlz_drvd_vars b
					ON        rlz.basel_acct_id = b.basel_acct_id
					AND       rlz.process_mth_tm_id = b.process_mth_tm_id
					AND       b.model_dft_f = 1
				LEFT JOIN &rrap_db..dt4_rt20_rlz_drvd_vars c
					ON        rlz.basel_acct_id = c.basel_acct_id
					AND       rlz.process_mth_tm_id = c.process_mth_tm_id
					AND       c.model_dft_f = 0
				LEFT JOIN &rrap_db..dt4_seg_dim d
					ON        d.rrap_seg_num = rlz.pd_basel_seg_num
					AND       d.model_type='PD'
					AND       &yrmth. BETWEEN CAST(d.eff_from_yr_mth AS integer) AND       CAST(d.eff_to_yr_mth AS integer)
				LEFT JOIN &rrap_db..rptg_risk_rt_sys_dim g
					ON        rlz.dt4_risk_rt_key_val = g.ncr_risk_rt_key_val
					AND       &yrmth. BETWEEN CAST(g.eff_from_yr_mth AS integer) AND       CAST(g.eff_to_yr_mth AS integer)
				WHERE     rlz.process_mth_tm_id = &mth_tm_id.
					GROUP BY  rlz.process_mth_tm_id,
						rlz.obs_mth_tm_id,
						rlz.dt4_risk_rt_key_val,
						g.ncr_risk_rt_desc,
						d.dt4_seg_desc,
						seg.model_nm,
						d.dt4_seg_key_val,
						rlz.pd_basel_seg_num,
						rlz.pd_basel_seg_id,
						rlz.pd_basel_model_id
					ORDER BY  rlz.process_mth_tm_id,
						rlz.obs_mth_tm_id,
						rlz.dt4_risk_rt_key_val,
						g.ncr_risk_rt_desc,
						d.dt4_seg_desc,
						seg.model_nm,
						d.dt4_seg_key_val,
						rlz.pd_basel_seg_num,
						rlz.pd_basel_seg_id,
						rlz.pd_basel_model_id ) part1
					LEFT JOIN
						(
					SELECT    seg.dt4_risk_rt_key_val,
						seg.dt4_pd_seg_key_val,
						seg.ead_seg,
						mdl.ead_model,
						(ead_seg/ead_model) AS expsr_per_model
					FROM      (
						/* Calculate-SEG-EAD */
					SELECT   dt4_pd_seg_key_val,
						dt4_risk_rt_key_val,
						COUNT(1)            AS COUNT,
						round(sum(ead)/1,0) AS ead_seg
					FROM     (
						SELECT *
							FROM   (
								SELECT    a.mth_tm_id,
									a.src_sys_cd,
									a.basel_acct_id,
									a.mort_num,
									d.dt4_risk_rt_key_val,
									a.dt4_expsr_cl_key_val,
									a.bcar_sched_num,
									d.dt4_pd_seg_key_val,
									d.dt4_pd_seg_desc,
								CASE
									WHEN a.pit_stat_cd = 'CUR' THEN (c.netead_drawn + c.netead_undrawn)
									ELSE c.netead_drawn
								END 
							AS ead
								FROM      edrtlrp1d.dt4_rptg_drvd_vars a
									LEFT JOIN edrtlrp1d.dt4_segment_xref b
										ON        a.mth_tm_id = b.mth_tm_id
										AND       a.basel_acct_id = b.basel_acct_id
										AND       &yrmth. BETWEEN b.eff_from_yr_mth AND       b.eff_to_yr_mth
									LEFT JOIN edrtlrp1d.dt4_rt18_est_er_vars c
										ON        a.basel_acct_id = c.basel_acct_id
										AND       a.mth_tm_id = c.mth_tm_id
									LEFT JOIN
										(
									SELECT    rlz.process_mth_tm_id,
										rlz.obs_mth_tm_id,
										rlz.dt4_risk_rt_key_val,
										g.ncr_risk_rt_desc AS dt4_risk_rt_desc,
										seg.model_nm ,
										d.dt4_seg_key_val AS dt4_pd_seg_key_val,
										d.dt4_seg_desc    AS dt4_pd_seg_desc ,
										rlz.pd_basel_seg_num,
										rlz.pd_basel_seg_id,
										rlz.pd_basel_model_id ,
										sum(rlz.model_dft_f)                                       AS defaulters ,
										COUNT(rlz.model_dft_f)                                     AS accounts ,
										avg(rlz.est_pd_final_rptg_rto)                             AS pd_final_rptg_rto ,
										sum(b.ead_after_crm_drawn)/1000000                         AS ead_def ,
										sum(c.ead_after_crm_drawn+c.ead_after_crm_undrawn)/1000000 AS ead_nondef ,
										now()                                                      AS insrt_process_tmstmp ,
										now()                                                      AS updt_process_tmstmp
									FROM      &rrap_db..dt4_rt20_rlz_drvd_vars rlz
										LEFT JOIN &rrap_db..dt4_basel_seg_incl seg
											ON        rlz.pd_basel_seg_id = seg.basel_seg_id
											AND       rlz.pd_basel_model_id = seg.basel_model_id
											AND       &yrmth. BETWEEN CAST(seg.eff_from_yr_mth AS integer) AND       CAST(seg.eff_to_yr_mth AS integer)
										LEFT JOIN &rrap_db..dt4_rt20_rlz_drvd_vars b
											ON        rlz.basel_acct_id = b.basel_acct_id
											AND       rlz.process_mth_tm_id = b.process_mth_tm_id
											AND       b.model_dft_f = 1
										LEFT JOIN &rrap_db..dt4_rt20_rlz_drvd_vars c
											ON        rlz.basel_acct_id = c.basel_acct_id
											AND       rlz.process_mth_tm_id = c.process_mth_tm_id
											AND       c.model_dft_f = 0
										LEFT JOIN &rrap_db..dt4_seg_dim d
											ON        d.rrap_seg_num = rlz.pd_basel_seg_num
											AND       d.model_type='PD'
											AND       &yrmth. BETWEEN CAST(d.eff_from_yr_mth AS integer) AND       CAST(d.eff_to_yr_mth AS integer)
										LEFT JOIN &rrap_db..rptg_risk_rt_sys_dim g
											ON        rlz.dt4_risk_rt_key_val = g.ncr_risk_rt_key_val
											AND       &yrmth. BETWEEN CAST(g.eff_from_yr_mth AS integer) AND       CAST(g.eff_to_yr_mth AS integer)
										WHERE     rlz.process_mth_tm_id = &mth_tm_id.
											GROUP BY  rlz.process_mth_tm_id,
												rlz.obs_mth_tm_id,
												rlz.dt4_risk_rt_key_val,
												g.ncr_risk_rt_desc,
												d.dt4_seg_desc,
												seg.model_nm,
												d.dt4_seg_key_val,
												rlz.pd_basel_seg_num,
												rlz.pd_basel_seg_id,
												rlz.pd_basel_model_id
											ORDER BY  rlz.process_mth_tm_id,
												rlz.obs_mth_tm_id,
												rlz.dt4_risk_rt_key_val,
												g.ncr_risk_rt_desc,
												d.dt4_seg_desc,
												seg.model_nm,
												d.dt4_seg_key_val,
												rlz.pd_basel_seg_num,
												rlz.pd_basel_seg_id,
												rlz.pd_basel_model_id ) d
												ON        a.mth_tm_id = d.process_mth_tm_id
												AND       b.pd_basel_seg_id = d.pd_basel_seg_id
												AND       b.pd_basel_model_id = d.pd_basel_model_id
											WHERE     a.mth_tm_id = &mth_tm_id. )
											WHERE  ead IS NOT NULL
												AND    dt4_risk_rt_key_val IS NOT NULL
												AND    dt4_pd_seg_key_val IS NOT NULL )
											GROUP BY dt4_pd_seg_key_val,
												dt4_risk_rt_key_val
											ORDER BY dt4_pd_seg_key_val,
												dt4_risk_rt_key_val ) seg
											LEFT JOIN
												(
												/* Calculate-MODEL-EAD */
											SELECT   dt4_risk_rt_key_val,
												COUNT(1)            AS COUNT,
												round(sum(ead)/1,0) AS ead_model
											FROM     (
												SELECT *
													FROM   (
														SELECT    a.mth_tm_id,
															a.src_sys_cd,
															a.basel_acct_id,
															a.mort_num,
															d.dt4_risk_rt_key_val,
															a.dt4_expsr_cl_key_val,
															a.bcar_sched_num,
															d.dt4_pd_seg_key_val,
															d.dt4_pd_seg_desc,
														CASE
															WHEN a.pit_stat_cd = 'CUR' THEN (c.netead_drawn + c.netead_undrawn)
															ELSE c.netead_drawn
														END 
													AS ead
														FROM      edrtlrp1d.dt4_rptg_drvd_vars a
															LEFT JOIN edrtlrp1d.dt4_segment_xref b
																ON        a.mth_tm_id = b.mth_tm_id
																AND       a.basel_acct_id = b.basel_acct_id
																AND       &yrmth. BETWEEN b.eff_from_yr_mth AND       b.eff_to_yr_mth
															LEFT JOIN edrtlrp1d.dt4_rt18_est_er_vars c
																ON        a.basel_acct_id = c.basel_acct_id
																AND       a.mth_tm_id = c.mth_tm_id
															LEFT JOIN
																(
															SELECT    rlz.process_mth_tm_id,
																rlz.obs_mth_tm_id,
																rlz.dt4_risk_rt_key_val,
																g.ncr_risk_rt_desc AS dt4_risk_rt_desc,
																seg.model_nm ,
																d.dt4_seg_key_val AS dt4_pd_seg_key_val,
																d.dt4_seg_desc    AS dt4_pd_seg_desc ,
																rlz.pd_basel_seg_num,
																rlz.pd_basel_seg_id,
																rlz.pd_basel_model_id ,
																sum(rlz.model_dft_f)                                       AS defaulters ,
																COUNT(rlz.model_dft_f)                                     AS accounts ,
																avg(rlz.est_pd_final_rptg_rto)                             AS pd_final_rptg_rto ,
																sum(b.ead_after_crm_drawn)/1000000                         AS ead_def ,
																sum(c.ead_after_crm_drawn+c.ead_after_crm_undrawn)/1000000 AS ead_nondef ,
																now()                                                      AS insrt_process_tmstmp ,
																now()                                                      AS updt_process_tmstmp
															FROM      &rrap_db..dt4_rt20_rlz_drvd_vars rlz
																LEFT JOIN &rrap_db..dt4_basel_seg_incl seg
																	ON        rlz.pd_basel_seg_id = seg.basel_seg_id
																	AND       rlz.pd_basel_model_id = seg.basel_model_id
																	AND       &yrmth. BETWEEN CAST(seg.eff_from_yr_mth AS integer) AND       CAST(seg.eff_to_yr_mth AS integer)
																LEFT JOIN &rrap_db..dt4_rt20_rlz_drvd_vars b
																	ON        rlz.basel_acct_id = b.basel_acct_id
																	AND       rlz.process_mth_tm_id = b.process_mth_tm_id
																	AND       b.model_dft_f = 1
																LEFT JOIN &rrap_db..dt4_rt20_rlz_drvd_vars c
																	ON        rlz.basel_acct_id = c.basel_acct_id
																	AND       rlz.process_mth_tm_id = c.process_mth_tm_id
																	AND       c.model_dft_f = 0
																LEFT JOIN &rrap_db..dt4_seg_dim d
																	ON        d.rrap_seg_num = rlz.pd_basel_seg_num
																	AND       d.model_type='PD'
																	AND       &yrmth. BETWEEN CAST(d.eff_from_yr_mth AS integer) AND       CAST(d.eff_to_yr_mth AS integer)
																LEFT JOIN &rrap_db..rptg_risk_rt_sys_dim g
																	ON        rlz.dt4_risk_rt_key_val = g.ncr_risk_rt_key_val
																	AND       &yrmth. BETWEEN CAST(g.eff_from_yr_mth AS integer) AND       CAST(g.eff_to_yr_mth AS integer)
																WHERE     rlz.process_mth_tm_id = &mth_tm_id.
																	GROUP BY  rlz.process_mth_tm_id,
																		rlz.obs_mth_tm_id,
																		rlz.dt4_risk_rt_key_val,
																		g.ncr_risk_rt_desc,
																		d.dt4_seg_desc,
																		seg.model_nm,
																		d.dt4_seg_key_val,
																		rlz.pd_basel_seg_num,
																		rlz.pd_basel_seg_id,
																		rlz.pd_basel_model_id
																	ORDER BY  rlz.process_mth_tm_id,
																		rlz.obs_mth_tm_id,
																		rlz.dt4_risk_rt_key_val,
																		g.ncr_risk_rt_desc,
																		d.dt4_seg_desc,
																		seg.model_nm,
																		d.dt4_seg_key_val,
																		rlz.pd_basel_seg_num,
																		rlz.pd_basel_seg_id,
																		rlz.pd_basel_model_id ) d
																		ON        a.mth_tm_id = d.process_mth_tm_id
																		AND       b.pd_basel_seg_id = d.pd_basel_seg_id
																		AND       b.pd_basel_model_id = d.pd_basel_model_id
																	WHERE     a.mth_tm_id = &mth_tm_id. )
																	WHERE  ead IS NOT NULL
																		AND    dt4_risk_rt_key_val IS NOT NULL
																		AND    dt4_pd_seg_key_val IS NOT NULL )
																	GROUP BY dt4_risk_rt_key_val
																		ORDER BY dt4_risk_rt_key_val ) mdl
																			ON        seg.dt4_risk_rt_key_val = mdl.dt4_risk_rt_key_val
																		ORDER BY  seg.dt4_risk_rt_key_val,
																			dt4_pd_seg_key_val ) part2
																			ON        part1.dt4_risk_rt_key_val = part2.dt4_risk_rt_key_val
																			AND       part1.dt4_pd_seg_key_val=part2.dt4_pd_seg_key_val
																		ORDER BY  part1.dt4_risk_rt_key_val,
																			part1.dt4_pd_seg_key_val
						);
quit;

/*2024-01-11: Kalind Patel - RRMSS-2396 - Changes to DT-4/BRDR - Breach Calculation*/
/* Added EXPSR_PER_MODEL>=0.02 condition in breach as per the requirement */
data portfolio_calibration;
	set SEGMENT_AGG;
	realized_pd = defaulters/accounts;
	predicted_pd = PD_FINAL_RPTG_RTO;
	zvalue = probit(.95);
	lower_ci = max(0, realized_pd - zvalue*sqrt(realized_pd*(1-realized_pd)/accounts));
	upper_ci = min(1, realized_pd + zvalue*sqrt(realized_pd*(1-realized_pd)/accounts));
	breach= predicted_pd < lower_ci and EXPSR_PER_MODEL>=0.02;
	drop PD_FINAL_RPTG_RTO;
run;

proc sort data=portfolio_calibration;
	by process_mth_tm_id dt4_risk_rt_key_val DT4_PD_SEG_KEY_VAL;
run;

proc sql;
	connect using nzrrap as nzcon;
	execute(delete from &RRAP_DB..DT4_RT20_FINAL_RPTG_VARS where process_mth_tm_id = &mth_tm_id.;
	) by nzcon;
	execute(commit;
	) by nzcon;
quit;

proc append base=nzrrap.DT4_RT20_FINAL_RPTG_VARS data=portfolio_calibration force;
run;