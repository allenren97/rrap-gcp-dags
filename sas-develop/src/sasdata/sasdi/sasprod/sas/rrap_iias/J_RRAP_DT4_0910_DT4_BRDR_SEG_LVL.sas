***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0910_DT4_BRDR_SEG_LVL.sas;

***************************************************************************************************************************;
**************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_BRDR_SEG_LVL
*  
*  Purpose: 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  	 NCR BRDR Breach Reporting and Disclosure Report  
* 	 Part I - Segment Level Explanations  
*
*	Change Log:
*	2022-01-12: Hadi Dimashkieh - Initial Development
*	2023-03-30: Hadi Dimashkieh - add eff from/to condition to DT4_SEGMENT_XREF join 
*	2025-02-18: Kalind Patel - RRMSS-3515 - Reset NCR BRDR breaches for newly introduced segments
* 	2025-02-18: Kalind Patel - RRMSS-3516 - NCR BRDR: DT4_BRDR_MODEL_SEG_RESET_DIM
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();

proc sql;
	connect using nzrrap as nzcon;
	execute(drop table &RRAP_WRK..DT4_BRDR_DATA_PREP if exists;) by nzcon;
	execute(commit;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(create table &RRAP_WRK..DT4_BRDR_DATA_PREP as
(SELECT 
	 a.MTH_TM_ID
	,a.SRC_SYS_CD
	,a.BASEL_ACCT_ID, a.MORT_NUM
	,a.DT4_RISK_RT_KEY_VAL
	,a.DT4_EXPSR_CL_KEY_VAL
	,a.BCAR_SCHED_NUM

	,case 
		when a.pit_Stat_cd = 'CUR' then (c.NETEAD_DRAWN + c.NETEAD_UNDRAWN) 
		else c.NETEAD_DRAWN 
	 end as EAD


FROM &RRAP_DB..DT4_RPTG_DRVD_VARS a, &RRAP_DB..DT4_SEGMENT_XREF b, &RRAP_DB..DT4_RT18_EST_ER_VARS c

WHERE	a.MTH_TM_ID = b.mth_tm_id AND a.BASEL_ACCT_ID = b.basel_acct_id AND &yrmth. BETWEEN cast(b.EFF_FROM_YR_MTH AS integer) AND cast(b.EFF_TO_YR_MTH AS integer)
	AND a.MTH_TM_ID = c.MTH_TM_ID AND a.BASEL_ACCT_ID = c.basel_acct_id 
	
AND a.mth_tm_id = &mth_tm_id.) WITH DATA;) by nzcon;
execute(commit;) by nzcon;
quit;



%macro brdr(param=,rt=);
	%macro a; %mend a;
proc sql;
connect using nzrrap as nzcon;
execute(drop table &RRAP_WRK..DT4_BRDR_RT&rt._DATA_PREP if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(create table  &RRAP_WRK..DT4_BRDR_RT&rt._DATA_PREP as
(SELECT 
	 a.MTH_TM_ID
	,a.SRC_SYS_CD
	,a.BASEL_ACCT_ID, a.MORT_NUM
	,d.DT4_RISK_RT_KEY_VAL
	,a.DT4_EXPSR_CL_KEY_VAL
	,a.BCAR_SCHED_NUM
	,d.DT4_&param._SEG_KEY_VAL, d.DT4_&param._SEG_DESC
	,case 
		when a.pit_Stat_cd = 'CUR' then (c.NETEAD_DRAWN + c.NETEAD_UNDRAWN) 
		else c.NETEAD_DRAWN 
	 end as EAD
	 ,d.breach ,d.PREDICTED_&param., d.REALIZED_&param.

FROM &RRAP_DB..DT4_RPTG_DRVD_VARS a, &RRAP_DB..DT4_SEGMENT_XREF b, &RRAP_DB..DT4_RT18_EST_ER_VARS c, &RRAP_DB..DT4_RT&rt._FINAL_RPTG_VARS d

WHERE	a.MTH_TM_ID = b.mth_tm_id AND a.BASEL_ACCT_ID = b.basel_acct_id AND &yrmth. BETWEEN cast(b.EFF_FROM_YR_MTH AS integer) AND cast(b.EFF_TO_YR_MTH AS integer)
	AND a.MTH_TM_ID  = c.MTH_TM_ID AND a.BASEL_ACCT_ID = c.basel_acct_id 
	AND a.MTH_TM_ID = d.PROCESS_MTH_TM_ID  
	%if &param. EQ LGD %then %do;
		AND CASE WHEN substr(d.DT4_LGD_SEG_DESC,1,6) = 'LGD-ND' AND a.pit_Stat_cd = 'CUR' THEN b.LGD_ND_BASEL_SEG_ID
	 			 WHEN substr(d.DT4_LGD_SEG_DESC,1,5) = 'LGD-D'  AND a.pit_Stat_cd = 'DEF' THEN b.LGD_D_BASEL_SEG_ID END 			= d.LGD_BASEL_SEG_ID
		AND CASE WHEN substr(d.DT4_LGD_SEG_DESC,1,6) = 'LGD-ND' AND a.pit_Stat_cd = 'CUR' THEN b.LGD_ND_BASEL_MODEL_ID
	 			 WHEN substr(d.DT4_LGD_SEG_DESC,1,5) = 'LGD-D' AND a.pit_Stat_cd = 'DEF' THEN b.LGD_D_BASEL_MODEL_ID END 			= d.LGD_BASEL_MODEL_ID
	%end;
	%else %do;
		AND b.&param._BASEL_SEG_ID = d.&param._BASEL_SEG_ID
		AND b.&param._BASEL_MODEL_ID = d.&param._BASEL_MODEL_ID
	%end;
AND a.mth_tm_id = &mth_tm_id.) WITH DATA;) by nzcon;
execute(commit;) by nzcon;

quit;


** 1. Breached Segments.;
proc sql;
	connect using nzrrap as nzcon;
	create table breach_segs as select * from connection to nzcon(
	SELECT DISTINCT DT4_&param._SEG_KEY_VAL, DT4_&param._SEG_DESC, DT4_RISK_RT_KEY_VAL,DT4_EXPSR_CL_KEY_VAL,PREDICTED_&param., REALIZED_&param.
		FROM &RRAP_WRK..DT4_BRDR_RT&rt._DATA_PREP 
	WHERE breach = 1
	ORDER BY DT4_&param._SEG_KEY_VAL, DT4_&param._SEG_DESC, DT4_RISK_RT_KEY_VAL,DT4_EXPSR_CL_KEY_VAL);
quit;

** 2. EAD of Breached Segments by Exposure Class.;
proc sql;
	connect using nzrrap as nzcon;
	create table SEG_x_EXPSR_EAD as select * from connection to nzcon(
	SELECT DT4_&param._SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL, DT4_EXPSR_CL_KEY_VAL,  count(1) AS count, round(sum(ead)/1,0) AS ead_seg_x_expsr
		FROM &RRAP_WRK..DT4_BRDR_RT&rt._DATA_PREP 
	WHERE breach = 1
	GROUP BY DT4_&param._SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL, DT4_EXPSR_CL_KEY_VAL
	ORDER BY DT4_&param._SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL,DT4_EXPSR_CL_KEY_VAL );
quit;

** 3. Combine 1 and 2. Sort by desc EAD so dominant exposure class is first.;
data breach_segs1;
	merge breach_segs(in=a) SEG_x_EXPSR_EAD(in=b);
	by DT4_&param._SEG_KEY_VAL DT4_RISK_RT_KEY_VAL DT4_EXPSR_CL_KEY_VAL;
run;
proc sort data=breach_segs1; by DT4_&param._SEG_KEY_VAL DT4_RISK_RT_KEY_VAL descending ead_seg_x_expsr; run;

** 4. List all exposure classes the segment falls into, with the dominant one listed first.;
data breach_segs2;
	set breach_segs1;
	retain EXPOSURE_CLASSES ead_dom_seg_x_expsr;
	by DT4_&param._SEG_KEY_VAL DT4_RISK_RT_KEY_VAL descending ead_seg_x_expsr;
	length EXPOSURE_CLASSES $30.;
	if first.DT4_RISK_RT_KEY_VAL then do;
		EXPOSURE_CLASSES = DT4_EXPSR_CL_KEY_VAL;
		ead_dom_seg_x_expsr = ead_seg_x_expsr;
	end;
	else do;
		EXPOSURE_CLASSES = catx(',',EXPOSURE_CLASSES,DT4_EXPSR_CL_KEY_VAL);
		ead_dom_seg_x_expsr = ead_dom_seg_x_expsr;
	end;
	if last.DT4_RISK_RT_KEY_VAL;
	drop DT4_EXPSR_CL_KEY_VAL ead_seg_x_expsr;
/*	BREACHING_PARAMETER = substr(DT4_&param._SEG_DESC,1,6);*/
run;

/*	2025-02-18: Kalind Patel - RRMSS-3515 - Reset NCR BRDR breaches for newly introduced segments */
/* 	2025-02-18: Kalind Patel - RRMSS-3516 - NCR BRDR: DT4_BRDR_MODEL_SEG_RESET_DIM */
** 5.1 Filter SEGMENTS based on BRDR RESET logic - RRMSS-3516 ;
** Exclude newly introduced segments for 5 consecutive qtrs check;

proc sql;
	create table breach_segs2_1 as
		select a.*
		from breach_segs2 a
	EXCEPT
	select ab.*
		from breach_segs2 as ab 
left join NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM as b
		on ab.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL 
				and ab.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL 
 WHERE b.MODEL_SEG_START_DT <= &YRMTH. AND  b.MODEL_SEG_END_DT > &YRMTH.;
quit;

** 5.2 Look back 5 quarters for the old breached segments and check if they breached.;
proc sql;
	create table breach_segs3_1_1 as
	select a.*, b.breach, b.process_mth_tm_id
		from breach_segs2_1 a 
		left join NZRRAP.DT4_RT&rt._FINAL_RPTG_VARS b
			on a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL 
				and a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL 
				and b.process_mth_tm_id between &mth_tm_id. and &mth_tm_id. -4*3*40
		order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, b.process_mth_tm_id;
quit;

** 5.3 Filter SEGMENTS based on BRDR RESET logic - RRMSS-3516 ;
** Extract newly introduced segments for consecutive qtrs check;
proc sql;
	create table breach_segs2_2 as
	select a.*
		from breach_segs2 a 
		left join NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM b
			on a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL 
				and a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL WHERE MODEL_SEG_START_DT <= &YRMTH. AND  MODEL_SEG_END_DT > &YRMTH.
		order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL;
quit;


PROC SQL;
SELECT COUNT(*) INTO:new_seg_count FROM breach_segs2_2 ;
quit;

%put &new_seg_count.;

%if &new_seg_count > 0 %then %do;
%put "*********************************************************************";
%put "****** Running BRDR RESET logic for the segments from DT4_BRDR_MODEL_SEG_RESET_DIM *********";
/********** PREPARATION TO COUNT EVERY SEGMENT BASED ON START & END DT OF ITS VALUE - FROM DT4_BRDR_MODEL_SEG_RESET_DIM ***********/
** 5.4 Look back to quarters for the newly breached segments (based on DT4_BRDR_MODEL_SEG_RESET_DIM table) and check if they breached.;
PROC SQL;
CREATE TABLE NEW_SEGS AS
	SELECT DISTINCT DT4_RISK_RT_KEY_VAL, DT4_EAD_SEG_KEY_VAL, DT4_LGD_SEG_KEY_VAL, DT4_PD_SEG_KEY_VAL, MODEL_SEG_START_DT,  MODEL_SEG_END_DT, &YRMTH. as current_qtr
		FROM NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM a
 	WHERE MODEL_SEG_START_DT <= &YRMTH. AND  MODEL_SEG_END_DT > &YRMTH. ;
	format MODEL_SEG_START_DT $8.; 
QUIT;

PROC SQL;
CONNECT USING NZRRAP AS NZCON;
CREATE TABLE work.TEMP_QTR_TM_DIM as 
SELECT rn, INPUT(yymm_mth,10.) as yymm_mth FROM CONNECTION TO NZCON
(
SELECT  DISTINCT ROW_NUMBER() OVER (ORDER BY FNCL_YR, TM_YR_SEQ_NUM, FNCL_MTH_KEY) AS rn, FNCL_YR||TM_YR_SEQ_NUM AS yymm_mth FROM (
SELECT DISTINCT FNCL_YR, LPAD(TM_YR_SEQ_NUM, 2,0) AS TM_YR_SEQ_NUM, FNCL_MTH_KEY   FROM &RRAP_DB..TM_DIM 
WHERE FNCL_YR IS NOT NULL AND TM_YR_SEQ_NUM IS NOT NULL  AND TM_LVL = 'Month' 
ORDER BY FNCL_MTH_KEY, FNCL_YR 
) );
quit;

PROC SQL;
CREATE TABLE QTR_DIFF AS
SELECT a.*, ((currnt_mth - start_mth)/3)+1 as diff FROM (SELECT ns.*, case when ns.MODEL_SEG_START_DT=tmdm.yymm_mth then tmdm.rn end as start_mth,case when ns.current_qtr=tmdmc.yymm_mth then tmdmc.rn end as currnt_mth
FROM NEW_SEGS ns LEFT JOIN TEMP_QTR_TM_DIM tmdm ON ns.MODEL_SEG_START_DT=tmdm.yymm_mth
LEFT JOIN TEMP_QTR_TM_DIM tmdmc ON ns.current_qtr=tmdmc.yymm_mth) a;
quit;

data QTR_DIFF;
set QTR_DIFF;
if diff >=5 then diff = 5;
run;

data QTR_DIFF;
set QTR_DIFF;
diff = input(compress(diff),1.);
run;

PROC SQL;
	create table breach_segs2_2_diff as
SELECT brs.*, Qd.diff FROM  breach_segs2_2 brs 
lEFT JOIN QTR_DIFF QD ON BRS.DT4_&param._SEG_KEY_VAL = QD.DT4_&param._SEG_KEY_VAL 
				and BRS.DT4_RISK_RT_KEY_VAL = QD.DT4_RISK_RT_KEY_VAL;
quit;

proc sql;
select distinct DT4_RISK_RT_KEY_VAL into:RT_KEY_VAL1- from breach_segs2_2 ;
select count(distinct DT4_RISK_RT_KEY_VAL) into:total_rt_seg_cnt from breach_segs2_2 ;
quit;

PROC SQL;
select distinct RTFN.DT4_RISK_RT_KEY_VAL, RTFN.DT4_&param._SEG_KEY_VAL , BRSR.diff 
from breach_segs2_2 RTFN left join QTR_DIFF BRSR on RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL and RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL ;
QUIT;


proc delete data=breach_segs3_1_2; run;
%do i=1 %to &total_rt_seg_cnt;

proc sql;
select distinct  DT4_&param._SEG_KEY_VAL into:SEG_KEY_VAL1- from breach_segs2_2 where DT4_RISK_RT_KEY_VAL="&&RT_KEY_VAL&i.";
select count(distinct DT4_&param._SEG_KEY_VAL) into:total_seg_cnt from breach_segs2_2 where DT4_RISK_RT_KEY_VAL="&&RT_KEY_VAL&i." ;
quit;


%do j=1 %to &total_seg_cnt;

data _null_;
set QTR_DIFF;
call symputx('diff',diff);
where DT4_&param._SEG_KEY_VAL = "&&SEG_KEY_VAL&j." AND DT4_RISK_RT_KEY_VAL = "&&RT_KEY_VAL&i.";
RUN;

%put "&&SEG_KEY_VAL&j. & &&RT_KEY_VAL&i. & Diff: &diff." ;
proc sql;
	create table breach_segs3_1_TMP as
	select a.*, b.breach, b.process_mth_tm_id
		from breach_segs2_2 a 
		left join NZRRAP.DT4_RT&rt._FINAL_RPTG_VARS b
			on a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL 
				and a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL 
		left join breach_segs2_2_diff diff 
		 		on a.DT4_&param._SEG_KEY_VAL = diff.DT4_&param._SEG_KEY_VAL 
				and a.DT4_RISK_RT_KEY_VAL = diff.DT4_RISK_RT_KEY_VAL 

				WHERE a.DT4_&param._SEG_KEY_VAL = "&&SEG_KEY_VAL&j." AND a.DT4_RISK_RT_KEY_VAL = "&&RT_KEY_VAL&i."

				%if &diff. = 1 %then %do;
				AND b.PROCESS_MTH_TM_ID = &mth_tm_id.  
				%end;

				%if &diff.= 2 %then %do;
				AND b.PROCESS_MTH_TM_ID <= &mth_tm_id. and b.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*1 
				%end;
	
				%if &diff.=3 %then %do;
				AND b.PROCESS_MTH_TM_ID <= &mth_tm_id. and b.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*2 
				%end;

				%if &diff.=4 %then %do;
				AND b.PROCESS_MTH_TM_ID <= &mth_tm_id. and b.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*3  
				%end;

				%if &diff. = 5 %then %do;
				AND b.PROCESS_MTH_TM_ID <= &mth_tm_id. and b.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*4 
				%end;
		order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, b.process_mth_tm_id;

quit;

PROC APPEND BASE=breach_segs3_1_2 data=breach_segs3_1_TMP;
run;

%end;
%end;

PROC APPEND BASE=breach_segs3_1_1 data=breach_segs3_1_2;
run;

PROC SQL;
CREATE TABLE breach_segs3_1_1 AS SELECT * FROM breach_segs3_1_1 a
order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, a.process_mth_tm_id;
%end;


%if &new_seg_count=0 %then %do;
%put "*********************************************************************";
%put "****** BRDR RESET logic will be skipped for this run *********";
%put "****** No breaches identified in new segments from the table DT4_BRDR_MODEL_SEG_RESET_DIM *********";
%end;

** 6. Count consecutive breaches only from the past 5 quarters.;
data breach_segs3_2;
set breach_segs3_1_1;
	by DT4_&param._SEG_KEY_VAL DT4_RISK_RT_KEY_VAL;
	retain NUM_BREACHES 0;
	if first.DT4_RISK_RT_KEY_VAL then num_breaches = 0;
	if breach = 0 then num_breaches = 0;
	num_breaches + breach;
	if last.DT4_RISK_RT_KEY_VAL;
run;

** 7. Get current RRS descriptions;
data RRS_DIM;
	set NZRRAP.RPTG_RISK_RT_SYS_DIM;
	where &yrmth. between input(EFF_FROM_YR_MTH,6.) and input(EFF_TO_YR_MTH,6.);
	keep NCR_RISK_RT_KEY_VAL NCR_RISK_RT_DESC;
run;

** 8. Append RRS description.;
proc sql;
	create table breach_segs3 as
	select a.*, b.NCR_RISK_RT_DESC as DT4_RRS_DESC
	from breach_segs3_2 a left join RRS_DIM b
	on a.DT4_RISK_RT_KEY_VAL = b.NCR_RISK_RT_KEY_VAL
	order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL;
quit;

** 9. EAD by Segment.;
proc sql;
	connect using nzrrap as nzcon;
	create table SEG_EAD as select * from connection to nzcon(
	SELECT DT4_&param._SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL, count(1) AS count, round(sum(ead)/1,0) AS ead_seg
		FROM &RRAP_WRK..DT4_BRDR_RT&rt._DATA_PREP 
	WHERE breach = 1
	GROUP BY DT4_&param._SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL
	ORDER BY DT4_&param._SEG_KEY_VAL, DT4_RISK_RT_KEY_VAL);
quit;

** 10. EAD by RRS.;
proc sql;
	connect using nzrrap as nzcon;
	create table EAD_RRS as select * from connection to nzcon(
	SELECT DT4_RISK_RT_KEY_VAL, count(1) AS count, round(sum(ead)/1,0) AS ead_RRS
		FROM &RRAP_WRK..DT4_BRDR_DATA_PREP 
	GROUP BY DT4_RISK_RT_KEY_VAL
	ORDER BY DT4_RISK_RT_KEY_VAL);
quit;

** 11. EAD by RRS and SAC. Sort by EAD desc so that the dominant SAC is first.;
proc sql;
	connect using nzrrap as nzcon;
	create table RRS_x_SUB_ASSET_1 as select * from connection to nzcon(
	SELECT DT4_RISK_RT_KEY_VAL,BCAR_SCHED_NUM,  round(sum(ead)/1,0) AS ead_rrs_x_sub_asset_class
		FROM &RRAP_WRK..DT4_BRDR_DATA_PREP  
	group by DT4_RISK_RT_KEY_VAL,BCAR_SCHED_NUM
	ORDER BY 1,3 desc);
quit;

** 12. List all SAC per RRS with dominant SAC first.;
data RRS_x_SUB_ASSET2;
	set RRS_x_SUB_ASSET_1;
	retain SUB_ASSET_CLASS;* cum_ead_sub_asset_class;
	by DT4_RISK_RT_KEY_VAL descending EAD_RRS_X_SUB_ASSET_CLASS ;* BCAR_SCHED_NUM;
	length SUB_ASSET_CLASS $20.;
	if first.DT4_RISK_RT_KEY_VAL then do;
		SUB_ASSET_CLASS = BCAR_SCHED_NUM;
		*cum_ead_sub_asset_class = ead_sub_asset_class;
	end;
	else do;
		SUB_ASSET_CLASS = catx(',',SUB_ASSET_CLASS,BCAR_SCHED_NUM);
		*cum_ead_sub_asset_class + ead_sub_asset_class;
	end;
	if last.DT4_RISK_RT_KEY_VAL;
	drop BCAR_SCHED_NUM EAD_RRS_X_SUB_ASSET_CLASS;* ead_sub_asset_class;
run;

** 13. List only dominant SAC per RRS.;
data RRS_x_SUB_ASSET;
	set RRS_x_SUB_ASSET_1;
	by DT4_RISK_RT_KEY_VAL;
	if first.DT4_RISK_RT_KEY_VAL;
	rename ead_rrs_x_sub_asset_class = ead_rrs_x_dom_sub_asset_class;
run;

** 14. EAD by SAC.;
proc sql;
	connect using nzrrap as nzcon;
	create table EAD_SUB_ASSET_CLASS as select * from connection to nzcon(
	SELECT BCAR_SCHED_NUM, count(1) AS count, round(sum(ead)/1,0) AS ead_sub_asset_class
	FROM &RRAP_WRK..DT4_BRDR_DATA_PREP  
	GROUP BY BCAR_SCHED_NUM
	ORDER BY BCAR_SCHED_NUM;);
quit;

** 15. EAD of dominant SAC per RRS vs EAD of SAC.; 
proc sql;
	create table RRS_x_SUB_ASSET_EAD as
	select a.*, b.ead_sub_asset_class
	from RRS_x_SUB_ASSET a left join EAD_SUB_ASSET_CLASS b
	on a.BCAR_SCHED_NUM = b.BCAR_SCHED_NUM
	order by 1,2;
quit;

** 16. EAD of AC.;
proc sql;
	connect using nzrrap as nzcon;
	create table EAD_RETAIL as select * from connection to nzcon(
	SELECT count(1) AS count, round(sum(ead)/1,0) AS ead_RETAIL
	FROM &RRAP_WRK..DT4_BRDR_DATA_PREP);
quit;


** 17. Get current Model Type to Identify breaching parameter for LGD D/ND.;
data SEG_DIM;
	set NZRRAP.DT4_SEG_DIM;
	where &yrmth. between input(EFF_FROM_YR_MTH,6.) and input(EFF_TO_YR_MTH,6.);
	keep MODEL_TYPE DT4_SEG_KEY_VAL;
run;

** 18. Merge all data together and calculate reporting ratios.;
proc sql;
create table PART1_BREACHES_&param. as
select 

	 a.PROCESS_MTH_TM_ID as MTH_TM_ID

	,a.DT4_RISK_RT_KEY_VAL as RRS
	,a.DT4_&param._SEG_KEY_VAL as SEGMENT_ID
	,f.MODEL_TYPE  as BREACHING_PARAMETER 
	,a.NUM_BREACHES
	,a.DT4_RRS_DESC as RRS_DESC
	,a.DT4_&param._SEG_DESC as SEGMENT_DESC

	,a.PREDICTED_&param. as ESTIMATED_PARAMETER
	,a.REALIZED_&param. as REALIZED_PARAMETER

	,a.EXPOSURE_CLASSES

	,b.ead_seg/c.ead_RRS as EAD_SEG_v_RRS
	,c.ead_RRS/e.EAD_SUB_ASSET_CLASS as EAD_RRS_v_SAC
	,e.EAD_SUB_ASSET_CLASS/f.ead_retail as EAD_SAC_v_AC

	,a.COUNT as ACCOUNT_COUNT
	,a.ead_dom_seg_x_expsr

	,b.ead_seg
	,c.ead_RRS
	,d.SUB_ASSET_CLASS
	,e.BCAR_SCHED_NUM as DOM_SUB_ASSET_CLASS
	,e.EAD_SUB_ASSET_CLASS as EAD_DOM_SUB_ASSET_CLASS
	,f.ead_retail


from breach_segs3 a left join SEG_EAD b
	on a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL and a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL
left join EAD_RRS c
	on a.DT4_RISK_RT_KEY_VAL = c.DT4_RISK_RT_KEY_VAL
left join RRS_x_SUB_ASSET2 d
	on a.DT4_RISK_RT_KEY_VAL = d.DT4_RISK_RT_KEY_VAL
left join RRS_x_SUB_ASSET_EAD e
	on a.DT4_RISK_RT_KEY_VAL = e.DT4_RISK_RT_KEY_VAL
left join SEG_DIM f
	on a.DT4_&param._SEG_KEY_VAL = f.DT4_SEG_KEY_VAL
inner join EAD_RETAIL f on 1=1
order by 1,2;
quit;


%mend brdr;

%brdr(param=PD,rt=20);
%brdr(param=LGD,rt=30);
%brdr(param=EAD,rt=40);

data PART1_BREACHES;
	set PART1_BREACHES_PD PART1_BREACHES_LGD PART1_BREACHES_EAD;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP =datetime();  
	UPDT_PROCESS_TMSTMP =datetime(); 
run;

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_BRDR_SEG_LVL where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=NZRRAP.DT4_BRDR_SEG_LVL(BULKLOAD=YES BL_METHOD=CLILOAD) data=PART1_BREACHES force ; run;
