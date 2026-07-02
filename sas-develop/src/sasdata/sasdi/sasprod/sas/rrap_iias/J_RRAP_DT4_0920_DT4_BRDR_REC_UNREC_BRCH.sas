***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0920_DT4_BRDR_REC_UNREC_BRCH.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_BRDR_REC_UNREC_BRCH
*  
*  Purpose: 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  	 NCR BRDR Breach Reporting and Disclosure Report  
* 	 Part III � Explanations for Recurring Unrecognized Breaches 
*
*	Change Log:
*	2022-01-17: Hadi Dimashkieh - Initial Development
*	2024-01-30: Kalind Patel - RRMSS-2396 - Changes to DT-4/BRDR - Breach Calculation
*	2025-02-18: Kalind Patel - RRMSS-3515 - Reset NCR BRDR breaches for newly introduced segments
* 	2025-02-18: Kalind Patel - RRMSS-3516 - NCR BRDR: DT4_BRDR_MODEL_SEG_RESET_DIM
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();

%macro brdr(param=,rt=);
	%macro a; %mend a;

** Last 5 quarters of data.;
	** Exclude newly introduced segments for 5 consecutive qtrs check;
	** Filter SEGMENTS based on BRDR RESET logic - RRMSS-3516 ;
proc sql;
create table rlzbreaches1_1 as
	SELECT RTFN.PROCESS_MTH_TM_ID, RTFN.OBS_MTH_TM_ID, RTFN.DT4_RISK_RT_KEY_VAL, RTFN.DT4_RISK_RT_DESC, RTFN.MODEL_NM, RTFN.DT4_&param._SEG_KEY_VAL, RTFN.DT4_&param._SEG_DESC
	,RTFN.PREDICTED_&param., RTFN.REALIZED_&param., breach as Part1Breach,
case when RTFN.REALIZED_&param. > RTFN.PREDICTED_&param. then 1 else 0 end as P3_BREACH
	FROM NZRRAP.DT4_RT&rt._FINAL_RPTG_VARS RTFN
	WHERE RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*4 
EXCEPT
	SELECT RTFN.PROCESS_MTH_TM_ID, RTFN.OBS_MTH_TM_ID, RTFN.DT4_RISK_RT_KEY_VAL, RTFN.DT4_RISK_RT_DESC, RTFN.MODEL_NM, RTFN.DT4_&param._SEG_KEY_VAL, RTFN.DT4_&param._SEG_DESC
	,RTFN.PREDICTED_&param., RTFN.REALIZED_&param., breach as Part1reach,case when RTFN.REALIZED_&param. > RTFN.PREDICTED_&param. then 1 else 0 end as P3_BREACH
	FROM NZRRAP.DT4_RT&rt._FINAL_RPTG_VARS RTFN
	LEFT JOIN NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM BRSR  
		on RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL 
				and RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL AND RTFN.MODEL_NM = BRSR.MODEL_Name
	WHERE RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*4 
	AND RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL AND BRSR.DT4_&param._SEG_KEY_VAL IS NOT NULL
	AND RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL 
	AND RTFN.MODEL_NM = BRSR.MODEL_Name ORDER BY 3,6, 1 DESC;
	quit;


** Reset all non-consecutive breachs to 0;
data rlzbreaches1_1(drop=P3_breach rename=(br_P3_breach=P3_breach));
	set rlzbreaches1_1;
	by DT4_RISK_RT_KEY_VAL DT4_&param._SEG_KEY_VAL descending PROCESS_MTH_TM_ID p3_breach;
	retain br_P3_breach;

	if first.DT4_&param._SEG_KEY_VAL then
		br_P3_breach=P3_breach;

	if P3_breach=0 then
		br_P3_breach=0;
run;

** Inclusion criteria for Part 3 BRDR ;

*************************************************************************************************************************
Data processed by RRS/SEG historically for all available process_mth_tm_id�s (in descending order)

From query rlzbreaches1:
Part1Breach
Equal to 1 if the segment breached in P1 for that quarter, else 0

P3_BREACH
Equal to 1 if the segment breached in P3 (REALIZED_&param. > PREDICTED_&param.) for that quarter, else 0

Derived in SAS code:
first3qtrs
counts consecutive P3Breachs in the first 3 quarters (from &mth_tm_id. descending)

NUM_BREACHES
Counts the number of consecutive P3Breaches, resetting when it hits a non-P3Breach

curQTRPart1breach
only looks at &mth_tm_id. and holds the value for Part1Breach to the end

in the end, keep the last record by RRS/SEG and if first3qtrs=3 and curQTRPart1breach=0 
then the segment qualifies for the P3 report. 
Keep the num_breaches value and move on to gathering EAD$ amounts etc..

*************************************************************************************************************************;




data rlzbreaches;
set rlzbreaches1_1;
	by DT4_RISK_RT_KEY_VAL DT4_&param._SEG_KEY_VAL descending PROCESS_MTH_TM_ID;
	retain counter NUM_BREACHES first3qtrs curQTRPart1breach;
	if first.DT4_&param._SEG_KEY_VAL then do;
		num_breaches = 0;  counter = 0; first3qtrs = 0; curQTRPart1breach=Part1breach;
	end;
	if P3_breach=1 then do;
		counter + 1; num_breaches = counter;
		if process_mth_tm_id LE &mth_tm_id. and process_mth_tm_id GE &mth_tm_id. - 40*3*2 then do;
			first3qtrs + 1;
		end;
	end;
			
	if REALIZED_&param. <= PREDICTED_&param. then num_breaches = counter; 

	if last.DT4_&param._SEG_KEY_VAL and first3qtrs = 3 and curQTRPart1breach = 0;
	keep  DT4_RISK_RT_KEY_VAL DT4_&param._SEG_KEY_VAL num_breaches first3qtrs  curQTRPart1breach;* process_mth_tm_id P3_BREACH Part1breach;

run;

/*	2025-02-18: Kalind Patel - RRMSS-3515 - Reset NCR BRDR breaches for newly introduced segments */
/* 	2025-02-18: Kalind Patel - RRMSS-3516 - NCR BRDR: DT4_BRDR_MODEL_SEG_RESET_DIM */

proc sql;
create table rlzbreaches1_2_1 as
	SELECT RTFN.PROCESS_MTH_TM_ID, RTFN.OBS_MTH_TM_ID, RTFN.DT4_RISK_RT_KEY_VAL, RTFN.DT4_RISK_RT_DESC, RTFN.MODEL_NM, RTFN.DT4_&param._SEG_KEY_VAL, RTFN.DT4_&param._SEG_DESC
	,RTFN.PREDICTED_&param., RTFN.REALIZED_&param., breach as Part1Breach,case when RTFN.REALIZED_&param. > RTFN.PREDICTED_&param. then 1 else 0 end as P3_BREACH
	FROM NZRRAP.DT4_RT&rt._FINAL_RPTG_VARS RTFN
	LEFT JOIN NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM BRSR  
		on RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL 
				and RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL AND RTFN.MODEL_NM = BRSR.MODEL_Name
	WHERE RTFN.PROCESS_MTH_TM_ID = &mth_tm_id. 
	AND RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL AND BRSR.DT4_&param._SEG_KEY_VAL IS NOT NULL
	AND RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL 
	AND RTFN.MODEL_NM = BRSR.MODEL_Name AND BRSR.MODEL_SEG_START_DT <= &YRMTH. AND  BRSR.MODEL_SEG_END_DT > &YRMTH. ORDER BY 3,6, 1 DESC;
	quit;

PROC SQL;
SELECT COUNT(*) INTO:new_seg_count FROM rlzbreaches1_2_1 ;
quit;
%put &new_seg_count;
%if &new_seg_count.>0 %then %do;
%put "*********************************************************************";
%put "****** Running BRDR RESET logic for the segments from DT4_BRDR_MODEL_SEG_RESET_DIM *********";


** Extract newly introduced segments for consecutive qtrs check;;
	** Filter SEGMENTS based on BRDR RESET logic - RRMSS-3516;

/********** PREPARATION TO COUNT EVERY SEGMENT BASED ON START & END DT OF ITS VALUE - FROM DT4_BRDR_MODEL_SEG_RESET_DIM ***********/
PROC SQL;
CREATE TABLE NEW_SEGS AS
	SELECT DISTINCT DT4_RISK_RT_KEY_VAL, DT4_EAD_SEG_KEY_VAL, DT4_LGD_SEG_KEY_VAL, DT4_PD_SEG_KEY_VAL, MODEL_SEG_START_DT,  MODEL_SEG_END_DT, &YRMTH. as current_qtr
		FROM NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM a
 	WHERE MODEL_SEG_START_DT <= &YRMTH. AND  MODEL_SEG_END_DT > &YRMTH. ;
	format MODEL_SEG_START_DT $8.; 
QUIT;

/*data NEW_SEGS;*/
/*set NEW_SEGS;*/
/*if substr(MODEL_SEG_START_DT,length(MODEL_SEG_START_DT)-1,2)=12 then MODEL_SEG_START_DT+89 ;*/
/*else if substr(MODEL_SEG_START_DT,length(MODEL_SEG_START_DT)-1,2)<>12 then MODEL_SEG_START_DT+1;*/
/*run;*/

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
SELECT brs.*, Qd.diff FROM  rlzbreaches1_2_1 brs 
lEFT JOIN QTR_DIFF QD ON BRS.DT4_&param._SEG_KEY_VAL = QD.DT4_&param._SEG_KEY_VAL 
				and BRS.DT4_RISK_RT_KEY_VAL = QD.DT4_RISK_RT_KEY_VAL;
quit;

proc sql;
select distinct DT4_RISK_RT_KEY_VAL into:RT_KEY_VAL1- from rlzbreaches1_2_1 ;
select count(distinct DT4_RISK_RT_KEY_VAL) into:total_rt_seg_cnt from rlzbreaches1_2_1 ;
quit;

PROC SQL;
select distinct RTFN.DT4_RISK_RT_KEY_VAL, RTFN.DT4_&param._SEG_KEY_VAL , BRSR.diff 
from rlzbreaches1_2_1 RTFN left join QTR_DIFF BRSR on RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL and RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL ;
QUIT;


proc delete data=rlzbreaches_part2; run;
%do i=1 %to &total_rt_seg_cnt;

proc sql;
select distinct  DT4_&param._SEG_KEY_VAL into:SEG_KEY_VAL1- from rlzbreaches1_2_1 where DT4_RISK_RT_KEY_VAL="&&RT_KEY_VAL&i.";
select count(distinct DT4_&param._SEG_KEY_VAL) into:total_seg_cnt from rlzbreaches1_2_1 where DT4_RISK_RT_KEY_VAL="&&RT_KEY_VAL&i." ;
quit;

%do j=1 %to &total_seg_cnt;

data _null_;
set QTR_DIFF;
call symputx('diff',diff);
where DT4_&param._SEG_KEY_VAL = "&&SEG_KEY_VAL&j." AND DT4_RISK_RT_KEY_VAL = "&&RT_KEY_VAL&i.";
RUN;

%put "&&SEG_KEY_VAL&j. & &&RT_KEY_VAL&i. & Diff: &diff." ;

proc sql;
create table rlzbreaches1_2 as
	SELECT RTFN.PROCESS_MTH_TM_ID, RTFN.OBS_MTH_TM_ID, RTFN.DT4_RISK_RT_KEY_VAL, RTFN.DT4_RISK_RT_DESC, RTFN.MODEL_NM, RTFN.DT4_&param._SEG_KEY_VAL, RTFN.DT4_&param._SEG_DESC
	,RTFN.PREDICTED_&param., RTFN.REALIZED_&param., breach as Part1Breach,case when RTFN.REALIZED_&param. > RTFN.PREDICTED_&param. then 1 else 0 end as P3_BREACH
	FROM NZRRAP.DT4_RT&rt._FINAL_RPTG_VARS RTFN
	LEFT JOIN NZRRAP.DT4_BRDR_MODEL_SEG_RESET_DIM BRSR  
		on RTFN.DT4_&param._SEG_KEY_VAL = BRSR.DT4_&param._SEG_KEY_VAL 
				and RTFN.DT4_RISK_RT_KEY_VAL = BRSR.DT4_RISK_RT_KEY_VAL AND RTFN.MODEL_NM = BRSR.MODEL_Name
		%if &diff. = 1 %then %do;
	
				AND RTFN.PROCESS_MTH_TM_ID = &mth_tm_id.  
				%end;

				%if &diff. = 2 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*1 
				%end;
	
				%if &diff. = 3 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*2 
				%end;

				%if &diff. = 4 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*3  
				%end;

				%if &diff. = 5 %then %do;

				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*4 
				%end;

	WHERE RTFN.DT4_&param._SEG_KEY_VAL = "&&SEG_KEY_VAL&j." AND RTFN.DT4_RISK_RT_KEY_VAL = "&&RT_KEY_VAL&i."
	
				%if &diff. = 1 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID = &mth_tm_id.  
				%end;

				%if &diff.= 2 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*1 
				%end;
	
				%if &diff.=3 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*2 
				%end;

				%if &diff.=4 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*3  
				%end;

				%if &diff. = 5 %then %do;
				AND RTFN.PROCESS_MTH_TM_ID <= &mth_tm_id. and RTFN.PROCESS_MTH_TM_ID >= &mth_tm_id.-40*3*4 
				%end;
	ORDER BY 3,6, 1 DESC;
	quit;


** Reset all non-consecutive breachs to 0;
data rlzbreaches1_2(drop=P3_breach rename=(br_P3_breach=P3_breach));
	set rlzbreaches1_2;
	by DT4_RISK_RT_KEY_VAL DT4_&param._SEG_KEY_VAL descending PROCESS_MTH_TM_ID p3_breach;
	retain br_P3_breach;

	if first.DT4_&param._SEG_KEY_VAL then
		br_P3_breach=P3_breach;

	if P3_breach=0 then
		br_P3_breach=0;
run;


** Inclusion criteria for Part 3 BRDR ;

*************************************************************************************************************************
Data processed by RRS/SEG historically for all available process_mth_tm_id�s (in descending order)

From query rlzbreaches1:
Part1Breach
Equal to 1 if the segment breached in P1 for that quarter, else 0

P3_BREACH
Equal to 1 if the segment breached in P3 (REALIZED_&param. > PREDICTED_&param.) for that quarter, else 0

Derived in SAS code:
first3qtrs
counts consecutive P3Breachs in the first 3 quarters (from &mth_tm_id. descending)

NUM_BREACHES
Counts the number of consecutive P3Breaches, resetting when it hits a non-P3Breach

curQTRPart1breach
only looks at &mth_tm_id. and holds the value for Part1Breach to the end

in the end, keep the last record by RRS/SEG and if first3qtrs=3 and curQTRPart1breach=0 
then the segment qualifies for the P3 report. 
Keep the num_breaches value and move on to gathering EAD$ amounts etc..

*************************************************************************************************************************;

data rlzbreaches_part2_TMP;
set rlzbreaches1_2;
	by DT4_RISK_RT_KEY_VAL DT4_&param._SEG_KEY_VAL descending PROCESS_MTH_TM_ID;
	retain counter NUM_BREACHES first3qtrs curQTRPart1breach;
	if first.DT4_&param._SEG_KEY_VAL then do;
		num_breaches = 0;  counter = 0; first3qtrs = 0; curQTRPart1breach=Part1breach;
	end;
	if P3_breach=1 then do;
		counter + 1; num_breaches = counter;
		if process_mth_tm_id LE &mth_tm_id. and process_mth_tm_id GE &mth_tm_id. - 40*3*2 then do;
			first3qtrs + 1;
		end;
	end;
			
	if REALIZED_&param. <= PREDICTED_&param. then num_breaches = counter; 

	if last.DT4_&param._SEG_KEY_VAL and first3qtrs = 3 and curQTRPart1breach = 0;
	keep  DT4_RISK_RT_KEY_VAL DT4_&param._SEG_KEY_VAL num_breaches first3qtrs  curQTRPart1breach;* process_mth_tm_id P3_BREACH Part1breach;

run;


PROC APPEND BASE=rlzbreaches_part2 data=rlzbreaches_part2_TMP force;
run;

%end;


%end;
PROC APPEND BASE=rlzbreaches data=rlzbreaches_part2 force;
run;
%end;

%if &new_seg_count=0 %then %do;
%put "*********************************************************************";
%put "****** BRDR RESET logic will be skipped for this run *********";
%put "****** No breaches identified in new segments from the table DT4_BRDR_MODEL_SEG_RESET_DIM *********";
%end;

/***********************************************************************************************************************/
/***********************************************************************************************************************/
/***********************************************************************************************************************/
/***********************************************************************************************************************/
/***********************************************************************************************************************/

** 1. Breached Segments.;
proc sql;

create table breach_segs as 
 SELECT DISTINCT a.DT4_&param._SEG_KEY_VAL, a.DT4_&param._SEG_DESC, a.DT4_RISK_RT_KEY_VAL, a.DT4_EXPSR_CL_KEY_VAL, a.PREDICTED_&param., a.REALIZED_&param.
FROM NZUSER.DT4_BRDR_RT&rt._DATA_PREP a, rlzbreaches b

WHERE a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL and a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL
ORDER BY DT4_&param._SEG_KEY_VAL, DT4_&param._SEG_DESC, DT4_RISK_RT_KEY_VAL,DT4_EXPSR_CL_KEY_VAL;
quit;

** 2. EAD of Breached Segments by Exposure Class.;
proc sql;
	create table SEG_x_EXPSR_EAD as 
	SELECT a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, a.DT4_EXPSR_CL_KEY_VAL,  count(1) AS count, round(sum(a.ead)/1,1) AS ead_seg_x_expsr
	FROM NZUSER.DT4_BRDR_RT&rt._DATA_PREP a,  rlzbreaches b
	WHERE a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL and a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL
	GROUP BY a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, a.DT4_EXPSR_CL_KEY_VAL
	ORDER BY a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, a.DT4_EXPSR_CL_KEY_VAL ;
quit;

** 3. Combine 1 and 2. Sort by desc EAD so dominant exposure class is first.;
data breach_segs1;
merge breach_segs(in=a) SEG_x_EXPSR_EAD(in=b);
by DT4_&param._SEG_KEY_VAL DT4_RISK_RT_KEY_VAL DT4_EXPSR_CL_KEY_VAL;
run;
proc sort;by DT4_&param._SEG_KEY_VAL DT4_RISK_RT_KEY_VAL descending ead_seg_x_expsr; run;


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
	drop DT4_EXPSR_CL_KEY_VAL;
/*	BREACHING_PARAMETER = substr(DT4_&param._SEG_DESC,1,6);*/
run;
	
** 5. Add number of breaches.;
proc sql;
create table breach_segs3_1 as
select a.*, b.num_breaches 
from breach_segs2 a left join rlzbreaches b
on a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL and a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL 
order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL
;
quit;

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
	from breach_segs3_1 a left join RRS_DIM b
	on a.DT4_RISK_RT_KEY_VAL = b.NCR_RISK_RT_KEY_VAL
	order by a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL;
quit;

** 9. EAD by Segment.;
proc sql;
	create table SEG_EAD as 
	SELECT a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL, count(1) AS count, round(sum(a.ead)/1,1) AS ead_seg
	FROM NZUSER.DT4_BRDR_RT&rt._DATA_PREP a, rlzbreaches b
	WHERE a.DT4_RISK_RT_KEY_VAL = b.DT4_RISK_RT_KEY_VAL and a.DT4_&param._SEG_KEY_VAL = b.DT4_&param._SEG_KEY_VAL

	GROUP BY a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL
	ORDER BY a.DT4_&param._SEG_KEY_VAL, a.DT4_RISK_RT_KEY_VAL;
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
	FROM &RRAP_WRK..DT4_BRDR_DATA_PREP );
quit;

** 17. Get current Model Type to Identify breaching parameter for LGD D/ND.;
data SEG_DIM;
	set NZRRAP.DT4_SEG_DIM;
	where &yrmth. between input(EFF_FROM_YR_MTH,6.) and input(EFF_TO_YR_MTH,6.);
	keep MODEL_TYPE DT4_SEG_KEY_VAL;
run;

** 18. Merge all data together and calculate reporting ratios.;

proc sql;
create table PART3_BREACHES_&param. as
select 

	 &mth_tm_id. as MTH_TM_ID

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

data PART3_BREACHES;
	set PART3_BREACHES_PD PART3_BREACHES_LGD PART3_BREACHES_EAD;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP =datetime();  
	UPDT_PROCESS_TMSTMP =datetime(); 
run;


proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_BRDR_REC_UNREC_BRCH where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=NZRRAP.DT4_BRDR_REC_UNREC_BRCH(BULKLOAD=YES BL_METHOD=CLILOAD) data=PART3_BREACHES force ; run;
