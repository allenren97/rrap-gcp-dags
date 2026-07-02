%macro etl_job_start(TARGET_TABLE=,CONDITION=,PROCESS_MTH_TM_ID=MTH_TM_ID,TRUNCATE=,MULTI_MONTH=);
LIBNAME RRAP BASE "&rrap_dir./data/rrap";
%GLOBAL 
	_TARGET_TABLE 
	_PROCESS_MTH_TM_ID 
	STARTTIME 
	_CONDITION 
	_CONDITION_FULL
	_TM_LVL_END_DT
	_TRUNCATE
	_MULTI_MONTH
;

%let _PROCESS_MTH_TM_ID=&PROCESS_MTH_TM_ID;
%let _TM_LVL_END_DT=;
%let _TARGET_TABLE=&TARGET_TABLE.;
%let _CONDITION=&CONDITION.;
%let _TRUNCATE=&TRUNCATE.;
%let _MULTI_MONTH=&MULTI_MONTH.;

DATA _NULL_;
	CALL SYMPUTX('STARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

PROC SQL NOPRINT;
	SELECT TM_LVL_END_DT FORMAT=DATE9. INTO :_TM_LVL_END_DT
	FROM RRAP.TM_DIM
	WHERE TM_LVL='Month' AND TM_ID=&MTH_TM_ID.;
QUIT;

%PUT NOTE: ******* NOW STARTING MTH_TM_ID: &MTH_TM_ID. *******;
%PUT NOTE: ******* CORRESPONDING TO: &_TM_LVL_END_DT.   *******;
%PUT NOTE: ******* START TIME: &STARTTIME. *******;

%let _CONDITION_FULL=;
%if &_CONDITION. NE %then %let _CONDITION_FULL= AND &_CONDITION.; 

%let _LIBREF = %sysfunc(scan(&_TARGET_TABLE.,1));
%let _TABLE = %sysfunc(scan(&_TARGET_TABLE.,2));

/****** NOTES ON THIS MACRO *******/
/* For Multi-Month Runs pass start and end tm_id's from the job 
proc sql noprint;
	select TM_ID into :mth_tm_id
	from nzrrap.TM_DIM
	where tm_lvl='Month' and tm_lvl_end_dt = "&start_period_dt"d;

	select TM_ID into :end_mth_tm_id
	from nzrrap.TM_DIM
	where tm_lvl='Month' and tm_lvl_end_dt = "&end_period_dt"d;
quit;
*/

/* CODE TO CREATE CONTROL TABLE */

/*
proc sql;
	drop table nzuser.control_table;
quit;

data NZUSER.CONTROL_TABLE;
	attrib SEQUENCE LENGTH=8;
	attrib MODULE LENGTH=$10;
	attrib BATCH LENGTH=$20;
	attrib JOB_NAME LENGTH=$60;
	attrib TARGET_TABLE LENGTH=$60;
	attrib MTH_TM_ID LENGTH=8;
	attrib TM_LVL_END_DT LENGTH=8 FORMAT=DATE9.;
	attrib JOB_START LENGTH=8 FORMAT=DATETIME25.0;
	attrib JOB_END LENGTH=8 FORMAT=DATETIME25.0;
	attrib JOB_RUNTIME LENGTH=8 FORMAT=TIME.;
	attrib ROWS_INSERTED LENGTH=8 FORMAT=COMMA11.;
	attrib CONDITION LENGTH=$1000;
	attrib INSRT_PROCESS_TMSTMP LENGTH=8 FORMAT=DATETIME25.0;
run;

PROC SQL;
	DELETE FROM NZUSER.CONTROL_TABLE;
QUIT;
*/
%mend etl_job_start;
