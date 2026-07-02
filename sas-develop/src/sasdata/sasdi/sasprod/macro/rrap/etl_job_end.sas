%macro etl_job_end();

DATA _NULL_;
	CALL SYMPUTX('ENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('RUNTIME',PUT((DATETIME()-"&STARTTIME."dt),TIME.));
RUN;
data _null_;call sleep(1,1);run;

%let mth_tm_id=&mth_tm_id.;

%let libref=%upcase(%scan(&_TARGET_TABLE,1,'.'));
%let tbl_name=%upcase(%scan(&_target_table,2,'.'));
proc sql noprint;
	select count(*) into :tmstmp_check
	from dictionary.columns
	where libname="&libref" and memname = "&tbl_name." and name = 'INSRT_PROCESS_TMSTMP';
QUIT;
data _null_;call sleep(1,1);run;

	%let num_month=1;
%if &_MULTI_MONTH EQ Y %then %do;
	%let end_mth_tm_id=&end_mth_tm_id.;

	%let num_month=%eval(((&end_mth_tm_id. - &mth_tm_id.)/40)+1);
	%let _CONDITION=%bquote(&mth_tm_id. - &end_mth_tm_id. : &start_period_dt. - &end_period_dt.);
%end;
data _null_;call sleep(1,1);run;

%IF &_TRUNCATE EQ Y %THEN %DO;
	PROC SQL NOPRINT;
		SELECT COUNT(*) INTO :ROWS_INSERTED
		FROM &_TARGET_TABLE.
	QUIT;
data _null_;call sleep(1,1);run;

	%if %EVAL(&ROWS_INSERTED) GT 0 and %eval(&tmstmp_check) GT 0 %then %do;
	PROC SQL NOPRINT;
		SELECT DISTINCT INSRT_PROCESS_TMSTMP INTO :INSRT_PROCESS_TMSTMP
		FROM &_TARGET_TABLE.
	QUIT;
	%end;
data _null_;call sleep(1,1);run;

	PROC SQL NOPRINT;
		SELECT COALESCE((MAX(SEQUENCE)+1),1) INTO :SEQUENCE
		FROM NZUSER.CONTROL_TABLE;
	QUIT;
data _null_;call sleep(1,1);run;

	PROC SQL NOPRINT;
		INSERT INTO NZUSER.CONTROL_TABLE
		SET
		SEQUENCE=&SEQUENCE.,
		MODULE="&MODULE.",
		BATCH="&BATCH.",
		JOB_NAME="&etls_jobName.",
		MTH_TM_ID = &MTH_TM_ID.,
		TM_LVL_END_DT = "&_TM_LVL_END_DT."d,
		JOB_START = "&STARTTIME."dt,
		JOB_END = "&ENDTIME."dt,
		JOB_RUNTIME = "&RUNTIME."t,
		TARGET_TABLE = "&_TARGET_TABLE.",
		ROWS_INSERTED = &ROWS_INSERTED.,
		CONDITION = "&_CONDITION.",
		%if &ROWS_INSERTED EQ 0 OR %eval(&tmstmp_check) EQ 0 %THEN %DO;
			INSRT_PROCESS_TMSTMP = .
		%end;
		%else %do;
		INSRT_PROCESS_TMSTMP = "&INSRT_PROCESS_TMSTMP."dt
		%end;
		;
	QUIT;
%END;
%ELSE %DO;
	%do i = 0 %to %eval(&num_month. -1);
data _null_;call sleep(1,1);run;
		proc sql noprint;
			select tm_lvl_end_dt into :tm_lvl_end_dt_&i.
			from nzrrap.tm_dim
			where tm_lvl='Month' and tm_id=%eval(&mth_tm_id. + &i.*40);
		quit;
data _null_;call sleep(1,1);run;

		PROC SQL NOPRINT;
			SELECT COUNT(*) INTO :ROWS_INSERTED_&i.
			FROM &_TARGET_TABLE.
			WHERE 
			%if &_TM_ID. eq N %then %do;
				&_PROCESS_MTH_TM_ID. = "&&tm_lvl_end_dt_&i."d
			%end;
			%else %do;
				&_PROCESS_MTH_TM_ID. = %eval(&MTH_TM_ID. + &i.*40)
			%end;
				&_CONDITION_FULL.;
		QUIT;
data _null_;call sleep(1,1);run;

		%if %EVAL(&&ROWS_INSERTED_&i.) GT 0 and %eval(&tmstmp_check) GT 0 %then %do;
		PROC SQL NOPRINT;
			SELECT DISTINCT INSRT_PROCESS_TMSTMP INTO :INSRT_PROCESS_TMSTMP_&i.
			FROM &_TARGET_TABLE.
			WHERE 
			%if &_TM_ID. eq N %then %do;
				&_PROCESS_MTH_TM_ID. = "&&tm_lvl_end_dt_&i."d
			%end;
			%else %do;
				&_PROCESS_MTH_TM_ID. = %eval(&MTH_TM_ID. + &i.*40)
			%end;			
				&_CONDITION_FULL. 
				ORDER BY 1 DESC;
		QUIT;
		%end;
data _null_;call sleep(1,1);run;

		PROC SQL NOPRINT;
			SELECT COALESCE((MAX(SEQUENCE)+1),1) INTO :SEQUENCE
			FROM NZUSER.CONTROL_TABLE;
		QUIT;
data _null_;call sleep(1,1);run;

		PROC SQL NOPRINT;
			INSERT INTO NZUSER.CONTROL_TABLE
			SET
			SEQUENCE=&SEQUENCE.,
			MODULE="&MODULE.",
			BATCH="&BATCH.",
			JOB_NAME="&etls_jobName.",
			MTH_TM_ID = %eval(&MTH_TM_ID. + &i.*40),
			TM_LVL_END_DT = "&&TM_LVL_END_DT_&i."d,
			JOB_START = "&STARTTIME."dt,
			JOB_END = "&ENDTIME."dt,
			JOB_RUNTIME = "&RUNTIME."t,
			TARGET_TABLE = "&_TARGET_TABLE.",
			ROWS_INSERTED = &&ROWS_INSERTED_&i.,
			CONDITION = "&_CONDITION.",
			%if &&ROWS_INSERTED_&i. EQ 0 OR %eval(&tmstmp_check) EQ 0 %THEN %DO;
				INSRT_PROCESS_TMSTMP = .
			%end;
			%else %do;
			INSRT_PROCESS_TMSTMP = "&&INSRT_PROCESS_TMSTMP_&i."dt
			%end;
			;
		QUIT;
	%end;
%END;


%PUT NOTE: ******* NOW ENDING MTH_TM_ID: &MTH_TM_ID. *******;
%PUT NOTE: ******* CORRESPONDING TO: &_TM_LVL_END_DT. *******;
%PUT NOTE: ******* END TIME: &ENDTIME. *******;
%PUT NOTE: ******* RUNTIME: &RUNTIME.  *******;

%mend etl_job_end;
