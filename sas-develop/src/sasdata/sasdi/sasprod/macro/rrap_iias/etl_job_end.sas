%macro etl_job_end();

DATA _NULL_;
	CALL SYMPUTX('ENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('RUNTIME',PUT((DATETIME()-"&STARTTIME."dt),TIME.));
RUN;

%let libref=%scan(&_TARGET_TABLE,1,'.');
%let tbl_name=%scan(&_target_table,2,'.');
proc sql noprint;
	select count(*) into :tmstmp_check
	from dictionary.columns
	where libname="&libref" and memname = "&tbl_name." and name = 'INSRT_PROCESS_TMSTMP';
QUIT;

	%let num_month=1;
%if &_MULTI_MONTH EQ Y %then %do;
	%let num_month=%eval(((&end_mth_tm_id. - &mth_tm_id.)/40)+1);
	%let _CONDITION=%bquote(PART OF MULTI MONTH LOAD: &start_period_dt. - &end_period_dt.);
%end;

%IF &_TRUNCATE EQ Y %THEN %DO;
	PROC SQL NOPRINT;
		SELECT COUNT(*) INTO :ROWS_INSERTED
		FROM &_TARGET_TABLE.
	QUIT;

	%if %EVAL(&ROWS_INSERTED) GT 0 and %eval(&tmstmp_check) GT 0 %then %do;
	PROC SQL NOPRINT;
		SELECT DISTINCT INSRT_PROCESS_TMSTMP INTO :INSRT_PROCESS_TMSTMP
		FROM &_TARGET_TABLE.
	QUIT;
	%end;

	PROC SQL NOPRINT;
		SELECT COALESCE((MAX(SEQUENCE)+1),1) INTO :SEQUENCE
		FROM NZUSER.CONTROL_TABLE;
	QUIT;

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
		proc sql noprint;
			select tm_lvl_end_dt into :tm_lvl_end_dt_&i.
			from nzrrap.tm_dim
			where tm_lvl='Month' and tm_id=%eval(&mth_tm_id. + &i.*40);
		quit;

		PROC SQL NOPRINT;
			SELECT COUNT(*) INTO :ROWS_INSERTED_&i.
			FROM &_TARGET_TABLE.
			WHERE &_PROCESS_MTH_TM_ID. = %eval(&MTH_TM_ID. + &i.*40)
			&_CONDITION_FULL.;
		QUIT;

		%if %EVAL(&&ROWS_INSERTED_&i.) GT 0 and %eval(&tmstmp_check) GT 0 %then %do;
		PROC SQL NOPRINT;
			SELECT DISTINCT INSRT_PROCESS_TMSTMP INTO :INSRT_PROCESS_TMSTMP_&i.
			FROM &_TARGET_TABLE.
			WHERE &_PROCESS_MTH_TM_ID. = %eval(&MTH_TM_ID. + &i.*40)
			&_CONDITION_FULL.;
		QUIT;
		%end;

		PROC SQL NOPRINT;
			SELECT COALESCE((MAX(SEQUENCE)+1),1) INTO :SEQUENCE
			FROM NZUSER.CONTROL_TABLE;
		QUIT;

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
