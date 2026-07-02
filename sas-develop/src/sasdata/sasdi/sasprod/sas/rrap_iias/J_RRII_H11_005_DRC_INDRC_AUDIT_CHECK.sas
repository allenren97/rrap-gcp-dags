/**************************************************************************** 
 * Job:             J_RRII_H11_003_COST_AUDIT_CHECK  			    * 
 * Description:     Audit checks for direct & indirect cost                 *                                                                         
 * Created on:    	Apr 01, 2021                 			    * 
 * Changes								    *
 ****************************************************************************/ 

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%let tgtTbl1 = ASST_DRC_COST_MTH_SNAPSHOT;
%let tgtTbl2 = MBR_INDRCT_COST_MTH_SNAPSHOT;
%let srcTbl1 = ASST_SRC_CURR;
%let srcTbl2 = MBR_SRC_CURR;

OPTIONS MPRINT;
%MACRO ZERO_COUNT_CHECK(tbl);	
	PROC SQL NOPRINT;
	    SELECT COUNT(*) INTO :ROW_COUNT FROM NZRRAP.&tbl WHERE MTH_TM_ID = &MTH_TM_ID;
	QUIT;

 	%IF &ROW_COUNT=0 %THEN %DO;
	%PUT ZERO COUNT CHECK FAILED ;
	%ABORT ABEND; 
	%END;
%MEND;

%MACRO DUPLICATE_RECORD_CHECK(tbl);	
	PROC SQL NOPRINT;
		SELECT COUNT(*) INTO :LOAD
		FROM (
				SELECT DISTINCT INSRT_PROCESS_TMSTMP FROM NZRRAP.&tbl WHERE MTH_TM_ID = &MTH_TM_ID
			) A;
	QUIT;

	%IF &LOAD > 1 %THEN %DO;
	%PUT DUPLICATE LOAD CHECK FAILED ;
	%ABORT ABEND; 
	%END;
%MEND;

%MACRO VOLUME_DIFF_CHECK(tbl);
	PROC SQL NOPRINT;
	SELECT ABS(((curr_cnt - prev_cnt)*100)/prev_cnt) INTO :ROW_DIFF_PERC
	FROM
		(SELECT MTH_TM_ID AS curr_tm_id, COUNT(*) AS curr_cnt
		  FROM NZRRAP.&tbl
		  WHERE MTH_TM_ID = &MTH_TM_ID
		  GROUP BY MTH_TM_ID) cur
	 INNER JOIN
		  (SELECT MTH_TM_ID AS prev_tm_id, COUNT(*) AS prev_cnt
		  FROM NZRRAP.&tbl
		  WHERE MTH_TM_ID = &MTH_TM_ID - 40
		  GROUP BY MTH_TM_ID) prev
	 ON 1=1;

	 %IF %SYSEVALF(&ROW_DIFF_PERC>10) %THEN %DO;
		%PUT THE ROW COUNT DIFFERENCE BETWEEN THE CURRENT AND THE PREVIOUS MONTH LOAD IS GREATER THAN 10 PERCENT;
		%PUT &ROW_DIFF_PERC;
		%ABORT ABEND; 
	%END;
	QUIT;
%MEND;

%ZERO_COUNT_CHECK (&tgtTbl1);
%ZERO_COUNT_CHECK (&tgtTbl2);

%DUPLICATE_RECORD_CHECK (&tgtTbl1);
%DUPLICATE_RECORD_CHECK (&tgtTbl2);

%VOLUME_DIFF_CHECK (&tgtTbl1);
%VOLUME_DIFF_CHECK (&tgtTbl2);

/************** JOB LOG *****************/

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

PROC SQL NOPRINT;
	insert into NZRRAP.AUDIT_JOB_TIMER_CHECK (JOB_NAME, MTH_TM_ID, START_TIME, END_TIME, SOURCE_COUNT, TARGET_COUNT)
		values 	('J_RRII_H11_005_DRC_INDRC_AUDIT_CHECK', &MTH_TM_ID., "&PROCESSSTARTTIME"dt, "&PROCESSENDTIME"dt, 1, 1);
QUIT;