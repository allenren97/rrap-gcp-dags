***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name:J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE 
*  Target Database: EDRTLRPLL
*  Target Table: BASEL_RCA_SCORE_DTL_SNAPSHOT 
*  
*  Purpose: MODEL CHANGE PARALLEL RUN
*
*  Frequency: MONTHLY
*
*  Notes: 
* 
* 
*
*	Change Log: INITIAL DEVELOPMENT - JANUARY 6, 2025
*	
*  
*
***************************************************************************************************************************;

%put WORK LOCATION: %sysfunc(getoption(work));
%include '&rrap_dir/macro/rrap_iias/rrap_pll_autoexec.sas';
%rrap_pll_autoexec(RRAPENV=REVOLVING_CREDIT);

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;
%PUT NOTE: ******* PROCESS START TIME: &PROCESSSTARTTIME. *******;

PROC SQL NOPRINT;
      CONNECT USING NETCON AS NZCON1;
      EXECUTE(DELETE FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON1;
QUIT;


PROC SQL NOPRINT;
      CONNECT USING NETCON AS NZCON1;
      EXECUTE(INSERT INTO &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT (BASEL_MODEL_SCORECRD_DTL_ID,MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,BASEL_MODEL_SCORECRD_HDR_ID,BIN,PT_CNT,INSRT_PROCESS_TMSTMP) 
										      SELECT BASEL_MODEL_SCORECRD_DTL_ID,MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,BASEL_MODEL_SCORECRD_HDR_ID,BIN,PT_CNT,INSRT_PROCESS_TMSTMP FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_HC
                                                                  UNION
                                                                  SELECT BASEL_MODEL_SCORECRD_DTL_ID,MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,BASEL_MODEL_SCORECRD_HDR_ID,BIN,PT_CNT,INSRT_PROCESS_TMSTMP FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_CC
                                                                  UNION
                                                                  SELECT BASEL_MODEL_SCORECRD_DTL_ID,MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,BASEL_MODEL_SCORECRD_HDR_ID,BIN,PT_CNT,INSRT_PROCESS_TMSTMP FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_LOC) BY NZCON1;
QUIT;

PROC SQL NOPRINT;
      CONNECT USING NETCON AS NZCON1;
      EXECUTE(DROP TABLE &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_HC IF EXISTS) BY NZCON1;
      EXECUTE(DROP TABLE &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_CC IF EXISTS) BY NZCON1;
      EXECUTE(DROP TABLE &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_LOC IF EXISTS) BY NZCON1;
QUIT;

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

%PUT NOTE: ******* PROCESS END TIME: &PROCESSENDTIME.  *******;
%PUT NOTE: ******* PROCESS RUNTIME:  &PROCESSRUNTIME.  *******;