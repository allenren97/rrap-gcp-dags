***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS &net_db.
*  Target Table: BASEL_RCA_SCORE_DTL_SNAPSHOT 
*  
*  Source Table - 1: BASEL_RCA_SCORE_DTL_SNAPSHOT_HC
*  Source Table - 2: BASEL_RCA_SCORE_DTL_SNAPSHOT_CC
*  Source Table - 3: BASEL_RCA_SCORE_DTL_SNAPSHOT_LOC
*  
*  Purpose: Scoring
* 
*  Frequency: Monthly
*
*  Notes: 
*
* 
*
*	Change Log:
*	2024-05-22: Dhaivat Patel - Combine individual DTL Snapshot tables into final target table
*  
*
***************************************************************************************************************************;

%put WORK LOCATION: %sysfunc(getoption(work));
%include '/sasdata/sasdi/sasprod/macro/rrap_iias/rrap_iias_ifrs_autoexec.sas';
%rrap_ifrs9_autoexec(ENV=PROD);

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