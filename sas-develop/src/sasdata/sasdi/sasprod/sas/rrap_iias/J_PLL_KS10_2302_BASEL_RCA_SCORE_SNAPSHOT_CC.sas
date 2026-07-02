***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name:J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC
*  Target Database: EDRTLRPLL
*  Target Table:  BASEL_RCA_SCORE_SNAPSHOT BASEL_RCA_SCORE_SNAPSHOT_CC_PD
*  
*  Purpose: MODEL CHANGE PARALLEL RUN
*
*  Frequency: MONTHLY
*
*  Notes: 
*
*	Change Log: INITIAL DEVELOPMENT - JANUARY 6, 2025
***************************************************************************************************************************;



%put WORK LOCATION: %sysfunc(getoption(work));
%include '&rrap_dir/macro/rrap_iias/rrap_pll_autoexec.sas';
%rrap_pll_autoexec(RRAPENV=REVOLVING_CREDIT);

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

%PUT NOTE: ******* PROCESS START TIME: &PROCESSSTARTTIME. *******;

%let TIME_DIM_ID=&MTH_TM_ID;
%PUT MTH_TM_ID=&MTH_TM_ID;
  
/* STEP 1: All other PD models except CC - no change*/
PROC SQL NOPRINT;
  CONNECT USING NZRRAP AS NZCON1;
   EXECUTE(DELETE FROM &net_db..BASEL_RCA_SCORE_SNAPSHOT WHERE MTH_TM_ID=&MTH_TM_ID
   and basel_model_id in (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC%')) BY NZCON1;
QUIT;
         
PROC SQL NOPRINT;
    connect using NZRRAP as dbcon;
    EXECUTE (
        INSERT INTO &net_db..BASEL_RCA_SCORE_SNAPSHOT
        SELECT MTH_TM_ID, BASEL_ACCT_ID, BASEL_MODEL_ID, BASEL_MODEL_SCORECRD_HDR_ID, SUM(PT_CNT) AS CALC_SCORE, CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT 
		WHERE MTH_TM_ID=&TIME_DIM_ID
        and basel_model_id in (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC%' AND BASEL_SCORECRD_NM NOT LIKE 'CC PD%')
        GROUP BY MTH_TM_ID, BASEL_ACCT_ID, BASEL_MODEL_ID, BASEL_MODEL_SCORECRD_HDR_ID) BY DBCON;
        DISCONNECT FROM DBCON;
QUIT;


/* STEP 2: CC PD - Created new table for CC PD for 3 models - T/R/D Basel III */
PROC SQL NOPRINT;
  CONNECT USING NZRRAP AS NZCON1;
   EXECUTE(DELETE FROM &net_db..BASEL_RCA_SCORE_SNAPSHOT_CC_PD WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON1;
QUIT;
         
PROC SQL NOPRINT;
    connect using NZRRAP as dbcon;
    EXECUTE (
        INSERT INTO &net_db..BASEL_RCA_SCORE_SNAPSHOT_CC_PD
        SELECT MTH_TM_ID, BASEL_ACCT_ID, BASEL_MODEL_ID, BASEL_MODEL_SCORECRD_HDR_ID, SUM(PT_CNT) AS CALC_SCORE, CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT 
		WHERE MTH_TM_ID=&TIME_DIM_ID
        and basel_model_id in (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC PD%')
        GROUP BY MTH_TM_ID, BASEL_ACCT_ID, BASEL_MODEL_ID, BASEL_MODEL_SCORECRD_HDR_ID) BY DBCON;
        DISCONNECT FROM DBCON;
QUIT;

/* STEP 3: CC PD - Select one model based on T/R/D Basel III */
PROC SQL NOPRINT;
CONNECT USING NZRRAP AS NZCON;
EXECUTE (
	INSERT INTO &net_db..BASEL_RCA_SCORE_SNAPSHOT
	SELECT a.mth_tm_id AS MTH_TM_ID, 
	a.basel_Acct_id AS BASEL_ACCT_ID, 
	'8011' AS BASEL_MODEL_ID, 
	(SELECT BASEL_MODEL_SCORECRD_HDR_ID FROM &net_db..BASEL_MODEL_SCORECRD_HDR WHERE BASEL_SCORECRD_DESC LIKE '%CC PD Scorecard%') AS BASEL_MODEL_SCORECARD_HDR_ID, 
	a.calc_score AS CALC_SCORE, 
	CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
	CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
	FROM &net_db..BASEL_RCA_SCORE_SNAPSHOT_CC_PD a
	left join &net_db..BASEL_KS_ACCT_TRANSACTOR_ROLE b 
	on a.mth_tm_id = b.mth_tm_id
	and a.basel_acct_id = b.basel_acct_id
	where a.mth_tm_id = &MTH_TM_ID
	and a.basel_model_id = (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC PD Transactor%')
	and b.role_ind = 'T'
	UNION 
	SELECT a.mth_tm_id AS MTH_TM_ID, 
	a.basel_Acct_id AS BASEL_ACCT_ID, 
	'8011' AS BASEL_MODEL_ID, 
	(SELECT BASEL_MODEL_SCORECRD_HDR_ID FROM &net_db..BASEL_MODEL_SCORECRD_HDR WHERE BASEL_SCORECRD_DESC LIKE '%CC PD Scorecard%') AS BASEL_MODEL_SCORECARD_HDR_ID,  
	a.calc_score AS CALC_SCORE, 
	CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
	CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
	FROM &net_db..BASEL_RCA_SCORE_SNAPSHOT_CC_PD a
	left join &net_db..BASEL_KS_ACCT_TRANSACTOR_ROLE b 
	on a.mth_tm_id = b.mth_tm_id
	and a.basel_acct_id = b.basel_acct_id
	where a.mth_tm_id = &MTH_TM_ID
	and a.basel_model_id = (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC PD Revolver%')
	and b.role_ind = 'R'
	UNION 
	SELECT a.mth_tm_id AS MTH_TM_ID, 
	a.basel_Acct_id AS BASEL_ACCT_ID, 
	'8011' AS BASEL_MODEL_ID, 
	(SELECT BASEL_MODEL_SCORECRD_HDR_ID FROM &net_db..BASEL_MODEL_SCORECRD_HDR WHERE BASEL_SCORECRD_DESC LIKE '%CC PD Scorecard%') AS BASEL_MODEL_SCORECARD_HDR_ID, 
	a.calc_score AS CALC_SCORE, 
	CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
	CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
	FROM &net_db..BASEL_RCA_SCORE_SNAPSHOT_CC_PD a
	left join &net_db..BASEL_KS_ACCT_TRANSACTOR_ROLE b 
	on a.mth_tm_id = b.mth_tm_id
	and a.basel_acct_id = b.basel_acct_id
	where a.mth_tm_id = &MTH_TM_ID
	and a.basel_model_id = (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC PD Delinquent%')
	and b.role_ind = 'D' 
 ) BY NZCON;
QUIT;
	 
		 
DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

%PUT NOTE: ******* PROCESS END TIME: &PROCESSENDTIME.  *******;
%PUT NOTE: ******* PROCESS RUNTIME:  &PROCESSRUNTIME.  *******;