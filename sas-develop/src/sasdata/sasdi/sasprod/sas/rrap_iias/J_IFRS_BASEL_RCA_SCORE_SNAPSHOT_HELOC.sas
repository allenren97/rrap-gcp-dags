***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS &RRAP_DB.
*  Target Table:  
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
*	2022-09-14: Hadi Dimashkieh - Cleanup production version of the code and pass parameter to embedded queries
*  
*
***************************************************************************************************************************;

%put WORK LOCATION: %sysfunc(getoption(work));
%include '/sasdata/sasdi/sasprod/macro/rrap_iias/rrap_iias_ifrs_autoexec.sas';
%rrap_ifrs9_autoexec(ENV=PROD);
     

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

%PUT NOTE: ******* PROCESS START TIME: &PROCESSSTARTTIME. *******;
         
         /*MACRO TO EXTRACT AND LOAD ACCOUNT IDS FOR EVERY CRITERIA LISTED IN CRITERIAS TABLE. USING A TYPICAL DO LOOP*/
         %MACRO EXTRACT();
         
         %LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); 
         %LET i=1;
         
         /* HARD CODE TM_DIM_ID FOR TESTING PURPOSES*/
         %let TIME_DIM_ID=&MTH_TM_ID;
         %PUT MTH_TM_ID=&MTH_TM_ID;
         
         PROC SQL NOPRINT;
         CONNECT USING NETCON AS NZCON1;
         EXECUTE(DELETE FROM &net_db..BASEL_RCA_SCORE_SNAPSHOT WHERE MTH_TM_ID=&TIME_DIM_ID
         and basel_model_id in (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'HELOC%')) BY NZCON1;
         QUIT;
         
         
         /*LOAD AGGREGATED DATA OF BASEL_RCA_SCORE_DTL_SNAPSHOT AND INTO NETEZZA TABLE BASEL_RCA_SCORE_SNAPSHOT */
         PROC SQL NOPRINT;
         /*SETTING UP PASS THROUGH CONNECTION TO NETEZZA*/
         
         connect using NZRRAP as dbcon;
         EXECUTE (
         INSERT INTO &net_db..BASEL_RCA_SCORE_SNAPSHOT
         SELECT MTH_TM_ID, BASEL_ACCT_ID, BASEL_MODEL_ID, BASEL_MODEL_SCORECRD_HDR_ID, SUM(PT_CNT) AS CALC_SCORE, CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
         FROM &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT WHERE MTH_TM_ID=&TIME_DIM_ID
         and basel_model_id in (select basel_model_id from &net_db..BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'HELOC%')
         GROUP BY MTH_TM_ID, BASEL_ACCT_ID, BASEL_MODEL_ID, BASEL_MODEL_SCORECRD_HDR_ID) BY DBCON;
         DISCONNECT FROM DBCON;
         QUIT;
         
         
         /*END THE MACRO DEFINITION*/
         %MEND;
         
         /*EXECUTING THE MACRO*/
         %EXTRACT();
         

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

%PUT NOTE: ******* PROCESS END TIME: &PROCESSENDTIME.  *******;
%PUT NOTE: ******* PROCESS RUNTIME:  &PROCESSRUNTIME.  *******;
