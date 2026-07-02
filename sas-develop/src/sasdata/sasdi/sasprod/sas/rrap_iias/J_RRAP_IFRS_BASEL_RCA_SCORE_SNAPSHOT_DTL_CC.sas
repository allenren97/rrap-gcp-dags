
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
         EXECUTE(DROP TABLE &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_CC IF EXISTS)BY NZCON1;
         QUIT;

         %put &=syserr;
         %put &=sqlrc;
		 
		%if &syserr>0 %then %do;
			%abort abend 255;
		%end;
         
         /*DEFINE AND CREATE TABLE STRUCTURE TO STORE ALL VALID CRITERIAS*/
         DATA prg_data.CRITERIAS_CC;
         LENGTH PRIM_ID 8 BASEL_MODEL_SCORECRD_DTL_ID 8 BASEL_MODEL_SCORECRD_HDR_ID 8 CRTRIA_DESC $255 BIN 8 BIN_CRTRIA_SQL_CD_STRG $2000;
         STOP;
         RUN;

         %put &=syserr;
         %put &=sqlrc;
         
         
         /*DEFINE AND CREATE TABLE STRUCTURE TO STORE RESULTING ACCOUNTS FOR EACH CRITERIA*/
         DATA prg_data.CRITERIAS_DTL_CC;
         LENGTH BASEL_MODEL_SCORECRD_DTL_ID 8 MTH_TM_ID 8 BASEL_ACCT_ID 8 BASEL_MODEL_ID 8 BASEL_MODEL_SCORECRD_HDR_ID 8 BIN 8 PT_CNT 8 INSRT_PROCESS_TMSTMP 8;
         FORMAT INSRT_PROCESS_TMSTMP DATETIME.;
         STOP;
         RUN;

         %put &=syserr;
         %put &=sqlrc;
         
         /*SELECT ALL VALID CRITERIAS AND LOAD THEM INTO THE TABLE CRITERIAS. THIS TABLE WILL BE SAVED AT AIX PATH prg_data DEFINED EARLIER*/
         PROC SQL NOPRINT;
         
         INSERT INTO prg_data.CRITERIAS_CC
         SELECT
         MONOTONIC() AS PRIM_ID, /* SIMPLE SEQUENCE*/
         DTL.BASEL_MODEL_SCORECRD_DTL_ID,
         DTL.BASEL_MODEL_SCORECRD_HDR_ID,
         'NODATA' AS CRTRIA_DESC,
         /*TRANWRD(DTL.CRTRIA_DESC,'0D'x,'') AS CRTRIA_DESC, *//*REPLACE CARRIAGE RETURN CHARACTERS WITH NULL*/
         DTL.BIN,
/*DTL.BIN_CRTRIA_SQL_CD_STRG */
tranwrd(DTL.BIN_CRTRIA_SQL_CD_STRG,'EDRTLRP1D.',"&net_db..") length=2000
         FROM NETCON.BASEL_MODEL_SCORECRD_DTL DTL, NETCON.BASEL_MODEL_SCORECRD_HDR HDR 
         WHERE DTL.BASEL_MODEL_SCORECRD_HDR_ID=HDR.BASEL_MODEL_SCORECRD_HDR_ID
         AND HDR.SRC_SYS_CD='KS' AND (HDR.SCORECRD_END_DT IS NULL OR HDR.SCORECRD_END_DT='31DEC9999'd)
         AND DTL.BIN_CRTRIA_SQL_CD_STRG IS NOT NULL
         AND HDR.BASEL_MODEL_ID in (select basel_model_id from NETCON.BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC%'); 
         /*AND DTL.BASEL_MODEL_SCORECRD_HDR_ID=4009;*/
         
         QUIT;

         %put &=syserr;
         %put &=sqlrc;
         
         
         PROC SQL NOPRINT;
         /*SETTING UP PASS THROUGH CONNECTION TO NETEZZA*/
         
         connect using NZRRAP as nzcon;
         /*PERFORM A COUNT OF VALID CRITERIAS FOR WHICH THE ITERATIVE LOOP WILL EXECUTE*/
         SELECT COUNT(*) INTO :CRITERIA_COUNT FROM prg_data.CRITERIAS_CC;
         
         %DO i=1 %TO &CRITERIA_COUNT;
         
         /*FOR EVERY VALUE OF i, SELECT THE SUBQUERY INTO A SAS VARIABLE*/
         SELECT BIN_CRTRIA_SQL_CD_STRG INTO :VAR1 FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i;
         
         /*INSERT ROW BY ROW INTO THE PREDEFINED SAS TABLE THE ACCOUNTS FOR EVERY CRITERIA*/
         INSERT INTO prg_data.CRITERIAS_DTL_CC (BASEL_MODEL_SCORECRD_DTL_ID,MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,BASEL_MODEL_SCORECRD_HDR_ID,BIN,PT_CNT,INSRT_PROCESS_TMSTMP)
         SELECT D.BASEL_MODEL_SCORECRD_DTL_ID, X.MTH_TM_ID, X.BASEL_ACCT_ID, C.BASEL_MODEL_ID, C.BASEL_MODEL_SCORECRD_HDR_ID, D.BIN, D.PT_CNT, &SESSIONTIME AS INSRT_PROCESS_TMSTMP
         FROM NETCON.BASEL_MODEL_SCORECRD_HDR C, NETCON.BASEL_MODEL_SCORECRD_DTL D, (SELECT * FROM CONNECTION TO NZCON (&VAR1)) X
         WHERE C.BASEL_MODEL_SCORECRD_HDR_ID=D.BASEL_MODEL_SCORECRD_HDR_ID
         AND D.BASEL_MODEL_SCORECRD_HDR_ID=(SELECT BASEL_MODEL_SCORECRD_HDR_ID FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i) 
         AND D.BASEL_MODEL_SCORECRD_DTL_ID=(SELECT BASEL_MODEL_SCORECRD_DTL_ID FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i)
         /*AND TRIM(D.CRTRIA_DESC)=(SELECT CRTRIA_DESC FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i) */
         AND D.BIN=(SELECT BIN FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i);
         
         /*END THE DO LOOP*/
         %END;
         
         /*QUIT THE PROC SQL PROCEDURE*/
         DISCONNECT FROM NZCON;
         QUIT;

         %put &=syserr;
         %put &=sqlrc;
         
         /*END THE MACRO DEFINITION*/
         %MEND;
         
         /*EXECUTING THE MACRO*/
         %EXTRACT();
         
		 PROC APPEND BASE=NETCON.BASEL_RCA_SCORE_DTL_SNAPSHOT_CC ( BULKLOAD=YES BL_METHOD=CLILOAD ) DATA=prg_data.CRITERIAS_DTL_CC FORCE; RUN;

       %put &=syserr;
       %put &=sqlrc;



DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

%PUT NOTE: ******* PROCESS END TIME: &PROCESSENDTIME.  *******;
%PUT NOTE: ******* PROCESS RUNTIME:  &PROCESSRUNTIME.  *******;
