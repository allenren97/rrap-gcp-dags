***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS &RRAP_DB. 
*  Target Table:  
*  
*  Purpose: Segmentation
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

         %let dt = %SYSFUNC(datetime(),best18.);
         
         
         %MACRO load_to_LGD();
         
         %LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); 
         %LET i=1;
         
         /* HARD CODE TM_DIM_ID FOR TESTING PURPOSES*/
         %let TM_DIM_ID=&MTH_TM_ID;
         %PUT MTH_TM_ID = &MTH_TM_ID;
         
         PROC SQL NOPRINT;
         CONNECT USING NETCON AS NZCON1;
         EXECUTE(DELETE FROM &net_db..LGD_SEG_ACCT_XREF WHERE MTH_TM_ID=&TM_DIM_ID) BY NZCON1;
         QUIT;
         
         PROC SQL Noprint;
		 select 
			/* BIN_CRTRIA_SQL_CD_STRG, BASEL_SEG_ID */
			tranwrd(BIN_CRTRIA_SQL_CD_STRG,'EDRTLRP1D.',"&net_db..") length=2000, BASEL_SEG_ID
         	into :sql_str1 - :sql_str&SysMaxLong, :seg_id1 - :seg_id&SysMaxLong
         	FROM NETCON.basel_seg
         	where BASEL_SEG_NM like '%LGD%'
                 and (BASEL_SEG_NM like ('%HELOC%') OR BASEL_SEG_NM like ('%CC%') OR BASEL_SEG_NM like ('%LOC%') OR BASEL_SEG_NM like ('%SL%'))
         	and BIN_CRTRIA_SQL_CD_STRG is not null;
         	%let total_query = &sqlObs;
         quit;
         
         %DO i=1 %TO &total_query;
         
         	%let BASEL_SEG_ID = &&seg_id&i;
         
         /*	BELOW QUERY CHANGED DUE TO REQUIREMENT AS PART OF Q4 PHASE - SEP162015*/
         
         Proc sql;
         CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD AUTHDOMAIN="IIAS_Auth");
         EXECUTE(
                     INSERT INTO EDRTLRIFRS9.LGD_SEG_ACCT_XREF (
                            BASEL_ACCT_ID
                      , MTH_TM_ID
                      , BASEL_SEG_ID
                        , BASEL_MODEL_REL_ID
                      , BASEL_MODEL_ID
                      , INSRT_PROCESS_TMSTMP
                        , UPDT_PROCESS_TMSTMP
                        )
                        SELECT 
                            BASEL_ACCT_ID
                      , MTH_TM_ID
                      , BASEL_SEG_ID
                        , BASEL_MODEL_REL_ID
                      , BASEL_MODEL_ID
                      , CURRENT TIMESTAMP AS INSRT_PROCESS_TMSTMP
                        , CURRENT TIMESTAMP AS UPDT_PROCESS_TMSTMP
                     FROM (&&sql_str&i))BY IIASCON;
            DISCONNECT FROM IIASCON;
               quit;
         %END;
         %mend;
         
         %load_to_LGD();
         
DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

%PUT NOTE: ******* PROCESS END TIME: &PROCESSENDTIME.  *******;
%PUT NOTE: ******* PROCESS RUNTIME:  &PROCESSRUNTIME.  *******;
