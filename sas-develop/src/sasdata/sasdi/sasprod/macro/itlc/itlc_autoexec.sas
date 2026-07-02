/* THIS VERSION IS FOR DEV */

%let itlc_dir=/sasdata/sasdi/sasprod;
options mautosource sasautos=("&itlc_dir./macro/itlc", sasautos);

%MACRO itlc_autoexec(ITLCEnv=);
/*-----------------------------------------------------------------------------------------------
Maintenance Log:
-------------------------------------------------------------------------------------------------
THIS MACRO IS USED IN ALL ITLC SAS PROGRAMS AND DIS BASED DEPLOYED JOBS.
THIS MACRO INITIALIZES PARAMETERS AND LIBRARIES
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------*/

%IF &ITLCEnv=
%THEN %LET ITLCEnv=TERM_LOANS;

/* global SAS session options */
OPTIONS COMPRESS=Y STIMER THREADS DBSLICEPARM=(ALL,10);
/*OPTIONS NOTES NOSOURCE NOSYMBOLGEN NOMPRINT NOMLOGIC;*/

%GLOBAL MASTER_TABLE;
%GLOBAL MTH_TM_ID;
%GLOBAL DLY_TM_ID;
%GLOBAL RUN_START_DATE;
%GLOBAL RUN_END_DATE;
%GLOBAL DLY_RUN_START_DATE;
%GLOBAL DLY_RUN_END_DATE;
%GLOBAL INPATH;
%GLOBAL OUTPATH;
%GLOBAL SESSIONTIME;
%GLOBAL DB2SOURCE;
%GLOBAL NZSOURCE;
%GLOBAL DB2TARGET;
%GLOBAL ITLC_PATH;
%GLOBAL FOOTER_PATH;
%GLOBAL RPT_DATE_FORMAT;
%GLOBAL FTP_PATH;
%GLOBAL FTP_ARCHIVE_PATH;
%GLOBAL DB2INTG;
%GLOBAL YEARMONTH;
%GLOBAL DT_RUN;
%GLOBAL scorecrd_path;

%IF %INDEX(%UPCASE(&ITLCEnv),TERM_LOANS) %THEN %DO;

/*%LET DB2SOURCE=DM1P1D;*/
%LET NZSOURCE=EDRTLRP1D;
%LET DB2TARGET=DM1P1D;
%LET DB2INTG=INTGRP1D;

%LET MASTER_TABLE=TM_DIM; /*changed after batch1.1 didnt extract the latest month*/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET OUTPATH=/owpftp;

LIBNAME CONTROL BASE "&itlc_dir/params/itlc";
LIBNAME DB2ITL DB2 DATASRC=&DB2TARGET SCHEMA=EDRTLRT authdomain=db2_auth;
/*LIBNAME NZITL NETEZZA SERVER=CS2IWNTZP01 DATABASE=&NZSOURCE AUTHDOMAIN="NZ_AUTH";*/
LIBNAME NZITL DB2  DATABASE=BLUDBPRD  SCHEMA=EDRTLRP1D   AUTHDOMAIN="IIAS_Auth" ;
LIBNAME SASITLC BASE "&itlc_dir/data/itlc";

/*SET DAILY DATES TO CURRENT DATE MINUS 1*/
data A;
/*birthd='02may2016'd;*/
  birthd="&SYSDATE"D;
  DAY=PUT(birthd,DOWNAME.);
  date_new = COMPRESS(PUT(birthd,yymmdd10.),'-');
  	IF STRIP(DAY)='Monday' then DLY_RUN_START_DATE=INTNX('DAY',birthd,-2);
		ELSE DLY_RUN_START_DATE=INTNX('DAY',birthd,-1);
	IF STRIP(DAY)='Monday' then DLY_RUN_END_DATE=INTNX('DAY',birthd,-2);
		ELSE DLY_RUN_END_DATE=INTNX('DAY',birthd,-1);
  FORMAT DLY_RUN_START_DATE DATE9.;
  FORMAT DLY_RUN_END_DATE DATE9.;
  FORMAT DAY $15.;
run;

PROC SQL NOPRINT;
SELECT compress(date_new) FORMAT $8.,DLY_RUN_START_DATE, DLY_RUN_END_DATE 
INTO :RPT_DATE_FORMAT, :DLY_RUN_START_DATE, :DLY_RUN_END_DATE FROM A;
QUIT;

/*FETCH THE LATEST PROCESSING MONTH AND DAY FROM THE MASTER TABLE AND LOAD INTO A VARIABLE MTH_TM_ID*/
PROC SQL NOPRINT;
	SELECT TM_ID, TM_LVL_END_DT, TM_LVL_END_DT, TM_LVL_END_DT FORMAT YYMMN. 
	INTO :MTH_TM_ID, :RUN_START_DATE, :RUN_END_DATE, :YEARMONTH 
	FROM DB2ITL.TM_DIM
	WHERE TM_ID = (SELECT MAX(MTH_TM_ID) FROM NZITL.BASEL_PSNL_LN_ACCT_SC_DRVD_VAR) 
	AND TM_LVL='Month';

	SELECT TM_ID INTO :DLY_TM_ID FROM DB2ITL.TM_DIM
	WHERE TM_LVL_END_DT = "&DLY_RUN_END_DATE"d AND TM_LVL='Day';
QUIT;

%LET itlc_path="&itlc_dir/flat_files/itlc/eim_out_coll_score_itl_f_d_&RPT_DATE_FORMAT..ascii";
%LET footer_path="&itlc_dir/flat_files/itlc/eim_out_coll_score_itl_f_d_footer.ascii";
%LET ftp_path='/owpftp/probe/out';
%LET ftp_archive_path='/owpftp/probe/out/history';

%PUT 
MTH_TM_ID=&MTH_TM_ID 
DLY_TM_ID=&DLY_TM_ID 
RUNSTARTDATE=&RUN_START_DATE 
RUNENDDATE=&RUN_END_DATE 
DAILYRUNSTARTDATE=&DLY_RUN_START_DATE 
DAILYRUNENDDATE=&DLY_RUN_END_DATE
YEARMONTH=&YEARMONTH;

/*MODEL PROGRAM INITIALIZATION STARTS*/
/*libname uat base "/sasdata/sasdi/sasdev/data/itlc";*/
/*libname prd_db2 db2 schema=EDRTLR datasrc=DM1DBT  dbconinit="SET CURRENT DEGREE='ANY'" INSERT_SQL=YES readbuff=10000 INSERTBUFF=10000;*/
libname modeldb2 db2 schema=EDRTLRT datasrc=DM1P1D dbconinit="SET CURRENT DEGREE='ANY'" INSERT_SQL=YES readbuff=10000 INSERTBUFF=10000 authdomain=db2_auth;
%let scorecrd_path=&itlc_dir/sas/itlc/;
%let dt_run="&sysdate."d;
/*MODEL PROGRAM INITIALIZATION ENDS*/

%END;

%IF %INDEX(%UPCASE(&ITLCEnv),BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%MEND itlc_autoexec;


