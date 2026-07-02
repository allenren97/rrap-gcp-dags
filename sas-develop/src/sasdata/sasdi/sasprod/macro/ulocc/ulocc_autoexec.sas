/* THIS VERSION IS FOR DEV */

%let ulocc_dir=/sasdata/sasdi/sasprod;
options mautosource sasautos=("&ulocc_dir./macro/ulocc", sasautos);

%MACRO ulocc_autoexec(uloccEnv=);
/*-----------------------------------------------------------------------------------------------
Maintenance Log:
-------------------------------------------------------------------------------------------------
THIS MACRO IS USED IN ALL ulocc SAS PROGRAMS AND DIS BASED DEPLOYED JOBS.
THIS MACRO INITIALIZES PARAMETERS AND LIBRARIES
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------*/

%IF &uloccEnv=
%THEN %LET uloccEnv=TERM_LOANS;

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
%GLOBAL DB2STG;
%GLOBAL DB2INTG;
%GLOBAL ulocc_PATH;
%GLOBAL FOOTER_PATH;
%GLOBAL HEADER_PATH;
%GLOBAL DATA_PATH;
%GLOBAL RPT_DATE_FORMAT;
%GLOBAL FTP_PATH;
%GLOBAL FTP_ARCHIVE_PATH;
%GLOBAL DB2INTG;
%GLOBAL YEARMONTH;
%GLOBAL scorecrd_path;
%GLOBAL DT_RUN;
%GLOBAL TEST_TRNST;

%IF %INDEX(%UPCASE(&uloccEnv),TERM_LOANS) %THEN %DO;

/*%LET DB2SOURCE=DM1P1D;*/
%LET NZSOURCE=EDRTLRP1D;
%LET DB2TARGET=DM1P1D;
%LET DB2STG=OWSTGNDB;
%LET DB2INTG=INTGRP1D;

%LET MASTER_TABLE=TM_DIM; /*changed after batch1.1 didnt extract the latest month*/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET OUTPATH=/owpftp;

LIBNAME CONTROL BASE "&ulocc_dir/params/ulocc";
LIBNAME DB2ULOCC DB2 DATASRC=&DB2TARGET SCHEMA=EDRTLRT authdomain="db2_auth";
LIBNAME DB2ULOC1 DB2 DATASRC=&DB2TARGET SCHEMA=EDRRAPT authdomain="db2_auth";
/* LIBNAME NZULOCC NETEZZA SERVER=CS2IWNTZP01 DATABASE=&NZSOURCE AUTHDOMAIN="NZ_AUTH"; */
LIBNAME SASULOCC BASE "&ulocc_dir/data/ulocc";
LIBNAME STGULOCC DB2 DATASRC=&DB2STG SCHEMA=STUSPOT authdomain="db2_auth";
LIBNAME INTULOCC DB2 DATASRC=&DB2INTG SCHEMA=INARRGT authdomain="db2_auth";
LIBNAME INTULOC1 DB2 DATASRC=&DB2INTG SCHEMA=INPARTYT authdomain="db2_auth";

/*SET DAILY DATES TO CURRENT DATE MINUS 1*/
data A;
  birthd="&SYSDATE"D;
/*  birthd='18AUG2016'd;*/
  DAY=PUT(birthd,DOWNAME.);
  date_new = COMPRESS(PUT(birthd,yymmdd10.),'-');
        IF STRIP(DAY)='Monday' then DLY_RUN_START_DATE=INTNX('DAY',birthd,-3);
                ELSE DLY_RUN_START_DATE=INTNX('DAY',birthd,-1);
        IF STRIP(DAY)='Monday' then DLY_RUN_END_DATE=INTNX('DAY',birthd,-3);
                ELSE DLY_RUN_END_DATE=INTNX('DAY',birthd,-1);
  ULOCC_DATE=COMPRESS(PUT(DLY_RUN_START_DATE,yymmdd10.),'-');
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
        FROM DB2ULOCC.TM_DIM
        WHERE TM_ID = (SELECT MAX(MTH_TM_ID) FROM DB2ULOC1.BASEL_ANALYTCL_BL_INSTRMNT_FACT) 
        AND TM_LVL='Month';

        SELECT TM_ID INTO :DLY_TM_ID FROM DB2ULOCC.TM_DIM
        WHERE TM_LVL_END_DT = "&DLY_RUN_END_DATE"d AND TM_LVL='Day';
QUIT;

%LET ulocc_path="&ulocc_dir/flat_files/ulocc/eim_out_col_score_uloc_f_d_&RPT_DATE_FORMAT..ascii";
%LET data_path="&ulocc_dir/flat_files/ulocc/eim_out_col_score_uloc_f_d_data..ascii";
%LET footer_path="&ulocc_dir/flat_files/ulocc/eim_out_col_score_uloc_f_d_footer.ascii";
%LET header_path="&ulocc_dir/flat_files/ulocc/eim_out_col_score_uloc_f_d_header.ascii";
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

%END;

%IF %INDEX(%UPCASE(&uloccEnv),BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%MEND ulocc_autoexec;


