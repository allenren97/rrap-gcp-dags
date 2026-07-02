%MACRO rrap_autoexec(RRAPEnv=);
/*-----------------------------------------------------------------------------------------------
Maintenance Log:
-------------------------------------------------------------------------------------------------
THIS MACRO IS USED IN ALL REVOLVING CREDIT SAS PROGRAMS AND DIS BASED DEPLOYED JOBS.
THIS MACRO INITIALIZES PARAMETERS AND LIBRARIES
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------*/

%IF &RRAPEnv =
%THEN %LET RRAPEnv = REVOLVING_CREDIT;

/* global SAS session options */
OPTIONS COMPRESS=Y STIMER THREADS DBSLICEPARM=(ALL,10);
/*OPTIONS NOTES NOSOURCE NOSYMBOLGEN NOMPRINT NOMLOGIC;*/
%GLOBAL DB2PASSTHRU;
%GLOBAL DBSERVER;
%GLOBAL DBNAME;
%GLOBAL DBSCHEMA;
%GLOBAL DBUSER;
%GLOBAL DBPASS;
%GLOBAL USER_ID;
%GLOBAL NETE_PASS_THROUGH;
%GLOBAL PARMFILE;
%GLOBAL PARMFILE_PATH;
%GLOBAL DB;
%GLOBAL DBSCHEMA1;
%GLOBAL MASTER_TABLE;
%GLOBAL MTH_TM_ID;
%GLOBAL MTH_END_DT;
%GLOBAL YEARMONTH;
%GLOBAL INPATH;
%GLOBAL OUTPATH;
%GLOBAL DPATH;
%GLOBAL LIB;
%GLOBAL SESSIONTIME;
%GLOBAL INPUT00;
%GLOBAL TGT00;
%GLOBAL Processing_Month_Time_ID;
%GLOBAL net_serv net_db net_sche net_user net_pwd dsnn schma usr passw;
%IF %INDEX(%UPCASE(&RRAPEnv), REVOLVING_CREDIT) %THEN %DO;
/*	USER ID WITH WHICH PROGRAMS WILL EXECUTE*/
/*	%LET USER_ID ="OWDEV";
	%IF %INDEX(%UPCASE(&USER_ID), OWDEV) %THEN %DO;*/

/* INITIALIZE RRAP NETEZZA LIBRARIES */
/*PARAMETERS BELOW ARE USED IN SAS BATCHES 1.1 TO 2.4*/

	%LET net_serv = cs2iwntzp01; *Netezza server name;
	%LET net_db = EDRTLRP1D; *Netezza database name;
	%LET net_sche = GDAW;
	%LET net_user= owprdntz; /*this needs to change to owprdntz for prod*/
	%LET net_pwd = "{SAS002}B0E0AA3D417B508E4D50C0CB2ABCF12E";  

    LIBNAME NETCON NETEZZA SERVER=&net_serv DATABASE=&net_db USER=&net_user PASSWORD=&net_pwd;
    %LET NETE_PASS_THROUGH=SERVER=&net_serv DATABASE=&net_db USER=&net_user PASSWORD=&net_pwd;

	LIBNAME PRG_DATA '/sasdata/sasdi/sasprod/data/rrap/reporting';
	%LET PARMFILE_PATH = '/sasdata/sasdi/sasprod/params/rrap/rrap_batch.sasprm';

libname rrap_lib '/sasdata/sasdi/sasprod/data/rrap';
/*PARAMETERS BELOW ARE USED IN SAS REPORTING (NCR, ECAP, CCAR)*/
%LET DB2PASSTHRU=OWSTAR;  
%LET DBNAME=DM1P1D;
%LET DB=&DBNAME;
%LET LIB=DRAPT;
%LET DBSCHEMA=EDRRAPT;
%LET DBSCHEMA1=EDRTLRT;
%LET MASTER_TABLE=BASEL_CUST_ACCT_RLTNP_SNAPSHOT; /*changed after batch1.1 didnt extract the latest month*/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET DPATH=/sasdata/sasdi/sasprod/data/rrap/reporting;
%LET OUTPATH=/owpftp;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET DIM_NOT= ;
LIBNAME &LIB. "&DPATH.";
LIBNAME LDD	DB2 Database=&db schema=EDRRAPT ;
LIBNAME DDB2CON DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA";
LIBNAME &DB.    DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA";
LIBNAME EDRRAPT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA";
LIBNAME EDRTLRT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA1";
LIBNAME INPATH "&DPATH.";
LIBNAME OUTFILE "&DPATH.";
%LET INPATH=INPATH;


/*DB2 LIBRARIES INITIALIZED*/
/*ONLY DEP TXN AND POSTN SUM FACT IN REVOLVING CREDIT USE THIS PASSTHROUGH/OWSTAR*/
LIBNAME OWSTAR DB2 DATASRC=&DB2PASSTHRU SCHEMA=OWSTAR;

/*FETCH THE LATEST PROCESSING MONTH FROM THE MASTER TABLE AND LOAD INTO A VARIABLE MTH_TM_ID*/

/*%let mth_tm_id=16036;*/
PROC SQL NOPRINT;
SELECT MAX(MTH_TM_ID) INTO :MTH_TM_ID FROM NETCON.&MASTER_TABLE.;
SELECT TM_LVL_END_DT INTO :MTH_END_DT FROM NETCON.TM_DIM WHERE TM_ID = &MTH_TM_ID.;
QUIT;

%LET Processing_Month_Time_ID = &MTH_TM_ID.;

/*FETCH MORE TIME PERIOD DETAILS*/
proc sql noprint;
connect to db2 as dbcon (database=&dbname.);
	CREATE table _temp_yearmonth as 
	select * from connection to dbcon (
		select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from 
EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID.;
	);
quit;

/*CONVERT THE INTEGER VALUES TO CHARACTER AS NEEDED*/
data _temp_yearmonth;
set _temp_yearmonth;
if tm_yr_seq_num < 10 then do;
	charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;
	yearmonth=clndr_yr||charmonth;
	format yearmonth $char6.;
run;

/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;

%PUT >>> MTH_TM_ID IS &MTH_TM_ID.;
%PUT >>> MTH_END_DT IS &MTH_END_DT.;
%PUT >>> YEARMONTH IS &YEARMONTH.;
%PUT >>> DB= &DB. and Processing_Month_Time_ID = &Processing_Month_Time_Id;

/*INITIALIZING SQL SERVER LIBRARIES*/
%let dsnn=BSL_BRDM_DR;
%let schma=brdm_dr;
%let usr=usr_rrap;
%let passw={SAS002}160E9F27213F11B320B8B6EA10D19624;
libname IST sqlsvr 	dsn=&dsnn  user=&usr	password= "&passw.";

/*%END;*/
%END;

%IF %INDEX(%UPCASE(&RRAPEnv), BASELDB2) %THEN %DO;      

/* INITIALIZE RRAP DB2 DEV LIBRARIES */
/*%LET DBNAME=DM1D1D;*/
/*%LET DBSCHEMA=EDRRAPT;*/
/*%LET DBSCHEMA1=EDRTLRT;*/
/**/
/*LIBNAME DDB2CON DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA";*/
/*LIBNAME EDRRAPT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA";*/
/*LIBNAME EDRTLRT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA1";*/
%PUT NOTHING TO DECLARE;
%END;

%MEND rrap_autoexec;
