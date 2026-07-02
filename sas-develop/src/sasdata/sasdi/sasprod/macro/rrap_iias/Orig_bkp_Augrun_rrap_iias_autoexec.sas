
/* THIS VERSION IS FOR PROD */

/*Safe guard against accidental runs */
/*data _null_;abort;run;*/
/* Create metadata macro variables */

/* Set metadata options */
      
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
metaserver     = "&metaServer"; 


%let rrap_dir=/sasdata/sasdi/sasprod;
options mautosource sasautos=("&rrap_dir./macro/rrap_iias", sasautos);
%LET OUTPATH=/owpftp;

%macro rrap_dlgd_autoexec();

     %global mth_tm_id start_period_dt;
	%LET IIASDB=BLUDBPRD; 
	%Let CREDIT_DB=CREDIT_RISK;
	%let FRG_USR=FRG_USER_DATA;
	%let EDRTLRP1D=EDRTLRP1D;
	%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
	%let TNGSTP1D=TNGSTP1D;

	LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
	LIBNAME NZUSER DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRFRGP1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
	LIBNAME NZFRGUSR DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
	LIBNAME NZTNG DB2 DATABASE="&IIASDB" SCHEMA="&TNGSTP1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

    libname EDRRAPT db2 datasrc=DM1P1D schema=EDRRAPT authdomain=db2_auth;
    LIBNAME control BASE "&rrap_dir/params/rrap_iias";

     %get_model_period_dates(product=spl);

/*   %let start_period_dt=31OCT2016;*/

     proc sql noprint;
           select TM_ID into :mth_tm_id from nzrrap.TM_DIM
           where tm_lvl='Month' and tm_lvl_end_dt = "&start_period_dt"d;
     quit;
     %PUT PROCESS MONTH: &start_period_dt.. MTH_TM_ID: &mth_tm_id.;

%mend rrap_dlgd_autoexec;

%macro rrap_ncrbtt_autoexec();

		%global dbname dbschema rundate bttdb RRAP_DB FRG_DB;
		%let rundate=&sysdate9.;
		/*%let rundate=15may2017;*/

		%let bttdb=EDRTLRBTWRKP1D;
		%let RRAP_DB=EDRTLRP1D;
		%let FRG_DB=FRG_USER_DATA;

		libname tngdata DB2 database=BLUDBPRD SCHEMA=TNGSTP1D       authdomain="IIAS_Auth" access=readonly;
		libname nz_frg  DB2 database=BLUDBPRD SCHEMA=frg_user_data  authdomain="IIAS_Auth" access=readonly; 
		libname NZRRAP  DB2 database=BLUDBPRD SCHEMA=EDRTLRP1D      authdomain="IIAS_Auth" access=readonly;
		libname NZWRKBT DB2 database=BLUDBPRD SCHEMA=EDRTLRBTWRKP1D authdomain="IIAS_Auth";

		%if %sysfunc(substr(&etls_jobname.,15,2)) eq TL %then %do;
			libname nzuser  DB2 database=BLUDBPRD SCHEMA=EDRTLRBTWRKP1D authdomain="IIAS_Auth";
		%end;
		%else %do;
			libname nzuser  DB2 database=BLUDBPRD SCHEMA=frg_user_data authdomain="IIAS_Auth" access=readonly;
		%end;

			libname db2_org db2 database=DM1P1D schema=EDEDW   authdomain=DB2_AUTH access=readonly;
			libname db2_prd db2 database=DM1P1D SCHEMA=EDRRAPT authdomain=DB2_AUTH access=readonly;

		%let DBNAME=DM1P1D; %let DBSCHEMA=EDRRAPBTT;
/*		%let DBNAME=DM1U5D; %let DBSCHEMA=EDRRAPBTT;*/

			*LIBNAME BTT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user="&db2user" pwd="&db2password";
			LIBNAME BTT DB2 database="&DBNAME" schema="&DBSCHEMA" authdomain=DB2_AUTH;
%mend rrap_ncrbtt_autoexec;

%macro rrap_spl_autoexec();
/*Safe guard against accidental runs */
/*data _null_;abort;run;*/

%GLOBAL RRAP_DB FRG_DB RRAP_DB2 FRG_USR NZRRAP nzuser nzintmed NZFRG Owstar DB2RRAP;

LIBNAME control BASE "&rrap_dir/params/rrap_iias";
LIBNAME intmed  BASE "&rrap_dir/data/rrap_iias";

%LET IIASDB=BLUDBPRD; 
%Let CREDIT_DB=CREDIT_RISK;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;

%let RRAP_DB   = EDRTLRP1D; 
%let FRG_DB    = EDRTLRFRGP1D;
%let FRG_USR = FRG_USER_DATA;
%let RRAP_DB2  = DM1P1D;
%LET DB2PASSTHRU=OWSTAR;

LIBNAME nzuser  DB2 DATABASE="&IIASDB"  SCHEMA="&FRG_DB"  authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME nzintmed  DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP  DB2 DATABASE="&IIASDB" SCHEMA="&RRAP_DB"  authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZFRG  DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR"   authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

LIBNAME DB2RRAP DB2 DATABASE=&RRAP_DB2 SCHEMA=EDRRAPT authdomain=DB2_AUTH;
LIBNAME OWSTAR  DB2 DATABASE=OWSTAR SCHEMA=OWSTAR authdomain=db2_auth;

%let sqluser=usr_rrap_frg;
%let passwd="{SAS002}BA67F42C2F2D41B230B50A7A4AA46EF04FEFE7A7";

libname AIRBSQL sqlsvr dsn=AIRB_RECON SCHEMA=dbo user= &sqluser password= &passwd readbuff=10000 access=readonly; 

/*In order to hard code dates, run the code below in Enterprise Guide; */

/*************************************************************************

     %include '/sasdata/sasdi/sasprod/macro/rrap/rrap_autoexec.sas';
     %rrap_spl_autoexec;
     %HARD_CODE_DATES(RUN_START_DATE=30APR2015,RUN_END_DATE=30APR2015);
     %LIST_RRAP_PARAMETERS;

*************************************************************************/
%mend rrap_spl_autoexec;

%MACRO RRAP_MOR_TNG_AUTOEXEC();
/*data _null_;abort;run;*/
%GLOBAL FRG_DB frg_lib RRAP_DB TNG_DB EDRTLRFRGP1D IIASDB;


/* IIAS */
%let FRG_DB=FRG_USER_DATA;
%let FRG_LIB=NZUSER; 
%let RRAP_DB=EDRTLRP1D; 
%let TNG_DB=TNGSTP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;

%LET IIASDB=BLUDBPRD; 

LIBNAME netcon DB2 DATABASE="&IIASDB" SCHEMA="&TNG_DB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME tngdata DB2 DATABASE="&IIASDB" SCHEMA="&TNG_DB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME nz_frg DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME nzuser DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&RRAP_DB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZWRK DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRFRGP1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

/* DB2 */
libname db2_org db2 database=DM1P1D schema=EDEDW authdomain=DB2_AUTH;
libname db2_all db2 database=DM1P1D schema=EDRRAP authdomain=DB2_AUTH;
libname db2_tmdm db2 database=DM1P1D schema=EDEDWT authdomain=DB2_AUTH;/*instrument fact uses this, revert this before promotion*/
LIBNAME DB2RRAP DB2  Datasrc=DM1P1D  SCHEMA=EDRRAPT authdomain=DB2_AUTH;


/* SAS */
LIBNAME intmed BASE "&rrap_dir./data/rrap_iias/mortgage/intermediate";
LIBNAME results BASE "&rrap_dir./data/rrap_iias/mortgage/results";
LIBNAME control BASE "&rrap_dir/params/rrap_iias";
LIBNAME source BASE "&rrap_dir./data/rrap_iias/mortgage/source";
LIBNAME RRAP BASE "&rrap_dir./data/rrap_iias";

/*Compensation Control*/
%global threshold;%let threshold=0.0001;

%MEND RRAP_MOR_TNG_AUTOEXEC;

%macro rrap_mor_bns_autoexec();

options nosymbolgen nomlogic mprint compress=yes; 

%let db2user=owprdsas;
%let db2password={SAS002}BDE6030F492DC4274D4EB8E507712524;

LIBNAME control BASE "&rrap_dir/params/rrap_iias";

%include "&rrap_dir./macro/rrap_iias/rrap_mortgage_nz_del_dup.sas";
%include "&rrap_dir./macro/rrap_iias/rrap_mortgage_stat_formats.sas";


%global rrap_dir FRG_DB FRG_LIB RRAP_DB;
%global source_lib target_lib ds acct_key cust_key time_key target_lib2 target_lib3 db2tblspc frg_db cust_key2 time_keyc;

%LET RRAP_DB=EDRTLRP1D;
%let FRG_DB=FRG_USER_DATA;
%let FRG_LIB=NZUSER;

/*%let rrap_dir=/sastemp/rrap_bns;*/
LIBNAME intmed BASE "&rrap_dir./data/rrap_iias/mortgage/intermediate";
LIBNAME results BASE "&rrap_dir./data/rrap_iias/mortgage/results";

LIBNAME source BASE "&rrap_dir./data/rrap_iias/mortgage/source";
libname src         "&rrap_dir./data/rrap_iias/mortgage/source";
LIBNAME target BASE "&rrap_dir./data/rrap_iias/mortgage/target";


%LET IIASDB=BLUDBPRD; 

%let credit_db=CREDIT_RISK;
%let frg_usr=FRG_USER_DATA; 

%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;

/* IIAS LIB */
LIBNAME NZ     DB2 DATABASE="&IIASDB" SCHEMA="&credit_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZUSER DB2 DATABASE="&IIASDB" SCHEMA="&frg_usr" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZWRK  DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRFRGP1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

LIBNAME Owstar DB2  Datasrc=OWSTAR  SCHEMA=OWSTAR authdomain=db2_auth ;
LIBNAME CB DB2  Datasrc=OWSTAR  SCHEMA=CB authdomain=db2_auth;
LIBNAME SAS DB2  Datasrc=OWSTAR  SCHEMA=FRG authdomain=db2_auth readbuff=300;
libname owdss db2 datasrc=owstar schema=owdss authdomain=db2_auth;
LIBNAME OWTACT DB2 DATASRC=OWSTAR SCHEMA=OWTACT authdomain=db2_auth; 

LIBNAME EDRTLR DB2 DATASRC=DM1P1D SCHEMA=EDRTLR authdomain=DB2_AUTH; /*genl_lkp uses this*/
LIBNAME DB2RRAP db2 datasrc=DM1P1D Schema=EDRRAPT authdomain=DB2_AUTH;
/*OWTACT is used for source automation cust_xref*/

LIBNAME OWRRAP DB2 DATASRC=OWSTAR SCHEMA=RRAP authdomain=db2_auth;  /*used for 3 fixed views - Sep 2020 */

%let frg_db= FRG_USER_DATA; /*Job found = RRAP_MOR_MODEL_80_STACK_TNG_AND_BNS*/

*       Source data location:;
%let source_lib=mor;
%let target_lib=nzuser;
%let target_lib2=CREDIT_RISK;
%let target_lib3=FRG_USER_DATA;
%let db2tblspc=FRG_USER_DATA;

*       Population dataset;
%let ds=MOR_SAMPLE_DEV;

*       Natural Account Key;
%let acct_key=mortgage_no;

*       Natural Customer Key;
%let cust_key=cid;

*       Retail Datamart Customer Key;
%let cust_key2=cust_base_key;

*       Time dimension;
%let time_key=time_key;
%let time_keyc=yymth;

%let from_dt=;
%let to_dt=;

/***** Set dates for MOR DATA PULL *****/
%let MOR_DATES='200907' '200904' '200910' '201001' '201004' '201007';

/*Job found = SCRCD_55_LGD_COSTS*/
%let sqluser=usr_rrap_frg;
%let passwd="{SAS002}AF7A0E03464A39D9353A5E5D397CD9DA0219E761"; /*1RichmondStW*/
libname conMO sqlsvr dsn=BASEL_LGD SCHEMA=DBO user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000;

/*FOR MODEL SCHEDULES*/
/*Job found = MODEL_50_LOAD_BRDM_TO_NETEZZA*/
libname brdm sqlsvr dsn=BSL_IST_BRDM_DR SCHEMA=dbo user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000; 
/*PROD*/
/*libname brdm sqlsvr dsn=BSL_BRDM_DR SCHEMA=dbo user= &_userid password= &_pwd_sql readbuff=10000 INSERTBUFF=10000; */

/*Compensation Control*/
%global threshold; %let threshold=0.0001;
libname sql_dms sqlsvr dsn=dms schema=dbo authdomain="SQLSRV_Auth";

%mend rrap_mor_bns_autoexec;

%MACRO rrap_autoexec(RRAPEnv=);

/*data _null_;abort;run;*/

/*-----------------------------------------------------------------------------------------------
Maintenance Log:
-------------------------------------------------------------------------------------------------
THIS MACRO IS USED IN ALL REVOLVING CREDIT SAS PROGRAMS AND DIS BASED DEPLOYED JOBS.
THIS MACRO INITIALIZES PARAMETERS AND LIBRARIES
-------------------------------------------------------------------------------------------------*/
LIBNAME control BASE "&rrap_dir/params/rrap_iias";

%IF &RRAPEnv =
%THEN %LET RRAPEnv = REVOLVING_CREDIT;

%global threshold;
%let threshold=0.0001;
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
%GLOBAL net_serv net_db net_wrk net_sche net_user net_pwd dsnn schma usr passw;
%GLOBAL REPORT_PATH PATH PATH2 PATHLA PATH2LA;
%GLOBAL ACAPTEMP;
%GLOBAL ACAPRPT;
%IF %INDEX(%UPCASE(&RRAPEnv), REVOLVING_CREDIT) %THEN %DO;

%LET net_db =  EDRTLRP1D;
%LET net_wrk = EDRTLRFRGP1D; /*this database used for control table; */
%LET IIASDB=BLUDBPRD; 

LIBNAME NETCON   DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP   DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NETEPASS DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZUSER   DB2 DATABASE="&IIASDB" SCHEMA="&net_wrk" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;    

LIBNAME PRG_DATA "&rrap_dir./data/rrap_iias/reporting";
%LET PARMFILE_PATH = "&rrap_dir./params/rrap_iias/rrap_batch.sasprm";
libname rrap_lib "&rrap_dir./data/rrap_iias";
     
/*PARAMETERS BELOW ARE USED IN SAS REPORTING (NCR, ECAP, CCAR)*/
%LET DB2PASSTHRU=OWSTAR;  
%LET DBNAME=DM1P1D;
%LET DB=&DBNAME;
%LET LIB=DRAPT;
%LET DBSCHEMA=EDRRAPT;
%LET DBSCHEMA1=EDRTLRT;
%LET MASTER_TABLE=BASEL_CUST_ACCT_RLTNP_SNAPSHOT; /*changed after batch1.1 didnt extract the latest month*/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET DPATH=&rrap_dir./data/rrap_iias/reporting;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET DIM_NOT= ;
LIBNAME &LIB. "&DPATH.";

LIBNAME LDD	DB2 Database=&db schema=EDRRAPT authdomain=DB2_AUTH;
LIBNAME DB2RRAP DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" authdomain=DB2_AUTH;
LIBNAME DDB2CON DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" authdomain=DB2_AUTH;
LIBNAME &DB.    DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" authdomain=DB2_AUTH;
LIBNAME EDRRAPT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" authdomain=DB2_AUTH;
LIBNAME EDRTLRT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA1" authdomain=DB2_AUTH;
LIBNAME INPATH "&DPATH.";
LIBNAME OUTFILE "&DPATH.";
/*added acap libraries - Jan 2018*/
LIBNAME ACAPTEMP BASE "&rrap_dir./data/rrap_iias/reporting/acap";
%LET ACAPRPT=&rrap_dir./flat_files/rrap/acap;

%LET INPATH=INPATH;

/*DB2 LIBRARIES INITIALIZED */
/* ONLY DEP TXN AND POSTN SUM FACT IN REVOLVING CREDIT USE THIS PASSTHROUGH/OWSTAR  */

 LIBNAME OWSTAR DB2 DATABASE="&DB2PASSTHRU" SCHEMA=OWSTAR  authdomain=db2_auth;;
 LIBNAME OWRRAP DB2 DATABASE="&DB2PASSTHRU" SCHEMA=RRAP  authdomain=db2_auth;

/*FETCH THE LATEST PROCESSING MONTH FROM THE MASTER TABLE AND LOAD INTO A VARIABLE MTH_TM_ID */

/*%let mth_tm_id=18316;*/
PROC SQL NOPRINT;
SELECT MAX(MTH_TM_ID) INTO :MTH_TM_ID FROM NETCON.&MASTER_TABLE.;
SELECT TM_LVL_END_DT INTO :MTH_END_DT FROM NETCON.TM_DIM WHERE TM_ID = &MTH_TM_ID.;
QUIT;

%LET Processing_Month_Time_ID = &MTH_TM_ID.;

/*FETCH MORE TIME PERIOD DETAILS*/
proc sql noprint;
connect using EDRTLRT as dbcon ;
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
libname IST sqlsvr dsn=&dsnn user=&usr password= "&passw."; 

%END;

%IF %INDEX(%UPCASE(&RRAPEnv), BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%MEND rrap_autoexec;


%macro report_validation;
%GLOBAL MONTHYEAR PREV_MONTHYEAR;
/*CURRENT MONTHYEAR*/
proc sql noprint;
CREATE table _temp_monthyear as 
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as monthyear from EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID;
quit;
/*Convert the integer values to char values as needed*/
data _temp_monthyear;
set _temp_monthyear;
if tm_yr_seq_num < 10 then do;
     charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;

monthyear=charmonth||clndr_yr;
     format monthyear $char6.;
run;
/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a variable*/
proc sql noprint;
select monthyear into :monthyear from _temp_monthyear;
quit;

%PUT MONTHYEAR IS &MONTHYEAR;

/*PREVIOUS MONTHYEAR*/
proc sql noprint;
CREATE table _temp_prev_monthyear as 
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as prev_monthyear from EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID-40;
quit;
/*Convert the integer values to char values as needed*/
data _temp_prev_monthyear;
set _temp_prev_monthyear;
if tm_yr_seq_num < 10 then do;
     charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;

prev_monthyear=charmonth||clndr_yr;
prev_yearmonth=clndr_yr||charmonth;
     format prev_monthyear $char6.;
     format prev_yearmonth $char6.;
run;
/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a variable*/
proc sql noprint;
select prev_monthyear into :prev_monthyear from _temp_prev_monthyear;
select prev_yearmonth into :prev_yearmonth from _temp_prev_monthyear;
quit;

%PUT prev_MONTHYEAR IS &prev_MONTHYEAR;
%PUT prev_yearmonth is &prev_yearmonth;

%let time_id=&MTH_TM_ID;

%LET REPORT_PATH=&rrap_dir./flat_files/rrap_iias; /*used for ncr validation report*/
%let path="&OUTPATH./rk/outgoing/AE_BE_&MONTHYEAR..DAT"; /*used for ncr validation report*/
%let path2="&OUTPATH./rk/outgoing/AE_BD_&MONTHYEAR..DAT"; /*used for ncr validation report*/
%let pathla="&OUTPATH./rk/outgoing/history/&prev_yearmonth/AE_BE_&PREV_MONTHYEAR..DAT"; /*used for ncr validation report*/
%let path2la="&OUTPATH./rk/outgoing/history/&prev_yearmonth/AE_BD_&PREV_MONTHYEAR..DAT"; /*used for ncr validation report*/

%MEND report_validation;

%macro rrap_exception_rpt_autoexec();
%GLOBAL IIASDB CREDIT_DB FRG_USR EDRTLRP1D EDRTLRFRGP1D IIASUSER NZRRAP FRG DM1P1D NZUAT; 
%LET IIASDB=BLUDBPRD; 
%Let CREDIT_DB=CREDIT_RISK;
%let FRG_USR=FRG_USER_DATA;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;

/* for report 1 SPL, KS  */  
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" authdomain="IIAS_Auth"  readbuff=10000 INSERTBUFF=10000;

/* for report 1 MOR  */
LIBNAME FRG DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

/* for report 3 */
LIBNAME DM1P1D DB2 DATABASE="DM1P1D" SCHEMA="EDRRAP" authdomain=DB2_AUTH readbuff=10000 INSERTBUFF=10000;

/* report 4  */
LIBNAME NZUAT DB2 DATABASE="&IIASDB" SCHEMA="&NET_SCHE" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

%mend rrap_exception_rpt_autoexec;
