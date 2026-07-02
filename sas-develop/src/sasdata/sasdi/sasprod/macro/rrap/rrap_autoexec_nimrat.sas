/* THIS VERSION IS FOR DEV */

/*Safe guard against accidental runs */
/*data _null_;abort;run;*/
/* Create metadata macro variables */

%let rrap_dir=/sasdata/sasdi/sasdev;
options mautosource sasautos=("&rrap_dir./macro/rrap", sasautos);
%LET OUTPATH=/owftp;

options symbolgen;
%GLOBAL MTH_TM_ID;

%MACRO rrap_autoexec(RRAPEnv=);

/*data _null_;abort;run;*/

/*-----------------------------------------------------------------------------------------------
Maintenance Log:
-------------------------------------------------------------------------------------------------
THIS MACRO IS USED IN ALL REVOLVING CREDIT SAS PROGRAMS AND DIS BASED DEPLOYED JOBS.
THIS MACRO INITIALIZES PARAMETERS AND LIBRARIES
-------------------------------------------------------------------------------------------------*/
LIBNAME control BASE "&rrap_dir/params/rrap";

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

/*
%LET net_serv = cs2iwntzp01; */
%LET net_db =  EDRTLRP1D;
%LET net_wrk = EDRTLRFRGP1D; /*this database used for control table; */
%LET IIASDB=BLUDBPRD; 
%let db2user=owdev;
%let db2password={SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9;

/*
%let nzuser=owdev;
%let nzpassword={SAS002}0F833D4941CA4F644D8724214AE838AC3F916580; */

%let nzuser=s5197480;
%let nzpassword={SAS002}73919B4456A5EA4B408D9A4F2AE25019051828E4;


/* LIBNAME NETCON NETEZZA SERVER=&net_serv DATABASE=&net_db user="&nzuser" pwd="&nzpassword"; */
/* LIBNAME NZRRAP NETEZZA SERVER=&net_serv DATABASE=&net_db user="&nzuser" pwd="&nzpassword"; */

%let iiasuser=iiastestusr;
%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;

LIBNAME NETCON     DB2 DATABASE="&IIASDB" SCHEMA="&net_db" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZRRAP     DB2 DATABASE="&IIASDB" SCHEMA="&net_db" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NETEPASS DB2 DATABASE="&IIASDB" SCHEMA="&net_db" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZUSER     DB2 DATABASE="&IIASDB" SCHEMA="&net_wrk" user="&iiasuser" pwd="&iiaspassword";


/*NZRRAP added since the control table uses it*/
     
/* %LET NETE_PASS_THROUGH=SERVER=&net_serv DATABASE=&net_db user="&nzuser" pwd="&nzpassword";
LIBNAME NZUSER NETEZZA SERVER=&net_serv DATABASE=&net_wrk user="&nzuser" pwd="&nzpassword"; */

LIBNAME PRG_DATA "&rrap_dir./data/rrap/reporting";
%LET PARMFILE_PATH = "&rrap_dir./params/rrap/rrap_batch.sasprm";
libname rrap_lib "&rrap_dir./data/rrap";
     
/*PARAMETERS BELOW ARE USED IN SAS REPORTING (NCR, ECAP, CCAR)*/
%LET DB2PASSTHRU=OWSTAR;  
%LET DBNAME=DM1D1D;
%LET DB=&DBNAME;
%LET LIB=DRAPT;
%LET DBSCHEMA=EDRRAPT;
%LET DBSCHEMA1=EDRTLRT;
%LET MASTER_TABLE=BASEL_CUST_ACCT_RLTNP_SNAPSHOT; /*changed after batch1.1 didnt extract the latest month*/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET DPATH=&rrap_dir./data/rrap/reporting;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET DIM_NOT= ;
LIBNAME &LIB. "&DPATH.";
LIBNAME LDD     DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user="&db2user" pwd="&db2password";
LIBNAME DB2RRAP DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user="&db2user" pwd="&db2password";
LIBNAME DDB2CON DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user="&db2user" pwd="&db2password";
LIBNAME &DB.    DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user="&db2user" pwd="&db2password";
LIBNAME EDRRAPT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user="&db2user" pwd="&db2password";
LIBNAME EDRTLRT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA1" user="&db2user" pwd="&db2password";
LIBNAME INPATH "&DPATH.";
LIBNAME OUTFILE "&DPATH.";
/*added acap libraries - Jan 2018*/
LIBNAME ACAPTEMP BASE "&rrap_dir./data/rrap/reporting/acap";
%LET ACAPRPT=&rrap_dir./flat_files/rrap/acap;

%LET INPATH=INPATH;

/*DB2 LIBRARIES INITIALIZED */
/* ONLY DEP TXN AND POSTN SUM FACT IN REVOLVING CREDIT USE THIS PASSTHROUGH/OWSTAR  */

*** LIBNAME OWSTAR DB2 DATABASE="&DB2PASSTHRU" SCHEMA=OWSTAR  user=s5197480 pwd="{SAS002}73919B4456A5EA4B408D9A4F2AE25019051828E4" access=readonly;

/*FETCH THE LATEST PROCESSING MONTH FROM THE MASTER TABLE AND LOAD INTO A VARIABLE MTH_TM_ID */

/*%let mth_tm_id=16516;*/
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
/* %let passw={SAS002}160E9F27213F11B320B8B6EA10D19624;
libname IST sqlsvr dsn=&dsnn user=&usr password= "&passw."; */

%END;

%IF %INDEX(%UPCASE(&RRAPEnv), BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%MEND rrap_autoexec;





%macro rrap_spl_autoexec();
/*Safe guard against accidental runs */
/*data _null_;abort;run;*/

%GLOBAL RRAP_DB FRG_DB RRAP_DB2 FRG_USR NZRRAP nzuser nzintmed NZFRG Owstar DB2RRAP;

LIBNAME control BASE "&rrap_dir/params/rrap";
LIBNAME intmed  BASE "&rrap_dir/data/rrap";


%LET IIASDB=BLUDBPRD; 
%Let CREDIT_DB=CREDIT_RISK;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
%let iiasuser=iiastestusr;
%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;
%let db2user=owdev;
%let db2password={SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9;

%let RRAP_DB   = EDRTLRP1D; 
%let FRG_DB    = EDRTLRFRGP1D;
%let FRG_USR = FRG_USER_DATA;
%let RRAP_DB2  = DM1D1D;
%LET DB2PASSTHRU=OWSTAR;

* Netezza and DB2 Servers and Databases;
* When making changes here, make sure to change the corresponding metadata library;
* %let NZ_server = cs2iwntzp01;


/*
%let nzuser=owdev;
%let nzpassword={SAS002}0F833D4941CA4F644D8724214AE838AC3F916580;
*/

/* Used to define connections to Netezza and DB2 */
* libname nzuser   netezza server = &NZ_server database = &FRG_DB    user="&nzuser" pwd="&nzpassword";
* libname nzintmed netezza server = &NZ_server database = &FRG_DB    user="&nzuser" pwd="&nzpassword";
* libname NZRRAP   netezza server = &NZ_server database = &RRAP_DB   user="&nzuser" pwd="&nzpassword";
* libname NZFRG   netezza server = &NZ_server database = &FRG_USR   user="&nzuser" pwd="&nzpassword";
LIBNAME DB2RRAP  DB2  DATABASE="&RRAP_DB2"  SCHEMA=EDRRAPT user="&db2user" pwd="&db2password";
LIBNAME Owstar DB2  Datasrc="&DB2PASSTHRU"  SCHEMA=OWSTAR  user="s5197480" pwd="{SAS002}D5637855096E87730938B12F2E22D60C1606846E" access=readonly;

LIBNAME nzuser  DB2 DATABASE="&IIASDB"  SCHEMA="&FRG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME nzintmed  DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZRRAP  DB2 DATABASE="&IIASDB" SCHEMA="&RRAP_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZFRG  DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" user="&iiasuser" pwd="&iiaspassword";


/*
%let sqluser=usr_rrap_frg;
%let passwd="{SAS002}BA67F42C2F2D41B230B50A7A4AA46EF04FEFE7A7";
*/
/*
libname AIRBSQL sqlsvr dsn=AIRB_RECON SCHEMA=dbo user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000; 
*/
/*In order to hard code dates, run the code below in Enterprise Guide; */

/*************************************************************************

     %include '/sasdata/sasdi/sasprod/macro/rrap/rrap_autoexec.sas';
     %rrap_spl_autoexec;
     %HARD_CODE_DATES(RUN_START_DATE=30APR2015,RUN_END_DATE=30APR2015);
     %LIST_RRAP_PARAMETERS;

*************************************************************************/
%mend rrap_spl_autoexec;



%macro rrap_exception_rpt_autoexec();
%LET IIASDB=BLUDBPRD; 
%Let CREDIT_DB=CREDIT_RISK;
%let FRG_USR=FRG_USER_DATA;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
%let iiasuser=iiastestusr;
%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;


/* for report 1 SPL, KS  
libname NZRRAP netezza server = cs2iwntzp01 database = EDRTLRP1D_IC user=owdev pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580";
*/
LIBNAME NZRRAP     DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" user="&iiasuser" pwd="&iiaspassword";


/* for report 1 MOR  */
/*LIBNAME FRG NETEZZA SERVER=cs2iwntzp01 DATABASE=FRG_USER_DATA authdomain="NZ_Auth" bulkunload=yes ;
LIBNAME FRG NETEZZA SERVER=cs2iwntzp01 DATABASE=FRG_USER_DATA_IC user=owdev pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580";
*/
LIBNAME FRG DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" user="&iiasuser" pwd="&iiaspassword" bulkunload=yes;



/*%LET BNS_PREV_TABLE=FRG.BNS_WITH_ALL_RTOS_201602_19APR16;*/
/*%LET BNS_CURR_TABLE=FRG.BNS_WITH_ALL_RTOS_201603_19APR16;*/

/* for report 3 */
LIBNAME DM1P1D DB2 DATABASE="DM1D1D" SCHEMA="EDRRAP" user=owdev pwd="{SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9";

/* report 4 
%let net_serv = 'cs2iwntzp01'; *Netezza server name;
%let net_db = 'EDRTLRP1D_IC'; *Netezza database name; */
%let NET_SCHE = 'SZENG';
/* LIBNAME NZUAT NETEZZA server=&net_serv database=&net_db SCHEMA=&net_sche  user=owdev pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580"; */
LIBNAME NZUAT DB2 DATABASE="&IIASDB" SCHEMA="&NET_SCHE" user="&iiasuser" pwd="&iiaspassword";


%mend rrap_exception_rpt_autoexec;

%macro rrap_dlgd_autoexec();

     %global mth_tm_id start_period_dt;
	 %LET IIASDB=BLUDBPRD; 
	%Let CREDIT_DB=CREDIT_RISK;
	%let FRG_USR=FRG_USER_DATA;
	%let EDRTLRP1D=EDRTLRP1D;
	%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
	%let TNGSTP1D=TNGSTP1D;
	%let iiasuser=iiastestusr;
	%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;

	/*
     libname NZRRAP netezza server=cs2iwntzp01 database=EDRTLRP1D_IC user=owdev pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580";
     libname NZUSER netezza server=cs2iwntzp01 database=EDRTLRWRKP1D_IC user=owdev pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580";
	*/
		LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" user="&iiasuser" pwd="&iiaspassword";
		LIBNAME NZUSER DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRFRGP1D" user="&iiasuser" pwd="&iiaspassword";
		LIBNAME NZFRGUSR DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" user="&iiasuser" pwd="&iiaspassword";
		LIBNAME NZTNG DB2 DATABASE="&IIASDB" SCHEMA="&TNGSTP1D" user="&iiasuser" pwd="&iiaspassword";


/*     libname NZFRGUSR netezza server=cs2iwntzp01 DATABASE=FRG_USER_DATA_IC user=owdev pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580";
   libname NZTNG netezza server=cs2iwntzp01 database=TNGSTP1D AUTHDOMAIN="NZ_AUTH";*/

     libname EDRRAPT db2 datasrc=DM1D1D schema=EDRRAPT user=owdev pwd="{SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9";
     LIBNAME control BASE "&rrap_dir/params/rrap";


     %get_model_period_dates(product=spl);

/*   %let start_period_dt=31OCT2016;*/

     proc sql noprint;
           select TM_ID into :mth_tm_id from nzrrap.TM_DIM
           where tm_lvl='Month' and tm_lvl_end_dt = "&start_period_dt"d;
     quit;
     %PUT PROCESS MONTH: &start_period_dt.. MTH_TM_ID: &mth_tm_id.;

%mend rrap_dlgd_autoexec;



%macro rrap_mor_bns_autoexec();
/*data _null_;abort;run;*/

options nosymbolgen nomlogic mprint compress=yes; 
/*
%let nzuser=owdev;
%let nzpassword={SAS002}0F833D4941CA4F644D8724214AE838AC3F916580;  

%let nzuserdev=s5197480;
%let nzpassworddev={SAS002}73919B4456A5EA4B408D9A4F2AE25019051828E4;  
*/

%let db2user2=owdev;
%let db2password2={SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9;

%let db2user=s5197480;
%let db2password={SAS002}D5637855096E87730938B12F2E22D60C1606846E;



LIBNAME control BASE "&rrap_dir/params/rrap";

%include "&rrap_dir./macro/rrap/rrap_mortgage_nz_del_dup.sas";
%include "&rrap_dir./macro/rrap/rrap_mortgage_stat_formats.sas";

%LET DB2PASSTHRU=OWSTAR;  
%LET DBNAME=DM1D1D;

%global rrap_dir FRG_DB FRG_LIB RRAP_DB;
%global source_lib target_lib ds acct_key cust_key time_key target_lib2 target_lib3 db2tblspc frg_db cust_key2 time_keyc;

%LET RRAP_DB=EDRTLRP1D;
%let FRG_DB=FRG_USER_DATA;
%let FRG_LIB=NZUSER;

/*%let rrap_dir=/sastemp/rrap_bns;*/
LIBNAME intmed BASE "&rrap_dir./data/rrap/mortgage/intermediate";
LIBNAME results BASE "&rrap_dir./data/rrap/mortgage/results";

LIBNAME source BASE "&rrap_dir./data/rrap/mortgage/source";
libname src         "&rrap_dir./data/rrap/mortgage/source";
LIBNAME target BASE "&rrap_dir./data/rrap/mortgage/target";

/*
libname NZ netezza server=cs2iwntzu01 database=credit_risk_dev user="&nzuserdev" pwd="&nzpassworddev";* bulkunload=yes;
libname NZUSER netezza server=cs2iwntzp01 database=frg_user_data_IC bulkunload=NO user="&nzuser" pwd="&nzpassword";
libname NZRRAP netezza server=cs2iwntzp01 database=EDRTLRP1D_IC user="&nzuser" pwd="&nzpassword";
*/

%LET IIASDB=BLUDBPRD; 

/*%let credit_db=credit_risk;
%let frg_usr=frg_user_data; 
%Let CREDIT_DB=CREDIT_RISK;
%let FRG_USR=FRG_USER_DATA; */

%let credit_db=CREDIT_RISK;
%let frg_usr=FRG_USER_DATA; 

%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
%let iiasuser=iiastestusr;
%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;

/* IIAS LIB */

LIBNAME NZ     DB2 DATABASE="&IIASDB" SCHEMA="&credit_db" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZUSER     DB2 DATABASE="&IIASDB" SCHEMA="&frg_usr" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZRRAP     DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZWRK     DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRFRGP1D" user="&iiasuser" pwd="&iiaspassword";


/* DB2 Lib */
LIBNAME Owstar DB2  Datasrc="&DB2PASSTHRU"  SCHEMA=OWSTAR  user="&db2user" pwd="&db2password" ACCESS=READONLY;
LIBNAME CB DB2  Datasrc="&DB2PASSTHRU"  SCHEMA=CB  user="&db2user" pwd="&db2password" ACCESS=READONLY;
LIBNAME SAS DB2  Datasrc="&DB2PASSTHRU"  SCHEMA=FRG  user="&db2user" pwd="&db2password" readbuff=300 ACCESS=READONLY;
libname owdss db2 datasrc="&DB2PASSTHRU" schema=owdss  user="&db2user" pwd="&db2password" ACCESS=READONLY;
LIBNAME EDRTLR DB2 DATASRC="&DBNAME" SCHEMA=EDRTLR  user="&db2user2" pwd="&db2password2"; /*genl_lkp uses this*** Dev */ 
LIBNAME OWTACT DB2 DATASRC="&DB2PASSTHRU" SCHEMA=OWTACT user="&db2user" pwd="&db2password" ACCESS=READONLY; 
LIBNAME DB2RRAP db2 datasrc="&DBNAME" Schema=EDRRAPT  user="&db2user2" pwd="&db2password2"; /*Dev */
/*OWTACT is used for source automation cust_xref*/


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
/*libname conMO sqlsvr dsn=BASEL_LGD SCHEMA=DBO user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000;*/

/*FOR MODEL SCHEDULES*/
/*Job found = MODEL_50_LOAD_BRDM_TO_NETEZZA*/
/*libname brdm sqlsvr dsn=BSL_IST_BRDM_DR SCHEMA=dbo user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000; */
/*PROD*/
/*libname brdm sqlsvr dsn=BSL_BRDM_DR SCHEMA=dbo user= &_userid password= &_pwd_sql readbuff=10000 INSERTBUFF=10000; */

/*Compensation Control*/
%global threshold; %let threshold=0.0001;
/*libname sql_dms sqlsvr dsn=dms schema=dbo authdomain="SQLSRV_Auth";*/
/*libname NZWRK netezza server=cs2iwntzu01 database=EDRTLRBTWRKD1D user="&nzuserdev" pwd="&nzpassworddev"; */

%mend rrap_mor_bns_autoexec;




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

%LET REPORT_PATH=&rrap_dir./flat_files/rrap; /*used for ncr validation report*/
%let path="&OUTPATH./rk/outgoing/AE_BE_&MONTHYEAR..DAT"; /*used for ncr validation report*/
%let path2="&OUTPATH./rk/outgoing/AE_BD_&MONTHYEAR..DAT"; /*used for ncr validation report*/
%let pathla="&OUTPATH./rk/outgoing/history/&prev_yearmonth/AE_BE_&PREV_MONTHYEAR..DAT"; /*used for ncr validation report*/
%let path2la="&OUTPATH./rk/outgoing/history/&prev_yearmonth/AE_BD_&PREV_MONTHYEAR..DAT"; /*used for ncr validation report*/

%MEND report_validation;


%MACRO RRAP_MOR_TNG_AUTOEXEC();
/*data _null_;abort;run;*/
%GLOBAL FRG_DB frg_lib RRAP_DB TNG_DB EDRTLRFRGP1D IIASDB;

/*
%let nzlogin=owdev;
%let nzpassword={SAS002}0F833D4941CA4F644D8724214AE838AC3F916580;

%let nzUATlogin=s5197480;
%let nzUATpassword={SAS002}73919B4456A5EA4B408D9A4F2AE25019051828E4;	

*/

%let db2login=owdev;
%let db2password={SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9;


/* NETEZZA 
libname netcon netezza server=cs2iwntzu01 database=TNGSTD1D user="&nzUATlogin" pwd="&nzUATpassword";
libname tngdata netezza server=cs2iwntzu01 database=TNGSTD1D user="&nzUATlogin" pwd="&nzUATpassword";

libname nz_frg netezza database = frg_user_data_IC server = cs2iwntzp01 user="&nzlogin" pwd="&nzpassword" bulkunload=yes;
libname nzuser netezza database = frg_user_data_IC server = cs2iwntzp01 user="&nzlogin" pwd="&nzpassword" bulkunload=yes;

libname NZRRAP netezza server=cs2iwntzp01 database=EDRTLRP1D user="&nzlogin" pwd="&nzpassword";
*/


/* IIAS */
%let FRG_DB=FRG_USER_DATA;
%let FRG_LIB=NZUSER; 
%let RRAP_DB=EDRTLRP1D; 
%let TNG_DB=TNGSTP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;

%LET IIASDB=BLUDBPRD; 
%let iiasuser=iiastestusr;
%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;

LIBNAME netcon DB2 DATABASE="&IIASDB" SCHEMA="&TNG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME tngdata DB2 DATABASE="&IIASDB" SCHEMA="&TNG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME nz_frg DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME nzuser DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&RRAP_DB" user="&iiasuser" pwd="&iiaspassword";

/* DB2 */
libname db2_org db2 database=DM1D1D schema=EDEDW user="&db2login" pwd="&db2password";
libname db2_all db2 database=DM1D1D schema=EDRRAP user="&db2login" pwd="&db2password";
libname db2_tmdm db2 database=DM1D1D schema=EDEDWT user="&db2login" pwd="&db2password";/*instrument fact uses this, revert this before promotion*/
LIBNAME DB2RRAP DB2  Datasrc=DM1D1D  SCHEMA=EDRRAPT user="&db2login" pwd="&db2password";


/* SAS */
LIBNAME intmed BASE "&rrap_dir./data/rrap/mortgage/intermediate";
LIBNAME results BASE "&rrap_dir./data/rrap/mortgage/results";
LIBNAME control BASE "&rrap_dir/params/rrap";
LIBNAME source BASE "&rrap_dir./data/rrap/mortgage/source";
LIBNAME RRAP BASE "&rrap_dir./data/rrap";

/*Compensation Control*/
%global threshold;%let threshold=0.0001;
LIBNAME NZWRK DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRFRGP1D" user="&iiasuser" pwd="&iiaspassword";

%MEND RRAP_MOR_TNG_AUTOEXEC;

%MACRO rrap_autoexec2(RRAPEnv=);

/*data _null_;abort;run;*/

/*-----------------------------------------------------------------------------------------------
Maintenance Log:
-------------------------------------------------------------------------------------------------
THIS MACRO IS USED IN ALL REVOLVING CREDIT SAS PROGRAMS AND DIS BASED DEPLOYED JOBS.
THIS MACRO INITIALIZES PARAMETERS AND LIBRARIES
-------------------------------------------------------------------------------------------------*/
LIBNAME control BASE "&rrap_dir/params/rrap";
LIBNAME intmed  BASE "&rrap_dir/data/rrap";

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
%GLOBAL ACAPRPT;
%IF %INDEX(%UPCASE(&RRAPEnv), REVOLVING_CREDIT) %THEN %DO;

%LET net_serv = cs2iwntzu01;
%LET net_db =  EDRTLRU1D;
%LET net_wrk = EDRTLRFRGU1D; /*this database used for control table;*/
%LET net_sche = GDAW;
	
LIBNAME NETCON NETEZZA SERVER=&net_serv DATABASE=&net_db user=owetl pwd="{SAS002}9A8C3F4E0119DC870F23590F2992CC5E0C236093" access=readonly;
LIBNAME NZRRAP NETEZZA SERVER=&net_serv DATABASE=&net_db user=owetl pwd="{SAS002}9A8C3F4E0119DC870F23590F2992CC5E0C236093" access=readonly;
/*NZRRAP added since the control table uses it*/
	
/* %LET NETE_PASS_THROUGH=SERVER=&net_serv DATABASE=&net_db user=s5197480 pwd="{SAS002}C4A12048334B4ED916AC635C3E96C580"; */
LIBNAME NZUSER NETEZZA SERVER=&net_serv DATABASE=&net_wrk user=owetl pwd="{SAS002}9A8C3F4E0119DC870F23590F2992CC5E0C236093" access=readonly;
LIBNAME PRG_DATA "&rrap_dir./data/rrap/reporting";
%LET PARMFILE_PATH = "&rrap_dir./params/rrap/rrap_batch.sasprm";
libname rrap_lib "&rrap_dir./data/rrap";
	
/*PARAMETERS BELOW ARE USED IN SAS REPORTING (NCR, ECAP, CCAR)*/
%LET DB2PASSTHRU=OWSTAR;  
%LET DBNAME=DM1U1D;
%LET DB=&DBNAME;
%LET LIB=DRAPT;
%LET DBSCHEMA=EDRRAPT;
%LET DBSCHEMA1=EDRTLRT;
%LET MASTER_TABLE=BASEL_CUST_ACCT_RLTNP_SNAPSHOT; /*changed after batch1.1 didnt extract the latest month*/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET DPATH=&rrap_dir./data/rrap/reporting;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET DIM_NOT= ;
LIBNAME &LIB. "&DPATH.";
LIBNAME LDD	DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";
LIBNAME DB2RRAP DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";
LIBNAME DDB2CON DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";
LIBNAME &DB.    DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";
LIBNAME EDRRAPT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";
LIBNAME EDRTLRT DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA1" user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";
LIBNAME INPATH "&DPATH.";
LIBNAME OUTFILE "&DPATH.";
/*added acap libraries - Jan 2018*/
%LET ACAPRPT=&rrap_dir./flat_files/rrap/acap;

%LET INPATH=INPATH;

/*DB2 LIBRARIES INITIALIZED*/
/*ONLY DEP TXN AND POSTN SUM FACT IN REVOLVING CREDIT USE THIS PASSTHROUGH/OWSTAR*/
/*LIBNAME OWSTAR DB2 DATASRC=&DB2PASSTHRU SCHEMA=OWSTAR user=s5197480 password="{SAS002}5C1C7250259DC8A92EEDF5F157D9D62120D17721";*/

/*FETCH THE LATEST PROCESSING MONTH FROM THE MASTER TABLE AND LOAD INTO A VARIABLE MTH_TM_ID*/

%LET MTH_TM_ID=17996;


/*INITIALIZING SQL SERVER LIBRARIES*/
%let dsnn=BSL_BRDM_DR;
%let schma=brdm_dr;
%let usr=usr_rrap;
/* %let passw={SAS002}160E9F27213F11B320B8B6EA10D19624;
libname IST sqlsvr dsn=&dsnn user=&usr password= "&passw."; */

%END;

%IF %INDEX(%UPCASE(&RRAPEnv), BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%GLOBAL RRAP_DB FRG_DB RRAP_DB2 FRG_USR;

* Netezza and DB2 Servers and Databases;
* When making changes here, make sure to change the corresponding metadata library;

%let NZ_server = cs2iwntzu01;
%let RRAP_DB   = EDRTLRU1D; 
%let FRG_DB    = EDRTLRFRGU1D;
%let FRG_USR = FRG_USER_DATA_UAT;
%let RRAP_DB2  = DM1U1D;

/* Used to define connections to Netezza and DB2 */
libname nzuser   netezza server = &NZ_server database = EDRTLRFRGD1D   user=s2345125 pwd="{SAS002}0F833D4941CA4F644D8724214AE838AC3F916580";
libname nzintmed netezza server = &NZ_server database = &FRG_DB   user=owetl pwd="{SAS002}9A8C3F4E0119DC870F23590F2992CC5E0C236093" ;
libname NZRRAP   netezza server = &NZ_server database = &RRAP_DB  user=owetl pwd="{SAS002}9A8C3F4E0119DC870F23590F2992CC5E0C236093" ;
libname NZFRG   netezza server = &NZ_server database = &FRG_USR   user=owetl pwd="{SAS002}9A8C3F4E0119DC870F23590F2992CC5E0C236093" ;
LIBNAME DB2RRAP  DB2 	 DATABASE=&RRAP_DB2  SCHEMA=EDRRAPT  user=owuat password="{SAS002}8257DA2A339868A802B4DBC51871ADD2060C238E";

%MEND rrap_autoexec2;

%macro rrap_mor_bns_autoexec2();
/*data _null_;abort;run;*/

options nosymbolgen nomlogic mprint compress=yes; 

%let nzuser=owdev;
%let nzpassword={SAS002}0F833D4941CA4F644D8724214AE838AC3F916580;  

%let nzuserdev=s5197480;
%let nzpassworddev={SAS002}73919B4456A5EA4B408D9A4F2AE25019051828E4;  

LIBNAME control BASE "&rrap_dir/params/rrap";
%include "&rrap_dir./macro/rrap/rrap_mortgage_nz_del_dup.sas";
%include "&rrap_dir./macro/rrap/rrap_mortgage_stat_formats.sas";

%LET DB2PASSTHRU=OWSTAR;  
%LET DBNAME=DM1D1D;

%global rrap_dir FRG_DB FRG_LIB RRAP_DB;
%global source_lib target_lib ds acct_key cust_key time_key target_lib2 target_lib3 db2tblspc frg_db cust_key2 time_keyc;

%LET RRAP_DB=EDRTLRP1D;
%let FRG_DB=FRG_USER_DATA;
%let FRG_LIB=NZUSER;

/*%let rrap_dir=/sastemp/rrap_bns;*/
LIBNAME intmed BASE "&rrap_dir./data/rrap/mortgage/intermediate";
LIBNAME results BASE "&rrap_dir./data/rrap/mortgage/results";
LIBNAME source BASE "&rrap_dir./data/rrap/mortgage/source";
libname src         "&rrap_dir./data/rrap/mortgage/source";
LIBNAME target BASE "&rrap_dir./data/rrap/mortgage/target";

libname NZ netezza server=cs2iwntzu01 database=credit_risk_dev user="s2993929" pwd="{SAS002}B94E4C3C0F11A1CE4A40A9E25585999912466408";
libname NZUSER netezza server=cs2iwntzp01 database=frg_user_data user="s2993929" pwd="{SAS002}B94E4C3C0F11A1CE4A40A9E25585999912466408";
libname NZRRAP netezza server=cs2iwntzp01 database=EDRTLRP1D user="s2993929" pwd="{SAS002}B94E4C3C0F11A1CE4A40A9E25585999912466408";
libname NZRRAPIC netezza server=cs2iwntzp01 database=EDRTLRP1D_IC user="s4126431" pwd="{SAS002}B94E4C3C0F11A1CE4A40A9E25585999912466408";
libname NZUSERIC netezza server=cs2iwntzp01 database=frg_user_data_IC user="s4126431" pwd="{SAS002}B94E4C3C0F11A1CE4A40A9E25585999912466408";

%let credit_db=credit_risk;
%let frg_usr=frg_user_data; 
%Let CREDIT_DB=CREDIT_RISK;
%let FRG_USR=FRG_USER_DATA; 

%let credit_db=CREDIT_RISK;
%let frg_usr=FRG_USER_DATA; 
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
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
/*libname conMO sqlsvr dsn=BASEL_LGD SCHEMA=DBO user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000;*/

/*FOR MODEL SCHEDULES*/
/*Job found = MODEL_50_LOAD_BRDM_TO_NETEZZA*/
/*libname brdm sqlsvr dsn=BSL_IST_BRDM_DR SCHEMA=dbo user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000; */
/*PROD*/
/*libname brdm sqlsvr dsn=BSL_BRDM_DR SCHEMA=dbo user= &_userid password= &_pwd_sql readbuff=10000 INSERTBUFF=10000; */

/*Compensation Control*/
%global threshold; %let threshold=0.0001;
/*libname sql_dms sqlsvr dsn=dms schema=dbo authdomain="SQLSRV_Auth";*/
/*libname NZWRK netezza server=cs2iwntzu01 database=EDRTLRBTWRKD1D user="&nzuserdev" pwd="&nzpassworddev"; */

%mend rrap_mor_bns_autoexec2;



%macro rrap_spl_autoexec_NZ();
/*Safe guard against accidental runs */
/*data _null_;abort;run;*/

%GLOBAL RRAP_DB FRG_DB RRAP_DB2 FRG_USR NZRRAP nzuser nzintmed NZFRG Owstar DB2RRAP FRG_DB_D;

LIBNAME control BASE "&rrap_dir/params/rrap";
LIBNAME intmed  BASE "&rrap_dir/data/rrap";


%LET IIASDB=BLUDBPRD; 
%Let CREDIT_DB=CREDIT_RISK;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
%let iiasuser=iiastestusr;
%let iiaspassword={SAS002}B0137E2F2B94942D00EE2A8B3F7F443C5916C0101D8F8E710CECFD9948E623F9;
%let db2user=owdev;
%let db2password={SAS002}5BC89D53059BB20411C79D78391ED7260C683AC9;

%let RRAP_DB   = EDRTLRP1D; 
%let FRG_DB    = EDRTLRFRGP1D; /* For Writing */
%let FRG_DB_D   = EDRTLRFRGD1D; /* For Writing */
%let FRG_USR = FRG_USER_DATA;
%let RRAP_DB2  = DM1D1D;
%LET DB2PASSTHRU=OWSTAR;

* Netezza and DB2 Servers and Databases;
* When making changes here, make sure to change the corresponding metadata library;
%let NZ_server = cs2iwntzp01;
%let NZ_server_D = cs2iwntzu01;
%let nzuser_d =owetl;
%let nzpassword_d = {SAS002}73919B4456A5EA4B408D9A4F2AE25019051828E4;
%let nzuser=s1327057;
%let nzpassword={SAS002}0F833D4941CA4F644D87242158DDEADA525EAA72; 

/* Used to define connections to Netezza and DB2 */
/* NZ Lib */

libname nzuser  netezza server = &NZ_server_D database = &FRG_DB_D    user="&nzuser_d" pwd="&nzpassword_d"; /* For Delete & Write */

*libname nzuser   netezza server = &NZ_server database = &FRG_DB    user="&nzuser" pwd="&nzpassword" access=readonly;
libname nzintmed netezza server = &NZ_server database = &FRG_DB    user="&nzuser" pwd="&nzpassword" access=readonly;
libname NZRRAP   netezza server = &NZ_server database = &RRAP_DB   user="&nzuser" pwd="&nzpassword" access=readonly;
libname NZFRG   netezza server = &NZ_server database = &FRG_USR   user="&nzuser" pwd="&nzpassword" access=readonly;

/* DB2 Lib */
LIBNAME DB2RRAP  DB2  DATABASE="&RRAP_DB2"  SCHEMA=EDRRAPT user="&db2user" pwd="&db2password";
LIBNAME Owstar DB2  Datasrc="&DB2PASSTHRU"  SCHEMA=OWSTAR  user="s5197480" pwd="{SAS002}D5637855096E87730938B12F2E22D60C1606846E" access=readonly;

/* IIAS Lib */
/*
LIBNAME nzuser  DB2 DATABASE="&IIASDB"  SCHEMA="&FRG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME nzintmed  DB2 DATABASE="&IIASDB" SCHEMA="&FRG_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZRRAP  DB2 DATABASE="&IIASDB" SCHEMA="&RRAP_DB" user="&iiasuser" pwd="&iiaspassword";
LIBNAME NZFRG  DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" user="&iiasuser" pwd="&iiaspassword";
*/

/*
%let sqluser=usr_rrap_frg;
%let passwd="{SAS002}BA67F42C2F2D41B230B50A7A4AA46EF04FEFE7A7";
*/
/*
libname AIRBSQL sqlsvr dsn=AIRB_RECON SCHEMA=dbo user= &sqluser password= &passwd readbuff=10000 INSERTBUFF=10000; 
*/
/*In order to hard code dates, run the code below in Enterprise Guide; */

/*************************************************************************

     %include '/sasdata/sasdi/sasprod/macro/rrap/rrap_autoexec.sas';
     %rrap_spl_autoexec;
     %HARD_CODE_DATES(RUN_START_DATE=30APR2015,RUN_END_DATE=30APR2015);
     %LIST_RRAP_PARAMETERS;

*************************************************************************/

 %macro delete_table(table_name);
         
            %if %sysfunc(exist(&table_name)) %then %do;
               proc sql;
               drop table &table_name.;
               quit;
            %end;
         
 %mend delete_table;

%mend rrap_spl_autoexec_NZ;
