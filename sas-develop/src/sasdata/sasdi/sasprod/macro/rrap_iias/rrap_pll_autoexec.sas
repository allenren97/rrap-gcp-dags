/**TAKEN FROM RRAP_IIAS_AUTOEXEC.SAS */
/* Set metadata options */
      
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8563);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
metaserver     = "&metaServer"; 


%let rrap_dir=/sasdata/sasdi/sasprod;
options mautosource sasautos=("&rrap_dir./macro/rrap_iias", sasautos);
options emailid="RRAP <&sysuserid.@&syshostname..bns>";

%LET OUTPATH=/owpftp;
%let owftp=&outpath.;

%MACRO rrap_pll_autoexec(RRAPEnv=);

LIBNAME control BASE "&rrap_dir/params/rrap_iias";

%IF &RRAPEnv =
%THEN %LET RRAPEnv = REVOLVING_CREDIT;

%include "&rrap_dir./macro/rrap_iias/rrap_defaulter_model.sas";

%global threshold;
%let threshold=0.0001;
OPTIONS COMPRESS=Y STIMER THREADS DBSLICEPARM=(ALL,10);

%GLOBAL MASTER_TABLE;
%GLOBAL MTH_TM_ID; 
%GLOBAL MTH_END_DT;
%GLOBAL YEARMONTH;
%GLOBAL INPATH;
%GLOBAL OUTPATH;
%GLOBAL DPATH;
%GLOBAL LIB;
%GLOBAL SESSIONTIME;

%GLOBAL Processing_Month_Time_ID;
%GLOBAL net_serv net_db net_wrk net_sche net_user net_pwd dsnn schma usr passw;

%GLOBAL IIASDB;
%IF %INDEX(%UPCASE(&RRAPEnv), REVOLVING_CREDIT) %THEN %DO;

%LET net_db =  EDRTLRPLL; /**schema for parallel run */
%LET net_wrk = EDRTLRPLL; /*used for control table */
%LET IIASDB=BLUDBPRD;  /**database*/

LIBNAME NETCON   DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP   DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NETEPASS DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZUSER   DB2 DATABASE="&IIASDB" SCHEMA="&net_wrk" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;    

LIBNAME PRG_DATA "&rrap_dir./data/rrap_iias/reporting";
%LET PARMFILE_PATH = "&rrap_dir./params/rrap_iias/rrap_batch.sasprm";
libname rrap_lib "&rrap_dir./data/rrap_iias";
     

%LET MASTER_TABLE=BASEL_CUST_ACCT_RLTNP_SNAPSHOT; /*changed after batch1.1 didnt extract the latest month/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET DPATH=&rrap_dir./data/rrap_iias/reporting;

LIBNAME INPATH "&DPATH.";
LIBNAME OUTFILE "&DPATH.";
/*added acap libraries - Jan 2018*/
LIBNAME ACAPTEMP BASE "&rrap_dir./data/rrap_iias/reporting/acap";
%LET ACAPRPT=&rrap_dir./flat_files/rrap/acap;

%LET INPATH=INPATH;



/*%let mth_tm_id=20356;/**hardcoded for testing purposes */

%let rundate = &sysdate9.;

data rundate;
	format mth_end_dt date9.;
	mth_end_dt= intnx('MONTH',"&rundate."d,-1,'e');
run;

proc sql;
	create table time_key as
	select tm_id as MTH_TM_ID, mth_end_dt
	from nzrrap.tm_dim a, rundate b
	where tm_lvl='Month' and tm_lvl_end_dt = mth_end_dt
	order by 1;
quit;

data _null_;
	set time_key;
	call symputx('mth_tm_id',put(mth_tm_id,5.));
	call symputx('mth_end_dt',put(mth_end_dt,date9.));
	call symputx('mth_end_dt_nz',put(mth_end_dt,yymmdd10.));
	call symputx('yrmth',put(mth_end_dt,yymmn6.));
	call symputx('yrmth_prev',put(intnx('Month',mth_end_dt,-1,'e'),yymmn6.));
run;

options nosource ;
%PUT ************************************************************************************;
%PUT RUN ON &RUNDATE. at &SYSTIME. for MONTH ENDING &MTH_END_DT.;
%PUT MTH_TM_ID = &MTH_TM_ID.;
%PUT MTH_END_DT = &MTH_END_DT.;
%PUT MTH_END_DT_NZ = &MTH_END_DT_NZ.;
%PUT YRMTH = &YRMTH.;
%PUT ************************************************************************************;
options source;

%PUT >>> MTH_TM_ID IS &MTH_TM_ID.;


%END;

%IF %INDEX(%UPCASE(&RRAPEnv), BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%MEND rrap_pll_autoexec;

%macro rrap_pll_exception_rpt_autoexec();
%GLOBAL IIASDB CREDIT_DB FRG_USR EDRTLRP1D EDRTLRFRGP1D IIASUSER NZRRAP FRG DM1P1D NZUAT; 
%LET IIASDB=BLUDBPRD; 
/* %Let CREDIT_DB=CREDIT_RISK; */
%let FRG_USR=FRG_USER_DATA;
%let EDRTLRP1D=EDRTLRPLL;
/* %let EDRTLRFRGP1D  = EDRTLRFRGP1D ; */

/* for report 1 SPL, KS  */  
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" authdomain="IIAS_Auth"  readbuff=10000 INSERTBUFF=10000;

/* for report 1 MOR  */
LIBNAME FRG DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;

/* for report 3 */
/* LIBNAME DM1P1D DB2 DATABASE="DM1P1D" SCHEMA="EDRRAP" authdomain=DB2_AUTH readbuff=10000 INSERTBUFF=10000; */

/* report 4  */
/* LIBNAME NZUAT DB2 DATABASE="&IIASDB" SCHEMA="&NET_SCHE" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000; */

%mend rrap_pll_exception_rpt_autoexec;

%macro rrap_exception_rpt_autoexec();
%GLOBAL IIASDB CREDIT_DB FRG_USR EDRTLRP1D EDRTLRFRGP1D IIASUSER NZRRAP FRG DM1P1D NZUAT EDRPLL FRGPLL; 
%LET IIASDB=BLUDBPRD; 
%Let CREDIT_DB=CREDIT_RISK;
%let FRG_USR=FRG_USER_DATA;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
%LET EDRPLL = EDRTLRPLL;
%LET FRGPLL = FRGPLL;
/*%LET EDRPLL = EDRTLRP1D;
%LET FRGPLL = FRG_USER_DATA;*/


LIBNAME PASTHRU DB2 DATABASE="&IIASDB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" authdomain="IIAS_Auth"  readbuff=10000 INSERTBUFF=10000;
LIBNAME FRG DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME DM1P1D DB2 DATABASE="DM1P1D" SCHEMA="EDRRAP" authdomain=db2_auth PRESERVE_USER=YES readbuff=10000 INSERTBUFF=10000;

%mend rrap_exception_rpt_autoexec;

%MACRO RRAP_MOR_BNS_AUTOEXEC;

%GLOBAL FRGPLL FRG_ORIG;

%LET IIASDB=BLUDBPRD;
%LET FRG_ORIG = FRG_USER_DATA;
%let FRGPLL=FRGPLL;
LIBNAME PASTHRU DB2 DATABASE="&IIASDB" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME FRGPLL DB2 DATABASE="&IIASDB" SCHEMA=&FRGPLL authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME FRG_ORIG DB2 DATABASE="&IIASDB" SCHEMA=&FRG_ORIG authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME INTMED "&rrap_dir./data/rrap_iias/mortgage/intermediate";

%MEND RRAP_MOR_BNS_AUTOEXEC;


%MACRO rrap_pll_ksmor_autoexec(RRAPEnv=);

LIBNAME control BASE "&rrap_dir/params/rrap_iias";

%IF &RRAPEnv = 
%THEN %LET RRAPEnv = REVOLVING_CREDIT;

%include "&rrap_dir./macro/rrap_iias/rrap_defaulter_model.sas";

%global threshold;
%let threshold=0.0001;
OPTIONS COMPRESS=Y STIMER THREADS DBSLICEPARM=(ALL,10);

%GLOBAL MASTER_TABLE;
%GLOBAL MTH_TM_ID; 
%GLOBAL MTH_END_DT;
%GLOBAL YEARMONTH;
%GLOBAL INPATH;
%GLOBAL OUTPATH;
%GLOBAL DPATH;
%GLOBAL LIB;
%GLOBAL SESSIONTIME;
%GLOBAL DBNAME;
%GLOBAL YRMTH;

%GLOBAL Processing_Month_Time_ID;
%GLOBAL net_serv net_db net_wrk net_sche net_user net_pwd dsnn schma usr passw DBSCHEMA DB2DB DB net_db_P1D net_db_FRG pll_db_FRG;

%GLOBAL IIASDB;
%IF %INDEX(%UPCASE(&RRAPEnv), REVOLVING_CREDIT) %THEN %DO;

%LET net_db =  EDRTLRPLL; /**schema for parallel run */
%let net_db_P1D= EDRTLRP1D;   /**schema for EDRTLRP1D in  parallel run */
%LET net_wrk = EDRTLRPLL; /*used for control table */
%LET IIASDB=BLUDBPRD;  /**database*/
%LET DBNAME=DM1P1D;
%LET DB=&DBNAME;
%let DB2DB  =  DM1P1D;
%let DBSCHEMA=EDRRAPT;
%let net_db_FRG = FRG_USER_DATA;
%let pll_db_FRG = FRGPLL;

LIBNAME FRGUSER  DB2 DATABASE="&IIASDB" SCHEMA="&net_db_FRG" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME FRGPLL   DB2 DATABASE="&IIASDB" SCHEMA="&pll_db_FRG" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
libname DB2RRAP DB2 database="&DB2DB."   schema="&DBSCHEMA."   authdomain=db2_auth read_isolation_level=ur readbuff=32000;
LIBNAME NETCON   DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZRRAP   DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NETEPASS DB2 DATABASE="&IIASDB" SCHEMA="&net_db" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME NZUSER   DB2 DATABASE="&IIASDB" SCHEMA="&net_wrk" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;    
LIBNAME TLRP1D   DB2 DATABASE="&IIASDB" SCHEMA="&net_db_P1D" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;    

LIBNAME PRG_DATA "&rrap_dir./data/rrap_iias/reporting";
%LET PARMFILE_PATH = "&rrap_dir./params/rrap_iias/rrap_batch.sasprm";
libname rrap_lib "&rrap_dir./data/rrap_iias";
     

%LET MASTER_TABLE=BASEL_CUST_ACCT_RLTNP_SNAPSHOT; /*changed after batch1.1 didnt extract the latest month/
%LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); /*CURRENT TIMESTAMP FROM SAS */
%LET DPATH=&rrap_dir./data/rrap_iias/reporting;

LIBNAME INPATH "&DPATH.";
LIBNAME OUTFILE "&DPATH.";
/*added acap libraries - Jan 2018*/
LIBNAME ACAPTEMP BASE "&rrap_dir./data/rrap_iias/reporting/acap";
%LET ACAPRPT=&rrap_dir./flat_files/rrap/acap;

%LET INPATH=INPATH;



/*%let mth_tm_id=20356;/**hardcoded for testing purposes */

%let rundate = &sysdate9.;

data rundate;
	format mth_end_dt date9.;
	mth_end_dt= intnx('MONTH',"&rundate."d,-1,'e');
run;

proc sql;
	create table time_key as
	select tm_id as MTH_TM_ID, mth_end_dt
	from nzrrap.tm_dim a, rundate b
	where tm_lvl='Month' and tm_lvl_end_dt = mth_end_dt
	order by 1;
quit;

data _null_;
	set time_key;
	call symputx('mth_tm_id',put(mth_tm_id,5.));
	call symputx('mth_end_dt',put(mth_end_dt,date9.));
	call symputx('mth_end_dt_nz',put(mth_end_dt,yymmdd10.));
	call symputx('yrmth',put(mth_end_dt,yymmn6.));
	call symputx('yrmth_prev',put(intnx('Month',mth_end_dt,-1,'e'),yymmn6.));
run;

options nosource ;
%PUT ************************************************************************************;
%PUT RUN ON &RUNDATE. at &SYSTIME. for MONTH ENDING &MTH_END_DT.;
%PUT MTH_TM_ID = &MTH_TM_ID.;
%PUT MTH_END_DT = &MTH_END_DT.;
%PUT MTH_END_DT_NZ = &MTH_END_DT_NZ.;
%PUT YRMTH = &YRMTH.;
%PUT ************************************************************************************;
options source;

%PUT >>> MTH_TM_ID IS &MTH_TM_ID.;


%END;

%IF %INDEX(%UPCASE(&RRAPEnv), BASELDB2) %THEN %DO;      

%PUT NOTHING TO DECLARE;
%END;

%MEND rrap_pll_ksmor_autoexec;





%macro rrap_pllspl_autoexec();
/*Safe guard against accidental runs */
/*data _null_;abort;run;*/

%GLOBAL RRAP_DB FRG_DB RRAP_DB2 FRG_USR NZRRAP nzuser nzintmed NZFRG Owstar DB2RRAP EDRTLRFRGP1D;

LIBNAME control BASE "&rrap_dir/params/rrap_iias";
LIBNAME intmed  BASE "&rrap_dir/data/rrap_iias";

%LET IIASDB=BLUDBPRD;
%let EDRTLRP1D=EDRTLRP1D;
%let EDRTLRFRGP1D  = EDRTLRPLL ;

%let RRAP_DB   = EDRTLRPLL; 
%let FRG_DB    = EDRTLRPLL;
%let FRG_USR = FRGPLL;
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
%mend rrap_pllspl_autoexec;
