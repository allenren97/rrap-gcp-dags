



%let rrap_dir=/sasdata/sasdi/sasprod/macro/rrap_iias;
options mautosource sasautos=("&rrap_dir.", sasautos);


%macro rrap_ifrs9_autoexec(ENV=PROD);
options errorabend mprint source validvarname=any;

%global owftp IIASDB DB2DB net_db RRAP_DB RRAP_WRK DBSCHEMA mth_tm_id MTH_END_DT MTH_END_DT_NZ YRMTH
CREDIT_DB FRG_USR EDRTLRP1D EDRTLRFRGP1D IIASUSER NZRRAP FRG DM1P1D NZUAT rrap_dir;
%let rrap_dir=/sasdata/sasdi/sasprod/macro/rrap_iias;

	
%if &ENV. EQ PROD %then %do;
/**** Live PRODUCTION *****;*/
%let owftp  =  /owpftp;
********************************;
%let IIASDB =  BLUDBPRD;
%let DB2DB  =  DM1P1D;
********************************;
%let net_db = EDRTLRIFRS9;
%let RRAP_DB = EDRTLRIFRS9;
%let RRAP_WRK= EDRTLRIFRS9;
%let DBSCHEMA  = EDRRAPT;
%Let CREDIT_DB=CREDIT_RISK;
%let FRG_USR=FRG_USER_DATA;
%let EDRTLRP1D=EDRTLRIFRS9;
%let EDRTLRFRGP1D  = EDRTLRFRGP1D ;
/********************************/
%end;

libname NZUSER  db2 database="&IIASDB." schema=&RRAP_WRK. authdomain="IIAS_Auth"  readbuff=10000 INSERTBUFF=10000;
libname NETCON  db2 database="&IIASDB." schema=&RRAP_DB.  authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
libname NZRRAP  db2 database="&IIASDB." schema=&RRAP_DB. authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
LIBNAME OWSTAR  DB2 DATABASE=OWSTAR     SCHEMA=OWSTAR authdomain=db2_auth;
LIBNAME DB2RRAP DB2 DATABASE="&DB2DB."  SCHEMA="&DBSCHEMA." authdomain=DB2_AUTH;

libname prg_data %sysfunc(quote(%sysfunc(pathname(work)))) ;

/* for report 1 SPL, KS  */  
LIBNAME NZRRAP DB2 DATABASE="&IIASDB" SCHEMA="&EDRTLRP1D" authdomain="IIAS_Auth"  readbuff=10000 INSERTBUFF=10000;

/* for report 1 MOR  */
LIBNAME FRG DB2 DATABASE="&IIASDB" SCHEMA="&FRG_USR" authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;


%let rundate = &sysdate9.;
/*%let rundate=05Nov2022;*/

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

%mend rrap_ifrs9_autoexec;

