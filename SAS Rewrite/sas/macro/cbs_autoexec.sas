/* Create metadata macro variables */
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort
        metaserver     = "&metaServer";


%put WORK LOCATION: %sysfunc(getoption(work));

options mprint compress=yes  cpucount=actual threads;

%let env=prod;

%macro setenv;
%global RRAP_WRK cbspath owftp;
%if &env. EQ dev %then %do;
	%let RRAP_WRK=EDRTLRP1D_IC;
	%let cbspath=/sasdata/sasdi/ice/rrap/cbs/data;
	%let owftp=owftp;
%end;
%else %if &env. EQ prod %then %do;
	%let RRAP_WRK=EDRTLRFRGP1D;
	%let cbspath=/sasdata/sasdi/sasprod/data/cbs;
	%let owftp=owpftp;
%end;
%mend setenv;
%setenv;

%let RRAP_DB=EDRTLRP1D;
%let FRG_DB=FRG_USER_DATA;

libname NZWRK db2 database=BLUDBPRD schema=&RRAP_WRK. authdomain="IIAS_ETL_Auth" readbuff=10000 INSERTBUFF=10000;
libname NZPROD db2 database=BLUDBPRD schema=&RRAP_DB. authdomain="IIAS_ETL_Auth" readbuff=10000 INSERTBUFF=10000 access=readonly;

libname EDRRAPT db2 database=DM1P1D schema=EDRRAPT authdomain=db2_auth;
libname cbsdb2 db2 datasrc=OWSTAR schema=SAS authdomain=db2_auth;

libname cbs "&cbspath.";

***************************************************************************************************************************;
************************************************		Macros 	     ******************************************************;
***************************************************************************************************************************;

%let rundate=&sysdate9.;
/*%let rundate=21feb2019;*/

data _null_;
	call symputx('mth_end_dt',put(intnx('Month',"&rundate."d,-1,'e'),date9.));
run;

%put &mth_end_dt.;
/*%let mth_end_dt=30JUN2018;*/

data _null_;
	set nzprod.tm_dim;
	where tm_lvl_end_dt="&mth_end_dt."d and tm_lvl='Month';
	call symputx('string',cats("'",put("&mth_end_dt."d,yymmdd10.),"'"));
	call symputx('tm_id',tm_id);
	call symputx('dt2',put("&mth_end_dt."d,yymmn4.));
	call symputx('dt',cats("'",put("&mth_end_dt."d,yymmn4.),"'"));
call symputx('yyyymmdd',put("&mth_end_dt."d,yymmddn8.));


run;

%put NOTE: tm_id=&tm_id. string=&string. dt=&dt. dt2=&dt2.;
%put yyyymmdd=&yyyymmdd.;
