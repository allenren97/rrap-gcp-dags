options mprint COMPRESS=Y STIMER THREADS DBSLICEPARM=(ALL,10);

%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);
/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer";


%let env=prod;
options mautosource sasautos=("/sasdata/sasdi/sas&env./macro/triad", sasautos);

libname EDRRAPT db2 datasrc=DM1P1D schema=EDRRAPT access=readonly authdomain=db2_auth;
/* libname NZRRAP netezza server=cs2iwntzp01 database=EDRTLRP1D access=readonly authdomain=nz_auth; */
LIBNAME NZRRAP DB2 DATABASE=BLUDBPRD SCHEMA=EDRTLRP1D authdomain="IIAS_Auth" readbuff=10000 INSERTBUFF=10000;
libname TRIAD "/sasdata/sasdi/sas&env./data/triad";

%let owftp=/owpftp;

data _null_;
	set nzrrap.tm_dim;
	where tm_lvl='Month' and tm_lvl_end_dt=intnx('Month',"&sysdate9."d,-1,'e');
	call symputx('mth_tm_id',tm_id);
	call symputx('mth_end_dt',put(tm_lvl_end_dt,date9.));
run;

/*HARD CODE DATES IN THE STEP BELOW*/
/*%let mth_tm_id=17516; %let mth_end_dt=31AUG2018;*/

%put &mth_tm_id. &mth_end_dt.;