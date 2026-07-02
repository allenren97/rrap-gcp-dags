%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer";
		
%let usr_n =owprdntz; 
%let pwd_n =%STR({SAS002}B0E0AA3D417B508E4D50C0CB2ABCF12E);/* Paste encoded Netezza Password inside brackets from the log*/ 

/* !!!! THE FOLLOWING SHOULD BE SET TO 0 UNLESS YOU ARE RUNNING THE JOBS RETROACTIVELY !!!! */
%let daysago=0;

/* !!!! SET THE FOLLOWING TO 99999 UNLESS YOU ARE RUNNING THE JOBS RETROACTIVELY !!!! */
%let maxtime=99999;

/* JU File Name for Probe */
%let Start_date_code = %sysfunc(today(),YYMMDDn8.);
%let JU_FILE_NAME = %sysfunc(CATS(eim_edwout_dl_score_f_mthly_,&Start_date_code.,.ascii));

/* libname RRAP_NZ netezza   server=cs2iwntzp01 database=EDRTLRP1D user=&USR_n password="&pwd_n" ; */
LIBNAME RRAP_NZ DB2  DATABASE=BLUDBPRD  SCHEMA=EDRTLRP1D   AUTHDOMAIN="IIAS_Auth" ;
libname RRAP_R db2 dsn=DM1P1D schema=EDRRAP readbuff=30000 user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)";
libname crdmC db2 dsn=DM1P1D schema=EDRTLR readbuff=30000 user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)";

libname OUTTAB db2 dsn=DM1P1D schema=EDRTLRT user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)";

/* Calculate the month end for which the model needs to be run. -1 below means last month */
%let pme_dt = %sysfunc(intnx(month,%sysfunc(today())-&daysago.,-1,E)); /*date of the previous month end. -1 has been used for previous month end*/
%let pme_yr= %sysfunc(year(&pme_dt)); /*year of the previous month end */
%let pme_mnth =%sysfunc(month(&pme_dt));/*month of the previous month end */
%let pme_my=%sysfunc(putn(&pme_dt,yymmn6.));  /* previous month and year for data sets naming*/

%let curr_dt = %sysfunc(intnx(month,%sysfunc(today())-&daysago.,0,E)); /*date of the current month end. 0 has been used for current month end*/
%let curr_yr= %sysfunc(year(&curr_dt)); /*year of the current month end */
%let curr_mnth =%sysfunc(month(&curr_dt));/*month of the current month end */
%let curr_my=%sysfunc(putn(&curr_dt,yymmn6.));  /* current month and year for data sets naming*/


data _null_; /*generate month Id for previous month (from DB2) */
set CRDMC.TM_DIM;
call symput("TM",TM_ID);
where TM_LVL='Month' 
and TM_LVL_END_DT = &pme_dt;
run;

data _null_; /*generate Date Id for previous month (from DB2) */
set CRDMC.TM_DIM;
call symput("dly_TM",TM_ID);
where TM_LVL='Day' 
and TM_LVL_END_DT = &pme_dt;
run;

proc sql noprint;
(select  max(a.EFF_TM_ID) as lst_dt INTO :Last_Avl_dt_mth from 
CRDMC.RISK_REVLVNG_CR_DLY_SNAPSHOT a
left join CRDMC.TM_DIM b
on b.TM_ID= a.EFF_TM_ID
where 
b.TM_LVL='Day' and a.EFF_TM_ID < &maxtime.
and month(b.TM_LVL_END_DT)=&curr_mnth
and year(b.TM_LVL_END_DT)=&curr_yr
);
quit;

%let DB2_svr=DM1P1D ;


%let JU_FTP_PTH=/owpftp/probe/out/;
%let dldropf=/owpftp/ccc/;


/* for dev testing please use the following */
/*
%let JU_FTP_PTH=/owftp/ccc/;
%let dldropf=/owftp/ccc/;
*/
%put &dldropf &JU_FTP_PTH &JU_FILE_NAME &DB2_svr &pme_dt &pme_yr &pme_mnth &pme_my &TM &dly_TM &Last_Avl_dt_mth;
