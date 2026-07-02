/* ************************************************************************ 
	Job name : RRAP_SPLMC_MOGAP_ADJOSBAL_SAS_EML.sas

	Data Source: 
		Data: 
		Time variables: EDRTLRP1D...tm_dim

	Job dependency: 

	Description: This is for the SPL COMMERCIAL MISCLASSIFICATION BREAKDOWN
				ATER RECON APPROVAL.

************************************************************************ */

/* ----------------------------
	Set Job options
   ---------------------------- */

/* Start timer */
%let _timer_start = %sysfunc(datetime());

/* Set options */
/* Create metadata macro variables */
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort
        metaserver     = "&metaServer";

/*%let rrap_dir=/sasdata/sasdi/sasprod;*/
options mautosource sasautos=("&rrap_dir./macro/rrap", sasautos);
%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_spl_autoexec;

/* global SAS session options */
OPTIONS STIMER THREADS DBSLICEPARM=(ALL,10);
options nosymbolgen nomlogic mprint compress=yes;


/* USED ONLY TO POPULATE CONTROL TABLE WITH PROCESSING MONTH */
%get_model_period_dates(product=spl);
%put Start and End Dates for SPL Models:;

%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;

%let start_date= &start_period_dt.;
%let end_date= &end_period_dt.;

%let _time_dim_tbl = tm_dim;

%global mth_tm_id PIT_STATUS_V2 TOT_CRNT_BAL_AMT SUM_CUR SUM_DEF SUM_SUSP_AMT TOT_ADJUSTED_OS_BAL_IN_CCAR;


/* --- Set Variables --- */
/* --- Date Variables */
/*fetch the time id for process date*/

proc sql NOPRINT;
select tm_Id into :mth_tm_Id from nzuser.tm_dim where tm_lvl_End_dt="&end_period_dt"d and tm_lvl='Month';
quit;

%put mth_tm_id= &mth_tm_id;

/*----------------------------------------------------
       * Setting date macros variables for NZ date
       * literal format based off of start_period_dt and 
       * end_period_dt formats. NZ r=uires date literals to
       * be in following format: YYYY-MM-DD
*----------------------------------------------------*/
      data _null_;
         call symput('period_dt', cats(put(intnx('month', "&start_period_dt"d, 0, 'e'), date9.)));
         call symput('period_dt_nz', cats(put(intnx('month', "&start_period_dt"d, 0, 'e'), yymmdd10.)));
		 call symput('Yearmonth_nz', cats(put(intnx('month', "&start_period_dt"d, 0, 'e'), monyy7.)));
		 call symput('Yearmm_nz', cats(put(intnx('month', "&start_period_dt"d, 0, 'e'), yymmn6.)));
      run;  

%let BNS_ACCTS_IN_DEF=BNS_ACCTS_IN_DEF_&Yearmm_nz;
%put BNS_ACCTS_IN_DEF=BNS_ACCTS_IN_DEF_&Yearmm_nz;
%put period_dt=&period_dt;
%put period_dt_nz=&period_dt_nz;
%put Yearmonth_nz=&Yearmonth_nz;




proc sql;
connect USING NZRRAP as nzcon ; 
execute(
create table NZRRAP.SPL_COM_MISC_TMP1 as
select 
      C.PIT_STATUS_V2 as PIT_STATUS, 
      sum(a.TOT_CRNT_BAL_AMT) as TOT_CRNT_BAL_AMT 
      from 
         NZRRAP.AIRB_RECON_APRVD_SNAPSHOT as X 
 inner join
         BASEL_PSNL_LOAN_MTH_SNAPSHOT as A on 
         x.gl_acct_num = a.gl_acct_num
         and x.gl_trnst_num = a.gl_trnst_num
         and x.mth_end_dt = %nrbquote('&period_dt_nz')
         and a.mth_tm_id = &mth_tm_id
         inner join 
         NZRRAP.BASEL_PSNL_LOAN_ACCT_DRVD_VARS as B 
         on 
         (  A.MTH_TM_ID = B.MTH_TM_ID 
            and A.BASEL_ACCT_ID = B.BASEL_ACCT_ID 
         ) 
 inner join 
         NZRRAP.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 as C 
         on 
         (  A.MTH_TM_ID = C.MTH_TM_ID 
            and A.BASEL_ACCT_ID = C.BASEL_ACCT_ID 
         ) 
   where 
      A.COMM_LOAN_CD = '1' 
      and B.TRNST_EXCLSN_F = 'N' 
      and A.MTH_TM_ID = &mth_tm_id  
      and C.PIT_STATUS_V2 IN ('CUR','DEF') 
      and A.SCRTY_CD != '99' 
group by C.PIT_STATUS_V2; ) by nzcon;

select TOT_CRNT_BAL_AMT FORMAT=20.2 into :SUM_CUR from NZRRAP.SPL_COM_MISC_TMP1 where PIT_STATUS='CUR';
select TOT_CRNT_BAL_AMT FORMAT=20.2 into :SUM_DEF from NZRRAP.SPL_COM_MISC_TMP1 where PIT_STATUS='DEF';
select sum(TOT_CRNT_BAL_AMT) FORMAT=25.2 into :TOTAL from NZRRAP.SPL_COM_MISC_TMP1; 

%PUT SUM_CUR=&SUM_CUR;
%PUT SUM_DEF=&SUM_DEF;
%PUT TOTAL=&TOTAL;
execute (drop table NZRRAP.SPL_COM_MISC_TMP1) by nzcon;
disconnect from nzcon;

quit;

PROC SQL;
connect USING NZRRAP as nzcon ; 

execute(
create table NZRRAP.MORTGAGE_GAP_TMP1 as
select sum(TOT_SUSP_BAL_AMT) as TOT_SUSP_BAL_AMT from NZRRAP.BASEL_MORT_MTH_SNAPSHOT a 
inner join &FRG_USR...&BNS_ACCTS_IN_DEF. b
on a.MORT_NUM=CAST(b.MORTGAGE_NO AS VARCHAR(1000)) 
WHERE A.MTH_TM_ID=&mth_tm_id AND A.PD_OFF_F='Y' AND A.TOT_SUSP_BAL_AMT<0;
) by nzcon;

select TOT_SUSP_BAL_AMT FORMAT=20.3 into :SUM_SUSP_AMT from NZRRAP.MORTGAGE_GAP_TMP1;

%PUT SUM_SUSP_AMT=&SUM_SUSP_AMT;

execute (drop table NZRRAP.MORTGAGE_GAP_TMP1;) by nzcon;

disconnect from nzcon;

QUIT;

PROC SQL;

select TOT_ADJUSTED_OS_BAL_IN_CCAR FORMAT=20.2 into :TOT_ADJUSTED_OS_BAL_IN_CCAR 
from DB2RRAP.BASEL_SEC_ADJ_FACTR_MTH_SNAP WHERE MTH_TM_ID=&mth_tm_id;

%PUT TOT_ADJUSTED_OS_BAL_IN_CCAR=&TOT_ADJUSTED_OS_BAL_IN_CCAR;

QUIT;


data _null_;
	infile "&rrap_dir./params/rrap/spl_comm_rec_email_list.txt";
	input;
	if _N_ =2 then do;
	CALL SYMPUT("to_email_list",_infile_);
	end;
	if _N_ =3 then do;
	CALL SYMPUT("cc_email_list",_infile_);
	end;
run;


%put &to_email_list;
%put &cc_email_list;

%MACRO SECUREMAIL1;
FILENAME OUTMAIL EMAIL
SUBJECT= "SPL Commercial Misclassification Breakdown : &Yearmonth_nz";

DATA _NULL_;
FILE OUTMAIL
TO=(&to_email_list)
CC=(&cc_email_list);
PUT "Hi All,";
PUT " ";
PUT "Please find below SPL Commercial Misclassification breakdown for &Yearmonth_nz.";
PUT "The month time id for this run is &mth_tm_id.";
PUT " ";
PUT "1. CURRENT	: &SUM_CUR";
PUT "2. DEFAULT	: &SUM_DEF";
PUT "3. TOTAL	: &TOTAL";
PUT " ";
PUT "Please note: This is a system generated email and responses aren't monitored.";
run;

%MEND SECUREMAIL1;
%SECUREMAIL1;


%MACRO SECUREMAIL2;
FILENAME OUTMAIL EMAIL
SUBJECT= "Mortgage Gap : &Yearmonth_nz";

DATA _NULL_;
FILE OUTMAIL
TO=(&to_email_list)
CC=(&cc_email_list);
PUT "Hi All,";
PUT " ";
PUT "Please find Mortgage Gap for &Yearmonth_nz.";
PUT " ";
PUT "The month time id for this run is &mth_tm_id.";
PUT "&SUM_SUSP_AMT";
PUT " ";
PUT "Please note: This is a system generated email and responses aren't monitored.";
run;

%MEND SECUREMAIL2;
%SECUREMAIL2;


%MACRO SECUREMAIL3;
FILENAME OUTMAIL EMAIL
SUBJECT= "Total Adjustment Outstanding Balance After CC Securitization Process: &Yearmonth_nz";

DATA _NULL_;
FILE OUTMAIL
TO=(&to_email_list)
CC=(&cc_email_list);
PUT "Hi,";
PUT " ";
PUT "Total Adjustment Outstanding Balance for &Yearmonth_nz. is $&TOT_ADJUSTED_OS_BAL_IN_CCAR";
PUT " ";
PUT " ";
PUT "Please note: This is a system generated email and responses aren't monitored.";
run;

%MEND SECUREMAIL3;
%SECUREMAIL3;

