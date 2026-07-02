***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: 
*  Target Table:  
*  
*  Purpose: 
*
*  Frequency: 
*
*  Notes: 
* 
* 
*
*	Change Log:
*	
*  
*
***************************************************************************************************************************;

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A57SWEI7.BH000AEI);
%let etls_jobName = %nrquote(J_pll_exception_report_3_KS_Segmentation_Distribution);
%let etls_userID = %nrquote(s7026573);

/* Setup to capture return codes  */ 
%global job_rc trans_rc sqlrc syscc;
%let sysrc = 0;
%let job_rc = 0;
%let trans_rc = 0;
%let sqlrc = 0;
%let syscc = 0;
%global etls_stepStartTime; 


/* initialize syserr to 0 */
data _null_; run;

%macro rcSet(error); 
   %if (&error gt &trans_rc) %then 
      %let trans_rc = &error;
   %if (&error gt &job_rc) %then 
      %let job_rc = &error;
%mend rcSet; 

%macro rcSetDS(error); 
   if &error gt input(symget('trans_rc'),12.) then 
      call symput('trans_rc',trim(left(put(&error,12.))));
   if &error gt input(symget('job_rc'),12.) then 
      call symput('job_rc',trim(left(put(&error,12.))));
%mend rcSetDS; 

/* Create metadata macro variables */
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer"; 

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;

%global etls_debug; 
%macro etls_setDebug; 
   %if %str(&etls_debug) ne 0 %then 
      OPTIONS MPRINT%str(;); 
%mend; 
%etls_setDebug; 

/*==========================================================================* 
 * Step:            rrap_exception_report_autoexec        A57SWEI7.BK0011WP * 
 * Transform:       AM_rrap_exception_report_autoexec                       * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK0011WP);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let _INPUT_count = 0; 
%let _OUTPUT_count = 0; 


%put %sysfunc(getoption(work));
libname _wrk "%sysfunc(getoption(work))";

%include '&rrap_dir/macro/rrap_iias/rrap_pll_autoexec.sas';
%rrap_pll_autoexec(RRAPENV=REVOLVING_CREDIT);

%let file_path=&rrap_dir./flat_files/rrap/;




Data _null_;
infile "&rrap_dir/params/rrap/pll_exception_email_list.txt";
input;
if _N_ =2 then do;
CALL SYMPUT("email_list",_infile_);
end;
if _N_ =3 then do;
CALL SYMPUT("si_email_list",_infile_);
end;
run;

%put &email_list;
%put &si_email_list;

%rrap_pll_exception_rpt_autoexec;



proc sql;
select max(MTH_TM_ID) into: current_tm_id from NZRRAP.BASEL_RCA_SCORE_DTL_SNAPSHOT;
quit;

/*%let current_tm_id = %sysevalf(&current_tm_id - 40);*/


%put &current_tm_id;
%let base_tm_id = %sysevalf(&current_tm_id - 40);

/*%let base_tm_id= 15556;*/

proc sql;
select tm_lvl_end_dt into: current_day from NZRRAP.tm_dim where tm_id = &current_tm_id;
quit;

proc sql;
select tm_lvl_end_dt into: base_day from NZRRAP.tm_dim where tm_id = &base_tm_id;
quit;

/*%let day=today();*/
%put &current_day;
%put &base_day;
data _null_;
base=intnx('month',"&base_day"d,0,'E');
curr=intnx('month',"&current_day"d,0,'E');

base_b=intnx('month',"&base_day"d,0,'B');
curr_b=intnx('month',"&current_day"d,0,'B');

call symput("BASE_MTH",put(base,date9.));
call symput("CURRENT_MTH",put(curr,date9.));

call symput("base_ym",put(base,YYMMN.));
call symput("current_ym",put(curr,YYMMN.));

call symput("base_mth_end_dt",put(base,YYMMDD10.));
call symput("current_mth_end_dt",put(curr,YYMMDD10.));
call symput("file_name_month",put(curr,monname20.));
call symput("file_name_year",put(year(curr),4.));

call symput("prev_qname",put(base_b,MONYY7.));
call symput("prev_qm",put(base_b,DATE9.));

call symput("current_qname",put(curr_b,MONYY7.));
call symput("current_qm",put(curr_b,DATE9.));

CALL SYMPUT('MonthYear', CATX(' ',PUT(curr,monname10.),PUT(YEAR(curr),4.)));
run;


PROC SQL;

CONNECT USING NZRRAP AS NZCON;
CREATE TABLE work.List as 
SELECT * FROM CONNECTION TO NZCON (
SELECT NAME as TABLENAME, CTIME as createdate FROM SYSIBM.SYSTABLES WHERE TYPE = 'T' AND CREATOR = %nrbquote('&FRG_USR') AND
NAME LIKE 'BNS_WITH_ALL_RTOS_%' order by TABLENAME, createdate )
;
quit;

data work.list1;
set work.list;
dates=substr(TABLENAME,19,6);
if dates eq "&base_ym" then output;
run;

proc sort data=work.list1; by createdate; run;


/*%LET BNS_PREV_TABLE=FRG.BNS_WITH_ALL_RTOS_201602_19APR16;*/
/*%LET BNS_CURR_TABLE=FRG.BNS_WITH_ALL_RTOS_201603_19APR16;*/

data _null_;set work.list1;call symput("bns1",put(TABLENAME,40.));run;


data work.list2;
set work.list;
dates=substr(TABLENAME,19,6);
if dates eq "&current_ym" then output;
run;

proc sort data=work.list2; by createdate; run;


/*%LET BNS_PREV_TABLE=FRG.BNS_WITH_ALL_RTOS_201602_19APR16;*/
/*%LET BNS_CURR_TABLE=FRG.BNS_WITH_ALL_RTOS_201603_19APR16;*/

data _null_;set work.list2;call symput("bns2",put(TABLENAME,40.));run;


%LET BNS_PREV_TABLE=FRG.&bns1;
%LET BNS_CURR_TABLE=FRG.&bns2;
%put &BNS_PREV_TABLE.==;
%put &BNS_CURR_TABLE.==;


%let file_name_date = &file_name_month._&file_name_year.;

%let Threshold = 0.1;
%let email_alert = 0;



%let prev_qmth = %unquote(%str(%')&prev_qm%str(%'d)) ;
%put &prev_qmth ;
%let current_qmth = %unquote(%str(%')&current_qm%str(%'d)) ;
%put &current_qmth ;
%put &prev_qname &prev_qmth &current_qname &current_qmth;

%put base_time_id=&base_tm_id;
%put current_time_id=&current_tm_id;
%put BASE_MTH=&BASE_MTH;
%put CURRENT_MTH =&CURRENT_MTH;
%put base_ym=&base_ym;
%put current_ym=&current_ym;
%put base_mth_end_dt=&base_mth_end_dt;
%put current_mth_end_dt=&current_mth_end_dt;
%put file_name_date=&file_name_date;
%put file_path=&file_path;

%put prev_qmth=&prev_qmth;
%put current_qmth=&current_qmth;
%put current_qname=&current_qname;
%put prev_qname=&prev_qname;


%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 

%rcSet(&syscc); 



/**  Step end rrap_exception_report_autoexec **/

/*---- Start of User Written Code  ----*/ 

options source;
options mprint;

%LET datetime_start = %sysfunc(TIME());
%PUT RRAP Exception report 3 Start Time: %SYSFUNC(datetime(),datetime18.);



%put base_time_id=&base_tm_id;
%put current_time_id=&current_tm_id;
%put BASE_MTH=&BASE_MTH;
%put CURRENT_MTH =&CURRENT_MTH;
%put base_ym=&base_ym;
%put current_ym=&current_ym;
%put base_mth_end_dt=&base_mth_end_dt;
%put current_mth_end_dt=&current_mth_end_dt;
%put file_name_date=&file_name_date;


%let file_name_tl =TL_Segmentation_Distribution_&file_name_date..xls;
%let file_name_ks =KS_PLL_Segmentation_Distribution_&file_name_date..xls;
%let file_name_mor =BNS_TNG Segmentation_Distribution_&file_name_date..xls;

%put &file_name_tl &file_name_ks &file_name_mor;


%macro psi_3 (lib, data_in, subpop, value, base_ym, var, tbl_pref);
/**Subset dataset;*/
data all_data;
set &lib..&data_in;
%if &subpop ne 0 %then %do;
where &subpop = "&value.";
%end; 
run;


proc freq data=all_data noprint;
tables yrmth * &var / out=_freqs outpct;
where &var ne . and yrmth >= &base_ym;
run;

/**Process results;*/
proc sort data=_freqs; by &var; run;
proc transpose data=_freqs out=_freqs_trans prefix=y_;
id yrmth;
by &var;
var pct_row;
run;


proc sql noprint;
select count(distinct yrmth) into :num_ym from all_data;
select distinct yrmth into :y1 - :y%sysfunc(compress(&num_ym)) from all_data order by yrmth;
quit;


data _freqs_trans;
drop i;
set _freqs_trans;




array pts {*} y_: ;

do i = 1 to dim(pts);
	pts[i] = pts[i] / 100;
end;


%do j = 2 %to &num_ym;
	partpsi_&&y&j = (y_&&y&j - y_&y1) * log(y_&&y&j / y_&y1);
%end;
run;
proc means data=_freqs_trans noprint;
var partpsi: ;
output out=psi_results sum=;
run;


data psi_%substr(&tbl_pref,1,10)_%substr(&subpop.,1,4)_%substr(&value.,1,1);
retain subpop;
drop _freq_ _type_;
set psi_results;

subpop = "&subpop.&value";
run;
%mend; 



/***KS LGD;*/
proc sql;
create table WORK.ks_model_list_lgd as
select distinct MODEL_NM length=40,"KS" AS PRD length=3
from NZRRAP.BASEL_RCA_SCORE_SNAPSHOT a
inner join NZRRAP.LGD_SEG_ACCT_XREF b
on a.basel_acct_id=b.basel_acct_id and a.mth_tm_id=b.mth_tm_id and a.BASEL_MODEL_ID=b.BASEL_MODEL_ID
inner join NZRRAP.basel_seg c
on b.basel_seg_id=c.basel_seg_id
inner join NZRRAP.basel_model d
on a.BASEL_MODEL_ID=d.BASEL_MODEL_ID
where a.mth_tm_id in (&base_tm_id) 
and MODEL_NM like '%LGD%'
order by MODEL_NM;
quit;


/***KS PD;*/
proc sql;
create table work.ks_model_list_pd as
select distinct MODEL_NM length=40,"KS" AS PRD length=3
from NZRRAP.BASEL_RCA_SCORE_SNAPSHOT a
inner join NZRRAP.PD_SEG_ACCT_XREF b
on a.basel_acct_id=b.basel_acct_id and a.mth_tm_id=b.mth_tm_id and a.BASEL_MODEL_ID=b.BASEL_MODEL_ID
inner join NZRRAP.basel_seg c
on b.basel_seg_id=c.basel_seg_id
inner join NZRRAP.basel_model d
on a.BASEL_MODEL_ID=d.BASEL_MODEL_ID
where a.mth_tm_id in (&base_tm_id) 
and MODEL_NM like '%PD%'
order by MODEL_NM
;
quit;



/***KS EAD;*/
proc sql;
create table work.ks_model_list_ead as
select distinct MODEL_NM length=40,"KS" AS PRD length=3
from NZRRAP.BASEL_RCA_SCORE_SNAPSHOT a
inner join NZRRAP.EAD_SEG_ACCT_XREF b
on a.basel_acct_id=b.basel_acct_id and a.mth_tm_id=b.mth_tm_id and a.BASEL_MODEL_ID=b.BASEL_MODEL_ID
inner join NZRRAP.basel_seg c
on b.basel_seg_id=c.basel_seg_id
inner join NZRRAP.basel_model d
on a.BASEL_MODEL_ID=d.BASEL_MODEL_ID
where a.mth_tm_id in (&base_tm_id) 
and MODEL_NM like '%EAD%'
order by MODEL_NM
;
quit;


%symdel COUNT / nowarn;
%symdel TYPE  / nowarn;
%symdel List  / nowarn;

DATA _NULL_;
SET 
work.ks_model_list_lgd 
work.ks_model_list_pd 
work.ks_model_list_ead
;
CALL SYMPUT('TYPE'||LEFT(PUT(_N_, 2.)),PRD);
CALL SYMPUT('List'||LEFT(PUT(_N_, 2.)),TRIM(MODEL_NM));
CALL SYMPUT('COUNT',_N_);
RUN;


%macro lista;
   %do y=1 %to &COUNT;
   %put &&TYPE&y &&List&y;
   %end;

%mend;
%lista;


%macro process(typeList,list_var,count,final);

proc datasets library=work kill; run;



   %do z=1 %to &COUNT;
   %let product = &&&typeList.&z;
   %put Product is=&product.=;
   %put model is=&&&list_var.&z.=;

/*   KS LGD*/
   %if "&product"="KS" and (("&&&list_var.&z"="CC LGD-D")  or 
                            ("&&&list_var.&z"="CC LGD-ND") or
                            ("&&&list_var.&z"="HELOC LGD-D") or
                            ("&&&list_var.&z"="HELOC LGD-ND") or
                            ("&&&list_var.&z"="LOC LGD-D") or
                            ("&&&list_var.&z"="LOC LGD-ND")
)
%then %do;
   %put processing* &&&list_var.&z  .....;
   proc sql;
     create table work.&product._input_base_&z. as 
select 
case 
 when a.mth_tm_id in (&base_tm_id) then &base_ym 
 when a.mth_tm_id in (&current_tm_id) then &current_ym 
end as YRMTH,
a.MTH_TM_ID, BASEL_SEG_NM AS MODEL_NM, SEG_NUM as SEGMENT_ID 
from NZRRAP.LGD_SEG_ACCT_XREF a
     inner join NZRRAP.BASEL_MODEL_REL  b on a.BASEL_MODEL_REL_ID=b.BASEL_MODEL_REL_ID
     inner join NZRRAP.BASEL_MODEL c on a.BASEL_MODEL_ID=c.BASEL_MODEL_ID
     inner join NZRRAP.basel_seg d on a.BASEL_SEG_ID=d.BASEL_SEG_ID
where mth_tm_id in (&base_tm_id,&current_tm_id) and RPTG_USAGE_F='Y' and d.SRC_SYS_CD='KS'
and TRIM(MODEL_NM) = "&&&list_var.&z"
;
quit;
%end;
%else %if "&product"="KS" and (("&&&list_var.&z"="CC PD") or
                            ("&&&list_var.&z"="HELOC PD") or
                            ("&&&list_var.&z"="LOC PD") 
)
%then %do;
/**  KS PD;*/
   proc sql;
     create table work.&product._input_base_&z. as 
select 
case 
 when a.mth_tm_id in (&base_tm_id) then &base_ym 
 when a.mth_tm_id in (&current_tm_id) then &current_ym 
end as YRMTH,
a.MTH_TM_ID, BASEL_SEG_NM as MODEL_NM ,SEG_NUM as SEGMENT_ID 
  from NZRRAP.PD_SEG_ACCT_XREF a
     inner join NZRRAP.BASEL_MODEL_REL  b on a.BASEL_MODEL_REL_ID=b.BASEL_MODEL_REL_ID
     inner join NZRRAP.BASEL_MODEL c on a.BASEL_MODEL_ID=c.BASEL_MODEL_ID
     inner join NZRRAP.basel_seg d on a.BASEL_SEG_ID=d.BASEL_SEG_ID
where mth_tm_id in (&base_tm_id,&current_tm_id) and RPTG_USAGE_F='Y' and d.SRC_SYS_CD='KS'
and TRIM(MODEL_NM) = "&&&list_var.&z"
;
quit;
/* add CC PD accounts without segment, display them as SEG_NUM=0 */
%if "&&&list_var.&z"="CC PD" %then %do;
	proc sql;
	 create table work.accounts_no_seg as 
	 select case 
			when a.mth_tm_id in (&base_tm_id) then &base_ym 
 			when a.mth_tm_id in (&current_tm_id) then &current_ym 
		   end as YRMTH,
		   a.MTH_TM_ID, "CC PD" as MODEL_NM, 0 as SEGMENT_ID FROM (
			(select distinct MTH_TM_ID, BASEL_ACCT_ID from NZRRAP.BASEL_RCA_SCORE_SNAPSHOT_CC_PD where MTH_TM_ID in (&base_tm_id,&current_tm_id)) a 
			left join NZRRAP.BASEL_KS_ACCT_TRANSACTOR_ROLE f ON a.MTH_TM_ID=f.MTH_TM_ID and a.BASEL_ACCT_ID=f.BASEL_ACCT_ID) 
	 where f.ROLE_IND='';
	quit;
	%if %sysfunc(exist(work.accounts_no_seg)) %then %do;
 	   proc append base=work.&product._input_base_&z. data=work.accounts_no_seg force; run;
	%end;
%end;
%end;
%else %if "&product"="KS" and (("&&&list_var.&z"="CC EAD") or
                            ("&&&list_var.&z"="HELOC EAD") or
                            ("&&&list_var.&z"="LOC EAD") 
)
%then  %do;
/**KS EAD;*/
proc sql;
     create table work.&product._input_base_&z. as 
select 
case 
 when a.mth_tm_id in (&base_tm_id) then &base_ym 
 when a.mth_tm_id in (&current_tm_id) then &current_ym 
end as YRMTH,
a.MTH_TM_ID, BASEL_SEG_NM as MODEL_NM ,SEG_NUM as SEGMENT_ID
from NZRRAP.EAD_SEG_ACCT_XREF a
     inner join NZRRAP.BASEL_MODEL_REL  b on a.BASEL_MODEL_REL_ID=b.BASEL_MODEL_REL_ID
     inner join NZRRAP.BASEL_MODEL c on a.BASEL_MODEL_ID=c.BASEL_MODEL_ID
     inner join NZRRAP.basel_seg d on a.BASEL_SEG_ID=d.BASEL_SEG_ID
where mth_tm_id in (&base_tm_id,&current_tm_id) and RPTG_USAGE_F='Y' and d.SRC_SYS_CD='KS'
and TRIM(MODEL_NM) = "&&&list_var.&z"
;

quit;
%end;
%else %if "&product"="SPL" and (("&&&list_var.&z"="DTL LGD-D") or
                            ("&&&list_var.&z"="DTL LGD-ND") or
                            ("&&&list_var.&z"="ITL LGD-D") or
                            ("&&&list_var.&z"="ITL LGD-ND") 
)
%then  %do;

%end;
%else  %if "&product"="SPL" and (("&&&list_var.&z"="DTL PD") or
                            ("&&&list_var.&z"="ITL PD") 
)
%then  %do;

%end;
%else %if "&product"="BNS" and (("&&&list_var.&z" = "BNS MOR PD"))
%then %do;

%end;
%else %if "&product"="BNS" and (("&&&list_var.&z" = "BNS MOR LGD-D"))
%then %do;

%end;
%else %if "&product"="BNS" and (("&&&list_var.&z" = "BNS MOR LGD-ND"))
%then %do;

%end;
%else %if "&product"="TNG" and (("&&&list_var.&z" = "TNG PD"))
%then %do;

%end;
%else  %if "&product"="TNG" and (("&&&list_var.&z" = "TNG PD"))
%then %do;

%end;
%else  %if "&product"="TNG" and (("&&&list_var.&z" = "TNG MOR LGD-D") OR ("&&&list_var.&z" = "TNG MOR LGD-ND"))
%then %do;

%end;
%else %do;
   %put &&&list_var.&z  [is empty;
   %goto continue;
%end;

/**%psi_3 (work, temp_data, 0, , 201510, segment_id, general);*/
%psi_3 (work,&product._input_base_&z., 0, , &base_ym, segment_id, general);
%put &z ======;

Data work.result_&product._&z._base work.result_&product._&z._current;
set work._freqs;
if YRMTH EQ &base_ym then output work.result_&product._&z._base;
if YRMTH EQ &current_ym then output work.result_&product._&z._current;
run;

/*19MAR2018 - Change to left join*/
proc sql;
 create table work.&product._&z._result as 
 select 
 a.segment_id, 
 a.count as base_cnt, 
 b.count as current_cnt,
 c.y_&base_ym. as base_pct,
 c.y_&current_ym. as current_pct,
 c.partpsi_&current_ym. as si
 from work.result_&product._&z._base as a
 left join work.result_&product._&z._current as b
 on a.segment_id = b.segment_id
 left join work._freqs_trans as c
 on a.segment_id = b.segment_id and  a.segment_id = c.segment_id;
quit;

data work.&product._&z._result;
length product $40.;
length MODEL_NM $40.;
set work.&product._&z._result;

product = "&product";
MODEL_NM = "&&&list_var.&z";
/*rename partpsi_&current_ym. = si;*/
LABEL base_cnt = "BASE_CNT";
LABEL current_cnt = "CURRENT_CNT";
LABEL base_pct= "base_pct";
LABEL current_pct= "current_pct";
LABEL si = "SI";
run;

%PUT &final;

%if "&product"="SPL" %then %do;
  %if %sysfunc(exist(work.SPL_&final)) %then %do;
    proc append base=work.SPL_&final data = work.&product._&z._result force; run;
  %end;
  %else %do;
    data work.SPL_&final;
	set work.&product._&z._result; run;
  %end;
%end;


%if "&product"="KS" %then %do;
  %if %sysfunc(exist(work.KS_&final)) %then %do;
    proc append base=work.KS_&final data = work.&product._&z._result force; run;
  %end;
  %else %do;
    data work.KS_&final;
	set work.&product._&z._result; run;
  %end;
%end;

%if "&product"="BNS" or "&product"="TNG" %then %do;
  %if %sysfunc(exist(work.BNS_&final)) %then %do;
    proc append base=work.BNS_&final data = work.&product._&z._result force; run;
  %end;
  %else %do;
    data work.BNS_&final;
	set work.&product._&z._result; run;
  %end;
%end;

%continue:

%end;
%mend;
%process(TYPE,List,&count,psi_final);

proc sort data = work.ks_psi_final out =work.ks_psi_alert; by MODEL_NM; run;

%let email_alert = 0;
DATA work.ks_psi_alert;
SET work.ks_psi_alert;
BY MODEL_NM;
retain total_si 0;
if first.MODEL_NM then do;
  total_si=0;
end;
total_si = total_si +si;
run;

data work.a0;
set work.ks_psi_alert;
if total_si > &Threshold then do;
  output;
end;
run;

%macro countA();
%let DSNId = %sysfunc(open(work.a0));
%let DSObs = %sysfunc(attrn(&DSNId,nobs));
%let rc = %sysfunc(close(&DSNId.));
%put &DSObs;
%if  &DSObs. NE 0 %then %do; 
  %let email_alert = 1;
%end;
%mend;
%countA();

%put email_alert is:&email_alert after count;

data work.legend;
length BASEL_SCORECRD_NM $34.;
BASEL_SCORECRD_NM ="Legend:";output;
BASEL_SCORECRD_NM ="Stability Index Within Threshold";output;
BASEL_SCORECRD_NM ="Stability Index Exceeds Threshold";output;
run;

Title "";

%macro create_report;

Ods tagsets.ExcelXP path="&file_path" file="&file_name_ks" options(sheet_interval='none'); run;

%if &email_alert = 1 %then %do;

proc report data=work.KS_psi_final split='~'
style(summary)=[FONT_WEIGHT = BOLD]
style(lines)=[fontweight=bold fontsize=5 color=red]
;
column product MODEL_NM segment_id base_cnt current_cnt base_pct current_pct si;
define product /" " order width=5;
define MODEL_NM /group width=5;
define segment_id / display width=5;
define base_cnt  / "&base_ym~BASE_CNT" analysis width=5;
define current_cnt /"&current_ym~CURRENT_CNT" analysis width=5;
define base_pct /analysis  format=percent10.2;;
define current_pct /analysis  format=percent10.2;;
define si /analysis format=8.4;


compute segment_id;
if missing(_C3_) then call define('_C3_',"style","style={background=red}");
endcomp;

compute base_cnt;
if missing(_C4_) then call define('_C4_',"style","style={background=red}");
endcomp;

compute current_cnt;
if missing(_C5_) then call define('_C5_',"style","style={background=red}");
endcomp;

compute base_pct;
if missing(_C6_) then call define('_C6_',"style","style={background=red}");
endcomp;

compute current_pct;
if missing(_C7_) then call define('_C7_',"style","style={background=red}");
endcomp;

compute si;
if missing(_C8_) then call define('_C8_',"style","style={background=red}");
endcomp;

break AFTER MODEL_NM / summarize dol dul;
 
compute AFTER MODEL_NM;
 MODEL_NM = "sub total" ;
 BASEL_SCORECRD_NM = " ";
 product = " ";

  call define('_C3_',"style","style={background=grey}");

 call define('_C4_',"style","style={background=yellow}");
 call define('_C5_',"style","style={background=yellow}");
 call define('_C6_',"style","style={background=yellow}");
 call define('_C7_',"style","style={background=yellow}");
 call define('_C8_',"style","style={background=yellow}");
 
 if (_c8_) > &Threshold then call define('_C8_',"style","style={background=red}");
endcomp;

compute before _PAGE_;
line "KS Parallel Run Segmentation Distribution &file_name_date - SI above threshold";
endcomp;
run;
proc report data=work.legend split='~' missing  noheader
;
column BASEL_SCORECRD_NM;
define BASEL_SCORECRD_NM /width=25 style(column)={cellheight=3};

compute BASEL_SCORECRD_NM;

 if (_c1_) = "Legend:" then call define('_C1_',"style","style={background=white}");
 if (_c1_) = "Stability Index Within Threshold" then call define('_C1_',"style","style={background=yellow}");
 if (_c1_) = "Stability Index Exceeds Threshold" then call define('_C1_',"style","style={background=red}");
endcomp;
run;
%end;
%else %do;
proc report data=work.KS_psi_final split='~'
style(summary)=[FONT_WEIGHT = BOLD]
style(lines)=[fontweight=bold fontsize=5]
;
column product MODEL_NM segment_id base_cnt current_cnt base_pct current_pct si;
define product /" " order width=5;
define MODEL_NM /group width=5;
define segment_id / display width=5;
define base_cnt  / "&base_ym~BASE_CNT" analysis width=5;
define current_cnt /"&current_ym~CURRENT_CNT" analysis width=5;
define base_pct /analysis  format=percent10.2;;
define current_pct /analysis  format=percent10.2;;
define si /analysis format=8.4;


compute segment_id;
if missing(_C3_) then call define('_C3_',"style","style={background=red}");
endcomp;

compute base_cnt;
if missing(_C4_) then call define('_C4_',"style","style={background=red}");
endcomp;

compute current_cnt;
if missing(_C5_) then call define('_C5_',"style","style={background=red}");
endcomp;

compute base_pct;
if missing(_C6_) then call define('_C6_',"style","style={background=red}");
endcomp;

compute current_pct;
if missing(_C7_) then call define('_C7_',"style","style={background=red}");
endcomp;

compute si;
if missing(_C8_) then call define('_C8_',"style","style={background=red}");
endcomp;


break AFTER MODEL_NM / summarize dol dul;
 
compute AFTER MODEL_NM;
 MODEL_NM = "sub total" ;
 BASEL_SCORECRD_NM = " ";
 product = " ";

   call define('_C3_',"style","style={background=grey}");

 call define('_C4_',"style","style={background=yellow}");
 call define('_C5_',"style","style={background=yellow}");
 call define('_C6_',"style","style={background=yellow}");
 call define('_C7_',"style","style={background=yellow}");
 call define('_C8_',"style","style={background=yellow}");
 
 if (_c8_) > &Threshold then call define('_C8_',"style","style={background=red}");
endcomp;

compute before _PAGE_;
line "KS Parallel Run Segmentation Distribution &file_name_date";
endcomp;
run;
proc report data=work.legend split='~' missing  noheader
;
column BASEL_SCORECRD_NM;
define BASEL_SCORECRD_NM /width=25 style(column)={cellheight=3};

compute BASEL_SCORECRD_NM;

 if (_c1_) = "Legend:" then call define('_C1_',"style","style={background=white}");
 if (_c1_) = "Stability Index Within Threshold" then call define('_C1_',"style","style={background=yellow}");
 if (_c1_) = "Stability Index Exceeds Threshold" then call define('_C1_',"style","style={background=red}");
endcomp;
run;

%end;

ods tagsets.ExcelXP close; run;

%mend;
%create_report;


%macro sendemail;
%if &email_alert = 1 %then %do;
FILENAME OUTMAIL EMAIL ATTACH=("&file_path.&file_name_ks")
SUBJECT= "[RRAP] SI above threshold for KS Parallel Run Segmentation Distribution Report - &MonthYear.";
DATA _NULL_;
     FILE OUTMAIL
     TO= (&si_email_list);
     put;

RUN;
%end;
%else %do;
FILENAME OUTMAIL EMAIL ATTACH=("&file_path.&file_name_ks")
SUBJECT= "[RRAP] KS Parallel Run Segmentation Distribution Report - &MonthYear.";
DATA _NULL_;
     FILE OUTMAIL
     TO= (&email_list);
     
     put;

RUN;
%end;

%mend sendemail;
%sendemail;



%PUT RRAP Exception report 3 - Start Time     : %Sysfunc(PutN(&datetime_start.,datetime18.));
%PUT RRAP Exception report 3 - END TIME       : %sysfunc(datetime(),datetime18.);;
%put RRAP Exception report 3 - PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.));




/*---- End of User Written Code  ----*/ 

%let etls_endTime = %sysfunc(datetime(),datetime.);

