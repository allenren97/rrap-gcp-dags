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
%let jobID = %quote(A57SWEI7.BH000AEC);
%let etls_jobName = %nrquote(J_pll_exception_report_1_KS_Scorecard_Model_Variables);
%let etls_userID = %nrquote(owprdsas);

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
 * Step:            rrap_exception_report_autoexec        A57SWEI7.BK0011WD * 
 * Transform:       AM_rrap_exception_report_autoexec                       * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK0011WD);
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


data _null_;set work.list1;call symput("bns1",put(TABLENAME,40.));run;


data work.list2;
set work.list;
dates=substr(TABLENAME,19,6);
if dates eq "&current_ym" then output;
run;

proc sort data=work.list2; by createdate; run;


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

/*depending on: BASEL_RCA_SCORE_DTL_SNAPSHOT*/
%let datetime_start = %sysfunc(TIME());
%PUT RRAP Exception report 1 - KS Start Time: %SYSFUNC(datetime(),datetime18.);


%put &BASE_MTH &CURRENT_MTH &base_ym &current_ym &base_mth_end_dt &current_mth_end_dt &file_name_date;

%let file_name=KS_PLL_Scorecard_Model_Variables_&file_name_date..xls;
%put &file_name;

/*-- processing month and one month before, taking 201510 for example*/
proc sql;
	create table work.ks_SCORECRD_NM_1 as 
	/*	select 1 as seq, "LGD" as model_type, b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM, c.bin, count(*) ,avg(c.PT_CNT) as points_assigned  */
	    select 1 as seq, "LGD" as model_type, b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM, c.bin, count(*) as COUNT,avg(c.PT_CNT) as points_assigned 
			from NZRRAP.BASEL_RCA_SCORE_DTL_SNAPSHOT a
				left outer join NZRRAP.BASEL_MODEL_SCORECRD_HDR b
					on a.BASEL_MODEL_SCORECRD_HDR_ID=b.BASEL_MODEL_SCORECRD_HDR_ID
				left outer join NZRRAP.BASEL_MODEL_SCORECRD_DTL c
					on a.BASEL_MODEL_SCORECRD_DTL_ID=c.BASEL_MODEL_SCORECRD_DTL_ID
				inner join NZRRAP.LGD_SEG_ACCT_XREF d
					on a.basel_acct_id=d.basel_acct_id and a.mth_tm_id=d.mth_tm_id and a.BASEL_MODEL_ID=d.BASEL_MODEL_ID
				inner join NZRRAP.basel_seg e
					on d.basel_seg_id=e.basel_seg_id
				where a.mth_tm_id in (&base_tm_id, &current_tm_id) /*-- processing month and one month before, taking 201510 for example */
	and b.SRC_SYS_CD='KS' and e.SEG_NUM not in (90,98,99) and c.VAR_NM is not null and c.VAR_NM NE 'Intercept'
	group by b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM,c.BIN
		order by a.mth_tm_id,b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM,c.BIN
	;
quit;

* PD;
proc sql;
	create table work.ks_SCORECRD_NM_2 as 
	/*	select 2 as seq, "PD" as model_type, b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM, c.bin, count(*) ,avg(c.PT_CNT) as points_assigned */
		select 2 as seq, "PD" as model_type, b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM, c.bin, count(*)as COUNT ,avg(c.PT_CNT) as points_assigned 
			from NZRRAP.BASEL_RCA_SCORE_DTL_SNAPSHOT a
				left outer join NZRRAP.BASEL_MODEL_SCORECRD_HDR b
					on a.BASEL_MODEL_SCORECRD_HDR_ID=b.BASEL_MODEL_SCORECRD_HDR_ID
				left outer join NZRRAP.BASEL_MODEL_SCORECRD_DTL c
					on a.BASEL_MODEL_SCORECRD_DTL_ID=c.BASEL_MODEL_SCORECRD_DTL_ID
				/* select CC PD scorecard for each account based on transactor role */
				left outer join ( select mth_tm_id, basel_acct_id, role_ind, 
									case role_ind 
									when 'T' then 8033 
									when 'R' then 8034 
									when 'D' then 8035 
									 end as check_model_id  
									from NZRRAP.BASEL_KS_ACCT_TRANSACTOR_ROLE tr 
								   where tr.mth_tm_id in (&base_tm_id, &current_tm_id) ) f 
				    on a.mth_tm_id=f.mth_tm_id and a.basel_acct_id=f.basel_acct_id and a.basel_model_id=f.check_model_id 
				inner join NZRRAP.PD_SEG_ACCT_XREF d
					on a.basel_acct_id=d.basel_acct_id and a.mth_tm_id=d.mth_tm_id and 
					(a.BASEL_MODEL_ID not between 8033 and 8035 and a.BASEL_MODEL_ID=d.BASEL_MODEL_ID or     
					 a.BASEL_MODEL_ID between 8033 and 8035 and 8011=d.BASEL_MODEL_ID)
				inner join NZRRAP.basel_seg e
					on d.basel_seg_id=e.basel_seg_id
				where a.mth_tm_id in (&base_tm_id, &current_tm_id) 
					and b.SRC_SYS_CD='KS' and e.SEG_NUM not in (98,99) and c.VAR_NM is not null and c.VAR_NM NE 'Intercept'
					and not(a.BASEL_MODEL_ID=8007 and e.SEG_NUM=10) 
					and not(a.basel_model_id between 8033 and 8035 and f.role_ind is null)
				group by b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM,c.BIN
					order by a.mth_tm_id,b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM,c.BIN
	;
quit;

*EAD;
proc sql;
	create table work.ks_SCORECRD_NM_3 as 
	/*	select 3 as seq, "EAD" as model_type, b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM, c.bin, count(*) ,avg(c.PT_CNT) as points_assigned */
		select 3 as seq, "EAD" as model_type, b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM, c.bin, count(*)as COUNT ,avg(c.PT_CNT) as points_assigned 
			from NZRRAP.BASEL_RCA_SCORE_DTL_SNAPSHOT a
				left outer join NZRRAP.BASEL_MODEL_SCORECRD_HDR b
					on a.BASEL_MODEL_SCORECRD_HDR_ID=b.BASEL_MODEL_SCORECRD_HDR_ID
				left outer join NZRRAP.BASEL_MODEL_SCORECRD_DTL c
					on a.BASEL_MODEL_SCORECRD_DTL_ID=c.BASEL_MODEL_SCORECRD_DTL_ID
				inner join NZRRAP.EAD_SEG_ACCT_XREF d
					on a.basel_acct_id=d.basel_acct_id and a.mth_tm_id=d.mth_tm_id and a.BASEL_MODEL_ID=d.BASEL_MODEL_ID
				inner join NZRRAP.basel_seg e
					on d.basel_seg_id=e.basel_seg_id
				where a.mth_tm_id in (&base_tm_id, &current_tm_id) 
					and b.SRC_SYS_CD='KS' and e.SEG_NUM not in (98,99) and c.VAR_NM is not null and c.VAR_NM NE 'Intercept'
				group by b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM,c.BIN
					order by a.mth_tm_id,b.BASEL_SCORECRD_NM,a.mth_tm_id,c.VAR_NM,c.BIN
	;
quit;

DATA work.ks_SCORECRD_NM;
	SET work.ks_SCORECRD_NM_1 work.ks_SCORECRD_NM_2 work.ks_SCORECRD_NM_3;
RUN;

DATA WORK.KS_base_1 WORK.KS_current_1;
	/**LENGTH VAR_NM $40.;*/
	SET work.ks_SCORECRD_NM;

	if mth_tm_id = &base_tm_id then
		output WORK.KS_base_1;

	if mth_tm_id = &current_tm_id then
		output WORK.KS_current_1;

	/**CALL SYMPUT('COUNT',_N_);*/
RUN;

Data WORK.KS_base_1;
	set WORK.KS_base_1;
	rename count = BASE_CNT;
	DROP mth_tm_id bin;
run;

Data WORK.KS_current_1;
	set WORK.KS_current_1;
	rename count = CURRENT_CNT;
	DROP mth_tm_id bin;
run;

proc sql;
	create table work.KS_2 as 
		select A.*, B.CURRENT_CNT 
			from work.KS_base_1 a
				left join work.KS_current_1 b
					on 
					a.var_nm = b.VAR_NM and a.BASEL_SCORECRD_NM = b.BASEL_SCORECRD_NM 
					and a.points_assigned = b.points_assigned
	;
quit;

*15MAR2018 - Changed to left join;
proc sql;
	create table work.ks_3 as
		select seq, model_type, BASEL_SCORECRD_NM,var_nm, sum(BASE_CNT) as base_tot, sum(current_CNT) as current_tot
			from work.KS_2
				group by seq,model_type, BASEL_SCORECRD_NM,var_nm
	;
quit;

proc sql;
	create table work.KS_4 as 
		select A.*, b.base_tot, b.current_tot
			from work.KS_2 a
				inner join work.KS_3 b
					on 
					a.var_nm = b.VAR_NM and a.BASEL_SCORECRD_NM = b.BASEL_SCORECRD_NM 
				order by BASEL_SCORECRD_NM,VAR_NM,points_assigned
	;
quit;

data work.ks_5;
	set work.ks_4;
	retain total_si 0;
	by BASEL_SCORECRD_NM VAR_NM;

	if first.BASEL_SCORECRD_NM or first.VAR_NM then
		do;
			total_si=0;
		end;

	base_pct = base_cnt/base_tot;
	current_pct = current_cnt/current_tot;

	/*si1 = next_pct - base_pct;*/
	/*si2= log(next_pct/base_pct);*/
	si = (current_pct - base_pct) * log(current_pct/base_pct);
	total_si = total_si +si;
run;

proc print data = work.ks_5;
	var BASEL_SCORECRD_NM VAR_NM points_assigned si total_si;
run;

%let email_alert = 0;
%put &email_alert;


data work.ks_alert;
	set work.ks_5;

	/*thres=symget('Threshold')*1;*/
	if total_si > &Threshold then
		do;
			/*   %let email_alert = 1;*/
			/*   put BASEL_SCORECRD_NM=;*/
			/*   put VAR_NM=;*/
			output;
		end;
run;

%macro count();
	%let DSNId = %sysfunc(open(work.ks_alert));
	%let DSObs = %sysfunc(attrn(&DSNId,nobs));
	%let rc = %sysfunc(close(&DSNId.));

	%if  &DSObs. NE 0 %then
		%do;
			%let email_alert = 1;
		%end;
%mend;

%count;
%put &email_alert;
%put &email_alert;

proc sql;
	create table work.final as 
		select * from work.ks_5
			order by seq, model_type, BASEL_SCORECRD_NM, VAR_NM, points_assigned;
quit;

data work.final;
	set work.final;
	keep model_type BASEL_SCORECRD_NM VAR_NM points_assigned base_cnt current_cnt base_pct current_pct si;
run;


data work.legend;
	length BASEL_SCORECRD_NM $34.;
	length model_type $1.;
	BASEL_SCORECRD_NM ="Legend:";
	model_type =" ";
	output;
	BASEL_SCORECRD_NM ="Stability Index Within Threshold";
	model_type =" ";
	output;
	BASEL_SCORECRD_NM ="Stability Index Exceeds Threshold";
	model_type =" ";
	output;
run;

%macro create_report;
	Ods tagsets.ExcelXP path="&file_path" file="&file_name" options(sheet_interval='none');
	run;

	Title "";

	%if &email_alert = 1 %then
		%do;

			proc report data=work.final split='~'
				style(summary)=[FONT_WEIGHT = BOLD]
				style(lines)=[fontweight=bold fontsize=5 color=red]
			;
				column model_type BASEL_SCORECRD_NM VAR_NM 
					points_assigned base_cnt current_cnt base_pct current_pct si;
				define model_type /" " order width=3;
				define BASEL_SCORECRD_NM /order width=25;
				define VAR_NM /group width=10;
				define points_assigned / "points~assigned" order width=5;
				define base_cnt  / "&base_ym~BASE_CNT" across analysis width=5 format=comma12.0;
				define current_cnt /"&current_ym~CURRENT_CNT" analysis width=5 format=comma12.0;
				define base_pct /analysis format=percent10.2 width=4;
				define current_pct /analysis format=percent10.2 width=4;
				define si /analysis  format=8.4 width=5;

				compute points_assigned;

					if missing(_C4_) then
						call define('_C4_',"style","style={background=red}");
				endcomp;

				compute base_cnt;

					if missing(_C5_) then
						call define('_C5_',"style","style={background=red}");
				endcomp;

				compute current_cnt;

					if missing(_C6_) then
						call define('_C6_',"style","style={background=red}");
				endcomp;

				compute base_pct;

					if missing(_C7_) then
						call define('_C7_',"style","style={background=red}");
				endcomp;

				compute current_pct;

					if missing(_C8_) then
						call define('_C8_',"style","style={background=red}");
				endcomp;

				compute si;

					if missing(_C9_) then
						call define('_C9_',"style","style={background=red}");
				endcomp;

				break after VAR_NM / summarize dol dul;

				compute after VAR_NM;
					VAR_NM = "sub total";
					BASEL_SCORECRD_NM = " ";
					model_type = " ";
					call define('_C4_',"style","style={background=white}");
					call define('_C5_',"style","style={background=yellow}");
					call define('_C6_',"style","style={background=yellow}");
					call define('_C7_',"style","style={background=yellow}");
					call define('_C8_',"style","style={background=yellow}");
					call define('_C9_',"style","style={background=yellow}");

					if (_c9_) > &Threshold then
						call define('_C9_',"style","style={background=red}");
				endcomp;

				compute before _PAGE_;
					line "KS Parallel Run Scorecard Model Variables &file_name_date - SI above threshold";
				endcomp;
			run;

			Title "";

			proc report data=work.legend split='~' missing  noheader
			;
				column model_type BASEL_SCORECRD_NM;
				define model_type /width=3;
				define BASEL_SCORECRD_NM /width=25 style(column)={cellheight=3};

				compute BASEL_SCORECRD_NM;

					if (_c2_) = "Legend:" then
						call define('_C2_',"style","style={background=white}");

					if (_c2_) = "Stability Index Within Threshold" then
						call define('_C2_',"style","style={background=yellow}");

					if (_c2_) = "Stability Index Exceeds Threshold" then
						call define('_C2_',"style","style={background=red}");
				endcomp;
			run;

		%end;
	%else
		%do;
/*			Ods html file="/u/s2809211/temp.xls";			run;*/

			proc report data=work.final split='~'
				style(summary)=[FONT_WEIGHT = BOLD]
				style(lines)=[fontweight=bold fontsize=5]
			;
				column model_type BASEL_SCORECRD_NM VAR_NM 
					points_assigned base_cnt current_cnt base_pct current_pct si;
				define model_type /" " order width=3;
				define BASEL_SCORECRD_NM /order width=25;
				define VAR_NM /group width=10;
				define points_assigned / "points~assigned" order width=5;
				define base_cnt  / "&base_ym~BASE_CNT" across analysis width=5 format=comma12.0;
				define current_cnt /"&current_ym~CURRENT_CNT" analysis width=5 format=comma12.0;
				define base_pct /analysis format=percent10.2 width=4;
				define current_pct /analysis format=percent10.2 width=4;
				define si /analysis  format=8.4 width=5;

				compute points_assigned;

					if missing(_C4_) then
						call define('_C4_',"style","style={background=red}");
				endcomp;

				compute base_cnt;

					if missing(_C5_) then
						call define('_C5_',"style","style={background=red}");
				endcomp;

				compute current_cnt;

					if missing(_C6_) then
						call define('_C6_',"style","style={background=red}");
				endcomp;

				compute base_pct;

					if missing(_C7_) then
						call define('_C7_',"style","style={background=red}");
				endcomp;

				compute current_pct;

					if missing(_C8_) then
						call define('_C8_',"style","style={background=red}");
				endcomp;

				compute si;

					if missing(_C9_) then
						call define('_C9_',"style","style={background=red}");
				endcomp;

				break after VAR_NM / summarize dol dul;

				compute after VAR_NM;
					VAR_NM = "sub total";
					BASEL_SCORECRD_NM = " ";
					model_type = " ";
					call define('_C4_',"style","style={background=light grey}");
					call define('_C5_',"style","style={background=yellow}");
					call define('_C6_',"style","style={background=yellow}");
					call define('_C7_',"style","style={background=yellow}");
					call define('_C8_',"style","style={background=yellow}");
					call define('_C9_',"style","style={background=yellow}");

					if (_c9_) > &Threshold then
						call define('_C9_',"style","style={background=red}");

					if (_c6_) = '.' then
						call define('_C6_',"style","style={background=red}");
				endcomp;

				compute before _PAGE_;
					line "KS Parallel Run Scorecard Model Variables &file_name_date";
				endcomp;
			run;

			Title "";

			proc report data=work.legend split='~' missing  noheader
			;
				column model_type BASEL_SCORECRD_NM;
				define model_type /width=3;
				define BASEL_SCORECRD_NM /width=25 style(column)={cellheight=3};

				compute BASEL_SCORECRD_NM;

					if (_c2_) = "Legend:" then
						call define('_C2_',"style","style={background=white}");

					if (_c2_) = "Stability Index Within Threshold" then
						call define('_C2_',"style","style={background=yellow}");

					if (_c2_) = "Stability Index Exceeds Threshold" then
						call define('_C2_',"style","style={background=red}");
				endcomp;
			run;

			ods tagsets.ExcelXP close;
			run;

		%end;

	ods tagsets.ExcelXP close;
	run;

%mend;

%create_report;
%put email alert  &email_alert;

%macro sendemail;
	%if &email_alert = 1 %then
		%do;
			FILENAME OUTMAIL EMAIL ATTACH="&file_path.&file_name"
				SUBJECT= "[RRAP] SI above threshold for KS Parallel Run Scorecard Model Variables Report - &MonthYear.";

			DATA _NULL_;
				FILE OUTMAIL
					TO= (&si_email_list);
				put;
			RUN;

		%end;
	%else
		%do;
			FILENAME OUTMAIL EMAIL ATTACH="&file_path.&file_name"
				SUBJECT= "[RRAP] KS Parallel Run Scorecard Model Variables Report - &MonthYear.";

			DATA _NULL_;
				FILE OUTMAIL
					TO= (&email_list);
				put;
			RUN;

		%end;
%mend sendemail;

%sendemail;
%PUT RRAP Exception report 1 - KS Start Time: %Sysfunc(PutN(&datetime_start.,datetime18.));
%PUT                               END TIME  : %sysfunc(datetime(),datetime18.);;
%put PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.));
/*---- End of User Written Code  ----*/ 

%let etls_endTime = %sysfunc(datetime(),datetime.);

