%rrap_mor_bns_autoexec
*** UPDATE DATES BEFORE RUNNING ***
* Be careful, as:
* - Account Level deletes EVERYTHING BETWEEN START_TM_ID AND END_TM_ID from nzuser.BNSMORT_ACCT_LVL_DATA (BEFORE THE CODE CHANGE THIS USED TO DELETE EVERYTHING ON AND AFTER START_TM_ID (>=))
* - Recovery deletes everything during END_TM_ID from nzuser.BNSMORT_MONTH_RCVRY
* - LGDD Recovery deletes everything during END_TM_ID from nzuser.BNSMORT_MONTH_RCVRY_LGDD ;

%get_model_period_dates(product=mor);
%put start_period_dt: &start_period_dt;

data runmonth;
	format start_date end_date date9. start_date2 end_date2 yymmdd10.;
	start_date=intnx('month',"&start_period_dt"d,0,'end');
	start_date2=start_date;
run;

proc sql;
	select start_date into :beginning from runmonth;
	select start_date2 into :beginning2 from runmonth;
quit;

%put &beginning;
%put %bquote('&beginning2');


*Time Data exists up until;
%let start_DATE_num="&beginning"d;
/*%let start_date = '31-05-2020';*/ /*This was initially used when pointing to Netezza. This macro is used in Account Level Code.*/
%let start_date = %bquote('&beginning2'); /*For IIAS the format had to be changed to YYYY-MM-DD*/

%let end_DATE_num="&beginning"d;
/*%let end_DATE='31-05-2020';*/ /*This was initially used when pointing to Netezza. This macro is used in Account Level Code.*/
%let end_DATE=%bquote('&beginning2'); /*For IIAS the format had to be changed to YYYY-MM-DD*/




data _null_;
 n = intck('month',&start_DATE_num.,&end_DATE_num.);

 call symput ('num_mth', n);

run;

%put &num_mth.;


%macro get_date(mydate,freq,tm_name);
 
 %global START_TM_ID END_TM_ID ;

 data _null_;
  set NZRRAP.tm_dim(where=(TM_LVL=&freq. and TM_LVL_END_DT=&mydate.));
  call symput("&tm_name.",tm_id);
 run;

%mend;



	%get_date(&start_DATE_num.,'Month',START_TM_ID);
	%get_date(&end_DATE_num.,'Month',END_TM_ID);
    %put START_TM_ID is: &START_TM_ID;
	%put END_TM_ID is &END_TM_ID.;

/* ACCOUNT LEVEL DATA TABLE POPULATION CODE */

PROC SQL;
CONNECT using NZUSER as iiascon;
execute(
	delete from &FRG_DB..BNSMORT_ACCT_LVL_DATA)by iiascon;
Disconnect from iiascon;
QUIT;

proc sql;
	connect using NZRRAP as iiascon;
	create table BNSMORT_DATA as
	select * from connection to iiascon(select t1.Mort_num,
					t1.MTH_TM_ID,
					t2.TM_LVL_END_DT as PROCESS_DATE,
					t1.CRNT_BAL_AMT,
					t1.INTR_ACCR_AMT,
					t1.PD_OFF_F,
					t1.PD_OFF_DT,
					t1.FRCLSR_F,
					t1.TOT_SUSP_BAL_AMT
				from &RRAP_DB..BASEL_MORT_MTH_SNAPSHOT t1
					left join &RRAP_DB..TM_DIM t2
						on t1.MTH_TM_ID=t2.TM_ID;);
	disconnect from iiascon;
quit;


proc sql;
	CONNECT using NZUSER as iiascon;
	create table SCORE_SEG_ACCTS as
		SELECT *
			From connection to iiascon 
				(select 
					t1.MORTGAGE_NO,
					t1.PROCESS_DATE,
					t1.DEFAULT_DATE,
					t1.DEFAULT_IND,
					t1.NODE,
					t1.LTV,
					t1.AMORT,
					t1.STATUS1
				from &FRG_DB..SCORED_SEGMENTED_ACCTS_ANTQ t1
				where t1.PROCESS_DATE<=&END_DATE
				)
	;
	Disconnect from iiascon;
QUIT;


proc sql;
	create table WORK.BNSMORT_ACCT_LVL_DATA as
		select 
			input(t1.Mort_num,21.) as MORTGAGE_NO,
			t1.MTH_TM_ID,
			t1.PROCESS_DATE,
			t1.CRNT_BAL_AMT,
			t1.INTR_ACCR_AMT,
		case 
			when t3.STATUS1='CUR' then (t1.CRNT_BAL_AMT+t1.INTR_ACCR_AMT) 
			when (t3.STATUS1 ne 'CUR' and t1.TOT_SUSP_BAL_AMT<=0) then max((t1.CRNT_BAL_AMT+t1.INTR_ACCR_AMT),-t1.TOT_SUSP_BAL_AMT) 
			when (t3.STATUS1 ne 'CUR' and t1.TOT_SUSP_BAL_AMT>0) then max((t1.CRNT_BAL_AMT+t1.INTR_ACCR_AMT),-t1.TOT_SUSP_BAL_AMT) 
		end 
		as TOTAL_BAL,
		t1.PD_OFF_F,
		t1.PD_OFF_DT,
		t1.FRCLSR_F,
		t1.TOT_SUSP_BAL_AMT,
		t3.STATUS1,	
		intnx('month',t3.DEFAULT_DATE,1)-1 format=date9. as DEFAULT_DATE,
		case when t3.DEFAULT_IND=1 then (t6.CRNT_BAL_AMT+t6.INTR_ACCR_AMT) else . end  as default_bal,
		t3.DEFAULT_IND,
		t3.LTV,
		t3.AMORT,
		t3.NODE as PD_SEG
	from BNSMORT_DATA t1
	left join work.SCORE_SEG_ACCTS t3
		on input(t1.Mort_num,21.)=t3.MORTGAGE_NO
		and t1.PROCESS_DATE=t3.PROCESS_DATE
	left join (select Mort_num, process_date, CRNT_BAL_AMT, INTR_ACCR_AMT from BNSMORT_DATA) as t6
		on input(t1.Mort_num,21.)=input(t6.Mort_num,21.) 
		and intnx('month',t3.DEFAULT_DATE,1)-1=t6.process_date;
quit;

proc append base=NZUSER.BNSMORT_ACCT_LVL_DATA(bulkload = YES BL_METHOD=CLILOAD)
	data= WORK.BNSMORT_ACCT_LVL_DATA;
run;


/* LGDD RECOVERY DATA CREATION */

%macro lgdd_recovery;
PROC SQL;
CONNECT using NZUSER as iiascon;
execute(
	delete from &FRG_DB..BNSMORT_MONTH_RCVRY_LGDD t1
where t1.MTH_TM_ID = &END_TM_ID )by iiascon;
Disconnect from iiascon;
QUIT;

PROC SQL;
CONNECT using NZUSER as iiascon;
	CREATE TABLE work.MORT_TOTAL_BAL AS 
	select * from connection to iiascon(
		SELECT t1.MORTGAGE_NO,
					t1.MTH_TM_ID,
					t1.STATUS1,
					t1.PD_OFF_F,
					t1.FRCLSR_F,
					t1.CRNT_BAL_AMT,
					t1.INTR_ACCR_AMT,
					t1.TOT_SUSP_BAL_AMT,
					t1.TOTAL_BAL
				from &FRG_DB..BNSMORT_ACCT_LVL_DATA t1
				where t1.MTH_TM_ID = &END_TM_ID or t1.MTH_TM_ID = (&END_TM_ID-40));
	disconnect from iiascon;
QUIT;

proc sql;
select distinct mth_tm_id from MORT_TOTAL_BAL order by 1;
quit;

proc sql;
	create table work.MORT_TOTAL_BAL_WITH_LAG as
		select t1.*,
			t2.TOTAL_BAL as PREV_TOTAL_BAL,
			t2.TOT_SUSP_BAL_AMT as PREV_SUSP_BAL,  /* NEW: added this for case below */
			t2.STATUS1 as PREV_STATUS,
			t2.PD_OFF_F as PREV_PD_OFF_F
		from work.MORT_TOTAL_BAL t1
			left join work.MORT_TOTAL_BAL t2
				on t1.MORTGAGE_NO=t2.MORTGAGE_NO
				and (t1.MTH_TM_ID-40)=t2.MTH_TM_ID;
quit;

data work.MORT_MONTH_RCVRY_LGDD;
	set work.MORT_TOTAL_BAL_WITH_LAG;

	if status1="CUR" and PREV_STATUS ne "CUR" then
		recovery=PREV_TOTAL_BAL;
	else if (status1 in ('DEF','') and (FRCLSR_F ne '' or PD_OFF_F = "N") and TOTAL_BAL ne 0) then
		recovery=PREV_TOTAL_BAL-TOTAL_BAL;
/*	[FIX - updated May 31 2019]: add case for suspended balances showing as paid-off */
	else if (status1 ='' and TOTAL_BAL=0 and prev_total_bal ne 0 and 
			(PREV_PD_OFF_F='N' or (PREV_PD_OFF_F='Y' and PREV_SUSP_BAL ne . and PREV_SUSP_BAL < 0))) then
		recovery=PREV_TOTAL_BAL;
	else if STATUS1='' and PREV_STATUS in ('DEF') and total_BAL>0 and PD_OFF_F='Y' and PREV_TOTAL_BAL ne 0 then
		recovery=PREV_TOTAL_BAL-TOTAL_BAL;
	else recovery=.;
run;

proc sql;
	create table WORK.BNSMORT_MONTH_RCVRY_LGDD as
		select t1.MORTGAGE_NO,
			t1.MTH_TM_ID,
			t2.tm_lvl_end_dt as PROCESS_DATE,
			t1.TOTAL_BAL as CRNT_OS_BAL_AMT,
			t1.recovery as MTH_RCVRY_AMT,
			t1.PREV_TOTAL_BAL as PREV_MTH_OS_BAL_AMT,
			t1.STATUS1
		from work.MORT_MONTH_RCVRY_LGDD t1
			left join NZRRAP.TM_DIM as t2
				on t1.mth_tm_id = t2.tm_id
				where t1.MTH_TM_ID = &END_TM_ID
		order by t1.MORTGAGE_NO, t1.MTH_TM_ID;
quit;

PROC APPEND BASE=NZUSER.BNSMORT_MONTH_RCVRY_LGDD(BULKLOAD=YES BL_METHOD=CLILOAD)
			DATA=BNSMORT_MONTH_RCVRY_LGDD;
RUN;

%mend lgdd_recovery;

/* LGD-ND RECOVERY DATA CREATION */

%macro lgdnd_recovery;

PROC SQL;
CONNECT using NZUSER as iiascon;
execute(
	delete from &FRG_DB..BNSMORT_MONTH_RCVRY t1
where t1.MTH_TM_ID = &END_TM_ID) by iiascon;
Disconnect from iiascon;
QUIT;

PROC SQL;
CONNECT using NZUSER as iiascon;
	CREATE TABLE work.MORT_TOTAL_BAL AS 
	select * from connection to iiascon(
		SELECT t1.MORTGAGE_NO,
					t1.MTH_TM_ID,
					t1.STATUS1,
					t1.PD_OFF_F,
					t1.FRCLSR_F,
					t1.CRNT_BAL_AMT,
					t1.INTR_ACCR_AMT,
					t1.TOT_SUSP_BAL_AMT,
					t1.TOTAL_BAL
				from &FRG_DB..BNSMORT_ACCT_LVL_DATA t1
				where t1.MTH_TM_ID = &END_TM_ID or t1.MTH_TM_ID = (&END_TM_ID-40));
	disconnect from iiascon;
QUIT;

proc sql;
	create table work.MORT_TOTAL_BAL_WITH_LAG as
		select t1.*,
			t2.TOTAL_BAL as PREV_TOTAL_BAL,
			t2.STATUS1 as PREV_STATUS
		from work.MORT_TOTAL_BAL t1
			left join work.MORT_TOTAL_BAL t2
				on t1.MORTGAGE_NO=t2.MORTGAGE_NO
				and (t1.MTH_TM_ID-40)=t2.MTH_TM_ID;
quit;

data work.BNSMORT_MONTH_RCVRY;
	set work.MORT_TOTAL_BAL_WITH_LAG;
	if STATUS1 in ('DEF' 'CHG') and PREV_STATUS ne 'CUR' then
		recovery=-1*(TOTAL_BAL-PREV_TOTAL_BAL);
	else if STATUS1='CUR' and PREV_STATUS in ('DEF' 'CHG') then
		recovery=(PREV_TOTAL_BAL);
	else if STATUS1='' and PREV_STATUS in ('DEF') and PD_OFF_F='Y' and TOTAL_BAL>=0 and PREV_TOTAL_BAL ne 0 then
		recovery=PREV_TOTAL_BAL-TOTAL_BAL;
	else if STATUS1='' and PREV_STATUS in ('DEF') and PD_OFF_F='Y' and TOTAL_BAL<0 and PREV_TOTAL_BAL ne 0 then
		recovery=PREV_TOTAL_BAL-TOTAL_BAL;
	else if STATUS1='' and PREV_STATUS = '' and PD_OFF_F='Y' and FRCLSR_F='Y' and TOTAL_BAL>0 and PREV_TOTAL_BAL >=0 then
		recovery=PREV_TOTAL_BAL-TOTAL_BAL;
	else if STATUS1='' and PREV_STATUS = '' and PD_OFF_F='Y' and FRCLSR_F='Y' and TOTAL_BAL=0 and PREV_TOTAL_BAL >0 then
		recovery=0;
run;

proc sql;
create table work.BNSMORT_MONTH_RCVRY as
		select t1.MORTGAGE_NO,
			t1.MTH_TM_ID,
			t2.tm_lvl_end_dt as PROCESS_DATE,
			t1.TOTAL_BAL as CRNT_OS_BAL_AMT,
			t1.recovery as MTH_RCVRY_AMT,
			t1.PREV_TOTAL_BAL as PREV_MTH_OS_BAL_AMT,
			t1.STATUS1
		from work.BNSMORT_MONTH_RCVRY t1
			left join NZRRAP.TM_DIM as t2
				on t1.mth_tm_id = t2.tm_id
				where t1.MTH_TM_ID = &END_TM_ID
		order by t1.MORTGAGE_NO, t1.MTH_TM_ID;
quit;

PROC APPEND BASE=NZUSER.BNSMORT_MONTH_RCVRY(BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=BNSMORT_MONTH_RCVRY;
RUN;

%mend lgdnd_recovery;

/* MACRO CALL FOR RECOVERY CALCULATIONS */


%lgdnd_recovery
%lgdd_recovery