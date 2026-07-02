
%rrap_spl_autoexec

%macro itllgdnd(end,cut,month);

%let END_DATE=&end;
%let cutoff=&cut;
/*%let RES_cutoff='30APR2013'd;*/

%let LGD_DATA=ITL_LGD_SEG;
%let LGD_BASEL_MODEL_ID=8031;
%let PROD_LIB = work;
%let LGD_NAME=ITL_LGD_SEG;
%let Exclusions=(16155,16156);
%let PROD=ITL;
%let CC_DISCOUNT=0.1;
%let RES_DISCOUNT=0.05;
%let RCVY_TIMEFRAME=24;
%let RES_RCVY_TIMEFRAME=60;
%let TM_ID_START=10876;


PROC SQL;
CONNECT USING NZRRAP as nzcon;
   CREATE TABLE &LGD_DATA AS 
   SELECT *
From connection to nzcon 
(select t1.BASEL_ACCT_ID, 
          t1.MTH_TM_ID, 
		  t2.TM_LVL_END_DT as PROCESS_DATE, 
          t1.BASEL_SEG_ID, 
          t1.BASEL_MODEL_ID
      FROM &RRAP_DB..BASEL_PNL_LN_LGD_SEG_ACCT_XREF t1 Inner join &RRAP_DB..TM_DIM t2 on (t1.MTH_TM_ID=t2.TM_ID)
      WHERE  t1.BASEL_MODEL_ID = &LGD_BASEL_MODEL_ID and t2.TM_LVL='Month' and t2.MTH_CLNDR_CD = %bquote('&month')
	  and t1.MTH_TM_ID>=&TM_ID_START);
	  Disconnect from nzcon;
QUIT;

PROC SQL;
   CREATE TABLE WORK.NETEZZA_COSTS AS 
   SELECT unique t1.MTH_TM_ID, 
          t1.BASEL_ACCT_ID, 
          sum(t1.TOT_COST_EXPNS_AMT) as TOT_COST_EXPNS_AMT, 
		  t1.PGM_TP_CD,
          t1.HGHWY_CD,
		  t1.SRC_SYS_CD
      FROM NZRRAP.ASST_DRC_COST_MTH_SNAPSHOT t1
	  where t1.BASEL_ACCT_ID<>-1 and t1.SRC_SYS_CD='SPL' and t1.HGHWY_CD <> 'RH' and t1.MTH_TM_ID>=&TM_ID_START
		group by t1.basel_acct_id,PGM_TP_CD,MTH_TM_ID
		order by t1.basel_acct_id,PGM_TP_CD,MTH_TM_ID;
QUIT;

data work.netezza_costs;
	set work.netezza_costs;
	by basel_acct_id PGM_TP_CD;
	lag_cost=lag(TOT_COST_EXPNS_AMT);
	if first.PGM_TP_CD then do;
		DATE_HGHWY_COST=TOT_COST_EXPNS_AMT;
	end;
	else DATE_HGHWY_COST=TOT_COST_EXPNS_AMT-lag_cost;
	drop lag_cost;
run;


proc sql;
	create table netezza_dir_costs as
		select t1.BASEL_ACCT_ID, 
			t1.MTH_TM_ID, 
			t2.TM_LVL_END_DT as COST_DATE,
			sum(t1.DATE_HGHWY_COST) as costs
		from work.netezza_costs t1
			left join NZRRAP.tm_DIM t2
				on t1.MTH_TM_ID=t2.TM_ID
	where t1.DATE_HGHWY_COST>0
	group by t1.BASEL_ACCT_ID,t1.MTH_TM_ID, t2.TM_LVL_END_DT;
quit;  

PROC SQL;
   CREATE TABLE WORK.netezza_RCVRY AS 
   SELECT t1.MTH_TM_ID, 
          t1.BASEL_ACCT_ID, 
          t1.TOT_RCVRY_AMT, 
		  t1.PGM_TP_CD,
          t1.HGHWY_CD,
		  t1.SRC_SYS_CD
      FROM NZRRAP.ASST_DRC_COST_MTH_SNAPSHOT t1
	  where t1.BASEL_ACCT_ID<>-1 and t1.SRC_SYS_CD='SPL' and t1.HGHWY_CD <> 'RH' and t1.MTH_TM_ID>=&TM_ID_START. /*April 2005*/
	  order by t1.basel_acct_id,PGM_TP_CD,MTH_TM_ID,  TOT_RCVRY_AMT desc;
QUIT;

data netezza_RCVRY2;
  set netezza_RCVRY;
  by basel_acct_id PGM_TP_CD MTH_TM_ID;
  if first.MTH_TM_ID then output netezza_RCVRY2;
run;

/*NOTE: There were 7629941 observations read from the data set WORK.NETEZZA_RCVRY.*/
/*NOTE: The data set WORK.NETEZZA_RCVRY2 has 7364716 observations and 6 variables.*/

data work.netezza_RCVRY3;
	set work.netezza_RCVRY2;
	by basel_acct_id PGM_TP_CD;
	lag_RCVRY=lag(TOT_RCVRY_AMT);
	if first.PGM_TP_CD then do;
		DATE_HGHWY_RCVRY=TOT_RCVRY_AMT;
	end;
	else DATE_HGHWY_RCVRY=TOT_RCVRY_AMT-lag_RCVRY;
	drop lag_RCVRY;
run;


proc sql;
	create table netezza_RECVY_final as
		select t1.BASEL_ACCT_ID, 
			t1.MTH_TM_ID, 
			t2.TM_LVL_END_DT as RECVY_DATE,
			sum(t1.DATE_HGHWY_RCVRY) as netezza_RECVY
		from work.netezza_RCVRY3 t1
			left join NZRRAP.tm_DIM t2
				on t1.MTH_TM_ID=t2.TM_ID
	where t1.DATE_HGHWY_RCVRY>0
	group by t1.BASEL_ACCT_ID,t1.MTH_TM_ID, t2.TM_LVL_END_DT;
quit;
/*NOTE: Table WORK.NETEZZA_RECVY_FINAL created, with 371129 rows and 4 columns. */

PROC SQL;
CONNECT USING NZRRAP as nzcon;
   CREATE TABLE ACCT_DFLT_DATES1 AS 
   SELECT BASEL_ACCT_ID,
   		LAST_NEW_DFT_DT,
		LAST_NEW_DFT_BAL_AMT,
		RCVRY_WINDOW_CUTOFF_DT
From connection to nzcon 
(select *
      FROM &RRAP_DB..PSNL_LOAN_OBSVTN_PT_DRVD_VAR
	  where OBSVTN_MTH_TM_ID >= &TM_ID_START);
	  Disconnect from nzcon;
QUIT;

data ACCT_DFLT_DATES1;
	set ACCT_DFLT_DATES1;
	RCVRY_WINDOW_CUTOFF_DT=intnx('month',LAST_NEW_DFT_DT,&RCVY_TIMEFRAME,'end');
run;

proc sort data=ACCT_DFLT_DATES1 nodupkey;
	by BASEL_ACCT_ID LAST_NEW_DFT_DT;
run; 

proc transpose 	data=ACCT_DFLT_DATES1
				out= TRANSPOSED_DFLT_DATES
				prefix=LAST_NEW_DFT_DT;
	var LAST_NEW_DFT_DT;
	by BASEL_ACCT_ID;
run;

proc transpose 	data=ACCT_DFLT_DATES1
				out= TRANSPOSED_DFLT_AMT
				prefix=LAST_NEW_DFT_BAL_AMT;
	var LAST_NEW_DFT_BAL_AMT;
	by BASEL_ACCT_ID;
run;


proc transpose 	data=ACCT_DFLT_DATES1
				out= TRANSPOSED_RCVRY_CUTOFF_DT
				prefix=RCVRY_WINDOW_CUTOFF_DT;
	var RCVRY_WINDOW_CUTOFF_DT;
	by BASEL_ACCT_ID;
run;

data ACCT_DFLT_DATES_LGD;
	merge TRANSPOSED_DFLT_DATES TRANSPOSED_DFLT_AMT TRANSPOSED_RCVRY_CUTOFF_DT;
	drop _LABEL_ _NAME_;
run;


*Get Default dates for accounts;
*Only look at accounts that have defaulted and the right product;

proc sql;
   	create table LGD_ACCOUNTS as 
	select *
	from &LGD_DATA. t1 
	Left join ACCT_DFLT_DATES_LGD  t2
	on t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID
	where t2.last_new_dft_dt1>0 and t1.basel_model_id=&LGD_BASEL_MODEL_ID and process_date<=&cutoff;
quit;

*Find the latest default date within a year of the process date;
*Get the default date, balance and recovery date;
Data &PROD_LIB..LGD12_DFLTS;
	set LGD_ACCOUNTS;
	if BASEL_SEG_ID NOT in &exclusions;
	format DFLT_DT date9.;
	format DFLT_RCVRY date9.;
	ARRAY DFLTS(*) LAST_NEW_DFT_DT: ;
	ARRAY DFLTS_BAL(*) LAST_NEW_DFT_BAL_AMT: ;
	ARRAY DFLTS_RCVRY(*) RCVRY_WINDOW_CUTOFF_DT: ;
	do i = DIM(DFLTS) to 1 by -1;
		IF INTCK('MONTH',PROCESS_DATE,DFLTS[i])<=12 
		and INTCK('DAY',PROCESS_DATE,DFLTS[i]) >=0 
		and DFLTS_RCVRY[i] <=&END_DATE then do;
			DFLT_DT=DFLTS[i];
			DFLT_BAL=DFLTs_BAL[i];
			DFLT_RCVRY=DFLTS_RCVRY[i];
			output;
			leave;
		end;
	end;
	keep 	BASEL_ACCT_ID Process_DATE MTH_TM_ID BASEL_MODEL_ID BASEL_SEG_ID
			DFLT_DT DFLT_BAL DFLT_RCVRY;
run;

PROC SQL;
   CREATE TABLE WORK.DT_TM AS 
   SELECT DISTINCT t1.PROCESS_DATE
      FROM NZRRAP.TERM_MONTH_RCVRY t1;
QUIT;

*Get Recovery Dates;
proc sql;
	create table &PROD_LIB..ACCT_24_MTH_RCVY as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t2.PROCESS_DATE as RCVY_DT format=date9.
	from &PROD_LIB..LGD12_DFLTS t1
		left join WORK.DT_TM t2 
		ON 	t2.PROCESS_DATE BETWEEN (t1.DFLT_DT+1) AND (t1.DFLT_RCVRY+1);
quit;

proc delete data=DT_TM; run;

*Find all recovery data within 24 months of the default date;
proc sql;
	create table &PROD_LIB..ACCT_24_MTH_RCVY as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
          t2.MTH_RCVRY_AMT,
		  t1.RCVY_DT, 
		  t2.STATUS1 as ACCT_STAT,
		   t3.netezza_RECVY
	from &PROD_LIB..ACCT_24_MTH_RCVY t1
		left join NZRRAP.TERM_MONTH_RCVRY (RENAME=(PROCESS_DATE=RCVY_DT))t2 
		ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
  		and t1.RCVY_DT=t2.RCVY_DT
	 left join netezza_RECVY_final t3
	  	ON 	t1.BASEL_ACCT_ID=t3.BASEL_ACCT_ID 
		and t1.RCVY_DT=t3.RECVY_DATE
;
quit;

*sort and transpose Recovery amount, recovery date, and account status;
proc sort data=&PROD_LIB..ACCT_24_MTH_RCVY;
	by BASEL_ACCT_ID PROCESS_DATE RCVY_DT;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_MTH_RCVY_TRANSPOSE
				prefix=MTH_RCVRY_AMT;
	var MTH_RCVRY_AMT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_MTH_RCVY_DATE_TRANSPOSE
				prefix=MTH_RCVRY_DATE;
	var RCVY_DT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var ACCT_STAT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_MTH_nz_RCVY_TRANSPOSE
				prefix=MTH_netezza_RCVRY_AMT;
	var netezza_RECVY;
	by BASEL_ACCT_ID PROCESS_DATE;
run;


data LGD_RCVY_DATA;
	merge ACCT_24_MTH_RCVY_TRANSPOSE ACCT_24_MTH_RCVY_DATE_TRANSPOSE ACCT_24_STAT_TRANSPOSE  ACCT_24_MTH_nz_RCVY_TRANSPOSE;
	by BASEL_ACCT_ID PROCESS_DATE;
	drop _name_ _label_;
run;

*Merge transposed recovery data back onto the table with the rest of the necessary info;
proc sql;
	create table INTMED.&PROD._LGD12_RECVY_DATA as
	SELECT *
	from &PROD_LIB..LGD12_DFLTS t1 INNER JOIN LGD_RCVY_DATA t2
		ON t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;


PROC SQL;
   CREATE TABLE WORK.DT_TM AS 
   SELECT DISTINCT t1.PROCESS_DATE
      FROM NZRRAP.TERM_MONTH_RCVRY t1;
QUIT;

*Find costs data within 24 months of default;
proc sql;
	create table ACCT_24_MTH_COSTS_BASE as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t2.PROCESS_DATE as COST_DATE format=date9.,
		  t1.DFLT_DT
	from &PROD_LIB..LGD12_DFLTS t1
		left join WORK.DT_TM t2 
		ON 	t2.PROCESS_DATE BETWEEN (t1.DFLT_DT+1) AND (t1.DFLT_RCVRY+1);
quit;

proc delete data=DT_TM; run;

/*proc sql;*/
/*	create table IND_COST as*/
/*	select t2.TM_LVL_END_DT as PROCESS_DATE,*/
/*			(case when t1.MTH_TM_ID=16756 then 6.08 else t1.UNIT_INDRCT_COST_AMT end) as UNIT_COST*/
/*	from NZRRAP.DRVD_MTH_INDRCT_COST_SUM t1*/
/*	left join NZRRAP.TM_DIM t2 on (t1.MTH_TM_ID=t2.TM_ID)*/
/*		order by t1.MTH_TM_ID;*/
/*quit;*/
*Find indirect costs data within 24 months of default;
proc sql;
	create table ACCT_24_MTH_COSTS_BASE as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t1.COST_DATE,
		  t2.UNIT_COST as ind_cost,
		  t1.DFLT_DT
	from ACCT_24_MTH_COSTS_BASE t1
		left join NZFRG.ind_cost t2 
		ON 	t2.PROCESS_DATE = t1.COST_DATE;
quit;

*Find costs data within 24 months of default;
proc sql;
	create table ACCT_24_MTH_COSTS_BASE as 
		select t1.BASEL_ACCT_ID, 
			t1.PROCESS_DATE, 
			t2.COSTS,
			t1.ind_cost,
			t1.COST_DATE,
			t1.DFLT_DT
		from ACCT_24_MTH_COSTS_BASE t1
			left join netezza_dir_costs t2 
				ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
				and t2.cost_date=t1.cost_date;
quit;

*Find Account Status;
proc sql;
	create table ACCT_24_MTH_COSTS as 
		select 	t1.BASEL_ACCT_ID, 
			t1.PROCESS_DATE, 
			t1.COSTS,
			t1.ind_cost,
			t1.COST_DATE,
			t2.ACCT_STAT,
			t1.DFLT_DT
		from ACCT_24_MTH_COSTS_BASE t1
			left join &PROD_LIB..ACCT_24_MTH_RCVY t2 
				ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
				AND t2.RCVY_DT=t1.COST_DATE 
				AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

data ACCT_24_MTH_COSTS;
	set ACCT_24_MTH_COSTS;
	if DFLT_DT=COST_DATE then ACCT_STAT='DEF';
run;

data ACCT_24_MTH_COSTS;
	set ACCT_24_MTH_COSTS;
	if (DFLT_DT=COST_DATE or ACCT_STAT ne 'DEF') then ind_cost=0;
run;

*Sort and transpose data on Costs, costs date and account status;
proc sort data=ACCT_24_MTH_COSTS;
	by BASEL_ACCT_ID PROCESS_DATE COST_DATE;
run;

proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_COSTS_TRANSPOSE
				prefix=COSTS;
	var COSTS;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_ind_cost_TRANSPOSE
				prefix=ind_cost;
	var ind_cost;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_COSTS_DATE_TRANSPOSE
				prefix=COST_DATE;
	var COST_DATE;
	by BASEL_ACCT_ID PROCESS_DATE;
run;


proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var ACCT_STAT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

data LGD_COSTS_DATA;
	merge ACCT_24_MTH_COSTS_TRANSPOSE ACCT_24_MTH_ind_cost_TRANSPOSE ACCT_24_MTH_COSTS_DATE_TRANSPOSE ACCT_24_MTH_STAT_TRANSPOSE;
	by BASEL_ACCT_ID PROCESS_DATE;
	drop _name_ _label_;
run;

*Join costs data onto rest of the necessary data for accounts;
proc sql;
	create table INTMED.&PROD._LGD12_COST_DATA as
	SELECT *
	from &PROD_LIB..LGD12_DFLTS t1 INNER JOIN LGD_COSTS_DATA t2
		ON t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

*Discount recovery data;
%let cc_discount=0.1;
data DISCOUNT_RECVY;
	set INTMED.&PROD._LGD12_RECVY_DATA;
	ARRAY RCVY_AMT[*] MTH_RCVRY_AMT: ;
	ARRAY DATES[*] MTH_RCVRY_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
     ARRAY RCVY_AMT_netezza [*] MTH_netezza_RCVRY_AMT: ; 


	do i= 1 to dim(RCVY_AMT);
		if i=1 then do; total_RECY=0 ; total_RECY_netezza=0; end;
		*Calculate total recovery with positive numbers;
		if RCVY_AMT(i) >0 and RCVY_AMT(i) ne . then total_RECY = total_RECY+RCVY_AMT(i); 
        if i > 1 and RCVY_AMT_netezza(i) ne . then total_RECY_netezza = total_RECY_netezza+RCVY_AMT_netezza(i);
		*IF status is cured then no more recoveries, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;

    *Discount the recovery surplus for 24 months;
	if total_RECY_netezza > total_RECY then recy_surplus = total_RECY_netezza - total_RECY;
    DISCT_RECY_surplus = recy_surplus/(1+&CC_DISCOUNT)**2;
    if DISCT_RECY_surplus = . then DISCT_RECY_surplus = 0;


	do i= 1 to dim(RCVY_AMT);
		if i=1 then DISCT_RECY=0;
		RCVY_AMT(i)=RCVY_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',DFLT_DT,DATES(i))/12));
		if RCVY_AMT(i) ne . then DISCT_RECY = DISCT_RECY+RCVY_AMT(i);
		*IF status is cured then no more recoveries, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if dflt_rcvry<=&end_date;
	drop _label_ _name_ i MTH_RCVRY_AMT: MTH_RCVRY_DATE: ACCT_STAT: ;*;
run;
%let cc_discount=0.1;
*discount Costs data;
data DISCOUNT_COSTS;
	set INTMED.&PROD._LGD12_COST_DATA;
	ARRAY COST_AMT[*] COSTS: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',DFLT_DT,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_COSTS = DISCT_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if DISCT_COSTS=. then DISCT_COSTS=0;
	if dflt_rcvry<=&end_date;
	drop _label_ _name_ i COSTS: ind_cost: COST_DATE: ACCT_STAT:;
run;

data DISCOUNT_IND_COSTS;
	set INTMED.&PROD._LGD12_COST_DATA;
	ARRAY COST_AMT[*] ind_cost: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_IND_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',DFLT_DT,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_IND_COSTS = DISCT_IND_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if DISCT_IND_COSTS=. then DISCT_IND_COSTS=0;
	if dflt_rcvry<=&end_date;
	drop _label_ _name_ i ind_cost: COSTS: COST_DATE: ACCT_STAT:;
run;

*Join discounted costs and recovery to obtain LGD;
proc sql;
	create table &PROD_LIB..LGD12_DATA as 
		select t1.*,
			t2.DISCT_COSTS,
			t3.DISCT_IND_COSTS,
			(t1.DFLT_BAL-t1.DISCT_RECY - t1.DISCT_RECY_surplus)/t1.DFLT_BAL as LGDND_NO_COST,
			(t1.DFLT_BAL-t1.DISCT_RECY - t1.DISCT_RECY_surplus +t2.DISCT_COSTS)/t1.DFLT_BAL as LGD_DRC_COST,
			(t1.DFLT_BAL-t1.DISCT_RECY- t1.DISCT_RECY_surplus +t2.DISCT_COSTS+t3.DISCT_IND_COSTS)/t1.DFLT_BAL as LGDND_ALL_COST_MOD,
			(t1.DFLT_BAL-t1.DISCT_RECY+t3.DISCT_IND_COSTS-t1.DISCT_RECY_surplus)/t1.DFLT_BAL as LGDD_INDRC_COST
		from DISCOUNT_RECVY t1 
			full join DISCOUNT_COSTS t2 
				on t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
				AND t1.PROCESS_DATE=t2.PROCESS_DATE
			full join DISCOUNT_IND_COSTS t3
				on t1.BASEL_ACCT_ID=t3.BASEL_ACCT_ID 
				AND t1.PROCESS_DATE=t3.PROCESS_DATE;
quit;

data &PROD_LIB..LGD12_DATA;
	set &PROD_LIB..LGD12_DATA;
	LGDND_ALL_COST_FLR_CAP125=LGDND_ALL_COST_MOD;
	if LGDND_ALL_COST_FLR_CAP125<0 then LGDND_ALL_COST_FLR_CAP125=0;
	if LGDND_ALL_COST_FLR_CAP125>1.25 then LGDND_ALL_COST_FLR_CAP125=1.25;

	LGDD_ALL_COST_FLR_CAP100=LGDND_ALL_COST_MOD;
	if LGDD_ALL_COST_FLR_CAP100<0 then LGDD_ALL_COST_FLR_CAP100=0;
	if LGDD_ALL_COST_FLR_CAP100>1 then LGDD_ALL_COST_FLR_CAP100=1;

	LGDND_NO_COST_CAP=LGDND_NO_COST;
	if LGDND_NO_COST_CAP<0 then LGDND_NO_COST_CAP=0;
	if LGDND_NO_COST_CAP>1.25 then LGDND_NO_COST_CAP=1.25;
run;

data INTMED.&PROD._LGD_ND_DATA;
	set &PROD_LIB..LGD12_DATA;
	where process_date<=&cutoff;
run;

%if %sysfunc(exist(NZRRAP.SPLI_LGD_ND_UPDATE_INTERIM_&PREV_TM_ID.)) %then %do;
PROC SQL;
	DROP TABLE NZRRAP.SPLI_LGD_ND_UPDATE_INTERIM_&PREV_TM_ID.;
QUIT;
%END;

%if %sysfunc(exist(NZRRAP.SPLI_LGD_ND_UPDATE_INTERIM_&TM_ID.)) %then %do;
PROC SQL;
	DROP TABLE NZRRAP.SPLI_LGD_ND_UPDATE_INTERIM_&TM_ID.;
QUIT;
%END;

proc sql;
	connect using nzrrap as iiascon;
	execute(create table &RRAP_DB..SPLI_LGD_ND_UPDATE_INTERIM_&TM_ID. AS(SELECT * FROM &RRAP_DB..PSNL_LN_LGD_OBSVT_PT_RELZ_VAL WHERE OBSVTN_MTH_TM_ID = &TM_ID AND LGD_TYPE='LGD-ND' AND SUB_PORTFOLIO='INDIRECT')WITH DATA ORGANIZE BY ROW)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CONNECT USING NZRRAP AS IIASCON;
	EXECUTE(UPDATE &RRAP_DB..SPLI_LGD_ND_UPDATE_INTERIM_&TM_ID.
			SET 
				LGD_RTO_BKUP = LGD_RTO,
				LGD_NPV_ALL_COST_100_RTO_BKP = LGD_NPV_ALL_COST_FLR_CAP_100_RTO,
				LGD_NPV_ALL_COST_150_RTO_BKP = LGD_NPV_ALL_COST_FLR_CAP_150_RTO,
				LGD_NPV_ALL_COST_RTO_BKUP = LGD_NPV_ALL_COST_RTO,
				LGD_NPV_DRC_COST_RTO_BKUP = LGD_NPV_DRC_COST_RTO,
				LGD_NPV_INDRCT_COST_RTO_BKUP = LGD_NPV_INDRCT_COST_RTO)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	UPDATE NZRRAP.SPLI_LGD_ND_UPDATE_INTERIM_&TM_ID. AS T
	SET LGD_RTO = (SELECT LGDND_NO_COST FROM INTMED.&PROD._LGD_ND_DATA AS A
					WHERE T.basel_acct_id = A.basel_acct_id AND T.obsvtn_date = A.process_Date),
		LGD_NPV_ALL_COST_RTO = (SELECT LGDND_ALL_COST_MOD FROM INTMED.&PROD._LGD_ND_DATA AS B
					WHERE T.basel_acct_id = B.basel_acct_id AND T.obsvtn_date = B.process_Date),
		LGD_NPV_DRC_COST_RTO = (SELECT LGD_DRC_COST FROM INTMED.&PROD._LGD_ND_DATA AS C
					WHERE T.basel_acct_id = C.basel_acct_id AND T.obsvtn_date = C.process_Date),
		LGD_NPV_INDRCT_COST_RTO = (SELECT LGDD_INDRC_COST FROM INTMED.&PROD._LGD_ND_DATA AS D
					WHERE T.basel_acct_id = D.basel_acct_id AND T.obsvtn_date = D.process_Date),
		LGD_NO_COST_FLR_CAP_150_RTO = (SELECT LGDND_NO_COST_CAP FROM INTMED.&PROD._LGD_ND_DATA AS E
					WHERE T.basel_acct_id = E.basel_acct_id AND T.obsvtn_date = E.process_Date),
		LGD_NPV_ALL_COST_FLR_CAP_150_RTO = (SELECT LGDND_ALL_COST_FLR_CAP125 FROM INTMED.&PROD._LGD_ND_DATA AS F
					WHERE T.basel_acct_id = F.basel_acct_id AND T.obsvtn_date = F.process_Date),
		LGD_NPV_ALL_COST_FLR_CAP_100_RTO = (SELECT LGDD_ALL_COST_FLR_CAP100 FROM INTMED.&PROD._LGD_ND_DATA AS G
					WHERE T.basel_acct_id = G.basel_acct_id AND T.obsvtn_date = G.process_Date)
	WHERE T.basel_acct_id IN (SELECT basel_acct_id FROM INTMED.&PROD._LGD_ND_DATA) AND T.obsvtn_date = "&COMPLETE"D AND T.LGD_TYPE='LGD-ND' AND SUB_PORTFOLIO='INDIRECT';
QUIT;

PROC SQL;
	CONNECT USING NZRRAP AS IIASCON;
	EXECUTE(DELETE FROM &RRAP_DB..PSNL_LN_LGD_OBSVT_PT_RELZ_VAL WHERE OBSVTN_MTH_TM_ID = &TM_ID AND LGD_TYPE='LGD-ND' AND SUB_PORTFOLIO='INDIRECT')BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

proc append base=NZRRAP.PSNL_LN_LGD_OBSVT_PT_RELZ_VAL(bulkload=yes bl_method=cliload) data=NZRRAP.SPLI_LGD_ND_UPDATE_INTERIM_&TM_ID.;
run;

%mend itllgdnd;


%macro dtllgdnd(end,cut,month);

%let END_DATE=&end;*change date: the current end moth;
%let cutoff=&cut;*change date: 3 year cutoff;
%let LGD_DATA=DTL_LGD_SEG;
%let LGD_BASEL_MODEL_ID=8028;
%let PROD_LIB=work;
%let LGD_NAME=DTL_LGD_SEG;
%let Exclusions=(16128);
%let PROD=DTL;
%let CC_DISCOUNT=0.1;
%let RES_DISCOUNT=0.05;
%let RCVY_TIMEFRAME=24;
%let RES_RCVY_TIMEFRAME=60;
%let TM_ID_START=10996;




PROC SQL;
CONNECT USING NZRRAP as nzcon;
   CREATE TABLE &LGD_DATA AS 
   SELECT *
From connection to nzcon 
(select t1.BASEL_ACCT_ID, 
          t1.MTH_TM_ID, 
		  t2.TM_LVL_END_DT as PROCESS_DATE, 
          t1.BASEL_SEG_ID, 
          t1.BASEL_MODEL_ID
      FROM &RRAP_DB..BASEL_PNL_LN_LGD_SEG_ACCT_XREF t1 Inner join &RRAP_DB..TM_DIM t2 on (t1.MTH_TM_ID=t2.TM_ID)
      WHERE  t1.BASEL_MODEL_ID = &LGD_BASEL_MODEL_ID and t2.TM_LVL='Month' and t2.MTH_CLNDR_CD = %bquote('&month')
	  and t1.MTH_TM_ID>=&TM_ID_START);
	  Disconnect from nzcon;
QUIT;


PROC SQL;
   CREATE TABLE WORK.NETEZZA_COSTS AS 
   SELECT unique t1.MTH_TM_ID, 
          t1.BASEL_ACCT_ID, 
          sum(t1.TOT_COST_EXPNS_AMT) as TOT_COST_EXPNS_AMT, 
		  t1.PGM_TP_CD,
          t1.HGHWY_CD,
		  t1.SRC_SYS_CD
      FROM NZRRAP.ASST_DRC_COST_MTH_SNAPSHOT t1
	  where t1.BASEL_ACCT_ID<>-1 and t1.SRC_SYS_CD='SPL' and t1.HGHWY_CD <> 'RH' and t1.MTH_TM_ID>=&TM_ID_START
	  group by t1.basel_acct_id,PGM_TP_CD,MTH_TM_ID
	  order by t1.basel_acct_id,PGM_TP_CD,MTH_TM_ID;
QUIT;

data work.netezza_costs;
	set work.netezza_costs;
	by basel_acct_id PGM_TP_CD;
	lag_cost=lag(TOT_COST_EXPNS_AMT);
	if first.PGM_TP_CD then do;
		DATE_HGHWY_COST=TOT_COST_EXPNS_AMT;
	end;
	else DATE_HGHWY_COST=TOT_COST_EXPNS_AMT-lag_cost;
	drop lag_cost;
run;


proc sql;
	create table netezza_dir_costs as
		select t1.BASEL_ACCT_ID, 
			t1.MTH_TM_ID, 
			t2.TM_LVL_END_DT as COST_DATE,
			sum(t1.DATE_HGHWY_COST) as costs
		from work.netezza_costs t1
			left join NZRRAP.tm_DIM t2
				on t1.MTH_TM_ID=t2.TM_ID
	where t1.DATE_HGHWY_COST>0
	group by t1.BASEL_ACCT_ID,t1.MTH_TM_ID, t2.TM_LVL_END_DT;
quit;

PROC SQL;
   CREATE TABLE WORK.netezza_RCVRY AS 
   SELECT t1.MTH_TM_ID, 
          t1.BASEL_ACCT_ID, 
          t1.TOT_RCVRY_AMT, 
		  t1.PGM_TP_CD,
          t1.HGHWY_CD,
		  t1.SRC_SYS_CD
      FROM NZRRAP.ASST_DRC_COST_MTH_SNAPSHOT t1
      where t1.BASEL_ACCT_ID<>-1 and t1.SRC_SYS_CD='SPL' and t1.HGHWY_CD <> 'RH' and t1.MTH_TM_ID>=&TM_ID_START
	  order by t1.basel_acct_id,PGM_TP_CD,MTH_TM_ID,  TOT_RCVRY_AMT desc;
QUIT;

data netezza_RCVRY2;
  set netezza_RCVRY;
  by basel_acct_id PGM_TP_CD MTH_TM_ID;
  if first.MTH_TM_ID then output netezza_RCVRY2;
run;

/*NOTE: There were 7684724 observations read from the data set WORK.NETEZZA_RCVRY.*/
/*NOTE: The data set WORK.NETEZZA_RCVRY2 has 7418033 observations and 6 variables.*/

data work.netezza_RCVRY3;
	set work.netezza_RCVRY2;
	by basel_acct_id PGM_TP_CD;
	lag_RCVRY=lag(TOT_RCVRY_AMT);
	if first.PGM_TP_CD then do;
		DATE_HGHWY_RCVRY=TOT_RCVRY_AMT;
	end;
	else DATE_HGHWY_RCVRY=TOT_RCVRY_AMT-lag_RCVRY;
	drop lag_RCVRY;
run;


proc sql;
	create table netezza_RECVY_final as
		select t1.BASEL_ACCT_ID, 
			t1.MTH_TM_ID, 
			t2.TM_LVL_END_DT as RECVY_DATE,
			sum(t1.DATE_HGHWY_RCVRY) as netezza_RECVY
		from work.netezza_RCVRY3 t1
			left join NZRRAP.tm_DIM t2
				on t1.MTH_TM_ID=t2.TM_ID
	where t1.DATE_HGHWY_RCVRY>0
	group by t1.BASEL_ACCT_ID,t1.MTH_TM_ID, t2.TM_LVL_END_DT;
quit;
/*NOTE: Table WORK.NETEZZA_RECVY_FINAL created, with 372861 rows and 4 columns. */


PROC SQL;
CONNECT USING NZRRAP as nzcon;
   CREATE TABLE ACCT_DFLT_DATES1 AS 
   SELECT BASEL_ACCT_ID,
   		LAST_NEW_DFT_DT,
		LAST_NEW_DFT_BAL_AMT,
		RCVRY_WINDOW_CUTOFF_DT
From connection to nzcon 
(select *
      FROM &RRAP_DB..PSNL_LOAN_OBSVTN_PT_DRVD_VAR
	  where OBSVTN_MTH_TM_ID >= &TM_ID_START
);
	  Disconnect from nzcon;
QUIT;

data ACCT_DFLT_DATES1;
	set ACCT_DFLT_DATES1;
	RCVRY_WINDOW_CUTOFF_DT=intnx('month',LAST_NEW_DFT_DT,&RCVY_TIMEFRAME,'end');
run;

proc sort data=ACCT_DFLT_DATES1 nodupkey;
	by BASEL_ACCT_ID LAST_NEW_DFT_DT;
run; 

proc transpose 	data=ACCT_DFLT_DATES1
				out= TRANSPOSED_DFLT_DATES
				prefix=LAST_NEW_DFT_DT;
	var LAST_NEW_DFT_DT;
	by BASEL_ACCT_ID;
run;

proc transpose 	data=ACCT_DFLT_DATES1
				out= TRANSPOSED_DFLT_AMT
				prefix=LAST_NEW_DFT_BAL_AMT;
	var LAST_NEW_DFT_BAL_AMT;
	by BASEL_ACCT_ID;
run;


proc transpose 	data=ACCT_DFLT_DATES1
				out= TRANSPOSED_RCVRY_CUTOFF_DT
				prefix=RCVRY_WINDOW_CUTOFF_DT;
	var RCVRY_WINDOW_CUTOFF_DT;
	by BASEL_ACCT_ID;
run;

data ACCT_DFLT_DATES_LGD;
	merge TRANSPOSED_DFLT_DATES TRANSPOSED_DFLT_AMT TRANSPOSED_RCVRY_CUTOFF_DT;
	drop _LABEL_ _NAME_;
run;



*Get Default dates for accounts;
*Only look at accounts that have defaulted and the right product;

proc sql;
   	create table LGD_ACCOUNTS as 
	select *
	from &LGD_DATA. t1 
	Left join ACCT_DFLT_DATES_LGD  t2
	on t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID
	where t2.last_new_dft_dt1>0 and t1.basel_model_id=&LGD_BASEL_MODEL_ID and process_date<=&cutoff;
quit;

*Find the latest default date within a year of the process date;
*Get the default date, balance and recovery date;
Data &PROD_LIB..LGD12_DFLTS;
	set LGD_ACCOUNTS;
	if BASEL_SEG_ID NOT in &exclusions;
	format DFLT_DT date9.;
	format DFLT_RCVRY date9.;
	ARRAY DFLTS(*) LAST_NEW_DFT_DT: ;
	ARRAY DFLTS_BAL(*) LAST_NEW_DFT_BAL_AMT: ;
	ARRAY DFLTS_RCVRY(*) RCVRY_WINDOW_CUTOFF_DT: ;
	do i = DIM(DFLTS) to 1 by -1;
		IF INTCK('MONTH',PROCESS_DATE,DFLTS[i])<=12 
		and INTCK('DAY',PROCESS_DATE,DFLTS[i]) >=0 
		and DFLTS_RCVRY[i] <=&END_DATE then do;
			DFLT_DT=DFLTS[i];
			DFLT_BAL=DFLTs_BAL[i];
			DFLT_RCVRY=DFLTS_RCVRY[i];
			output;
			leave;
		end;
	end;
	keep 	BASEL_ACCT_ID Process_DATE MTH_TM_ID BASEL_MODEL_ID BASEL_SEG_ID
			DFLT_DT DFLT_BAL DFLT_RCVRY;
run;

PROC SQL;
   CREATE TABLE WORK.DT_TM AS 
   SELECT DISTINCT t1.PROCESS_DATE
      FROM NZRRAP.TERM_MONTH_RCVRY t1;
QUIT;

*Get Recovery Dates;
proc sql;
	create table &PROD_LIB..ACCT_24_MTH_RCVY as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t2.PROCESS_DATE as RCVY_DT format=date9.
	from &PROD_LIB..LGD12_DFLTS t1
		left join WORK.DT_TM t2 
		ON 	t2.PROCESS_DATE BETWEEN (t1.DFLT_DT+1) AND (t1.DFLT_RCVRY+1);
quit;

proc delete data=DT_TM; run;

*Find all recovery data within 24 months of the default date;
proc sql;
	create table &PROD_LIB..ACCT_24_MTH_RCVY as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t1.RCVY_DT, 
          t2.MTH_RCVRY_AMT,
		  t2.STATUS1 as ACCT_STAT
	from &PROD_LIB..ACCT_24_MTH_RCVY t1
		left join NZRRAP.TERM_MONTH_RCVRY (RENAME=(PROCESS_DATE=RCVY_DT))t2 
		ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
		and t1.RCVY_DT=t2.RCVY_DT
;
quit;

proc sql;
	create table &PROD_LIB..ACCT_24_MTH_RCVY as 
	select t1.*,
		  t2.netezza_RECVY
	from &PROD_LIB..ACCT_24_MTH_RCVY t1
        left join netezza_RECVY_final t2
	  	ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
		and t1.RCVY_DT=t2.RECVY_DATE;
quit;

*sort and transpose Recovery amount, recovery date, and account status;
proc sort data=&PROD_LIB..ACCT_24_MTH_RCVY;
	by BASEL_ACCT_ID PROCESS_DATE RCVY_DT;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_MTH_RCVY_TRANSPOSE
				prefix=MTH_RCVRY_AMT;
	var MTH_RCVRY_AMT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_MTH_RCVY_DATE_TRANSPOSE
				prefix=MTH_RCVRY_DATE;
	var RCVY_DT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var ACCT_STAT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=&PROD_LIB..ACCT_24_MTH_RCVY
				out= ACCT_24_MTH_nz_RCVY_TRANSPOSE
				prefix=MTH_netezza_RCVRY_AMT;
	var netezza_RECVY;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

data LGD_RCVY_DATA;
	merge ACCT_24_MTH_RCVY_TRANSPOSE ACCT_24_MTH_RCVY_DATE_TRANSPOSE ACCT_24_STAT_TRANSPOSE ACCT_24_MTH_nz_RCVY_TRANSPOSE;
	by BASEL_ACCT_ID PROCESS_DATE;
	drop _name_ _label_;
run;

*Merge transposed recovery data back onto the table with the rest of the necessary info;
proc sql;
	create table intmed.&PROD._LGD12_RECVY_DATA as
	SELECT *
	from &PROD_LIB..LGD12_DFLTS as t1 INNER JOIN LGD_RCVY_DATA as t2
		ON t1.BASEL_ACCT_ID = t2.BASEL_ACCT_ID AND t1.PROCESS_DATE = t2.PROCESS_DATE;
quit;



PROC SQL;
   CREATE TABLE WORK.DT_TM AS 
   SELECT DISTINCT t1.PROCESS_DATE
      FROM NZRRAP.TERM_MONTH_RCVRY t1;
QUIT;

*Find costs data within 24 months of default;
proc sql;
	create table ACCT_24_MTH_COSTS_BASE as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t2.PROCESS_DATE as COST_DATE format=date9.,
		  t1.DFLT_DT
	from &PROD_LIB..LGD12_DFLTS t1
		left join WORK.DT_TM t2 
		ON 	t2.PROCESS_DATE BETWEEN (t1.DFLT_DT+1) AND (t1.DFLT_RCVRY+1);
quit;

proc delete data=DT_TM; run;

/*proc sql;*/
/*	create table IND_COST as*/
/*	select t2.TM_LVL_END_DT as PROCESS_DATE,*/
/*			(case when t1.MTH_TM_ID=16756 then 6.08 else t1.UNIT_INDRCT_COST_AMT end) as UNIT_COST*/
/*	from NZRRAP.DRVD_MTH_INDRCT_COST_SUM t1*/
/*	left join NZRRAP.TM_DIM t2 on (t1.MTH_TM_ID=t2.TM_ID)*/
/*		order by t1.MTH_TM_ID;*/
/*quit;*/
*Find indirect costs data within 24 months of default;
proc sql;
	create table ACCT_24_MTH_COSTS_BASE as 
	select t1.BASEL_ACCT_ID, 
          t1.PROCESS_DATE, 
		  t1.COST_DATE,
		  t2.UNIT_COST as ind_cost,
		  t1.DFLT_DT
	from ACCT_24_MTH_COSTS_BASE t1
		left join NZFRG.ind_cost t2 
		ON 	t2.PROCESS_DATE = t1.COST_DATE;
quit;

*Find costs data within 24 months of default;
proc sql;
	create table ACCT_24_MTH_COSTS_BASE as 
		select t1.BASEL_ACCT_ID, 
			t1.PROCESS_DATE, 
			t2.COSTS,
			t1.ind_cost,
			t1.COST_DATE,
			t1.DFLT_DT
		from ACCT_24_MTH_COSTS_BASE t1
			left join netezza_dir_costs t2 
				ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
				and t2.cost_date=t1.cost_date;
quit;

*Find Account Status;
proc sql;
	create table ACCT_24_MTH_COSTS as 
		select 	t1.BASEL_ACCT_ID, 
			t1.PROCESS_DATE, 
			t1.COSTS,
			t1.ind_cost,
			t1.COST_DATE,
			t2.ACCT_STAT,
			t1.DFLT_DT
		from ACCT_24_MTH_COSTS_BASE t1
			left join &PROD_LIB..ACCT_24_MTH_RCVY t2 
				ON 	t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
				AND t2.RCVY_DT=t1.COST_DATE 
				AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

data ACCT_24_MTH_COSTS;
	set ACCT_24_MTH_COSTS;
	if DFLT_DT=COST_DATE then ACCT_STAT='DEF';
run;

data ACCT_24_MTH_COSTS;
	set ACCT_24_MTH_COSTS;
	if (DFLT_DT=COST_DATE or ACCT_STAT ne 'DEF') then ind_cost=0;
run;

*Sort and transpose data on Costs, costs date and account status;
proc sort data=ACCT_24_MTH_COSTS;
	by BASEL_ACCT_ID PROCESS_DATE COST_DATE;
run;

proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_COSTS_TRANSPOSE
				prefix=COSTS;
	var COSTS;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_ind_cost_TRANSPOSE
				prefix=ind_cost;
	var ind_cost;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_COSTS_DATE_TRANSPOSE
				prefix=COST_DATE;
	var COST_DATE;
	by BASEL_ACCT_ID PROCESS_DATE;
run;


proc transpose 	data=ACCT_24_MTH_COSTS
				out= ACCT_24_MTH_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var ACCT_STAT;
	by BASEL_ACCT_ID PROCESS_DATE;
run;

data LGD_COSTS_DATA;
	merge ACCT_24_MTH_COSTS_TRANSPOSE ACCT_24_MTH_ind_cost_TRANSPOSE ACCT_24_MTH_COSTS_DATE_TRANSPOSE ACCT_24_MTH_STAT_TRANSPOSE;
	by BASEL_ACCT_ID PROCESS_DATE;
	drop _name_ _label_;
run;

*Join costs data onto rest of the necessary data for accounts;
proc sql;
	create table INTMED.&PROD._LGD12_COST_DATA as
	SELECT *
	from &PROD_LIB..LGD12_DFLTS t1 INNER JOIN LGD_COSTS_DATA t2
		ON t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

*Discount recovery data;

data DISCOUNT_RECVY;
	set INTMED.&PROD._LGD12_RECVY_DATA;
	ARRAY RCVY_AMT[*] MTH_RCVRY_AMT: ;
	ARRAY DATES[*] MTH_RCVRY_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	ARRAY RCVY_AMT_netezza [*] MTH_netezza_RCVRY_AMT: ; 

	do i= 1 to dim(RCVY_AMT);
		if i=1 then do; total_RECY=0 ; total_RECY_netezza=0; end;
		*Calculate total recovery with positive numbers;
		if RCVY_AMT(i) >0 and RCVY_AMT(i) ne . then total_RECY = total_RECY+RCVY_AMT(i); 
        if i > 1 and RCVY_AMT_netezza(i) ne . then total_RECY_netezza = total_RECY_netezza+RCVY_AMT_netezza(i);
		*IF status is cured then no more recoveries, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;

    *Discount the recovery surplus for 24 months;
	if total_RECY_netezza > total_RECY then recy_surplus = total_RECY_netezza - total_RECY;
    DISCT_RECY_surplus = recy_surplus/(1+&CC_DISCOUNT)**2;
    if DISCT_RECY_surplus = . then DISCT_RECY_surplus = 0;


	do i= 1 to dim(RCVY_AMT);
		if i=1 then DISCT_RECY=0;
		RCVY_AMT(i)=RCVY_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',DFLT_DT,DATES(i))/12));
		if RCVY_AMT(i) ne . then DISCT_RECY = DISCT_RECY+RCVY_AMT(i);
		*IF status is cured then no more recoveries, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if dflt_rcvry<=&end_date;
	drop _label_ _name_ i MTH_RCVRY_AMT: MTH_RCVRY_DATE: ACCT_STAT: ;*;
run;

*discount Costs data;
data DISCOUNT_COSTS;
	set INTMED.&PROD._LGD12_COST_DATA;
	ARRAY COST_AMT[*] COSTS: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',DFLT_DT,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_COSTS = DISCT_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if DISCT_COSTS=. then DISCT_COSTS=0;
	if dflt_rcvry<=&end_date;
	drop _label_ _name_ i COSTS: ind_cost: COST_DATE: ACCT_STAT:;
run;

data DISCOUNT_IND_COSTS;
	set INTMED.&PROD._LGD12_COST_DATA;
	ARRAY COST_AMT[*] ind_cost: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_IND_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',DFLT_DT,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_IND_COSTS = DISCT_IND_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if DISCT_IND_COSTS=. then DISCT_IND_COSTS=0;
	if dflt_rcvry<=&end_date;
	drop _label_ _name_ i ind_cost: COSTS: COST_DATE: ACCT_STAT:;
run;

*Join discounted costs and recovery to obtain LGD;
proc sql;
	create table &PROD_LIB..LGD12_DATA as 
		select t1.*,
			t2.DISCT_COSTS,
			t3.DISCT_IND_COSTS,
			(t1.DFLT_BAL-t1.DISCT_RECY-t1.DISCT_RECY_surplus)/t1.DFLT_BAL as LGD_NO_COST,
			(t1.DFLT_BAL-t1.DISCT_RECY-t1.DISCT_RECY_surplus+t2.DISCT_COSTS)/t1.DFLT_BAL as LGD_DRC_COST,
			(t1.DFLT_BAL-t1.DISCT_RECY-t1.DISCT_RECY_surplus+t2.DISCT_COSTS+t3.DISCT_IND_COSTS)/t1.DFLT_BAL as LGDND_ALL_COST_MOD,
			(t1.DFLT_BAL-t1.DISCT_RECY+t3.DISCT_IND_COSTS-t1.DISCT_RECY_surplus)/t1.DFLT_BAL as LGDD_INDRC_COST
		from DISCOUNT_RECVY t1 
			full join DISCOUNT_COSTS t2 
				on t1.BASEL_ACCT_ID=t2.BASEL_ACCT_ID 
				AND t1.PROCESS_DATE=t2.PROCESS_DATE
			full join DISCOUNT_IND_COSTS t3
				on t1.BASEL_ACCT_ID=t3.BASEL_ACCT_ID 
				AND t1.PROCESS_DATE=t3.PROCESS_DATE;
quit;

data &PROD_LIB..LGD12_DATA;
	set &PROD_LIB..LGD12_DATA;
	LGDND_ALL_COST_FLR_CAP125=LGDND_ALL_COST_MOD;
	if LGDND_ALL_COST_FLR_CAP125<0 then LGDND_ALL_COST_FLR_CAP125=0;
	if LGDND_ALL_COST_FLR_CAP125>1.25 then LGDND_ALL_COST_FLR_CAP125=1.25;

	LGDD_ALL_COST_FLR_CAP100=LGDND_ALL_COST_MOD;
	if LGDD_ALL_COST_FLR_CAP100<0 then LGDD_ALL_COST_FLR_CAP100=0;
	if LGDD_ALL_COST_FLR_CAP100>1 then LGDD_ALL_COST_FLR_CAP100=1;

	LGD_NO_COST_CAP=LGD_NO_COST;
	if LGD_NO_COST_CAP<0 then LGD_NO_COST_CAP=0;
	if LGD_NO_COST_CAP>1.25 then LGD_NO_COST_CAP=1.25;
run;


data INTMED.&PROD._LGD_ND_DATA;
	set &PROD_LIB..LGD12_DATA;
	where process_date <= &cutoff;
run;

%if %sysfunc(exist(NZRRAP.SPLD_LGD_ND_UPDATE_INTERIM_&PREV_TM_ID.)) %then %do;
PROC SQL;
	DROP TABLE NZRRAP.SPLD_LGD_ND_UPDATE_INTERIM_&PREV_TM_ID.;
QUIT;
%END;

%if %sysfunc(exist(NZRRAP.SPLD_LGD_ND_UPDATE_INTERIM_&TM_ID.)) %then %do;
PROC SQL;
	DROP TABLE NZRRAP.SPLD_LGD_ND_UPDATE_INTERIM_&TM_ID.;
QUIT;
%END;

proc sql;
	connect using nzrrap as iiascon;
	execute(create table &RRAP_DB..SPLD_LGD_ND_UPDATE_INTERIM_&TM_ID. AS(SELECT * FROM &RRAP_DB..PSNL_LN_LGD_OBSVT_PT_RELZ_VAL WHERE OBSVTN_MTH_TM_ID = &TM_ID AND LGD_TYPE='LGD-ND' AND SUB_PORTFOLIO='DIRECT')WITH DATA ORGANIZE BY ROW)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CONNECT USING NZRRAP AS IIASCON;
	EXECUTE(UPDATE &RRAP_DB..SPLD_LGD_ND_UPDATE_INTERIM_&TM_ID.
			SET 
				LGD_RTO_BKUP = LGD_RTO,
				LGD_NPV_ALL_COST_100_RTO_BKP = LGD_NPV_ALL_COST_FLR_CAP_100_RTO,
				LGD_NPV_ALL_COST_150_RTO_BKP = LGD_NPV_ALL_COST_FLR_CAP_150_RTO,
				LGD_NPV_ALL_COST_RTO_BKUP = LGD_NPV_ALL_COST_RTO,
				LGD_NPV_DRC_COST_RTO_BKUP = LGD_NPV_DRC_COST_RTO,
				LGD_NPV_INDRCT_COST_RTO_BKUP = LGD_NPV_INDRCT_COST_RTO)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	UPDATE NZRRAP.SPLD_LGD_ND_UPDATE_INTERIM_&TM_ID. AS T
	SET LGD_RTO = (SELECT LGD_NO_COST FROM INTMED.&PROD._LGD_ND_DATA AS A
					WHERE T.basel_acct_id = A.basel_acct_id AND T.obsvtn_date = A.process_Date),
		LGD_NPV_ALL_COST_RTO = (SELECT LGDND_ALL_COST_MOD FROM INTMED.&PROD._LGD_ND_DATA AS B
					WHERE T.basel_acct_id = B.basel_acct_id AND T.obsvtn_date = B.process_Date),
		LGD_NPV_DRC_COST_RTO = (SELECT LGD_DRC_COST FROM INTMED.&PROD._LGD_ND_DATA AS C
					WHERE T.basel_acct_id = C.basel_acct_id AND T.obsvtn_date = C.process_Date),
		LGD_NPV_INDRCT_COST_RTO = (SELECT LGDD_INDRC_COST FROM INTMED.&PROD._LGD_ND_DATA AS D
					WHERE T.basel_acct_id = D.basel_acct_id AND T.obsvtn_date = D.process_Date),
		LGD_NO_COST_FLR_CAP_150_RTO = (SELECT LGD_NO_COST_CAP FROM INTMED.&PROD._LGD_ND_DATA AS E
					WHERE T.basel_acct_id = E.basel_acct_id AND T.obsvtn_date = E.process_Date),
		LGD_NPV_ALL_COST_FLR_CAP_150_RTO = (SELECT LGDND_ALL_COST_FLR_CAP125 FROM INTMED.&PROD._LGD_ND_DATA AS F
					WHERE T.basel_acct_id = F.basel_acct_id AND T.obsvtn_date = F.process_Date),
		LGD_NPV_ALL_COST_FLR_CAP_100_RTO = (SELECT LGDD_ALL_COST_FLR_CAP100 FROM INTMED.&PROD._LGD_ND_DATA AS G
					WHERE T.basel_acct_id = G.basel_acct_id AND T.obsvtn_date = G.process_Date)
	WHERE T.basel_acct_id IN (SELECT basel_acct_id FROM INTMED.&PROD._LGD_ND_DATA) AND T.obsvtn_date = "&COMPLETE"D AND T.LGD_TYPE='LGD-ND' AND SUB_PORTFOLIO='DIRECT';
QUIT;

PROC SQL;
	CONNECT USING NZRRAP AS IIASCON;
	EXECUTE(DELETE FROM &RRAP_DB..PSNL_LN_LGD_OBSVT_PT_RELZ_VAL WHERE OBSVTN_MTH_TM_ID = &TM_ID AND LGD_TYPE='LGD-ND' AND SUB_PORTFOLIO='DIRECT')BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

proc append base=NZRRAP.PSNL_LN_LGD_OBSVT_PT_RELZ_VAL(bulkload=yes bl_method=cliload) data=NZRRAP.SPLD_LGD_ND_UPDATE_INTERIM_&TM_ID.;
run;

%mend dtllgdnd;

%get_model_period_dates(product=spl);
%put start_period_dt: &start_period_dt;

data runmonth;
	format start_date end_date date9. start_date2 end_date2 yymmdd10.;
	start_date=intnx('month',"&start_period_dt"d,0,'end');
	end_date=intnx('month',start_date,-36,'end');
	start_date2=start_date;
	end_date2=end_date;
run;

data _null_;
call symputx('month_PARAM',put(intnx('month',"&start_period_dt"d,0,'end'),monname.));
run;

proc sql;
	select start_date into :beginning from runmonth;
	select end_date into :complete from runmonth;
	select start_date2 into :beginning2 from runmonth;
	select end_date2 into :complete2 from runmonth;
	select TM_ID INTO :TM_ID FROM NZRRAP.TM_DIM
	WHERE TM_LVL='Month' and CLNDR_YR = PUT(year("&complete"d),4.) AND MTH_CLNDR_CD="&month_PARAM"; 
quit;

%LET PREV_TM_ID=%EVAL(&TM_ID-40);
%LET TM_ID=%EVAL(&TM_ID);

%put %bquote('&beginning'd);
%put %bquote('&complete'd);
%PUT %BQUOTE('&BEGINNING2');
%put %bquote('&complete2');
%PUT &month_PARAM;
%PUT &TM_ID;
%PUT &PREV_TM_ID;

%itllgdnd("&beginning"d,"&complete"d,&month_PARAM)
%dtllgdnd("&beginning"d,"&complete"d,&month_PARAM)