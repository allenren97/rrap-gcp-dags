
%rrap_mor_bns_autoexec

%macro lgdd_new(end,cut,DEL_RECS);

%let PROD_LIB=work;
%let LGD_DATA=MOR_LGD_D_SEG;
%let LGD_NAME=MOR_LGD_D_SEG;

%let PROD=BNSMOR;

%let START_TM_ID=10996;
%let END_DATE=&end;

%let CC_DISCOUNT=0.1;
%let RES_DISCOUNT=0.05;

%let cutoff=&cut;

%let RCVY_TIMEFRAME=24;
%let RES_RCVY_TIMEFRAME=60;

%let month= %sysfunc(month(&END_DATE));

PROC SQL;
CONNECT USING NZUSER as nzcon;
   CREATE TABLE LGD_DATA AS 
   SELECT *
From connection to nzcon 
(select t1.MORTGAGE_NO, 
		  t1.TIME_KEY as PROCESS_DATE, 
          t1.BNS_LGD_D_SEGMENT,
		  t1.insurance as LGDD_INSURANCE,
		  t2.TOTAL_BAL as OBS_BAL_AMT
      FROM &FRG_DB..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ t1
	  left join &FRG_DB..BNSMORT_ACCT_LVL_DATA t2
	  on t1.MORTGAGE_NO=t2.MORTGAGE_NO
	  and t1.TIME_KEY=t2.PROCESS_DATE
	  where date_part('Month',t1.TIME_KEY) in (&month)
);
	  Disconnect from nzcon;
QUIT;

PROC SQL;
   CREATE TABLE WORK.NETEZZA_COSTS AS 
   SELECT unique t1.MTH_TM_ID, 
          input(substr(t1.ACCT_NUM,13),21.) as MORTGAGE_NO,
          Compress(Catx(SRC_SYS_CD,PGM_TP_CD,put(calculated mortgage_no,13.),put(datepart(ASGNMN_DT),6.))) as REC_ID,
		  sum(t1.TOT_COST_EXPNS_AMT) as TOT_COST_EXPNS_AMT, 
          t1.ASGNMN_DT, 
          t1.PGM_TP_CD,
          t1.HGHWY_CD,
          t1.SRC_SYS_CD
      FROM NZRRAP.ASST_DRC_COST_MTH_SNAPSHOT t1
      where t1.SRC_SYS_CD in ('MOR','STEP') 
		and t1.BASEL_ACCT_ID<>-1 
		and t1.MTH_TM_ID>=&START_TM_ID
	group by Calculated REC_ID,MTH_TM_ID
	order by Calculated REC_ID,MTH_TM_ID;
QUIT;

data work.netezza_costs;
     set work.netezza_costs;
     by REC_ID mth_tm_id;
     lag_cost=lag(TOT_COST_EXPNS_AMT);
     if First.REC_ID then do;
           DATE_HGHWY_COST=TOT_COST_EXPNS_AMT;
     end;
     else DATE_HGHWY_COST=TOT_COST_EXPNS_AMT-lag_cost;
     drop lag_cost;
run;

proc sql;
     create table netezza_dir_costs as
           select t1.MORTGAGE_NO, 
                t1.MTH_TM_ID, 
                t2.TM_LVL_END_DT as COST_DATE,
                sum(t1.DATE_HGHWY_COST) as costs
           from work.netezza_costs t1
                left join NZRRAP.tm_DIM t2
                     on t1.MTH_TM_ID=t2.TM_ID
     where t1.DATE_HGHWY_COST>0
     group by t1.MORTGAGE_NO, t1.MTH_TM_ID, t2.TM_LVL_END_DT;
quit;

PROC SQL;
   CREATE TABLE WORK.netezza_RCVRY AS 
   SELECT t1.MTH_TM_ID, 
           	input(substr(t1.ACCT_NUM,13),21.) as MORTGAGE_NO,        
           	t1.PGM_TP_CD,
		   	Compress(Catx(SRC_SYS_CD,PGM_TP_CD,put(calculated mortgage_no,13.),put(datepart(ASGNMN_DT),6.))) as rec_id,
			t1.BASEL_ACCT_ID,
           	t1.TOT_RCVRY_AMT,
          	t1.HGHWY_CD,
            t1.SRC_SYS_CD
    FROM NZRRAP.ASST_DRC_COST_MTH_SNAPSHOT t1
    where t1.SRC_SYS_CD in ('MOR','STEP') 
		and t1.BASEL_ACCT_ID<>-1 
		and t1.MTH_TM_ID>=&START_TM_ID
	order by Calculated REC_ID, MTH_TM_ID, TOT_RCVRY_AMT desc;       
QUIT;

data netezza_RCVRY2;
  set netezza_RCVRY;
  by REC_ID MTH_TM_ID;
  if first.MTH_TM_ID then output netezza_RCVRY2;
run;

data work.netezza_RCVRY3;
	set work.netezza_RCVRY2;
	by REC_ID MTH_TM_ID;
	lag_RCVRY=lag(TOT_RCVRY_AMT);
	if first.REC_ID then do;
		DATE_HGHWY_RCVRY=TOT_RCVRY_AMT;
	end;
	else DATE_HGHWY_RCVRY=TOT_RCVRY_AMT-lag_RCVRY;
	drop lag_RCVRY;
run;

proc sql;
	create table netezza_RECVY_final as
		select t1.MORTGAGE_NO,
			t1.BASEL_ACCT_ID, 
			t1.rec_id,
			t1.MTH_TM_ID, 
			t2.TM_LVL_END_DT as RECVY_DATE,
			sum(t1.DATE_HGHWY_RCVRY) as netezza_RECVY
		from work.netezza_RCVRY3 t1
			left join NZRRAP.tm_DIM t2
				on t1.MTH_TM_ID=t2.TM_ID
	where t1.DATE_HGHWY_RCVRY>0
	group by t1.MORTGAGE_NO,t1.MTH_TM_ID, t2.TM_LVL_END_DT;
quit;

proc sql;
	create table DEFAULT_EVENTS as
	SELECT t1.MORTGAGE_NO, 
          t1.PROCESS_DATE, 
          t1.WINDOW_END_DT, 
          t1.STATUS1, 
          t1.DEFAULT_DATE, 
          t1.DEFAULT_BAL, 
          t1.DEFAULT_IND
      FROM NZUSER.SCORED_SEGMENTED_ACCTS_ANTQ t1
      ORDER BY t1.MORTGAGE_NO,
               t1.PROCESS_DATE;
quit;

data EARLY_DEF_EVENTS;
	set WORK.DEFAULT_EVENTS;
	BY MORTGAGE_NO STATUS1 NOTSORTED;
	RCVRY_WINDOW_CUTOFF_DT=intnx('month',PROCESS_DATE,&RCVY_TIMEFRAME,'E');
	if first.status1 and status1='DEF' then output;
run;

proc transpose 	data=EARLY_DEF_EVENTS
				out= TRANSPOSED_DFLT_DATES
				prefix=LAST_NEW_DFT_DT;
	var PROCESS_DATE;
	by Mortgage_no;
run;

proc transpose 	data=EARLY_DEF_EVENTS
				out= TRANSPOSED_DFLT_AMT
				prefix=LAST_NEW_DFT_BAL_AMT;
	var DEFAULT_BAL;
	by Mortgage_no;
run;

proc transpose 	data=EARLY_DEF_EVENTS
				out= TRANSPOSED_RCVRY_CUTOFF_DT
				prefix=RCVRY_WINDOW_CUTOFF_DT;
	var RCVRY_WINDOW_CUTOFF_DT;
	by Mortgage_no;
run;

data ACCT_DFLT_DATES_LGDD;
	merge TRANSPOSED_DFLT_DATES TRANSPOSED_DFLT_AMT TRANSPOSED_RCVRY_CUTOFF_DT;
	drop _LABEL_ _NAME_;
run;

proc sql;
   	create table LGD_ACCOUNTS as 
	select *
	from LGD_DATA t1 
	Left join ACCT_DFLT_DATES_LGDD  t2
	on t1.MORTGAGE_NO=t2.Mortgage_no
	where t2.last_new_dft_dt1>0 and process_date<="&complete"d;
quit;

Data work.LGD_D_DFLTS;
	set LGD_ACCOUNTS;
	format DFLT_DT date9.;
	format DFLT_RCVRY date9.;
	ARRAY DFLTS(*) LAST_NEW_DFT_DT: ;
	ARRAY DFLTS_RCVRY(*) RCVRY_WINDOW_CUTOFF_DT: ;
	do i = DIM(DFLTS) to 1 by -1;
		IF INTCK('DAY',PROCESS_DATE,DFLTS_RCVRY[i]) >0 
		and INTCK('DAY',PROCESS_DATE,DFLTS[i]) <=0 
		and DFLTS_RCVRY[i] <="&beginning"d then do;
			DFLT_DT=DFLTS[i];
			DFLT_RCVRY=DFLTS_RCVRY[i];
			output;
			leave;
		end;
	end;
	keep 	MORTGAGE_NO Process_DATE MTH_TM_ID  
			DFLT_DT DFLT_RCVRY OBS_BAL_AMT BNS_LGD_D_SEGMENT LGDD_INSURANCE;  
run;

PROC SQL;
   CREATE TABLE WORK.DT_TM AS 
   SELECT DISTINCT t1.PROCESS_DATE
      FROM NZUSER.BNSMORT_MONTH_RCVRY_LGDD t1;
QUIT;

proc sql;
	create table WORK.ACCT_24_MTH_RCVY_LGD_D as 
	select t1.MORTGAGE_NO, 
          t1.PROCESS_DATE, 
		  t1.DFLT_DT, 
		  t2.PROCESS_DATE as RCVY_DT format=date9.
	from WORK.LGD_D_DFLTS t1
		left join WORK.DT_TM t2 
		ON 	t2.PROCESS_DATE BETWEEN (t1.PROCESS_DATE+1) AND (t1.DFLT_RCVRY+1);
quit;

proc sql;
	create table work.ACCT_24_MTH_RCVY_LGD_D as 
	select t1.MORTGAGE_NO, 
          t1.PROCESS_DATE, 
		  t1.DFLT_DT, 
          t2.MTH_RCVRY_AMT,
		  t1.RCVY_DT, 
		  t2.STATUS1,
		  t3.netezza_RECVY

	from WORK.ACCT_24_MTH_RCVY_LGD_D t1
	left join NZUSER.BNSMORT_MONTH_RCVRY_LGDD t2 
		ON 	t1.MORTGAGE_NO=t2.MORTGAGE_NO 
		and t1.RCVY_DT=t2.PROCESS_DATE
	 left join netezza_RECVY_final t3
	  	ON 	t1.MORTGAGE_NO=t3.MORTGAGE_NO 
		and t1.RCVY_DT=t3.RECVY_DATE;
quit;

proc sort data=work.ACCT_24_MTH_RCVY_LGD_D;
	by MORTGAGE_NO PROCESS_DATE RCVY_DT;
run;

proc transpose 	data=work.ACCT_24_MTH_RCVY_LGD_D
				out= ACCT_24_MTH_RCVY_TRANSPOSE
				prefix=MTH_RCVRY_AMT;
	var MTH_RCVRY_AMT;
	by MORTGAGE_NO PROCESS_DATE;
run;

proc transpose 	data=work.ACCT_24_MTH_RCVY_LGD_D
				out= ACCT_24_MTH_RCVY_DATE_TRANSPOSE
				prefix=MTH_RCVRY_DATE;
	var RCVY_DT;
	by MORTGAGE_NO PROCESS_DATE;
run;

proc transpose 	data=work.ACCT_24_MTH_RCVY_LGD_D
				out= ACCT_24_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var STATUS1;
	by MORTGAGE_NO PROCESS_DATE;
run;

proc transpose 	data=work.ACCT_24_MTH_RCVY_LGD_D
				out= ACCT_24_MTH_nz_RCVY_TRANSPOSE
				prefix=MTH_netezza_RECVY_AMT;
	var netezza_RECVY;
	by MORTGAGE_NO PROCESS_DATE;
run;

data LGD_RCVY_DATA;
	merge ACCT_24_MTH_RCVY_TRANSPOSE ACCT_24_MTH_RCVY_DATE_TRANSPOSE ACCT_24_STAT_TRANSPOSE ACCT_24_MTH_nz_RCVY_TRANSPOSE;
	by MORTGAGE_NO PROCESS_DATE;
	drop _name_ _label_;
run;

proc sql;
	create table work._LGD_D_RECVY_DATA as
	SELECT *
	from LGD_D_DFLTS t1 INNER JOIN LGD_RCVY_DATA t2
		ON t1.MORTGAGE_NO=t2.MORTGAGE_NO AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

proc sql;
	create table ACCT_24_MTH_NEW_COSTS_BASE as 
	select t1.MORTGAGE_NO, 
          t1.PROCESS_DATE, 
		  	t1.DFLT_DT,
          t1.MTH_RCVRY_AMT,
		  t1.RCVY_DT as COST_DATE, 
		  t1.STATUS1
	from WORK.ACCT_24_MTH_RCVY_LGD_D t1;
quit;

proc sql;
	create table ACCT_24_MTH_NEW_COSTS_BASE as 
	select t1.MORTGAGE_NO, 
          t1.PROCESS_DATE, 
		  	t1.DFLT_DT,
          t1.MTH_RCVRY_AMT,
		  t1.COST_DATE, 
		  t1.STATUS1,
		  t2.UNIT_COST as ind_cost
	from ACCT_24_MTH_NEW_COSTS_BASE t1
		left join NZUSER.IND_COST t2 
		ON 	t2.PROCESS_DATE = t1.COST_DATE;
quit;

proc sql;
	create table ACCT_24_MTH_NEW_COSTS_BASE as 
		select t1.mortgage_no, 
			t1.PROCESS_DATE, 
			 t1.MTH_RCVRY_AMT,
			t2.COSTS,
			t1.ind_cost,
			t1.STATUS1,
			t1.COST_DATE,
			t1.DFLT_DT
		from ACCT_24_MTH_NEW_COSTS_BASE t1
			left join netezza_dir_costs t2 
				ON 	t1.mortgage_no=t2.mortgage_no 
				and t2.cost_date=t1.cost_date;
quit;

data ACCT_24_MTH_NEW_COSTS_BASE_1;
	set ACCT_24_MTH_NEW_COSTS_BASE;
	if DFLT_DT=COST_DATE then STATUS1='DEF';
	if MTH_RCVRY_AMT<0 and MTH_RCVRY_AMT ne . then costs=0;
run;

data ACCT_24_MTH_NEW_COSTS;
	set ACCT_24_MTH_NEW_COSTS_BASE_1;
	if (DFLT_DT=COST_DATE or STATUS1 ne 'DEF') then ind_cost=0;
run;

*Sort and transpose data on Costs, costs date and account status;
proc sort data=ACCT_24_MTH_NEW_COSTS;
	by mortgage_no PROCESS_DATE COST_DATE;
run;

proc transpose 	data=ACCT_24_MTH_NEW_COSTS
				out= ACCT_24_MTH_COSTS_TRANSPOSE
				prefix=COSTS;
	var COSTS;
	by mortgage_no PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_NEW_COSTS
				out= ACCT_24_MTH_ind_cost_TRANSPOSE
				prefix=ind_cost;
	var ind_cost;
	by mortgage_no PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_NEW_COSTS
				out= ACCT_24_MTH_COSTS_DATE_TRANSPOSE
				prefix=COST_DATE;
	var COST_DATE;
	by mortgage_no PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_NEW_COSTS
				out= ACCT_24_MTH_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var STATUS1;
	by mortgage_no PROCESS_DATE;
run;

data LGD_NEW_COSTS_DATA;
	merge ACCT_24_MTH_COSTS_TRANSPOSE ACCT_24_MTH_ind_cost_TRANSPOSE ACCT_24_MTH_COSTS_DATE_TRANSPOSE ACCT_24_MTH_STAT_TRANSPOSE;
	by mortgage_no PROCESS_DATE;
	drop _name_ _label_;
run;

proc sql;
	create table WORK._LGD_D_NEW_COST_DATA as
	SELECT *
	from WORK.LGD_D_DFLTS t1 INNER JOIN LGD_NEW_COSTS_DATA t2
		ON t1.mortgage_no=t2.mortgage_no AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

data ACCT_24_MTH_DUP_COSTS;
	set ACCT_24_MTH_NEW_COSTS_BASE;
	if DFLT_DT=COST_DATE then STATUS1='DEF';
	if MTH_RCVRY_AMT<0 and MTH_RCVRY_AMT ne . then DUP_COSTS=costs;
run;

data ACCT_24_MTH_DUP_COSTS;
	set ACCT_24_MTH_DUP_COSTS;
	if (DFLT_DT=COST_DATE or STATUS1 ne 'DEF') then ind_cost=0;
run;

*Sort and transpose data on Costs, costs date and account status;
proc sort data=ACCT_24_MTH_DUP_COSTS;
	by mortgage_no PROCESS_DATE COST_DATE;
run;

proc transpose 	data=ACCT_24_MTH_DUP_COSTS
				out= ACCT_24_MTH_D_COSTS_TRANSPOSE
				prefix=DUP_COSTS;
	var DUP_COSTS;
	by mortgage_no PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_DUP_COSTS
				out= ACCT_24_MTH_D_ind_cost_TRANSPOSE
				prefix=ind_cost;
	var ind_cost;
	by mortgage_no PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_DUP_COSTS
				out= ACCT_24_MTH_DCOST_DAT_TRANSPOSE
				prefix=COST_DATE;
	var COST_DATE;
	by mortgage_no PROCESS_DATE;
run;

proc transpose 	data=ACCT_24_MTH_DUP_COSTS
				out= ACCT_24_MTH_D_STAT_TRANSPOSE
				prefix=ACCT_STAT;
	var STATUS1;
	by mortgage_no PROCESS_DATE;
run;

data LGD_DUP_COSTS_DATA;
	merge ACCT_24_MTH_D_COSTS_TRANSPOSE ACCT_24_MTH_D_ind_cost_TRANSPOSE ACCT_24_MTH_DCOST_DAT_TRANSPOSE ACCT_24_MTH_D_STAT_TRANSPOSE;
	by mortgage_no PROCESS_DATE;
	drop _name_ _label_;
run;

*Join costs data onto rest of the necessary data for accounts;
proc sql;
	create table work.LGD_D_DUP_COST_DATA as
	SELECT *
	from LGD_D_DFLTS t1 INNER JOIN LGD_DUP_COSTS_DATA t2
		ON t1.mortgage_no=t2.mortgage_no AND t1.PROCESS_DATE=t2.PROCESS_DATE;
quit;

data DISCOUNT_RECVY;
	set WORK._LGD_D_RECVY_DATA;
	ARRAY RCVY_AMT[*] MTH_RCVRY_AMT: ;
	ARRAY DATES[*] MTH_RCVRY_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	ARRAY RCVY_AMT_netezza [*] MTH_netezza_RECVY_AMT: ; 

	if dflt_rcvry<="&beginning"d;
		
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
		RCVY_AMT(i)=RCVY_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',PROCESS_DATE,DATES(i))/12));
		if RCVY_AMT(i) ne . then DISCT_RECY = DISCT_RECY+RCVY_AMT(i);
		*IF status is cured then no more recoveries, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	drop _label_ _name_ i MTH_RCVRY_AMT: MTH_RCVRY_DATE: ACCT_STAT:;
run;

data DISCOUNT_NEW_COSTS;
	set WORK._LGD_D_NEW_COST_DATA;
	ARRAY COST_AMT[*] COSTS: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	if dflt_rcvry<="&beginning"d;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_NEW_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',PROCESS_DATE,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_NEW_COSTS = DISCT_NEW_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if DISCT_NEW_COSTS=. then DISCT_NEW_COSTS=0;
	drop _label_ _name_ i COSTS: ind_cost: COST_DATE: ACCT_STAT:;
run;

data DISCOUNT_NEW_IND_COSTS;
	set WORK._LGD_D_NEW_COST_DATA;
	ARRAY COST_AMT[*] ind_cost: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	if DISCT_NEW_IND_COSTS=. then DISCT_NEW_IND_COSTS=0;
	if dflt_rcvry<="&beginning"d;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_NEW_IND_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',PROCESS_DATE,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_NEW_IND_COSTS = DISCT_NEW_IND_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	drop _label_ _name_ i ind_cost: COSTS: COST_DATE: ACCT_STAT:;
run;

data DISCOUNT_DUP_COSTS;
	set work.LGD_D_DUP_COST_DATA;
	ARRAY COST_AMT[*] DUP_COSTS: ;
	ARRAY DATES[*] COST_DATE: ;
	ARRAY STATUS[*] ACCT_STAT: ;
	do i= 1 to dim(COST_AMT);
		if i=1 then DISCT_DUP_COSTS=0;
		COST_AMT(i)=COST_AMT(i)/((1+&CC_DISCOUNT)**(INTCK('month',PROCESS_DATE,DATES(i))/12));
		if cost_amt(i) ne . then DISCT_DUP_COSTS = DISCT_DUP_COSTS+COST_AMT(i);
		*IF status is cured then no more costs, or else LGD Estimate will be off;
		if STATUS(i)="CUR" then leave;
	end;
	if DISCT_DUP_COSTS=. then DISCT_DUP_COSTS=0;
	if dflt_rcvry<="&beginning"d;
	drop _label_ _name_ i DUP_COSTS: COSTS: COST_DATE: ACCT_STAT:;
run;

proc sql;
	create table WORK.LGD_D_DATA as 
		select t1.*,
		t4.DISCT_NEW_COSTS,
		t6.DISCT_NEW_IND_COSTS,
		t5.DISCT_DUP_COSTS,
		(t1.OBS_BAL_AMT-t1.DISCT_RECY-t1.DISCT_RECY_surplus-t5.DISCT_DUP_COSTS)/t1.OBS_BAL_AMT as LGD_NO_COST,
		(t1.OBS_BAL_AMT-t1.DISCT_RECY+t4.DISCT_NEW_COSTS+t6.DISCT_NEW_IND_COSTS-t1.DISCT_RECY_surplus)/t1.OBS_BAL_AMT as LGD_COST
			
		from DISCOUNT_RECVY t1
		full join DISCOUNT_NEW_COSTS t4 
				on t1.mortgage_no=t4.mortgage_no 
				AND t1.PROCESS_DATE=t4.PROCESS_DATE
		full join DISCOUNT_DUP_COSTS t5 
				on t1.mortgage_no=t5.mortgage_no 
				AND t1.PROCESS_DATE=t5.PROCESS_DATE
		full join DISCOUNT_NEW_IND_COSTS t6
				on t1.mortgage_no=t6.mortgage_no 
				AND t1.PROCESS_DATE=t6.PROCESS_DATE;
quit;

data WORK.LGD_D_DATA;
	set WORK.LGD_D_DATA;

	LGD_NO_COST_CAP=LGD_NO_COST;
	if LGD_NO_COST_CAP<0 and LGD_NO_COST_CAP >. then LGD_NO_COST_CAP=0;
	if LGD_NO_COST_CAP>1.25 then LGD_NO_COST_CAP=1.25;

	LGD_COST_CAP=LGD_COST;
	if LGD_COST_CAP<0 and LGD_COST_CAP >. then LGD_COST_CAP=0;
	if LGD_COST_CAP>1.25 then LGD_COST_CAP=1.25;
run;

proc sql;
	create table INTMED.MOR_LGDD_REALIZED as
	select t1.MORTGAGE_NO, 
          t1.PROCESS_DATE, 
          t1.BNS_LGD_D_SEGMENT, 
          t1.LGDD_INSURANCE, 
          t1.OBS_BAL_AMT, 
          t1.DFLT_DT, 
          t1.DFLT_RCVRY, 
          t1.total_RECY, 
          t1.total_RECY_netezza, 
          t1.recy_surplus, 
          t1.DISCT_RECY_surplus, 
          t1.DISCT_RECY, 
          t1.DISCT_NEW_COSTS, 
          t1.DISCT_NEW_IND_COSTS, 
          t1.DISCT_DUP_COSTS, 
          t1.LGD_NO_COST, 
          t1.LGD_COST, 
          t1.LGD_NO_COST_CAP, 
          t1.LGD_COST_CAP 
    from LGD_D_DATA t1
	where process_date="&complete"d;
quit;

PROC SQL;
    CREATE TABLE INTMED.MOR_LGDD_ACCTS_FNL_APPEND_TBL AS
    SELECT * FROM NZUSER.BNS_LGD_D_SCORED_SEG_ACCTS
    WHERE TIME_KEY = "&complete"d;
QUIT;

DATA INTMED.MOR_LGDD_ACCTS_FNL_APPEND_TBL;
	SET INTMED.MOR_LGDD_ACCTS_FNL_APPEND_TBL;
	ACCT_LGD24_NOCOST_BKUP = ACCT_LGD24_NOCOST;
	ACCT_LGD24_NOCOST_CAP_BKUP = ACCT_LGD24_NOCOST_CAP;
	ACCT_LGD24_COST_BKUP = ACCT_LGD24_COST;
	ACCT_LGD24_COST_CAP15_BKUP = ACCT_LGD24_COST_CAP15;
RUN;

PROC SQL;
    UPDATE INTMED.MOR_LGDD_ACCTS_FNL_APPEND_TBL AS T
    SET ACCT_LGD24_NOCOST = (SELECT LGD_NO_COST FROM INTMED.MOR_LGDD_REALIZED AS A
                             WHERE T.MORTGAGE_NO = A.MORTGAGE_NO AND T.TIME_KEY = A.PROCESS_DATE),
        ACCT_LGD24_NOCOST_CAP = (SELECT LGD_NO_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS B
                             WHERE T.MORTGAGE_NO = B.MORTGAGE_NO AND T.TIME_KEY = B.PROCESS_DATE),
        ACCT_LGD24_COST = (SELECT LGD_COST FROM INTMED.MOR_LGDD_REALIZED AS C
                             WHERE T.MORTGAGE_NO = C.MORTGAGE_NO AND T.TIME_KEY = C.PROCESS_DATE),
        ACCT_LGD24_COST_CAP15 = (SELECT LGD_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS D
                             WHERE T.MORTGAGE_NO = D.MORTGAGE_NO AND T.TIME_KEY = D.PROCESS_DATE)
    WHERE T.MORTGAGE_NO IN (SELECT MORTGAGE_NO FROM INTMED.MOR_LGDD_REALIZED) AND T.TIME_KEY IN (SELECT PROCESS_DATE FROM INTMED.MOR_LGDD_REALIZED);
QUIT;

PROC SQL;
    CONNECT USING NZUSER AS IIASCON;
    EXECUTE(DELETE FROM &FRG_DB..BNS_LGD_D_SCORED_SEG_ACCTS WHERE TIME_KEY = &DEL_RECS)BY IIASCON;
    DISCONNECT FROM IIASCON;
QUIT;

PROC APPEND BASE=NZUSER.BNS_LGD_D_SCORED_SEG_ACCTS(BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=INTMED.MOR_LGDD_ACCTS_FNL_APPEND_TBL;
RUN;


PROC SQL;
    CREATE TABLE INTMED.MOR_LGDD_ANTQ_FNL_APPEND_TBL AS
    SELECT * FROM NZUSER.BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ
    WHERE TIME_KEY = "&complete"d;
QUIT;

DATA INTMED.MOR_LGDD_ANTQ_FNL_APPEND_TBL;
	SET INTMED.MOR_LGDD_ANTQ_FNL_APPEND_TBL;
	ACCT_LGD24_NOCOST_BKUP = ACCT_LGD24_NOCOST;
	ACCT_LGD24_NOCOST_CAP_BKUP = ACCT_LGD24_NOCOST_CAP;
	ACCT_LGD24_COST_BKUP = ACCT_LGD24_COST;
	ACCT_LGD24_COST_CAP15_BKUP = ACCT_LGD24_COST_CAP15;
RUN;

PROC SQL;
    UPDATE INTMED.MOR_LGDD_ANTQ_FNL_APPEND_TBL AS T
    SET ACCT_LGD24_NOCOST = (SELECT LGD_NO_COST FROM INTMED.MOR_LGDD_REALIZED AS A
                             WHERE T.MORTGAGE_NO = A.MORTGAGE_NO AND T.TIME_KEY = A.PROCESS_DATE),
        ACCT_LGD24_NOCOST_CAP = (SELECT LGD_NO_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS B
                             WHERE T.MORTGAGE_NO = B.MORTGAGE_NO AND T.TIME_KEY = B.PROCESS_DATE),
        ACCT_LGD24_COST = (SELECT LGD_COST FROM INTMED.MOR_LGDD_REALIZED AS C
                             WHERE T.MORTGAGE_NO = C.MORTGAGE_NO AND T.TIME_KEY = C.PROCESS_DATE),
        ACCT_LGD24_COST_CAP15 = (SELECT LGD_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS D
                             WHERE T.MORTGAGE_NO = D.MORTGAGE_NO AND T.TIME_KEY = D.PROCESS_DATE)
    WHERE T.MORTGAGE_NO IN (SELECT MORTGAGE_NO FROM INTMED.MOR_LGDD_REALIZED) AND T.TIME_KEY IN (SELECT PROCESS_DATE FROM INTMED.MOR_LGDD_REALIZED);
QUIT;

PROC SQL;
    CONNECT USING NZUSER AS IIASCON;
    EXECUTE(DELETE FROM &FRG_DB..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ WHERE TIME_KEY = &DEL_RECS)BY IIASCON;
    DISCONNECT FROM IIASCON;
QUIT;

PROC APPEND BASE=NZUSER.BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ(BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=INTMED.MOR_LGDD_ANTQ_FNL_APPEND_TBL;
RUN;

PROC SQL;
    CREATE TABLE INTMED.MOR_LGDD_RSLTS_FNL_APPEND_TBL AS
    SELECT * FROM RESULTS.BNS_LGD_D_SCORED_SEG_ACCTS
    WHERE TIME_KEY = "&complete"d;
QUIT;

DATA INTMED.MOR_LGDD_RSLTS_FNL_APPEND_TBL;
	SET INTMED.MOR_LGDD_RSLTS_FNL_APPEND_TBL;
	ACCT_LGD24_NOCOST_BKUP = ACCT_LGD24_NOCOST;
	ACCT_LGD24_NOCOST_CAP_BKUP = ACCT_LGD24_NOCOST_CAP;
	ACCT_LGD24_COST_BKUP = ACCT_LGD24_COST;
	ACCT_LGD24_COST_CAP15_BKUP = ACCT_LGD24_COST_CAP15;
RUN;

PROC SQL;
    UPDATE INTMED.MOR_LGDD_RSLTS_FNL_APPEND_TBL AS T
    SET ACCT_LGD24_NOCOST = (SELECT LGD_NO_COST FROM INTMED.MOR_LGDD_REALIZED AS A
                             WHERE T.MORTGAGE_NO = A.MORTGAGE_NO AND T.TIME_KEY = A.PROCESS_DATE),
        ACCT_LGD24_NOCOST_CAP = (SELECT LGD_NO_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS B
                             WHERE T.MORTGAGE_NO = B.MORTGAGE_NO AND T.TIME_KEY = B.PROCESS_DATE),
        ACCT_LGD24_COST = (SELECT LGD_COST FROM INTMED.MOR_LGDD_REALIZED AS C
                             WHERE T.MORTGAGE_NO = C.MORTGAGE_NO AND T.TIME_KEY = C.PROCESS_DATE),
        ACCT_LGD24_COST_CAP15 = (SELECT LGD_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS D
                             WHERE T.MORTGAGE_NO = D.MORTGAGE_NO AND T.TIME_KEY = D.PROCESS_DATE)
    WHERE T.MORTGAGE_NO IN (SELECT MORTGAGE_NO FROM INTMED.MOR_LGDD_REALIZED) AND T.TIME_KEY IN (SELECT PROCESS_DATE FROM INTMED.MOR_LGDD_REALIZED);
QUIT;

PROC SQL;
	DELETE FROM RESULTS.BNS_LGD_D_SCORED_SEG_ACCTS WHERE TIME_KEY="&COMPLETE"d;
quit;

PROC APPEND BASE=RESULTS.BNS_LGD_D_SCORED_SEG_ACCTS
            DATA=INTMED.MOR_LGDD_RSLTS_FNL_APPEND_TBL;
RUN;


%mend lgdd_new;

%get_model_period_dates(product=mor);
%put start_period_dt: &start_period_dt;


data runmonth;
	format start_date end_date date9. start_date2 end_date2 yymmdd10.;
	start_date=intnx('month',"&start_period_dt"d,0,'end');
	end_date=intnx('month',start_date,-24,'end');
	start_date2=start_date;
	end_date2=end_date;
run;

proc sql;
	select start_date into :beginning from runmonth;
	select end_date into :complete from runmonth;
	select start_date2 into :beginning2 from runmonth;
	select end_date2 into :complete2 from runmonth;
quit;

%put %bquote('&beginning'd);
%put %bquote('&complete'd);
%put %bquote('&complete2');

%lgdd_new(%bquote('&beginning'd),%bquote('&complete'd),%bquote('&complete2'))