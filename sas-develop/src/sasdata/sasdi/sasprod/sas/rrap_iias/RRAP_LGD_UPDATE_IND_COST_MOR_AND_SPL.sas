%rrap_mor_bns_autoexec

PROC SQL;
CONNECT using NZRRAP as iiascon;
   CREATE TABLE REV_DLQNT_DIST AS 
   SELECT *
From connection to iiascon 
(
   SELECT 	sum(case when t1.BNS_DLQNT_DAY>=30 then 1 else 0 end)as DELQ,
			t1.MTH_TM_ID,
			t3.TM_LVL_END_DT as PROCESS_DATE,
			t2.BASEL_MODEL_ID
		from &RRAP_DB..pd_seg_acct_xref t2
			left join  &RRAP_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT t1
				on t1.BASEL_ACCT_ID=t2.basel_acct_id
				and t1.MTH_TM_ID=t2.MTH_TM_ID
			left join &RRAP_DB..TM_DIM t3 on (t1.MTH_TM_ID=t3.TM_ID)
			where t1.BNS_DLQNT_DAY>0
			GROUP BY t1.MTH_TM_ID, BASEL_MODEL_ID, t3.TM_LVL_END_DT
			ORDER BY BASEL_MODEL_ID,t1.MTH_TM_ID;
);
	  Disconnect from iiascon;
QUIT;


PROC SQL;
CONNECT using NZRRAP as iiascon;
   CREATE TABLE PSNL_DLQNT_DIST AS 
   SELECT *
From connection to iiascon 
(
   SELECT 	sum(case when t1.DAY_ODUE>0 then 1 else 0 end)as DELQ,
			t1.MTH_TM_ID,
			t3.TM_LVL_END_DT as PROCESS_DATE,
			t2.BASEL_MODEL_ID
		from &RRAP_DB..BASEL_PNL_LN_PD_SEG_ACCT_XREF t2
			left join  &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT t1
				on t1.BASEL_ACCT_ID=t2.basel_acct_id
				and t1.MTH_TM_ID=t2.MTH_TM_ID
			left join &RRAP_DB..TM_DIM t3 on (t1.MTH_TM_ID=t3.TM_ID)
			where t1.DAY_ODUE>0
			GROUP BY t1.MTH_TM_ID, BASEL_MODEL_ID, t3.TM_LVL_END_DT
			ORDER BY BASEL_MODEL_ID,t1.MTH_TM_ID;
);
	  Disconnect from iiascon;
QUIT;


proc sql;
	create table MORT_DLQNT_DIST as
		select 
			sum(case when t1.DLQNT_DAY_CNT>0 then 1 else 0 end)as DELQ,
			t1.MTH_TM_ID,
			t2.TM_LVL_END_DT as PROCESS_DATE,
			8021 as BASEL_MODEL_ID
		from NZRRAP.BASEL_MORT_ACCT_DRVD_VARS t1
		left join NZRRAP.TM_DIM t2 on t1.MTH_TM_ID=t2.TM_ID
			where DLQNT_DAY_CNT>0
		GROUP BY t1.MTH_TM_ID, t2.TM_LVL_END_DT
		ORDER BY t1.MTH_TM_ID;
quit;


proc sql;
	create table PROD_DLQNT_DIST as
		select *
			from REV_DLQNT_DIST
				union
			select * 
				from PSNL_DLQNT_DIST
					union
				select * 
					from MORT_DLQNT_DIST;
quit;
	
proc sql;
	create table DLQNT_DIST as
		select sum(t1.DELQ) as DELQ,
			t1.PROCESS_DATE
		from PROD_DLQNT_DIST t1
			where PROCESS_DATE>='31AUG2004'd
				GROUP BY t1.PROCESS_DATE
					order by t1.PROCESS_DATE;
quit;

proc sql;
	create table DLQNT_DIST as 
	select t1.*,
			t2.TM_ID as MTH_TM_ID
	from DLQNT_DIST t1
	left join NZRRAP.TM_DIM t2
	on t1.PROCESS_DATE=t2.TM_LVL_END_DT
	where TM_LVL='Month';
quit;



proc sql;
	create table Indirect_costs as
		select t1.*,
				t2.TOT_INDRCT_COST_AMT,
				t2.TOT_INDRCT_COST_AMT/t1.DELQ as UNIT_COST FORMAT=DOLLAR23.2
		from   DLQNT_DIST  t1
		left join   NZRRAP.MBR_INDRCT_COST_MTH_SNAPSHOT t2
		on t1.MTH_TM_ID=t2.MTH_TM_ID;
quit;


*for Missing data Periods,Total Indirect Costs is estimated by the average between the 
Total Indirect Costs for the period before and after the missing periods;

proc sql; 
	create table start_dates as
	select t1.MTH_TM_ID,
			t2.TM_LVL_END_DT as PROCESS_DATE,
			t1.TOT_INDRCT_COST_AMT FORMAT=DOLLAR23.2 ,
			125801 as DELQ,
			t1.TOT_INDRCT_COST_AMT/125801 as UNIT_COST format=DOLLAR23.2
	from NZRRAP.MBR_INDRCT_COST_MTH_SNAPSHOT t1
	left join NZRRAP.TM_DIM t2 on t1.MTH_TM_ID=t2.TM_ID
	where t1.MTH_TM_ID>=10316 and t1.MTH_TM_ID<=10396
order by t1.MTH_TM_ID;
run;

data MIS_DATES;
	input DELQ PROCESS_DATE DATE9. MTH_TM_ID TOT_INDRCT_COST_AMT UNIT_COST;
	format PROCESS_DATE DATE9. UNIT_COST DOLLAR23.2;
	datalines;
	. 30NOV2003 10436 . 4.03
	. 31DEC2003 10476 . 4.03
	. 31JAN2004 10516 . 4.03
	. 29FEB2004 10556 . 4.03
	. 31MAR2004 10596 . 4.03
	. 30APR2004 10636 . 4.03
	. 31MAY2004 10676 . 4.03
	. 30JUN2004 10716 . 4.03
	. 31JUL2004 10756 . 4.03
	;
run;

%if %sysfunc(exist(NZUSER.IND_COST)) %then %do;
	PROC SQL;
		DROP TABLE NZUSER.IND_COST;
	QUIT;
%END;

data NZUSER.IND_COST;
	set start_dates MIS_DATES Indirect_costs;
	if process_date>='30NOV2004'd and process_date<='31JUL2005'd then do;
		TOT_INDRCT_COST_AMT=647271.50; 
		UNIT_COST=TOT_INDRCT_COST_AMT/DELQ;
	end; 
	if process_date>='30NOV2005'd and process_date<='31JUL2006'd then do;
		TOT_INDRCT_COST_AMT=753136.00; 
		UNIT_COST=TOT_INDRCT_COST_AMT/DELQ;
	end; 
run;