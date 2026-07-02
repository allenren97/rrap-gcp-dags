%RRAP_MOR_TNG_AUTOEXEC

%get_model_period_dates(product=mor);
%put start_period_dt: &start_period_dt;


data runmonth;
	format start_date end_date date9. start_date2 end_date2 yymmdd10. TNG_PROCESS_DATE YYMMN. a_year YEAR4.;
	start_date=intnx('month',"&start_period_dt"d,0,'end');
	start_date2=start_date;
	TNG_PROCESS_DATE=START_DATE;
    A_YEAR=INTNX('YEAR',intnx('month',"&start_period_dt"d,0,'end'),0);
run;

proc sql;
	select start_date into :beginning from runmonth;
	select start_date2 into :beginning2 from runmonth;
	SELECT TNG_PROCESS_DATE INTO :P_DATE FROM RUNMONTH;
    SELECT A_YEAR INTO :A_YEAR FROM RUNMONTH;
quit;

%LET PROCESS_DATE=&P_DATE;
%LET ARREAR_YEAR=&A_YEAR;

%PUT &PROCESS_DATE;
%PUT &ARREAR_YEAR;

proc sql noprint;
	select intck('month','31DEC2004'd, max(a.month_end_dt))
		into: loop_num
			from NZUSER.tng_12mth_default as a;
quit;

%put &loop_num;

data tng_12mth_default;
	set NZUSER.tng_12mth_default;
	num1 = find(account_id, '~', 'i', 1);
	num2 = find(account_id, '~', 'i', num1+1);
	length = length(account_id);
	mortgage_num = substr(account_id, num1+1, num2 - num1-1) + 0;
	provider = substr(account_id, 1, 3);
	drop num1 num2 length;
	format write_off_month date9.;

	/*** Set the write_off_month to the last date of the month */
	write_off_month = intnx('month', write_off_month, 1)-1;

	if first_defaultbal=0 and write_off_month ne . then
		do;
			first_defaultbal=write_off_amt;
		end;
	else
		do;
			first_defaultbal=first_defaultbal;
		end;

	if final_defaultbal=0 and write_off_month ne . then
		do;
			final_defaultbal=write_off_amt;
		end;
	else
		do;
			final_defaultbal=final_defaultbal;
		end;
run;

proc sql noprint;
	create table default_ids as
		select account_id, 
			def_ind, 
			default_ind, 
			final_default_ind,
			final_defaultdate,
			final_defaultbal,
			month_end_dt,
			mortgage_num,
			/*-----------------------------------------------------------------------------------------
			* If this field cannot be found, then it can be created by extracting the numerical value
			* bewteen the "~" characters from the account_id field.
			*-----------------------------------------------------------------------------------------*/
	provider
	from work.tng_12mth_default 
		/*--------------------------------------------------------- 
		* This dataset should be available from Leon H's code
		*---------------------------------------------------------*/
	where def_ind = 1 or final_default_ind = 'Y' or default_ind = 'Y';
quit;

proc sort data=default_ids nodupkeys;
	by mortgage_num;
run;

proc sql noprint;
	create table all_default_obs as 
		select * 
			from tng_12mth_default
				where mortgage_num in (select mortgage_num from default_ids);
quit;

/* Add accrued interest to the default obs file */
proc sql noprint;
	create table def_obs_with_interest as 
		select *
			from (select * from all_default_obs) as pt1
				left join
					(select account_id, 
						accrued_interest_amt,
						month_end_dt,
						insurer_desc    /*Will added insurer_desc to the query becasue it is needed in the PT7 code*/
					from TNGDATA.tng_acct_mo) as pt2
						on pt1.account_id = pt2.account_id and pt1.month_end_dt = pt2.month_end_dt;
quit;

PROC SQL;
	CONNECT USING TNGDATA AS IIASCON;
	EXECUTE(DROP TABLE &TNG_DB..def_obs_with_interest)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CREATE TABLE TNGDATA.DEF_OBS_WITH_INTEREST AS
	SELECT * FROM DEF_OBS_WITH_INTEREST;
QUIT;

/*Identify individual "default events "*/
proc sql noprint;
	create table default_points as 
		select account_id,
			mortgage_num,
			final_defaultdate,
			final_defaultbal,
			write_off_amt,
			write_off_month,
			write_off_ind
		from def_obs_with_interest
			where final_defaultdate ~= .;
quit;

proc sort data=default_points nodupkeys;
	by account_id mortgage_num final_defaultdate final_defaultbal write_off_amt write_off_ind write_off_month;
run;

/* write offs only apply to the last default event */
data default_points;
	set default_points;
	by account_id;

	if not last.account_id then
		do;
			write_off_amt = .;
			write_off_month = .;
			write_off_ind = .;
		end;
run;

/*------------------------------------------------------------------------------------------------------- 
 * find out the beginning and end date of each default event
 * this can be used to a upper bound estimate for recovery period - not accurate for lgd calculation yet
 *-------------------------------------------------------------------------------------------------------*/
proc sort data=default_points;
	by account_id descending final_defaultdate;
run;

data default_points;
	set default_points;
	format final_defaultdate2 date9.;
	final_defaultdate2 = lag(final_defaultdate);
run;

data default_points;
	set default_points;
	by account_id;

	if first.account_id then
		do;
			if write_off_month ~= . then
				do;
					final_defaultdate2 = write_off_month;
				end;
			else
				do;
					final_defaultdate2 = '31-DEC-9999'd;
				end;
		end;
run;

/* create a unique key for each default event */
data default_points;
	set default_points;
	format aggregate_key $40.;
	aggregate_key = catx('-', account_id, final_defaultdate);
run;

/* identify the recovery period for each default event */
proc sql noprint;
	create table recov_obs as 
		select * 
			from (select account_id,
				aggregate_key,
				final_defaultdate,
				final_defaultbal,
				final_defaultdate2,
				write_off_ind,
				write_off_month,
				write_off_amt
			from default_points) as pt1
				left join (select account_id,
					month_end_dt,
					end_principal_balance,
					accrued_interest_amt,
					final_default_ind
				from def_obs_with_interest) as pt2
					on pt1.account_id = pt2.account_id and pt1.final_defaultdate <= pt2.month_end_dt < pt1.final_defaultdate2
				order by aggregate_key, pt2.month_end_dt;
quit;

data recov_obs;
	set recov_obs;
	by aggregate_key;
	format recovery_date default_start date9.;
	def_ind_lag = lag(final_default_ind);

	if first.aggregate_key then
		do;
			last_known_default = .;
			def_ind_lag = .;
		end;

	if final_default_ind = 'N' and def_ind_lag = 'Y' then
		do;
			recovery = 1;
			recovery_date = month_end_dt;
			default_start = final_defaultdate;
		end;
run;

/*--------------------------------------------------------------------
 * This part added by Will to convert write_off_ind to numeric.  I
 * verified the logic with Bill Qu.
 *--------------------------------------------------------------------*/
data recov_obs (drop=write_off_ind rename=(write_off_ind2=write_off_ind));
	set recov_obs;

	if upcase(write_off_ind)='Y' then
		write_off_ind2=1;
	else if upcase(write_off_ind)='N' then
		write_off_ind2=0;
	else if write_off_ind=' ' then
		write_off_ind2=.;
run;

/* Written off accounts */
proc sql noprint;
	create table written_off as
		select aggregate_key,
			account_id,
			final_defaultdate as default_start,
			write_off_month as default_end,
			sum(write_off_ind) as write_off_ind
		from recov_obs
			where write_off_ind = 1
				group by 1,2,3,4;
quit;

data written_off2;
	set written_off;
	wo_type = 0;
	diff = intck('month', default_start, default_end);

	if diff >= 24 then
		wo_type = 1;
run;

/* gather recovery periods */
proc sql noprint;
	create table recovered_accounts as
		select aggregate_key,
			account_id,
			default_start,
			min(recovery_date) as default_end format date9.
		from recov_obs
			where recovery = 1 and aggregate_key not in (select aggregate_key from written_off)
				group by 1,2,3;
quit;

/* add in recovery length */
data recovered_accounts2;
	set recovered_accounts;
	recov_type = 0;
	diff = intck('month', default_start, default_end);

	if diff >= 24 then
		recov_type = 1;
run;

/* sanity check - just making sure there are no duplicates */
proc sort data=written_off nodupkeys;
	by aggregate_key;
run;

proc sql noprint;
	create table defaults_only as 
		select aggregate_key,
			account_id,
			final_defaultdate as default_start,
			final_defaultdate2 as default_end,
			count(*) as obs
		from recov_obs
			where (aggregate_key not in (select aggregate_key from recovered_accounts)) and (aggregate_key not in (select aggregate_key from written_off))
				group by 1,2,3,4;
quit;

/*-----------------------------------------------------------------------------------------------------
 * 3 types of defaults are identified - type 1- cured, type 2- written_off, or type 3 -straight_default
 * added in type 4 for recovery outside of 24 month window type 5 for write_off oustside of 24 months
 *-----------------------------------------------------------------------------------------------------*/
data def_obs_with_recovery_period;
	set recovered_accounts2 ( keep =  aggregate_key account_id default_start default_end recov_type in = a ) 
		written_off2 ( keep =  aggregate_key account_id default_start default_end wo_type in = b) 
		defaults_only   ( keep =  aggregate_key account_id default_start default_end in = c);

	if a then
		do;
			if recov_type = 0 then
				type = 1;
			else type = 4;
		end;
	else if b then
		do;
			if wo_type = 0 then
				type = 2;
			else type = 5;
		end;
	else if c then
		type = 3;

	if type = 1 then
		type2 = 1;

	if type = 2 then
		type2 = 2;

	if type in (3,4,5) then
		type2 = 3;
	drop recov_type wo_type;
run;

proc sql noprint;
	create table default_points2 as 
		select * 
			from( select aggregate_key,
				account_id,
				final_defaultdate,
				final_defaultbal,
				write_off_ind,
				write_off_month,
				write_off_amt

			from default_points) as pt1
				left join
					(select * from def_obs_with_recovery_period) as pt2
						on pt1.aggregate_key = pt2.aggregate_key;
quit;

PROC SQL;
	CONNECT USING TNGDATA AS IIASCON;
	EXECUTE(DROP TABLE &TNG_DB..default_points2)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CREATE TABLE TNGDATA.default_points2 AS
	SELECT * FROM default_points2;
QUIT;

/* gather recovery data */
proc sql noprint;
	create table recovery_data as 
		select *
			from (select * from default_points2) as pt1
				left join
					(select account_id,
						month_end_dt,  /*Will removed the datepart because it was resulting in missing values*/
						final_default_ind,
						end_principal_balance,
						accrued_interest_amt,
						sum(end_principal_balance,accrued_interest_amt) as bal_with_interest /*will changed to sum function*/
					from def_obs_with_interest) as pt3
						on pt1.account_id  = pt3.account_id and pt1.default_start <= pt3.month_end_dt <= pt1.default_end
					order by pt1.aggregate_key, pt3.month_end_dt;
quit;

data recovery_data;
	set recovery_data;
	by aggregate_key;
	prev_end_balance = lag(bal_with_interest);

	if first.aggregate_key then
		do;
			prev_end_balance = .;
			balance_change = 0;
		end;

	/* if account is cured, then the last "default" balance is used for cured amount*/
	else if last.aggregate_key and type2 = 1 then
		do;
			balance_change = prev_end_balance;
		end;
	else
		do;
			balance_change = prev_end_balance - bal_with_interest;
		end;
run;

/*----------------------------------------------------------------------
 * solve missing month issue here by inserting blank recovery rows
 * this may not work well with deployment
 *----------------------------------------------------------------------*/

/* identify max number of months of recovery */
proc sql noprint;
	create table boundary as
		select aggregate_key,
			max(month_end_dt) as max_bound format date9.,
			min(montH_end_dt) as min_bound format date9.
		from recovery_data;
quit;

proc sort data=boundary nodupkeys;
	by aggregate_key;
run;

proc sql noprint;
	select intck('month', min_bound, max_bound)+1 
		from boundary;
quit;

/* create a dataset of all accounts with all possible dates, therefore no missing entry */
proc sql noprint;
	create table span as 
		select aggregate_key,
			min(month_end_dt) as start_dt format date9.,
			max(month_end_dt) as end_dt format date9.
		from recovery_data
			group by 1;
quit;

data span;
	set span;
	month_count = intck('month', start_dt, end_Dt) + 1;
run;

/*CODE CHANGE: WP- Changed loop number to 119 from 104 to capture appropriate time window*/
data span2;
	set
		%macro t;

	%DO i = 1 %TO &loop_num.;
		boundary (keep = aggregate_key)
	%END;
%mend;

%t;
;
run;

proc sort data=span2;
	by aggregate_key;
run;

data span3;
	set span2;
	format obs_dt date9.;
	by aggregate_key;
	i + 1;

	if first.aggregate_key then
		do;
			i = 1;
			obs_dt = '28-FEB-2005'd;
		end;
	else
		do;
			obs_dt = intnx('month', '28-FEB-2005'd, i)-1;
		end;
run;

proc sql noprint;
	create table recovery_span as 
		select * 
			from (select * 
			from span 
				where start_dt ~= .) as pt1
					left join
						(select * 
							from span3) as pt2
								on pt1.aggregate_key = pt2.aggregate_key and pt1.start_dt <= pt2.obs_dt <= pt1.end_dt
							order by pt1.aggregate_key,pt2.obs_dt;
quit;

/* create a dataset of all recovery data with no missing dates in the middle */
proc sql noprint;
	create table INTMED.recovery_data_all_&process_date. as 
		select * 
			from (select aggregate_key, obs_dt 
			from recovery_span) as pt1
				left join
					(select * from recovery_data) as pt2
						on pt1.aggregate_key = pt2.aggregate_key and pt1.obs_Dt = pt2.month_end_dt;
quit;

PROC SQL;
	CREATE TABLE WORK.indirect_costs AS 
		SELECT DISTINCT t1.ACCOUNT_ID, 
			(YEAR(t1.MONTH_END_DT)) AS Arrear_Year
		FROM TNGDATA.TNG_ACCT_MO t1
			WHERE t1.DAYS_ARREARS_CNT > 0;
QUIT;

PROC SQL;
	CREATE TABLE WORK.indirect_costs AS 
		SELECT (COUNT(t1.ACCOUNT_ID)) AS ACCTS_IN_ARREARS, 
			t1.Arrear_Year
		FROM WORK.indirect_costs t1
			GROUP BY t1.Arrear_Year;
QUIT;



proc sql;
	CREATE TABLE WORK.indirect_costs AS 
		select 	t1.Arrear_Year,
			t1.ACCTS_IN_ARREARS,
			t2.Amount as EST_COSTS
		from WORK.indirect_costs t1
			left join TNGDATA.TNG_ACCT_INDCOST t2
				on t1.ARREAR_YEAR=t2.YEAR
			where t2.COST_TYPE='Estimated costs' or Arrear_Year in (&arrear_year.)
;
quit;



proc sql noprint;
select 
mean(EST_COSTS/ACCTS_IN_ARREARS) into: est_ind_arrear_yr from indirect_costs
where ARREAR_YEAR in ( %sysevalf (&arrear_year.-1),%sysevalf (&arrear_year.-2), %sysevalf (&arrear_year.-3));
quit;


data INTMED.indirect_costs;
	set WORK.indirect_costs;
	ind_cost=EST_COSTS/ACCTS_IN_ARREARS;
	*Use the past 3 years unit_costs for arrear year;
	if ARREAR_YEAR=&arrear_year. then
		ind_cost= &est_ind_arrear_yr.;
	month_cost=ind_cost/12;
run;