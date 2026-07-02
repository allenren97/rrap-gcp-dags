
%RRAP_MOR_TNG_AUTOEXEC

%get_model_period_dates(product=mor);
%put start_period_dt: &start_period_dt;


data runmonth;
	format start_date end_date date9. start_date2 end_date2 yymmdd10. TNG_PROCESS_DATE YYMMN. a_year YEAR4.;
	start_date=intnx('month',"&start_period_dt"d,0,'end');
	end_date=intnx('month',start_date,-24,'end');
	start_date2=start_date;
	end_date2=end_date;
	TNG_PROCESS_DATE=START_DATE;
    A_YEAR=INTNX('YEAR',intnx('month',"&start_period_dt"d,0,'end'),0);
	A_MONTH=MONTH(START_DATE); 
run;

proc sql;
	select start_date into :beginning from runmonth;
	select end_date into :complete from runmonth;
	select start_date2 into :beginning2 from runmonth;
	select end_date2 into :complete2 from runmonth;
	SELECT TNG_PROCESS_DATE INTO :P_DATE FROM RUNMONTH;
    SELECT A_YEAR INTO :A_YEAR FROM RUNMONTH;
    SELECT A_MONTH INTO :A_MONTH FROM RUNMONTH;
quit;

%LET PROCESS_DATE=&P_DATE;
%LET LGDD_DEF_END="&beginning"d;
%LET _RUNDATELGD="&complete"d;
%LET DEL_RECS=%bquote('&complete2');
%LET MONTH=&A_MONTH;

%PUT &PROCESS_DATE;
%PUT &LGDD_DEF_END;
%PUT &_RUNDATELGD;
%PUT &DEL_RECS;
%PUT &MONTH;

proc sql;
create table default_ids as
select account_id, 
	def_ind, 
	final_default_ind,
	final_defaultdate,
	final_defaultbal,
	month_end_dt 
from NZUSER.tng_12mth_default 
where def_ind = 1 or final_default_ind = 'Y' 
;
quit;

proc sort data=default_ids nodupkeys;
by account_id;
run;

proc sql;
create table all_default_obs as select * from NZUSER.tng_12mth_default
where account_id in (select account_id from default_ids);
quit;


data all_default_obs;
set all_default_obs (drop=write_off_ind write_off_amt write_off_month);
run;

data temp_write_off;
set TNGDATA.TNG_ACCT_WRITEOFF;
format account_id $80.;
if substrn(mtg_num,1,1) ='F' or substrn(mtg_num,1,1) ='M' or substrn(mtg_num,1,1) ='R'
then account_id=mtg_num;
else if Provider="FNAL" then account_id=catt("FNAL~",mtg_num,"~1");
else if  Provider="MCAP" then account_id=catt("MCAP~",mtg_num,"~1");
else if  Provider="DIR" and substrn(mtg_num,1,1) ne '7' 
then account_id=catt("MCAP~",mtg_num,"~1");
else account_id=catt("MBS~",mtg_num);
run;

data temp_write_off;
set temp_write_off;
where account_id not in
(
'MBS~7000174901' 'MBS~7000225553'  
'MBS~7700206059' 'MCAP~8207220~1' 'MBS~7000068947' 'MBS~7000034722' 'MBS~203566'         
'MBS~7000039018' 'MBS~7000124332' 'MCAP~8268926~1' 'MBS~7700174435' 
);

run;

proc sql;
create table temp1 as
select account_id, min(month_end_dt) as month_end, min(writeoff_date) as write_off_month
from temp_write_off
group by account_id;
quit;

proc sql;
create table temp2 as
select account_id, sum(writeoff_amt) as write_off_amt
from temp_write_off
group by account_id;
quit;

proc sql;
create table write_offs as
select a.*,b.*
from temp1 as a left join temp2 as b
on a.account_id=b.account_id;
quit;

data write_offs;
set write_offs;
write_off_ind=1;
run;

proc sql;
create table all_default_obs2 as
select a.*,b.*
from all_default_obs as a left join write_offs as b
on a.account_id=b.account_id;
quit;


proc sql;
create table default_points as 
select account_id,
	 
		final_defaultdate,
		final_defaultbal,
		write_off_amt,
		write_off_month,
		write_off_ind

from all_default_obs2
where final_defaultdate ~= .;
quit;



proc sort data=default_points nodupkeys;
by account_id   final_defaultdate final_defaultbal write_off_amt write_off_ind write_off_month;
run;

/*** write offs only apply to the last default event ***/

data default_points;
set default_points;
by account_id;
if not last.account_id then do;
	write_off_amt = .;
	write_off_month = .;
	write_off_ind=.;
end;
run;


proc sort data=default_points;
by account_id descending final_defaultdate ;
run;

data default_points;
set default_points;
format final_defaultdate2 date9.;
final_defaultdate2 = lag(final_defaultdate);
run;

data default_points;
set default_points;
by account_id;
if first.account_id then do;
	if write_off_month ~= . then do;
		final_defaultdate2 = write_off_month;
	end;
	else do;
		final_defaultdate2 = '31-DEC-9999'd;
	end;
end;
run;



/*** create a unique key for each default event ***/

data default_points;
set default_points;
format aggregate_key $40.;
aggregate_key = catx('-', account_id, final_defaultdate);
run;




/*** identify the recovery period for each default event ***/

proc sql;
create table recov_obs as select * from
(
	select account_id,
			aggregate_key,
			final_defaultdate,
			final_defaultbal,
			final_defaultdate2,
            write_off_ind,
			write_off_month,
			write_off_amt
	from	default_points
) as pt1
left join
(
	select	account_id,
			month_end_dt as month_end_dt format date9.,
			end_principal_balance,
	
			final_default_ind
	from	 all_default_obs2
) as pt2

on pt1.account_id = pt2.account_id
and pt1.final_defaultdate <= pt2.month_end_dt < pt1.final_defaultdate2
order by aggregate_key, pt2.month_end_dt;
quit;

data recov_obs;
set recov_obs;
by aggregate_key;

format recovery_date default_start date9.;

def_ind_lag = lag(final_default_ind);

if first.aggregate_key then do;
	last_known_default = .;
	def_ind_lag = .;
end;

	
if final_default_ind = 'N' and def_ind_lag = 'Y' then do;
	recovery = 1;
	recovery_date = month_end_dt;
	default_start = final_defaultdate;
end;
run;



/*** Written off accounts ****/
proc sql;
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
if diff >= 24 then wo_type = 1;
run;



/*** gather recovery periods ***/
proc sql;
create table recovered_accounts as
select aggregate_key,
		account_id,
		default_start,
		min(recovery_date) as default_end format date9.
from recov_obs
where recovery = 1 and aggregate_key not in (select aggregate_key from written_off)
group by 1,2,3;
quit;


/*** add in recovery length ***/
data recovered_accounts2;
set recovered_accounts;
recov_type = 0;
diff = intck('month', default_start, default_end);
if diff >= 24 then recov_type = 1;
run;



/*** sanity check ***/

proc sort data=written_off nodupkeys;
by aggregate_key;
run;


proc sql;
create table defaults_only as 
select aggregate_key,
		account_id,
		final_defaultdate as default_start,
		final_defaultdate2 as default_end,
		count(*) as obs
from recov_obs
where (aggregate_key not in (select aggregate_key from recovered_accounts))
and (aggregate_key not in (select aggregate_key from written_off))
group by 1,2,3,4;
quit;




/*** 3 types of defaults are identified - type 1- cured, type 2- written_off, or type 3 -straight_default
	added in type 4 for recovery outside of 24 month window type 5 for write_off oustside of 24 months
****/

data def_obs_with_recovery_period;
set recovered_accounts2 ( keep =  aggregate_key account_id default_start default_end recov_type in = a ) 
 	written_off2 ( keep =  aggregate_key account_id default_start default_end wo_type in = b) 
	defaults_only   ( keep =  aggregate_key account_id default_start default_end in = c)
;
if a then do;
	if recov_type = 0 then type = 1;
	else type = 4;
end;
else if b then do;
	if wo_type = 0 then type = 2;
	else type = 5;
end;
else if c then type = 3;


if type = 1 then type2 = 1;
if type = 2 then type2 = 2;
if type in (3,4,5) then type2 = 3;


drop recov_type wo_type;
run;


proc sql;
create table default_points as select * from
(
	select aggregate_key,
			account_id,
			final_defaultdate,
			final_defaultbal,
			write_off_ind,
			write_off_month,
			write_off_amt

from default_points
) as pt1
left join
(
	select * from def_obs_with_recovery_period
) as pt2
on pt1.aggregate_key = pt2.aggregate_key;
quit;

data costs;
set TNGDATA.tng_acct_collecttrst;
format account_id $80.;
txn_cat2 = substr(txn_type_category,1,1);
txn_date2 = intnx('month', txn_date, 1)-1;


if length(mtg_num) =7 and substrn(mtg_num,1,1)  ne '7' and 
(mtg_num*1>4010000 or (mtg_num*1>3010000 and mtg_num*1<4000000))
then account_id=catt("MCAP~",mtg_num,"~1");
else account_id=catt("MBS~",mtg_num);


run;

data costs2;
set costs;
if txn_cat2 in ('A', 'I', 'J', 'K');

cost_amount = txn_amount;
if txn_cat2 in ('A') then cost_amount = abs(txn_amount);
if txn_cat2 in ('I', 'J', 'K') then cost_amount = abs(txn_amount);
run;

proc sql;
create table cost_all as
select account_id,
		txn_date2,
		count(*) as num_txn,
		sum(cost_amount) as cost
from costs2
group by 1,2;
quit;


proc sort data=cost_all out=cost_all_dedup nodupkeys dupout = cost_dups;
by account_id txn_date2 num_txn cost;
run;


proc sql;
create table mbs_costs as
select account_id, txn_date2, sum(cost) as cost
from cost_all_dedup
group by account_id, txn_date2;
quit;



data different_lgd_attempt;
set default_points; 

/* table of defaults based on a table built by Bill Qu. 
Fields used from this table are aggregate_key, account_id, type_2, default_start and default_end.
Follow up with Bill for more details on how these fields are calculated
*/
where default_start<=&LGDD_DEF_END; /*ensuring we have 2 years of default data*/
run;



proc sql;
create table different_accts as
select *
from TNGDATA.TNG_ACCT_MO
where account_id in (select account_id from  different_lgd_attempt);
quit;


proc sql;
create table different_acctsb as
select a.*,b.*
from different_accts as a left join different_lgd_attempt as b 
on a.account_id=b.account_id and a.month_end_dt>=default_start
and a.month_end_dt<=default_end;
quit;








/*check PIT PD code for PIT_def4*/
proc sql;
create table different_acctsb as select
a.*,b.final_default_ind as true_default
from different_acctsb  as a left join NZUSER.TNG_STATUS  as b 
on a.account_id=b.account_id and a.month_end_dt=b.month_end_dt;quit;



data different_acctsC;
set different_acctsb (drop=write_off_month WRITE_OFF_AMT);
where type2 ne .; /* type2 field created by Bill Qu*/
run;

proc sql;
create table  different_acctsC as
select a.*,b.Write_off_month, b.WRITE_OFF_AMT
from  different_acctsC as a left join write_offs as b 
on a.account_id=b.account_id ;
quit;

data different_acctsC;
format previous_wo date9.0;
set different_acctsC;
previous_wo=intnx('month',write_off_month,-1,'end');
run;

proc sort data=different_acctsC; by aggregate_key  month_end_dt; /* aggregate_key field created by Bill Qu. Follow up with him for more details */
run;

data final (keep= account_id month_end_dt);
set different_acctsC;
by account_id;
if last.account_id then output;
run;


proc sql;
create table different_acctsC as
select a.*,b.month_end_dt format date9.0 as final_month
from different_acctsC as a left join final as b
on a.account_id=b.account_id;
quit; 

/*new stuff to include costs*/


data different_acctsC_MBS;
set different_acctsC;
where mtg_provider_desc="Mortgage Broker Services";
run;

data different_acctsC_WL;
set different_acctsC;
where mtg_provider_desc ne "Mortgage Broker Services";
run;

proc sql;
create table different_acctsC_MBS as
select a.*,b.cost 
from different_acctsC_MBS as a left join mbs_costs as b
on a.account_id=b.account_id and a.month_end_dt=b.txn_date2;
quit;

data different_acctsC_MBS;
set different_acctsC_MBS;
if cost=. then cost=0;
run;

proc transpose data=different_acctsC_MBS out = direct_cost prefix = direct_cost_;
by aggregate_key;
var cost;
run;

proc sort data=different_acctsC; by aggregate_key  month_end_dt;
run;

data temp3;
set different_acctsC;
by aggregate_key;
recovery_time_point=intck('month',default_start, month_end_dt);
run;

proc transpose data=temp3 out = rec_time prefix = rec_time_;
by aggregate_key;
var recovery_time_point;
run;

data temp4;
set different_acctsC;
if ACCRUED_INTEREST_AMT ne . then 
balance_interest= end_principal_balance +ACCRUED_INTEREST_AMT;
else balance_interest= end_principal_balance ;
run;

proc sort data=temp4; by aggregate_key month_end_dt;
run;

data temp5;
set temp4;
by aggregate_key;

prev_end_balance = lag(balance_interest);

if first.aggregate_key then do;
                prev_end_balance = .;
                balance_change = 0;
end;
* if account is cured, then the last "default" balance is used for cured amount;
else if last.aggregate_key and type2 = 1 then do;
                balance_change = prev_end_balance;
end;
else do;
                balance_change = prev_end_balance - balance_interest;
end;

run;

/*** transpose to generate cash flow ***/
proc transpose data=temp5 out = cash_flow prefix = bal_;
by aggregate_key;
var balance_change;
run;


proc sql;
create table different_acctsC_MBS as
select a.*,b.*
from different_acctsC_MBS as a left join cash_flow as b
on a.aggregate_key=b.aggregate_key;
quit;

proc sql;
create table different_acctsC_WL as
select a.*,b.*
from different_acctsC_WL as a left join cash_flow as b
on a.aggregate_key=b.aggregate_key;
quit;

proc sql;
create table different_acctsC_MBS as
select a.*,b.*
from different_acctsC_MBS as a left join direct_cost as b
on a.aggregate_key=b.aggregate_key;
quit;


proc sql;
create table different_acctsC_MBS as
select a.*,b.*
from different_acctsC_MBS as a left join rec_time as b
on a.aggregate_key=b.aggregate_key;
quit;

proc sql;
create table different_acctsC_WL as
select a.*,b.*
from different_acctsC_WL as a left join rec_time as b
on a.aggregate_key=b.aggregate_key;
quit;

proc sort data=different_acctsC_WL; by account_id  month_end_dt;
run;

data different_acctsd_WL;
set different_acctsC_WL;
by account_id;
if month_end_dt=default_start then j=0;
j+1;
run;

/**starting change for indirect costs*/

data different_acctsd_WL;
set different_acctsd_WL;
indicator_bal=intck('month',default_start, month_end_dt) +1;
time_in_default=intck('month',default_start,default_end);
work_out_period=min(time_in_default+1,25);
months_in_default=indicator_bal-1;
indicator_bal2=indicator_bal+1;
indicator_bal3=j+1;
if ACCRUED_INTEREST_AMT ne . then 
balance_interest= end_principal_balance +ACCRUED_INTEREST_AMT;
else balance_interest= end_principal_balance ;
run;

data add_2005;
	ARREAR_YEAR = 2005;
	month_cost = 5.33;
run;

data indirect_costs_month(keep=ARREAR_YEAR month_cost);
   set  add_2005 INTMED.indirect_costs;
run;

proc sort data=indirect_costs_month nodup;
by ARREAR_YEAR;
run;


data temp1;
set different_acctsd_WL;
array bal_[96];
array rec_time_[96];
array cost_year_[96];
/*indirect_cost=0;*/
recovery=0;

if indicator_bal>=work_out_period then recovery=0;
else do;

     do i=indicator_bal3 to work_out_period;

if bal_(i) ne . and rec_time_(i)<25  then
  recovery= recovery + bal_(i)/((1.1)**((rec_time_(i) - months_in_default )/12));
else 
  recovery=recovery;


if rec_time_(i-1) ne . and rec_time_(i-1)<25 and intnx('month', default_start, rec_time_(i-1), 'end') <default_end
then do;
    cost_year_(i-1) = year(intnx('month', default_start, rec_time_(i-1))) ;
end;

end;

end;
run;



%macro assign_ind_cost;
%do i = 1 %to 96;

	proc sql;
	create table temp%sysevalf(&i+1) as 
	select a.*, 
	       b.month_cost as month_cost_&i.
	from temp&i. a left join indirect_costs_month b 
	on a.cost_year_&i. = b.ARREAR_YEAR;
	quit;

	proc delete data=temp&i.;
	run;

%end;
%mend;

%assign_ind_cost

data different_acctsd_WL;
set temp97;
array rec_time_[96];
array month_cost_[96];
indirect_cost=0;

     do i=indicator_bal3 to work_out_period;

		if rec_time_(i-1) ne . and rec_time_(i-1)<25 and intnx('month', default_start, rec_time_(i-1), 'end') <default_end
		     then 
	         	indirect_cost=indirect_cost+ month_cost_(i-1)/((1.1)**((rec_time_(i-1) - months_in_default )/12));
	end;
run;

proc sort data=different_acctsd_WL; by account_id  month_end_dt;
run;

data different_acctsd_WL;
set different_acctsd_WL;
if months_in_default>=24 then LGD_=1 ;
else if  balance_interest =0 then lgd_=0;
else if previous_wo=month_end_dt then LGD_=write_off_amt/balance_interest; 
else if final_month<write_off_month and month_end_dt= final_month then LGD_=write_off_amt/balance_interest; 
else LGD_=(balance_interest -recovery)/balance_interest;
if months_in_default>=24 then LGD_totalcost_=1 ;
else if  balance_interest =0 then LGD_totalcost_=0;
else if previous_wo=month_end_dt then LGD_totalcost_=write_off_amt/balance_interest; 
else if final_month<write_off_month and month_end_dt= final_month then LGD_totalcost_=write_off_amt/balance_interest; 
else LGD_totalcost_=(balance_interest -recovery + indirect_cost)/balance_interest;
run;

*recovery with indirect cost: bal_, indirect_cost;
*balance_interest, recovery, indirect_cost;
*LGDD no cost: LGD_, LGDD: LGD_totalcost_;

data different_acctsd_WL;
set different_acctsd_WL;
LGD_no_cost=LGD_;
run;

proc sort data=different_acctsC_MBS; by account_id  month_end_dt;
run;quit;

data different_acctsd_MBS;
set different_acctsC_MBS;
by account_id;
if month_end_dt=default_start then j=0;
j+1;
run;

data different_acctsd_MBS;
set different_acctsd_MBS;
indicator_bal=intck('month',default_start, month_end_dt) +1;
time_in_default=intck('month',default_start,default_end);
work_out_period=min(time_in_default+1,25);/*change range for indicators*/
months_in_default=indicator_bal-1;
indicator_bal2=indicator_bal+1;
indicator_bal3=j+1;
if ACCRUED_INTEREST_AMT ne . then 
balance_interest= end_principal_balance +ACCRUED_INTEREST_AMT;
else balance_interest= end_principal_balance ;
run;

data temp1;
set different_acctsd_MBS;
array bal_[96];
array rec_time_[96];
array direct_cost_[96];
array cost_year_[96];

recovery=0;
final_cost=0;
/*indirect_cost=0;*/

if indicator_bal>=work_out_period then recovery=0;
else do;

	do i=indicator_bal3 to work_out_period;

			if bal_(i) ne . and rec_time_(i)<25 then
			    recovery= recovery + bal_(i)/((1.1)**((rec_time_(i) - months_in_default )/12));
			else 
		     	recovery=recovery;

			if direct_cost_(i-1) ne .  and rec_time_(i-1)<25 then
		   	    final_cost=final_cost +direct_cost_(i-1)/((1.1)**((rec_time_(i-1) - months_in_default )/12));
			      else final_cost=final_cost;

			if rec_time_(i-1) ne . and rec_time_(i-1)<25 
			     then cost_year_(i-1) = year(intnx('month', default_start, rec_time_(i-1)));
	end;

	if direct_cost_(work_out_period) ne . /*and work_out_period ne 25*/ then 
    	final_cost=final_cost +direct_cost_(work_out_period)/((1.1)**((rec_time_(work_out_period) - months_in_default )/12));
	else 
		final_cost=final_cost;
end;
run;

%macro assign_ind_cost;
%do i = 1 %to 96;

	proc sql;
	create table temp%sysevalf(&i+1) as 
	select a.*, 
	       b.month_cost as month_cost_&i.
	from temp&i. a left join indirect_costs_month b 
	on a.cost_year_&i. = b.ARREAR_YEAR;
	quit;


	proc delete data=temp&i.;
	run;

%end;
%mend;

%assign_ind_cost

data different_acctsd_MBS;
 set temp97;
array rec_time_[96];
array month_cost_[96];

indirect_cost=0;

    do i=indicator_bal3 to work_out_period;
		if rec_time_(i-1) ne . and rec_time_(i-1)<25 then	     
	         indirect_cost=indirect_cost+ month_cost_(i-1)/((1.1)**((rec_time_(i-1) - months_in_default )/12));
	end;
run;

proc sort data=different_acctsd_MBS; by account_id  month_end_dt;
run;

data different_acctsd_MBS;
set different_acctsd_MBS;
if months_in_default>=24 then LGD_=1 ;
else if  balance_interest =0 then lgd_=0;
else if previous_wo=month_end_dt then LGD_=write_off_amt/balance_interest; 
else if final_month<write_off_month and month_end_dt= final_month then LGD_=write_off_amt/balance_interest; 
else LGD_=(balance_interest -recovery + final_cost)/balance_interest;

if months_in_default>=24 then LGD_no_cost=1 ;
else if  balance_interest =0 then lgd_no_cost=0;
else if previous_wo=month_end_dt then LGD_no_cost=write_off_amt/balance_interest; 
else if final_month<write_off_month and month_end_dt= final_month then LGD_no_cost=write_off_amt/balance_interest; 
else LGD_no_cost=(balance_interest -recovery)/balance_interest;

if months_in_default>=24 then LGD_totalcost_=1 ;
else if  balance_interest =0 then LGD_totalcost_=0;
else if previous_wo=month_end_dt then LGD_totalcost_=write_off_amt/balance_interest; 
else if final_month<write_off_month and month_end_dt= final_month then LGD_totalcost_=write_off_amt/balance_interest; 
else LGD_totalcost_=(balance_interest -recovery + indirect_cost + final_cost)/balance_interest;
run;

data different_accts_total;
set different_acctsd_MBS different_acctsd_WL;
run;

data different_accts_total;
set different_accts_total;
if write_off_month ne . and month_end_dt>=write_off_month then exclude_status=1;
else exclude_status=0;
run;

data LGD_realized;
set different_accts_total;
where month(month_end_dt) = &MONTH and true_default ne "N"
and exclude_status=0;
run;

data LGD_realized;
set LGD_realized;

if lgd_totalcost_ >1 then lgd_totalcost_cap1=1;
else lgd_totalcost_cap1=lgd_totalcost_;
if lgd_totalcost_ >1.5 then lgd_totalcost_cap2=1.5;
else lgd_totalcost_cap2=lgd_totalcost_;
if lgd_totalcost_ >1.25 then lgd_totalcost_cap3=1.25;
else lgd_totalcost_cap3=lgd_totalcost_;

if lgd_no_cost >1.25 then lgd_no_cost_cap3=1.25;
else lgd_no_cost_cap3=lgd_no_cost;
run;

PROC SQL;
	CREATE TABLE INTMED.TNG_LGD_D_UPDATE_INTERMEDIATE as
	SELECT * FROM NZUSER.TNG_LGDD_Segmentation
	WHERE MONTH_END_DT = &_rundatelgd;
QUIT;

PROC SQL;
    CREATE TABLE INTMED.LGD_D_PROD_BKUP_&process_date. AS
    SELECT * FROM INTMED.TNG_LGD_D_UPDATE_INTERMEDIATE;
QUIT;

PROC SQL;
	CONNECT USING NZUSER AS IIASCON;
	EXECUTE(DELETE FROM &FRG_DB..TNG_LGDD_Segmentation WHERE MONTH_END_DT = &DEL_RECS)BY IIASCON;
QUIT;

proc sql;
	create table Work.TNG_LGDD_MOD_ALL_&process_date. as
	select t1.ACCOUNT_ID, 
          t1.MONTH_END_DT, 
          t1.END_PRINCIPAL_BALANCE, 
          t1.ACCRUED_INTEREST_AMT, 
          t1.CLOSE_DT, 
          t1.MONTHS_IN_DEFAULT, 
          t1.RECOVERY, 
          t1.FINAL_COST, 
          t1.INDIRECT_COST, 
          t1.LGD_ as LGD, 
          t1.LGD_totalcost_ as LGD_COSTS, 
		  t1.lgd_totalcost_cap1 as LGD_COSTS_CAP_100,
		  t1.lgd_totalcost_cap3 as LGD_COSTS_CAP_150
	from LGD_realized t1;
quit;

proc sql;
	create table INTMED.TNG_LGD_D_NEW_LOGIC_&PROCESS_DATE. as
	select * from TNG_LGDD_MOD_ALL_&process_date.
	where month_end_dt = &_rundatelgd;
quit;

DATA INTMED.TNG_LGD_D_UPDATE_INTERMEDIATE;
	SET INTMED.TNG_LGD_D_UPDATE_INTERMEDIATE;
	LGD_BKUP = LGD;
	LGD_COSTS_BKUP = LGD_COSTS;
	LGD_COSTS_CAP_100_BKUP = LGD_COSTS_CAP_100;
	LGD_COSTS_CAP_150_BKUP = LGD_COSTS_CAP_150;
RUN;

proc sql;
	update INTMED.TNG_LGD_D_UPDATE_INTERMEDIATE as t1
		set LGD = (Select LGD from INTMED.TNG_LGD_D_NEW_LOGIC_&PROCESS_DATE. as t2 where t1.account_id = t2.account_id and t1.Month_end_dt = t2.Month_end_dt),
		LGD_COSTS = (Select LGD_COSTS from INTMED.TNG_LGD_D_NEW_LOGIC_&PROCESS_DATE. as t3 where t1.account_id = t3.account_id and t1.Month_end_dt = t3.Month_end_dt),
		LGD_COSTS_CAP_100 = (Select LGD_COSTS_CAP_100 from INTMED.TNG_LGD_D_NEW_LOGIC_&PROCESS_DATE. as t4 where t1.account_id = t4.account_id and t1.Month_end_dt = t4.Month_end_dt),
		LGD_COSTS_CAP_150 = (Select LGD_COSTS_CAP_150 from INTMED.TNG_LGD_D_NEW_LOGIC_&PROCESS_DATE. as t5 where t1.account_id = t5.account_id and t1.Month_end_dt = t5.Month_end_dt)
		where month_end_dt=&_rundatelgd AND T1.ACCOUNT_ID IN (SELECT ACCOUNT_ID FROM INTMED.TNG_LGD_D_NEW_LOGIC_&PROCESS_DATE.);
quit;

PROC APPEND BASE=NZUSER.TNG_LGDD_Segmentation(BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=INTMED.TNG_LGD_D_UPDATE_INTERMEDIATE;
RUN;