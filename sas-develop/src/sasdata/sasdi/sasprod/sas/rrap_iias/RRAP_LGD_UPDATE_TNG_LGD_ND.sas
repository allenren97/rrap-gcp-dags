
%RRAP_MOR_TNG_AUTOEXEC

%get_model_period_dates(product=mor);
%put start_period_dt: &start_period_dt;


data runmonth;
	format start_date end_date date9. start_date2 end_date2 yymmdd10. TNG_PROCESS_DATE YYMMN. a_year YEAR4.;
	start_date=intnx('month',"&start_period_dt"d,0,'end');
	end_date=intnx('month',start_date,-36,'end');
	start_date2=start_date;
	end_date2=end_date;
	TNG_PROCESS_DATE=START_DATE;
    A_YEAR=INTNX('YEAR',intnx('month',"&start_period_dt"d,0,'end'),0);
run;

proc sql;
	select start_date into :beginning from runmonth;
	select end_date into :complete from runmonth;
	select start_date2 into :beginning2 from runmonth;
	select end_date2 into :complete2 from runmonth;
	SELECT TNG_PROCESS_DATE INTO :P_DATE FROM RUNMONTH;
    SELECT A_YEAR INTO :A_YEAR FROM RUNMONTH;
quit;

%LET PROCESS_DATE=&P_DATE;
%LET _RUNDATELGDND="&complete"d;
%LET DEL_RECS=%bquote('&complete2');

%PUT &PROCESS_DATE;
%PUT &_RUNDATELGDND;
%PUT &DEL_RECS;


data recovery_data_all_&process_date.;
   set INTMED.recovery_data_all_&process_date.;
   if balance_change = . then balance_change = 0;
run;


/*--------------------------------------------------------------------
 * Obtaining the recoveries cashflow.
 *--------------------------------------------------------------------*/

proc transpose data=recovery_data_all_&process_date. out = cash_flow prefix = recov_;
   by aggregate_key;
   var balance_change;
run;


/*Change added here by will per Bill's change request via email. 1/17/15*/

proc sql noprint;
create table recovery_data2 as select * from
(
   select * from TNGDATA.default_points2
   where type2 = 2 and (final_defaultbal = write_off_amt) and (final_defaultdate = write_off_month)
) as pt1
left join
(
   select account_id,
         month_end_dt format date9.,
         final_default_ind,
         final_defaultdate,
         end_principal_balance,
         accrued_interest_amt,
         end_principal_balance + accrued_interest_amt as bal_with_interest
   from TNGDATA.def_obs_with_interest
   where def_ind = 1
) as pt3
on pt1.account_id  = pt3.account_id
and pt1.final_defaultdate = pt3.final_defaultdate
order by pt1.aggregate_key, pt3.month_end_dt
;
quit;


data recovery_data3;
set recovery_data2;
if bal_with_interest = . or bal_with_interest = 0 then delete;
run;

data write_off_amt_fix;
set recovery_data3;
by aggregate_key;
if last.aggregate_key;
run;





/*----------------------------------------------------------------------
 * LGD-ND Calculation without costs
 *----------------------------------------------------------------------*/

/*** join cashflow data into default dataset ***/
proc sql noprint;
create table def_points_w_cashflow as 
   select * 
            from(select * from TNGDATA.default_points2) as pt1
                left join
                (select * from cash_flow) as pt2
                on pt1.aggregate_key = pt2.aggregate_key
                left join
                (select aggregate_key,
                        month_end_dt,
                        bal_with_interest,
                        accrued_interest_amt,
                        bal_with_interest
                    from recovery_data_all_&process_date.) as pt3
                on pt1.aggregate_key = pt3.aggregate_key and pt1.final_defaultdate = pt3.month_end_dt
                left join
                (select aggregate_key, bal_with_interest as bal_fix
                   from write_off_amt_fix
                ) as pt4
                on pt1.aggregate_key = pt4.aggregate_key;
quit;


/*the following code added by will to make write_off_ind numeric*/

data def_points_w_cashflow (drop=write_off_ind rename=(write_off_ind2=write_off_ind)) ;
   set def_points_w_cashflow;
   if upcase(write_off_ind)='Y' then write_off_ind2=1;
      else if upcase(write_off_ind)='N' then write_off_ind2=0;
      else if write_off_ind=' ' then write_off_ind2=.;
run;




/*calculate raw lgd for each individual default point */

data def_points_w_lgd;
   set def_points_w_cashflow;
   /* CR 47 - Storing two years of non-discounted recoveries in new variable so it can be inserted into recovery table downstream */
   nondiscounted_recovery = sum(of recov_1 - recov_25);
   netvalue24 = netpv(0.1, 12, of recov_1 - recov_25);  /* Will changed the names of recov# to add underscore to match variable names from earlier code*/
   write_off_count = intck('month', final_defaultdate, write_off_month);
   if write_off_count <= 24 and write_off_amt ~= . then 
                                                       netvalue24 = (write_off_amt)/((1+0.1)**(write_off_count/12));
   lgd_def_bal_fixed = 0;
	if bal_with_interest  = 0 or bal_with_interest = . then
		lgd_def_bal = final_defaultbal + accrued_interest_amt;
	else lgd_def_bal = bal_with_interest;

	/* calculate 24 months lgd */
	if (lgd_def_bal - netvalue24) = 0 or (lgd_def_bal - netvalue24) = . then
		do;
			lgd24 = 0;
			lgd_def_bal_fixed = 0;
		end;
	else
		do;
			lgd24 = (lgd_def_bal - netvalue24)/lgd_def_bal;
			lgd_def_bal_fixed = lgd_def_bal;
		end;

	if write_off_ind = 1 and write_off_count<=24 then
		do;
			if bal_fix ~= . then
				do;
					lgd24 = netvalue24/bal_fix;
					lgd_def_bal_fixed = bal_fix;
				end;
			else
				do;
					lgd24 = netvalue24/lgd_def_bal;
					lgd_def_bal_fixed= lgd_def_bal;
				end;
		end;
run;


/*-------------------------------------------------------------------------------------------
 * During development, several accounts were detected to have charges posted after write off
 * which would result in abnormally high LGD, these accounts were manually adjusted below 
 *-------------------------------------------------------------------------------------------*/


data def_pts_with_lgd_fixed;
   set def_points_w_lgd;
   lgd_def_bal_fixed2 = lgd_def_bal_fixed;
   lgd24_fix = lgd24;
   if aggregate_key = 'MBS~200742-18566' then 
                                          do;
                                             lgd_def_bal_fixed2 = 285746.5;
                                             lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                          end;
   if aggregate_key = 'MBS~3392-18413' then 
                                        do;
                                           lgd_def_bal_FIXed2 = 45031.92;
                                           lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                       end;
   if aggregate_key = 'MBS~7000008018-17956' then 
                                              do;
                                                 lgd_def_bal_FIXed2 = 106136.65;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                             end;
   if aggregate_key = 'MBS~7000069059-18717' then 
                                              do;
                                                 lgd_def_bal_FIXed2 = 333455;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                             end;
   if aggregate_key = 'MBS~7000080794-18474' then 
                                              do;
                                                 lgd_def_bal_FIXed2= 110959.31;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;

                                             end;
   if aggregate_key = 'MBS~7000092181-18382' then 
                                              do;
                                                 lgd_def_bal_FIXed2= 100142.49;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                             end;
   if aggregate_key = 'MBS~7000134641-19023' then 
                                              do;
                                                 lgd_def_bal_FIXed2= 253101.55;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                             end;
   if aggregate_key = 'MBS~7700097393-18596' then 
                                              do;
                                                 lgd_def_bal_FIXed2= 48426.18;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                             end;
   if aggregate_key = 'MBS~7700109443-18747' then 
                                              do;
                                                 lgd_def_bal_FIXed2= 992243.1;
                                                 lgd24_fix = netvalue24/lgd_def_bal_fixed2;
                                             end;
run;


*OUTPUT monthly recovry amount recov_1 - recov_25;
/*data TNG_LGD.tng_lgdnd_recovey_data;*/
/*set def_pts_with_lgd_fixed (drop=recov_26-recov_121);*/
/*run;*/


/*-----------------------------------------------------------------
 * Calculating direct costs for LGD
 *-----------------------------------------------------------------*/


data direct_cost;
   set TNGDATA.tng_acct_collecttrst;
   format txn_date2 date9.;
   txn_cat = substr(txn_type_category, 1,1);
  if txn_cat in ('A', 'I', 'J', 'K');
   txn_date2 = intnx('month', txn_date, 1)-1;
  
   /* if the recovery data (record date not transaction date) is from before November 1st 2014 then use old calculation*/
   if month_end_dt < '01-NOV-2014'd then
                                     do;
                                        cost_amount = abs(txn_amount);
                                     end;
   /* if the recovery data is from before November 1st 2014 then use new calculation*/
      else do;
              cost_amount = txn_amount;
           end;
run;
 
/*Remove Duplictes and correct date boundary for recovery transactions*/
 
proc sql noprint;
create table direct_cost2 as
select mtg_num,
                month_end_dt,
                txn_date2 format date9.,
                count(*) as num_txn,
                sum(cost_amount) as cost
from direct_cost
group by 1,2,3;
quit;
 
/* added round function to cost variable on 10-Oct-2015
and added subsequent proc sort*/

data direct_cost3;
set direct_cost2 (rename=(cost=cost1));
cost=round(cost1,.01);
drop month_end_dt cost1;
run;
 
proc sort data=direct_cost3 out=direct_cost3_int;
by mtg_num txn_date2 num_txn cost;
run;

proc sort data=direct_cost3_int out=dir_costdedup nodupkeys;
by mtg_num txn_date2 num_txn cost;
run;
 
 
/*---------------------------------------------------------------------------
*NOTE DIRECT COST SHOULD ONLY BE APPLIED TO MBS ACCOUNTS,
*SINCE OTHER PORTFOLIO'S RECOVERY DATA INCLUDE COST DATA ALREADY
 *---------------------------------------------------------------------------*/
 
proc sql noprint;
   create table defs2 as
      select account_id,
             aggregate_key,
             final_defaultdate,
             final_defaultbal,
             default_start,
             default_end,
             lgd24_fix,
             netvalue24,
             lgd_def_bal_fixed2,
             write_off_ind,
             write_off_count
         from def_pts_with_lgd_fixed; /* dataset from raw LGD calculation */
quit;
 
data defs2;
   set defs2;
   num1 = find(account_id, '~', 'i', 1);
   num2 = find(account_id, '~', 'i', num1+1);
   length = length(account_id);
   mortgage_num = substr(account_id, num1+1, num2 - num1-1) + 0;  /*Bill confirmed that the invalid argument note here does not affect end results*/
   drop num1 num2 length;
run;
 
 
 
 
 
proc sql noprint;
   create table defs_with_cost_mbs as
      select *
         from (select aggregate_key,
                      account_id,
                      default_start,
                      default_end,
                      mortgage_num
                  from defs2 where substr(account_id, 1, 3) = 'MBS') as pt1
              left join
              (select * from dir_costdedup ) as pt2
              on pt1.mortgage_num = pt2.mtg_num and default_start <= txn_date2 <= default_end;  /*will change mort_num to mtg_num to get correct variable from source*/
quit;
 
 
data defs_with_cost_mbs2;
   set defs_with_cost_mbs;
   time_diff = intck('month', default_start, txn_date2);
   cost_npv = (cost)/((1+0.1)**(time_diff/12)); 
run;

*direct cost amount;
/*proc sql;*/
/*create table TNG_LGD.tng_lgdnd_cost_data as*/
/*select * from defs_with_cost_mbs2*/
/*where time_diff = . or time_diff <= 24*/
/*group by time_diff, aggregate_key, account_id, default_start, default_end;*/
/*quit;*/


/*------------------------------------------------------
 * 24 Months MBS Direct Cost 
 * can be adapted to calculate 36/48/60 months
 *------------------------------------------------------*/

proc sql noprint;
   create table mbs_cost24 as 
      select * 
         from(select aggregate_key,
                     account_id,      
                     default_start,
                     default_end,      
                     sum(cost_npv) as tot_cost
         from defs_with_cost_mbs2
         where time_diff = . or time_diff <= 24
         group by 1,2,3,4);
quit;


proc sql noprint;
   create table mbs_cost24_v2 as 
      select * 
         from(select * from defs2
                where substr(account_id, 1, 3) = 'MBS') as pt1
             left join
             (select aggregate_key,
                     tot_cost
                 from mbs_cost24) as pt2
             on pt1.aggregate_key = pt2.aggregate_key;
quit;


/* calculates LGD with direct cost */

data mbs_cost24_v3;
   set mbs_cost24_v2;
   if tot_cost = . then tot_cost = 0;
   if (lgd_def_bal_fixed2 - netvalue24 + tot_cost) = 0 then 
                                                        do;
                                                           lgd_cost = 0;
                                                        end;
      else do;
              lgd_cost = (lgd_def_bal_fixed2 - netvalue24 + tot_cost)/lgd_def_bal_fixed2;
           end;
   if write_off_ind = 1 and write_off_count <= 24 then 
                                                   do;
                                                      lgd_cost = (netvalue24 + tot_cost )/lgd_def_bal_fixed2;
                                                   end;
   if lgd_cost <= 0 then lgd_cost_cap125 = 0;
      else if lgd_cost > 1.5 then lgd_cost_cap125 = 1.5;
      else lgd_cost_cap125 = lgd_cost;
run;



/*---------------------------------------------------------------------------
 * Calculating the indirect costs for the LGD calculation with costs
 *---------------------------------------------------------------------------*/




proc sort data=INTMED.indirect_costs OUT=indirect_costs nodup;
by Arrear_Year;
run;



proc sql noprint;
	create table indirect_costs2 as 
	select t1.*,
			t2.ind_cost/12 as indirect_cost
	from Work.recovery_data_all_&process_date. t1
	left join indirect_costs t2
	on year(t1.month_end_dt) = t2.ARREAR_YEAR;
quit;

data indirect_costs2;
set indirect_costs2;
if indirect_cost = . then indirect_cost = 0;
run;

proc sort data=indirect_costs2;
by aggregate_key obs_dt;
run;


proc transpose data=indirect_costs2 out = indirect_flow prefix = ind_cost_;
   by aggregate_key;
   var indirect_cost;
run;

/* calculated the npv of indirect cost */
data indirect_flow;
   set indirect_flow;
   drop _name_;
   indirect_cost_npv24 = netpv(0.1, 12, of ind_cost_1 - ind_cost_25);
run;

*OUTPUT monthly indirect cost amount: ind_cost_1 - ind_cost_25;
/*data tng_lgd.tng_lgdnd_indirect_cost;*/
/*set indirect_flow (drop=ind_cost_26-ind_cost_122);*/
/*run;*/

/*------------------------------------------------------------------------------
 * Calculating LGD with direct and indirect costs.
 *------------------------------------------------------------------------------*/

/* JD - 27SEP2015 - Adding nondiscounted_recovery and lgd_def_bal for CR #47. */
proc sql noprint;
create table def_obs_with_lgd as 
   select * 
      from(select account_id,
                  insurer_desc,  /*will removed "insurance_final" from query because it could not be found in source data and is not used in the code*/
                  month_end_dt,
                  def_ind,
                  final_defaultdate,
                  final_defaultbal,
                  provider,
                  end_principal_balance
              from TNGDATA.def_obs_with_interest
              where def_ind = 1) as pt1
          left join
          (select account_id,
                  aggregate_key,
                  lgd24,
                  lgd_def_bal_fixed,
                  netvalue24,
                  final_defaultdate,
                  write_off_ind,
                  write_off_amt,
                  write_off_month,
                  type,
                  type2,
                  nondiscounted_recovery,
                  lgd_def_bal
               from def_pts_with_lgd_fixed) as pt2
         on pt1.account_id = pt2.account_id and pt1.final_defaultdate = pt2.final_defaultdate;
quit;

proc sql noprint;
   create table lgd_temp as 
      select * 
         from(select account_id,
                     aggregate_key, /*will removed "insurance_final" from query because it could not be found in source data and is not used in the code*/
                     month_end_dt,
					 final_defaultbal,
					 FINAL_DEFAULTDATE,
                     lgd24_fix,
                     lgd_def_bal_fixed2,
                     write_off_ind,
                     write_off_count,
                     netvalue24
                  from def_pts_with_lgd_fixed) as pt1
             left join
             (select aggregate_key,
                     tot_cost as dir_cost24,
                     lgd_cost as lgd_dircost24
				
                 from mbs_cost24_v3) as pt2
             on pt1.aggregate_key = pt2.aggregate_key
             left join(select aggregate_key,
                              indirect_cost_npv24
                          from indirect_flow) as pt3
             on pt1.aggregate_key = pt3.aggregate_key;
quit;



data Work.TNG_LGDND_MOD_ALL_&process_date. ;
   set lgd_temp;
/*extra direct_cost should only be used for tangerine direct accounts */
   if substr(account_id, 1, 3) ~= 'MBS' then 
                                         do;
                                            dir_cost24 = 0;
                                            lgd_dircost24 = lgd24_fix;
                                         end;
if final_defaultbal <2000 then 
       tot_cost24 = dir_cost24;
    else
       tot_cost24 = dir_cost24 + indirect_cost_npv24;

              lgd_costs = (lgd_def_bal_fixed2 - netvalue24 + tot_cost24)/lgd_def_bal_fixed2;
/*			  lgd_no_costs = (lgd_def_bal_fixed2 - netvalue24)/lgd_def_bal_fixed2;*/
  if write_off_ind = 1 and write_off_count <= 24 then 
       	lgd_costs = (netvalue24 + tot_cost24 )/lgd_def_bal_fixed2;
/*		lgd_no_costs = (netvalue24)/lgd_def_bal_fixed2;*/
	if lgd_costs <= 0 then lgd_cost_cap125 = 0;
      else if lgd_costs > 1.25 then lgd_cost_cap125 = 1.25;
      else lgd_cost_cap125 = lgd_costs;
	  lgd_costs_cap_150 = lgd_cost_cap125;
   	  if lgd_costs_cap_150 > 1 then lgd_costs_cap_100 = 1;
   	  else lgd_costs_cap_100 = lgd_costs_cap_150;
/*	if lgd_no_costs <= 0 then lgd_no_cost_cap125 = 0;*/
/*      else if lgd_no_costs > 1.25 then lgd_no_cost_cap125 = 1.25;*/
/*      else lgd_no_cost_cap125 = lgd_no_costs;*/
run;

proc sql noprint;
create table tng_lgd_nd_realized_final_temp as
select pt1.*,
pt2.tot_cost24,
pt2.lgd_costs,
pt2.lgd_costs_cap_150,
pt2.lgd_costs_cap_100
from (select a.*
from def_obs_with_lgd (rename=(lgd24=lgd ))as a) as pt1
left join
(select b.aggregate_key,
b.tot_cost24,
b.lgd_costs,
b.lgd_costs_cap_150,
b.lgd_costs_cap_100
from Work.TNG_LGDND_MOD_ALL_&process_date. as b) as pt2
on pt1.aggregate_key = pt2.aggregate_key;
quit;

proc sql;
create table INTMED.TNG_LGD_ND_NEW_LOGIC_&PROCESS_DATE. as
select * from tng_lgd_nd_realized_final_temp
where month_end_dt = &_rundatelgdnd;
quit;

PROC SQL;
   CREATE TABLE INTMED.TNG_LGD_ND_UPDATE_INTERMEDIATE as
   SELECT * FROM NZUSER.TNG_LGD_ND_REALIZED_FINAL
   WHERE month_end_dt = &_rundatelgdnd;
QUIT;

PROC SQL;
   CREATE TABLE INTMED.LGD_ND_PROD_BKUP_&process_date. AS
   SELECT * FROM INTMED.TNG_LGD_ND_UPDATE_INTERMEDIATE;
QUIT;

PROC SQL;
   CONNECT USING NZUSER AS IIASCON;
   EXECUTE(DELETE FROM &FRG_DB..TNG_LGD_ND_REALIZED_FINAL WHERE month_end_dt = &DEL_RECS)BY IIASCON;
   DISCONNECT FROM IIASCON;
QUIT;

DATA INTMED.TNG_LGD_ND_UPDATE_INTERMEDIATE;
   SET INTMED.TNG_LGD_ND_UPDATE_INTERMEDIATE;
   LGD_BKUP = LGD;
	LGD_COSTS_BKUP = LGD_COSTS;
	LGD_COSTS_CAP_100_BKUP = LGD_COSTS_CAP_100;
	LGD_COSTS_CAP_150_BKUP = LGD_COSTS_CAP_150;
RUN;

proc sql;
update INTMED.TNG_LGD_ND_UPDATE_INTERMEDIATE as t1
set LGD = (Select LGD from INTMED.TNG_LGD_ND_NEW_LOGIC_&PROCESS_DATE. as t2 where t1.account_id = t2.account_id and t1.Month_end_dt= t2.Month_end_dt),
LGD_COSTS = (Select LGD_COSTS from INTMED.TNG_LGD_ND_NEW_LOGIC_&PROCESS_DATE. as t3 where t1.account_id = t3.account_id and t1.Month_end_dt = t3.Month_end_dt),
LGD_COSTS_CAP_100 = (Select LGD_COSTS_CAP_100 from INTMED.TNG_LGD_ND_NEW_LOGIC_&PROCESS_DATE. as t4 where t1.account_id = t4.account_id and t1.Month_end_dt = t4.Month_end_dt),
LGD_COSTS_CAP_150 = (Select LGD_COSTS_CAP_150 from INTMED.TNG_LGD_ND_NEW_LOGIC_&PROCESS_DATE. as t5 where t1.account_id = t5.account_id and t1.Month_end_dt = t5.Month_end_dt)
where month_end_dt=&_rundatelgdnd AND T1.ACCOUNT_ID IN (SELECT ACCOUNT_ID FROM INTMED.TNG_LGD_ND_NEW_LOGIC_&PROCESS_DATE.);
quit;

PROC APPEND BASE=NZUSER.TNG_LGD_ND_REALIZED_FINAL(BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=INTMED.TNG_LGD_ND_UPDATE_INTERMEDIATE;
RUN;