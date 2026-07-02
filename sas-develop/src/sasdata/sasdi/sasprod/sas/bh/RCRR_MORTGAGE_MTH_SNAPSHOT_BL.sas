
proc sql;
connect using prodrcrr as hdcon;
create table bh.RCRR_MORTGAGE_MTH_SNAPSHOT_BL as select 
put(mort_num,7.) as an_acct_no
,an_transit
,loan_next_pymt_due_dt
,loan_non_revlvng_lmt_amt
,loan_revlvng_lmt_amt 
,LOAN_OS_AMT
,eff_tmstmp
,insrt_process_tmstmp

from connection to hdcon
(
select 
	mort_num 
	,cond_orig_branch_transit an_transit 
	,coalesce(state_next_sched_pymt_dt,date('1753-01-01')) loan_next_pymt_due_dt 
	,os_bal_coa_amt loan_non_revlvng_lmt_amt 
	,0 loan_revlvng_lmt_amt 
	,os_bal_coa_amt LOAN_OS_AMT 
	,mth_end_dt eff_tmstmp 
	,current_date insrt_process_tmstmp 
from 
prod_rcrr1.mortgage_mth_snapshot 
where 
(mth_end_dt >= (date_sub(current_date,30)) and mth_end_dt <= last_day(date_sub(current_date,30)))
and gl_acct_num = '1571664' 
and cond_prod_type_cd = 610 
and os_bal_coa_amt > 0

);
quit;

/*proc sql;*/
/*	connect using NZRRAP as nzcon;*/
/*	execute(delete from &rrap_db...RCRR_MORTGAGE_MTH_SNAPSHOT_BL) by nzcon;*/
/*quit;*/
/**/
/*proc append base=NZRRAP.RCRR_MORTGAGE_MTH_SNAPSHOT_BL data=bh.RCRR_MORTGAGE_MTH_SNAPSHOT_BL force; run;*/


proc sql;
	connect using EDBHT as dbcon;
	execute(delete from RISKSAS.RCRR_MORTGAGE_MTH_SNAPSHOT_BL) by dbcon;
/*change above schema to EDBHT in prod!!*/
quit;

proc append base=EDBHT.RCRR_MORTGAGE_MTH_SNAPSHOT_BL data=bh.RCRR_MORTGAGE_MTH_SNAPSHOT_BL force; run;