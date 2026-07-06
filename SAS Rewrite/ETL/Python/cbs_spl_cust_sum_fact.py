#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_spl_cust_sum_fact.py
#
#        USAGE: ./cbs_spl_cust_sum_fact.py bdate datetype
#
#  DESCRIPTION: SPL Customer Summary Fact -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for cbs_spl_cust_sum_fact table load, by Gordana, Suhel
#       AUTHOR: Justin Liu 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 07/20/2018 16:18:33 
#     REVIEWER: 
#     REVISION: ---
#    SRC_TABLE:  
#    TGT_TABLE: 
#===============================================================================
import sys,re,os
import subprocess
from datetime import datetime
import argparse
from hive_task import CBS_Configuration


parser = argparse.ArgumentParser(description='Usage for arguments')
parser.add_argument('bdate', type=str,
           help='business effective date')
parser.add_argument('datetype', type=str,
           help='date load type (m/w/d/a)')
args = parser.parse_args()

# prepare the two input arguments:

if (args.datetype.upper()  == 'M'):
	date_type = 'Monthly'
elif (args.datetype.upper()  == 'W'):
        date_type = 'Weekly'
elif (args.datetype.upper()  == 'D'):
        date_type = 'Dailyly'
elif (args.datetype.upper()  == 'A'):
        date_type = 'Adhoc'
else:
	print("[Fatal]: date load type is incorrect, please make sure the datetype argument should be m/w/d!")
	sys.exit(-1)
	
cf = CBS_Configuration(args.bdate)



# insert into table cbs_spl_cust_sum_fact table with monthly data

SQL1 = """
with acct_base as (
select cust_cid, product_type, account_num, primary_cust_flag, process_date, eff_dt, date_type 
from """ + cf.CBSDBName + """.cbs_acct_base
where eff_dt = '""" + cf.bdate + """'
AND date_type = '""" + date_type + """'
AND pit_status IN ('CUR', 'DEF', 'CHG') 
AND (MODEL_EXCL <> 'Y' or MODEL_EXCL IS NULL)
AND product_type = 'SPL'
), 
spl_cust as 
(
select x.cust_cid 
, count(x.account_num) as num_of_spl_acct
, sum(case when pit_status='CUR' then 1 else 0 end) as num_of_crnt
,sum(case when pit_status='DEF' then 1 else 0 end) as num_of_def
, sum(case when pit_status='CHG' then 1 else 0 end) as num_of_chg
, sum(case when pit_status = 'CUR' then principal_bal_amt else 0 end) as sum_crnt_bal
,sum(case when pit_status = 'DEF' then principal_bal_amt else 0 end) as sum_def_bal
, sum(case when pit_status = 'CHG' then principal_bal_amt else 0 end) as sum_chg_bal
, max(days_overdue) as spl_worst_dlqnt_days
, sum (case when pit_status ='CUR' and days_overdue > 0 then 1 else 0 end) as num_of_dlqnt_acct
, sum(guarnty_cnt) as grnty_cnt
, max(cast(months_between(add_months(date_add(last_day(x.eff_dt),1),-1),add_months(date_add(last_day(note_dt),1),-1)) as int)) max_time_on_books_2
, max(cast(months_between(add_months(date_add(last_day(x.eff_dt),1),-1),add_months(date_add(last_day(last_rgl_pymt_dt),1),-1)) as int)) as max_time_since_lst_orig_pymt
, sum(orig_loan_amt) as sum_orig_loan_amt
, sum(accr_intr_amt) as sum_accr_intr
, min(cast(months_between(add_months(date_add(last_day(early_mar_dt),1),-1),add_months(date_add(last_day(x.eff_dt),1),-1)) as int)) as min_closest_mat
, max(cast(months_between(add_months(date_add(last_day(last_pymt_dt),1),-1),add_months(date_add(last_day(x.eff_dt),1),-1)) as int)) as max_time_snc_last_pymt
, avg(motor_vehcl_val) as avg_motr_vhcl_val
, avg(scrty_oth_val) as avg_loan_val_oth
, sum(earned_mth_intr_amt) as sum_earned_mthly_intr
, sum(chrg_off_amt) as sum_chrg_off_amt
, avg(loan_term) as avg_loan_term
, min(loan_term) as min_loan_term
, max(loan_term) as max_loan_term
, avg(early_mat_term) as avg_early_mat_term
, min(early_mat_term) as min_early_mat_term
, max(early_mat_term) as max_early_mat_term
, avg(rgl_pymt_amt) as avg_reg_pymt_amt
, avg(intr_rt) as avg_intr_rt
, sum(booked_amt) as sum_booked_amt
, max (case when sub_prod_cd='Rate Subvented' then 1 else 0 end) as subvented_ind
, max (case when sub_portfl_cd='DIRECT' then 1 else 0 end) as direct_ind
, sum (case when upper(trim((prod_cd)))<> 'AUTO' then 1 else 0 end) as num_of_non_auto_loans
, sum (case when upper(trim((prod_cd)))= 'AUTO' then 1 else 0 end) as num_of_auto_loans
from acct_base x 
left outer join (select a.acct_num 
,a.eff_dt
,a.date_type
,principal_bal_amt
,days_overdue
,guarnty_cnt
,note_dt
,last_rgl_pymt_dt
,orig_loan_amt,accr_intr_amt
,early_mar_dt
,last_pymt_dt
,motor_vehcl_val
,scrty_oth_val
,earned_mth_intr_amt
,chrg_off_amt,loan_term
,early_mat_term
,rgl_pymt_amt
,intr_rt,booked_amt
,pit_status
,sub_prod_cd
,sub_portfl_cd
,prod_cd
from """ + cf.CBSDBName + """.risk_spl_acct_snapshot a 
left outer join """ + cf.CBSDBName + """.risk_spl_acct_drvd_vars b 
ON a.acct_num = b.acct_num and a.eff_dt = b.eff_dt and a.date_type = b.date_type
and a.date_type = '""" + date_type + """'
and a.eff_dt = '""" + cf.bdate + """') y 
ON lpad(x.account_num, 23, '0') = lpad(y.acct_num, 23, '0')
and x.eff_dt = y.eff_dt 
and x.date_type = y.date_type 
where x.eff_dt = '""" + cf.bdate + """'
and x.date_type = '""" + date_type + """'
group by x.cust_cid
)
insert overwrite table """ + cf.CBSDBName + """.cbs_spl_cust_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,num_of_spl_acct
,num_of_crnt
,num_of_def
,num_of_chg
,sum_crnt_bal
,sum_def_bal
,sum_chg_bal
,spl_worst_dlqnt_days
,num_of_dlqnt_acct
,grnty_cnt
,max_time_on_books_2
,max_time_since_lst_orig_pymt
,sum_orig_loan_amt
,sum_accr_intr
,min_closest_mat
,max_time_snc_last_pymt
,avg_motr_vhcl_val
,avg_loan_val_oth
,sum_earned_mthly_intr
,sum_chrg_off_amt
,avg_loan_term
,min_loan_term
,max_loan_term
,avg_early_mat_term
,min_early_mat_term
,max_early_mat_term
,avg_reg_pymt_amt
,avg_intr_rt
,sum_booked_amt
,subvented_ind
,direct_ind
,num_of_non_auto_loans
,num_of_auto_loans
from spl_cust
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
