#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_mort_cust_sum_fact.py
#
#        USAGE: ./cbs_mort_cust_sum_fact.py bdate datetype
#
#  DESCRIPTION: Mortgage Customer Summary Fact -- """ + date_type + """ job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for cbs_mort_cust_sum_fact table load, by Gordana
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



# insert into table cbs_mort_cust_sum_fact table with """ + date_type + """ data

SQL1 = """
with acct_base as (
select cust_cid, product_type, account_num, primary_cust_flag, process_date, eff_dt, date_type 
from """ + cf.CBSDBName + """.cbs_acct_base
where eff_dt = '""" + cf.bdate + """'
AND date_type = '""" + date_type + """'

AND date_type = '""" + date_type + """'
AND pit_status IN ('CUR', 'DEF', 'CHG') 
and (MODEL_EXCL <> 'Y' or MODEL_EXCL IS NULL)
AND product_type = 'MOR'
), 
mort_cust as
(
select x.cust_cid 
,sum(prpy_amt) as sum_prpty_amt
,avg(amort_mths) as avg_amort 
,sum(case when pit_stat_ver_1_cd='DEF' then crnt_bal_amt else 0 end) as sum_def_bal
,count(x.account_num) as num_mort
,sum(case when pit_stat_ver_1_cd='CUR' then 1 else 0 end) as num_mort_crnt
,sum(case when pit_stat_ver_1_cd='DEF' then 1 else 0 end) as num_mort_def
,sum(case when pit_stat_ver_1_cd='CUR' then crnt_bal_amt else 0 end) as sum_crnt_bal
,max(dlqnt_day_cnt) as worst_mort_dlqnt_days
,sum(case when pit_stat_ver_1_cd='CUR' and dlqnt_day_cnt >0 then 1 else 0 end) as num_mort_dlqnt
,max(cast(months_between(add_months(date_add(last_day(x.eff_dt),1),-1),add_months(date_add(last_day(intr_adj_dt),1),-1)) as int)) as max_time_on_books
,min(cast(months_between(add_months(date_add(last_day(crnt_term_mat_dt),1),-1),add_months(date_add(last_day(x.eff_dt),1),-1)) as int)) as min_time_to_term_mat
,min(cast(months_between(add_months(date_add(last_day(x.eff_dt),1),-1),add_months(date_add(last_day(rnewl_dt),1),-1)) as int)) as min_time_since_rcnt_rnewl
,sum(intr_due_amt) as sum_intr_due_amt
,avg(lend_val) as avg_prpty_val
,sum(intr_accr_amt) as sum_intr_accr_amt
,min(
case when float_cd in ('W', 'B', 'S') then (cast(months_between(add_months(date_add(last_day(x.eff_dt),1),-1),add_months(date_add(last_day(week_frst_unpaid_dt),1),-1)) as int))
else (cast(months_between(add_months(date_add(last_day(x.eff_dt),1),-1),add_months(date_add(last_day(frst_unpd_dt),1),-1)) as int)) 
end) as min_time_since_unpd
from acct_base x 
left outer join (select a.mort_num
,a.eff_dt
,a.date_type
,amort_mths
,crnt_bal_amt
,intr_adj_dt
,crnt_term_mat_dt
,rnewl_dt
,intr_due_amt
,lend_val
,intr_accr_amt
,week_frst_unpaid_dt
,frst_unpd_dt
,float_cd
,prpy_amt
,pit_stat_ver_1_cd
,dlqnt_day_cnt
from """ + cf.CBSDBName + """.risk_mort_acct_snapshot a 
left outer join """ + cf.CBSDBName + """.risk_mort_acct_drvd_vars b 
ON a.mort_num = b.mort_num and a.eff_dt = b.eff_dt and a.date_type = b.date_type
and a.eff_dt = '""" + cf.bdate + """'
and a.date_type = '""" + date_type + """') y 
ON lpad(x.account_num, 23,'0') = lpad(y.mort_num, 23, '0')
and x.eff_dt = y.eff_dt 
and x.date_type = y.date_type 
and x.date_type = '""" + date_type + """'
where x.eff_dt = '""" + cf.bdate + """'
group by x.cust_cid 
)
insert overwrite table """ + cf.CBSDBName + """.cbs_mort_cust_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_cid 
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,sum_prpty_amt
,avg_amort 
,sum_def_bal
,num_mort
,num_mort_crnt
,num_mort_def
,sum_crnt_bal
,worst_mort_dlqnt_days
,num_mort_dlqnt
,max_time_on_books
,min_time_to_term_mat
,min_time_since_rcnt_rnewl
,sum_intr_due_amt
,avg_prpty_val
,sum_intr_accr_amt
,min_time_since_unpd
from mort_cust
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
