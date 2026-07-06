#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_sav_cust_sum_fact.py
#
#        USAGE: ./risk_sav_cust_sum_fact.py bdate datetype
#
#  DESCRIPTION: Risk SAV account snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for risk_sav_cust_sum_fact table load, by Gordana
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
        date_type = 'Daily'
elif (args.datetype.upper()  == 'A'):
        date_type = 'Adhoc'
else:
	print("[Fatal]: date load type is incorrect, please make sure the datetype argument should be m/w/d!")
	sys.exit(-1)
	
cf = CBS_Configuration(args.bdate)



# insert into table risk_sav_cust_sum_fact table with monthly data

SQL1 = """
	set hive.execution.engine=tez;
	set hive.vectorized.execution.enabled = true;
	set hive.vectorized.execution.reduce.enabled = true;
	set hive.exec.parallel=true;
with sav_cust as 
(
select a. cust_cid as cust_cid
,sum(case when primary_cust_flag ='Y' then 1 else 0 end) as num_of_sav_acct_prim 
,sum(case when primary_cust_flag ='Y' then acct_bal_amt end) as sum_sav_bal_prim_amt 
,sum(case when primary_cust_flag ='N' then 1 else 0 end) as num_of_sav_acct_sec
,sum(case when primary_cust_flag ='N' then  acct_bal_amt end) as sum_sav_bal_sec_amt
,count(*) as num_of_sav_acct
,sum(acct_bal_amt) as sum_sav_bal_amt  
from
""" + cf.CBSDBName + """.cbs_acct_base a  join 
""" + cf.CBSDBName + """.risk_sav_acct_snapshot b
on substring(account_num,2,12)=b.acct_num
and a.eff_dt=b.eff_dt
and a.date_type = b.date_type
where trim(product_type)= 'SAV'
and a.eff_dt='""" + cf.bdate + """'
and a.date_type = '""" + date_type + """'
group by a.cust_cid 
)
insert overwrite table """ + cf.CBSDBName + """.risk_sav_cust_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_cid
,current_timestamp() as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,num_of_sav_acct_prim
,sum_sav_bal_prim_amt
,num_of_sav_acct_sec 
,sum_sav_bal_sec_amt
,num_of_sav_acct
,sum_sav_bal_amt
from sav_cust
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
