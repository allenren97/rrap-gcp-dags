#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_ip_mut_fund_sum_fact.py
#
#        USAGE: ./cbs_ip_mut_fund_sum_fact.py bdate datetype
#
#  DESCRIPTION: IP Mutual Fund Account Snapshot -- """ + date_type + """ job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for cbs_ip_mut_fund_sum_fact table load
#       AUTHOR: Justin Liu, Suhel 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 11/23/2018 16:18:33 
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



# insert into table cbs_ip_mut_fund_sum_fact table with """ + date_type + """ data

SQL1 = """

with mut_sum as (
select a.mfs_primary_owner_cif_id as cust_cid
, a.eff_dt
, a.date_type
, sum(case when b.ci_plan_product_code not like 'NR%' and  b.ci_plan_product_code not like 'BSAMP%' then 1 else 0 end) as num_mut_fund_reg_acct
, sum(case when b.ci_plan_product_code not like 'NR%' and  b.ci_plan_product_code not like 'BSAMP%' then a.mfs_fund_closing_book_value else 0 end) as sum_mut_fund_reg_amt
, sum(case when b.ci_plan_product_code like 'NR%' or  b.ci_plan_product_code like 'BSAMP%' then 1 else 0 end) as num_mut_fund_non_reg_acct
, sum(case when b.ci_plan_product_code like 'NR%' or  b.ci_plan_product_code like 'BSAMP%' then a.mfs_fund_closing_book_value else 0 end) as sum_mut_fund_non_reg_amt
, sum(case when b.ci_base_account_type='RRSP' then 1 else 0 end) as num_mut_fund_rrsp_acct
, sum(case when b.ci_base_account_type='RRSP' then a.mfs_fund_closing_book_value else 0 end) as sum_mut_fund_rrsp_amt
, sum(case when b.ci_base_account_type IN ('RESF','RESI') then 1 else 0 end) as num_mut_fund_resp_acct
, sum(case when b.ci_base_account_type IN ('RESF','RESI') then a.mfs_fund_closing_book_value else 0 end) as sum_mut_fund_resp_amt
, sum(case when b.ci_base_account_type='TFSA' then 1 else 0 end) as num_mut_fund_tfsa_acct
, sum(case when b.ci_base_account_type='TFSA' then a.mfs_fund_closing_book_value else 0 end) as sum_mut_fund_tfsa_amt
from """ + cf.CBSDBName + """.RISK_IP_MUT_FUND_ACCT_SNAPSHOT a 
inner join """ + cf.CBSDBName + """.RISK_IP_CI_CUST_PLAN_ACCT_SNAPSHOT b
ON a.mfs_plan_acct_num = b.ci_plan_acct_num 
and a.mfs_primary_owner_cif_id = b.ci_primary_owner_cif_id
and a.eff_dt = b.eff_dt
and a.date_type = b.date_type
where a.mfs_fund_status = '0'
and a.eff_dt = '""" + cf.bdate + """'
and a.date_type = '""" + date_type + """'
group by a.mfs_primary_owner_cif_id, a.eff_dt, a.date_type
)

insert overwrite table """ + cf.CBSDBName + """.cbs_ip_mut_fund_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')

select cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
, num_mut_fund_reg_acct
, sum_mut_fund_reg_amt
, num_mut_fund_non_reg_acct
, sum_mut_fund_non_reg_amt
, num_mut_fund_rrsp_acct
, sum_mut_fund_rrsp_amt
, num_mut_fund_resp_acct
, sum_mut_fund_resp_amt
, num_mut_fund_tfsa_acct
, sum_mut_fund_tfsa_amt
from mut_sum 

"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
