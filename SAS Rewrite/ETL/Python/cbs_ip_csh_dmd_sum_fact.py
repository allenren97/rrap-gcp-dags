#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_ip_dhs_cash_dmd_acct_snapshot.py
#
#        USAGE: ./risk_ip_dhs_cash_dmd_acct_snapshot.py bdate datetype
#
#  DESCRIPTION: DHS Snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for risk_ip_dhs_cash_dmd_acct_snapshot table load, by Gordana
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



# insert into table risk_ip_dhs_cash_dmd_acct_snapshot table with monthly data

SQL1 = """
with dhs_plan as
(
select dhs_primary_owner_cif_id as cust_cid
,sum(case when (ci_plan_product_code <> 'BSAMP' and ci_plan_product_code not like 'NRS%') then 1 else 0 end) as num_csh_dmd_reg_acct
,sum(case when (ci_plan_product_code <> 'BSAMP' and ci_plan_product_code not like 'NRS%') then dhs_curr_cash_balance else 0 end) as sum_csh_dmd_reg_amt 
,sum(case when (ci_plan_product_code = 'BSAMP' or ci_plan_product_code like 'NRS%') then 1 else 0 end) as num_csh_dmd_non_reg_acct
,sum(case when (ci_plan_product_code = 'BSAMP' or ci_plan_product_code like 'NRS%') then dhs_curr_cash_balance else 0 end) as sum_csh_dmd_non_reg_amt 
,sum(case when ci_base_account_type='RRSP' then 1 else 0 end) as num_csh_dmd_rrsp_acct
,sum(case when ci_base_account_type='RRSP' then dhs_curr_cash_balance else 0 end) as sum_csh_dmd_rrsp_amt
,sum(case when ci_base_account_type in ('RESF','RESI') then 1 else 0 end) as num_csh_dmd_resp_acct
,sum(case when ci_base_account_type in ('RESF','RESI') then dhs_curr_cash_balance else 0 end) as sum_csh_dmd_resp_amt
,sum(case when ci_base_account_type='TFSA' then 1 else 0 end) as num_csh_dmd_tfsa_acct
,sum(case when ci_base_account_type='TFSA' then dhs_curr_cash_balance else 0 end) as sum_csh_dmd_tfsa_amt
 from """ + cf.CBSDBName + """.risk_ip_dhs_cash_dmd_acct_snapshot cash,
      """ + cf.CBSDBName + """.risk_ip_ci_cust_plan_acct_snapshot plan
where dhs_plan_acct_num = ci_plan_acct_num 
and dhs_primary_owner_cif_id = ci_primary_owner_cif_id 
and cash.eff_dt = plan.eff_dt
and cash.date_type = plan.date_type
and cash.eff_dt = '""" + cf.bdate + """'
and cash.date_type = '""" + date_type + """'
group by dhs_primary_owner_cif_id
)
insert overwrite table """ + cf.CBSDBName + """.cbs_ip_csh_dmd_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,num_csh_dmd_reg_acct     
,sum_csh_dmd_reg_amt     
,num_csh_dmd_non_reg_acct
,sum_csh_dmd_non_reg_amt 
,num_csh_dmd_rrsp_acct   
,sum_csh_dmd_rrsp_amt    
,num_csh_dmd_resp_acct   
,sum_csh_dmd_resp_amt    
,num_csh_dmd_tfsa_acct   
,sum_csh_dmd_tfsa_amt    
from dhs_plan            
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
