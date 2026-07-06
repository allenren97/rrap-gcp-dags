#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_ip_term_acct_snapshot.py
#
#        USAGE: ./cbs_ip_term_sum_fact.py bdate datetype
#
#  DESCRIPTION: CBS IP Term Summary Fact -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for cbs_ip_term_sum_fact table load, by Rahim
#       AUTHOR: Justin Liu --Sql created by Rahim
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 11/23/2018 14:18:33 
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
insert overwrite table """ + cf.CBSDBName + """.cbs_ip_term_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select
a.TAS_PRIMARY_OWNER_CIF_ID as CUST_CID
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,sum(case when upper(b.ci_plan_product_code) <> 'BSAMP' and upper(ci_plan_product_code) not like 'NRS%' then 1 else 0 end) as num_term_reg_acct
,sum(case when upper(b.ci_plan_product_code) <> 'BSAMP' and upper(ci_plan_product_code) not like 'NRS%' then a.TAS_CURRENT_BOOK_VALUE else 0 end) as sum_term_reg_amt
,sum(case when upper(b.ci_plan_product_code) = 'BSAMP' or upper(ci_plan_product_code) like 'NRS%' then 1 else 0 end) as num_term_non_reg_acct
,sum(case when upper(b.ci_plan_product_code) = 'BSAMP' or upper(ci_plan_product_code) like 'NRS%' then a.TAS_CURRENT_BOOK_VALUE else 0 end) as sum_term_non_reg_amt
,sum(case when upper(b.CI_BASE_ACCOUNT_TYPE) = 'RRSP' then 1 else 0 end) as num_term_rrsp_acct
,sum(case when upper(b.CI_BASE_ACCOUNT_TYPE) = 'RRSP' then a.TAS_CURRENT_BOOK_VALUE else 0 end) as sum_term_rrsp_amt
,sum(case when upper(b.CI_BASE_ACCOUNT_TYPE) in ('RESF','RESI') then 1 else 0 end) as num_term_resp_acct
,sum(case when upper(b.CI_BASE_ACCOUNT_TYPE) in ('RESF','RESI') then a.TAS_CURRENT_BOOK_VALUE else 0 end) as sum_term_resp_amt
,sum(case when upper(b.CI_BASE_ACCOUNT_TYPE) = 'TFSA' then 1 else 0 end) as num_term_tfsa_acct
,sum(case when upper(b.CI_BASE_ACCOUNT_TYPE) = 'TFSA' then a.TAS_CURRENT_BOOK_VALUE else 0 end) as sum_term_tfsa_amt
from
""" + cf.CBSDBName + """.RISK_IP_TERM_ACCT_SNAPSHOT a,
""" + cf.CBSDBName + """.RISK_IP_CI_CUST_PLAN_ACCT_SNAPSHOT b
where 
a.eff_dt = '""" + args.bdate + """'
and a.date_type = '""" + date_type + """'
and a.eff_dt = b.eff_dt
and a.date_type = b.date_type
and a.tas_primary_owner_cif_id = b.ci_primary_owner_cif_id
and a.TAS_PLAN_ACCT_NUM = b.ci_plan_acct_num
and a.tas_certificate_status = '0'
group by a.tas_primary_owner_cif_id
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
