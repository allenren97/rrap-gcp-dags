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
insert overwrite table """ + cf.CBSDBName + """.risk_ip_ci_cust_plan_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select 
 ci_plan_account_no 
,ci_plan_product_code         
,ci_base_account_type         
,ci_primary_owner_cif_id
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field      
,ci_plan_status 
,cast(concat(ci_plan_status_eff_yyyy,'-',ci_plan_status_eff_mm,'-',ci_plan_status_eff_dd) as date) as ci_plan_status_eff_dt     
,cast(concat(ci_plan_open_date_yyyy ,'-',ci_plan_open_date_mm,'-',ci_plan_open_date_dd) as date) as ci_plan_open_dt  
,ci_dormant_indicator         
,cast(concat(ci_dormant_date_yyyy ,'-',ci_dormant_date_mm,'-',ci_dormant_date_dd) as date) as ci_dormant_dt
,ci_plan_servicing_transit_no
,ci_ownership_type            
,ci_plan_currency_code        
,ci_spousal_indicator         
,ci_residency_code            
,ci_cad_plan_book_value/100 as ci_cad_plan_book_value 
,ci_usd_plan_book_value/100  as ci_usd_plan_book_value     
,ci_customer_type             
,ci_primary_owner_percent/100 as ci_primary_owner_percent
,ci_title                     
,ci_business_or_personal_name 
,ci_relationship_type         
,ci_plan_prod_sub_type
from   """ + cf.TSZDBName + """.uf_ci_customer_plan_account_info_3     
where businesseffectivedate in (select max(x.businesseffectivedate)
from  """ + cf.TSZDBName + """.uf_ci_customer_plan_account_info_3 x
where x.businesseffectivedate <=  '""" + cf.bdate + """')
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
