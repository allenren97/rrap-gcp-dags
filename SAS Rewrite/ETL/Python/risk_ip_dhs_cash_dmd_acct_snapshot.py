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
insert overwrite table """ + cf.CBSDBName + """.risk_ip_dhs_cash_dmd_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select dhs_plan_account_no  
,dhs_demand_investment_code
,dhs_primary_owner_cif_id 
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,dhs_currency_code            
,dhs_curr_cash_balance_sign   
,dhs_curr_cash_balance/100        
,concat(dhs_curr_cash_bal_eff_yyyy,'-',dhs_curr_cash_bal_eff_mm ,'-',dhs_curr_cash_bal_eff_dd) as dhs_curr_cash_bal_eff_dt   
,dhs_prev_me_cash_balance_sign
,dhs_prev_me_cash_balance/100     
,concat(dhs_me_cash_bal_eff_yyyy,'-',dhs_me_cash_bal_eff_mm ,'-',dhs_me_cash_bal_eff_dd) as dhs_prev_me_cash_bal_eff_dt  
,dhs_term               
,dhs_fr_investment_type 
,dhs_available_balance/100  
from """ + cf.TSZDBName + """.uf_dhs_cash_demand_summary_3 
where businesseffectivedate in (select max(x.businesseffectivedate) from """ + cf.TSZDBName + """.uf_dhs_cash_demand_summary_3 x
where x.businesseffectivedate <= '""" + cf.bdate + """')
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
