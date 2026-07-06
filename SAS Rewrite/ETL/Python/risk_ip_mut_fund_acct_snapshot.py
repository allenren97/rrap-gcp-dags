#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_ip_mut_fund_acct_snapshot.py
#
#        USAGE: ./risk_ip_mut_fund_acct_snapshot.py bdate datetype
#
#  DESCRIPTION: IP Mutual Fund Account Snapshot -- """ + date_type + """ job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for risk_ip_mut_fund_acct_snapshot table load, by Gordana
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



# insert into table risk_ip_mut_fund_acct_snapshot table with """ + date_type + """ data

SQL1 = """
insert overwrite table """ + cf.CBSDBName + """.risk_ip_mut_fund_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')

select mfs_plan_account_number as mfs_plan_acct_num
,mfs_fund_investment_code as mfs_fund_investment_code
,mfs_primary_owner_cif_id as mfs_primary_owner_cif_id
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,cast(mfs_fund_closing_book_value/100 as decimal(18,2)) as mfs_fund_closing_book_value
,mfs_fund_currency as mfs_fund_currency
,mfs_fund_product_type as mfs_fund_product_type
,mfs_record_type as mfs_record_type
,mfs_fund_account_number as mfs_fund_acct_num
,mfs_fund_invest_issuer_cd as mfs_fund_invest_issuer_cd
,cast(concat(substring(cast(mfs_fund_account_open_date as varchar(8)),1,4),'-', substring(cast(mfs_fund_account_open_date as varchar(8)),5,2),'-', 
substring(cast(mfs_fund_account_open_date as varchar(8)),7,2)) as date) as mfs_fund_account_open_dt
,mfs_fund_status as mfs_fund_status
,mfs_fund_term as mfs_fund_term
,mfs_fund_risk as mfs_fund_risk
,mfs_fund_objective as mfs_fund_objective
from """ + cf.TSZDBName + """.uf_mfs_mutual_fund_summary_rec_1
where businesseffectivedate IN (select max(a.businesseffectivedate)
            from """ + cf.TSZDBName + """.uf_mfs_mutual_fund_summary_rec_1 a
            where a.businesseffectivedate <='""" + cf.bdate + """')

"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
