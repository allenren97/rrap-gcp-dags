#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_ip_term_acct_snapshot.py
#
#        USAGE: ./risk_ip_term_acct_snapshot.py bdate datetype
#
#  DESCRIPTION: Risk IP Term Account Snapshot -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for risk_ip_term_acct_snapshot table load, by Rahim
#       AUTHOR: Justin Liu --Sql created by Rahim
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 11/21/2018 16:18:33 
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
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.exec.parallel=true;
set hive.enforce.bucketing=true;
set hive.optimize.bucketmapjoin=true;

insert overwrite table """ + cf.CBSDBName + """.risk_ip_term_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select
 TAS_PLAN_ACCOUNT_NO as TAS_PLAN_ACCT_NUM
,TAS_TERM_INVESTMENT_CODE as TAS_TERM_INVESTMENT_CODE
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,TAS_PRIMARY_OWNER_CIF_ID as TAS_PRIMARY_OWNER_CIF_ID
,TAS_CURRENT_BOOK_VALUE_SIGN as TAS_CURRENT_BOOK_VALUE_SIGN
,cast(TAS_CURRENT_BOOK_VALUE/100 as decimal(18,2)) as TAS_CURRENT_BOOK_VALUE
,TAS_CERTIFICATE_NO as TAS_CERTIFICATE_NO
,TAS_CURRENCY_CODE as TAS_CURRENCY_CODE
,TAS_ISSUER_CODE as TAS_ISSUER_CODE
,TAS_BNS_GL_ACCOUNT_NO as TAS_BNS_GL_ACCOUNT_NO
,TAS_CERTIFICATE_STATUS as TAS_CERTIFICATE_STATUS
,cast(concat(TAS_CERTIFICATE_STATUS_YYYY,'-',TAS_CERTIFICATE_STATUS_MM,'-',TAS_CERTIFICATE_STATUS_DD) as date) as TAS_CERTIFICATE_STATUS_DT
,cast(concat(TAS_ISSUE_DATE_YYYY,'-',TAS_ISSUE_DATE_MM,'-',TAS_ISSUE_DATE_DD) as date) as TAS_ISSUE_DT
,cast(concat(TAS_TERM_YEARS,'-',TAS_TERM_MONTHS,'-',TAS_TERM_DAYS) as date) as TAS_TERM_DT
,cast(concat(TAS_MATURITY_DATE_YYYY,'-',TAS_MATURITY_DATE_MM,'-',TAS_MATURITY_DATE_DD) as date) as TAS_MATURITY_DT
,cast(TAS_ORIGINAL_FACE_VALUE/100 as decimal(18,2)) as TAS_ORIGINAL_FACE_VALUE
,cast(TAS_CURRENT_FACE_VALUE/100 as decimal(18,2)) as TAS_CURRENT_FACE_VALUE
,cast(TAS_PREV_BAL/100 as decimal(18,2)) as TAS_PREV_BAL
,TAS_RISK as TAS_RISK
,TAS_OBJECTIVE as TAS_OBJECTIVE
from """ + cf.TSZDBName + """.UF_TAS_TERM_SUMMARY_REC_3
where businesseffectivedate in (select max(a.businesseffectivedate) from """ + cf.TSZDBName + """.UF_TAS_TERM_SUMMARY_REC_3 a where a.businesseffectivedate <= '""" + args.bdate + """')
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
