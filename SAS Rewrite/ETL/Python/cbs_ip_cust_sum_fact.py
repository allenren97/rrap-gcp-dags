#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_ip_cust_sum_fact.py
#
#        USAGE: ./cbs_ip_cust_sum_fact.py bdate datetype
#
#  DESCRIPTION: CBS IP cust Summary Fact -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for cbs_ip_cust_sum_fact table load, by Rahim
#       AUTHOR: Justin Liu --Sql created by Rahim
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 11/29/2018 16:18:33 
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



# insert into table cbs_ip_cust_sum_fact table with monthly data

SQL1 = """
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=200;
set hive.support.quoted.identifiers=none;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.optimize.sort.dynamic.partition=true;
set hive.enforce.bucketing=true;
set hive.optimize.bucketmapjoin=true;
set hive.support.concurrency=true;
insert overwrite table """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select
a.cust_cid as cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,(coalesce(b.num_mut_fund_reg_acct,0) + coalesce(c.num_csh_dmd_reg_acct,0) + coalesce(d.num_term_reg_acct,0)) as tot_num_reg_acct
,(coalesce(b.sum_mut_fund_reg_amt,0) + coalesce(c.sum_csh_dmd_reg_amt,0) + coalesce(d.sum_term_reg_amt,0)) as tot_bal_reg_amt
,(coalesce(b.num_mut_fund_non_reg_acct,0) + coalesce(c.num_csh_dmd_non_reg_acct,0) + coalesce(d.num_term_non_reg_acct,0)) as tot_num_non_reg_acct
,(coalesce(b.sum_mut_fund_non_reg_amt,0) + coalesce(c.sum_csh_dmd_non_reg_amt,0) + coalesce(d.sum_term_non_reg_amt,0)) as tot_bal_non_reg_amt
,(coalesce(b.num_mut_fund_rrsp_acct,0) + coalesce(c.num_csh_dmd_rrsp_acct,0) + coalesce(d.num_term_rrsp_acct,0)) as tot_num_rrsp_acct
,(coalesce(b.sum_mut_fund_rrsp_amt,0) + coalesce(c.sum_csh_dmd_rrsp_amt,0) + coalesce(d.sum_term_rrsp_amt,0)) as tot_bal_rrsp_amt
,(coalesce(b.num_mut_fund_resp_acct,0) + coalesce(c.num_csh_dmd_resp_acct,0) + coalesce(d.num_term_resp_acct,0)) as tot_num_resp_acct
,(coalesce(b.sum_mut_fund_resp_amt,0) + coalesce(c.sum_csh_dmd_resp_amt,0) + coalesce(d.sum_term_resp_amt,0)) as tot_bal_resp_amt
,(coalesce(b.num_mut_fund_tfsa_acct,0) + coalesce(c.num_csh_dmd_tfsa_acct,0) + coalesce(d.num_term_tfsa_acct,0)) as tot_num_tfsa_acct
,(coalesce(b.sum_mut_fund_tfsa_amt,0) + coalesce(c.sum_csh_dmd_tfsa_amt,0) + coalesce(d.sum_term_tfsa_amt,0)) as tot_bal_tfsa_amt
from """ + cf.CBSDBName + """.cbs_customer_base a
left outer join """ + cf.CBSDBName + """.cbs_ip_mut_fund_sum_fact b on a.cust_cid = b.cust_cid and a.eff_dt = b.eff_dt and a.date_type=b.date_type
left outer join """ + cf.CBSDBName + """.cbs_ip_csh_dmd_sum_fact c on a.cust_cid = c.cust_cid and a.eff_dt = c.eff_dt and a.date_type = c.date_type
left outer join """ + cf.CBSDBName + """.cbs_ip_term_sum_fact d on a.cust_cid = d.cust_cid and a.eff_dt = d.eff_dt and a.date_type = d.date_type
where a.eff_dt = '""" + args.bdate + """'
and a.date_type = '""" + date_type + """'
"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
