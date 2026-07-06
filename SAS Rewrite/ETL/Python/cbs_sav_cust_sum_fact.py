#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_sav_cust_sum_fact.py
#
#        USAGE: ./cbs_sav_cust_sum_fact.py bdate datetype
#
#  DESCRIPTION: Risk CBS SAV customer summary table -- Monthly job
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



# insert into table cbs_mdm_flags table with monthly data

SQL1 = """

insert overwrite table """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_b.cust_cid 
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,cust_snp.num_of_sav_acct_prim as  num_sav_acct_prim   
,cust_snp.sum_sav_bal_prim_amt as      sum_sav_bal_prim_amt    
,cust_snp.num_of_sav_acct_sec as   num_sav_acct_sec    
,cust_snp.sum_sav_bal_sec_amt as       sum_sav_bal_sec_amt     
,cust_snp.num_of_sav_acct   as    num_of_sav_acct     
,cust_snp.sum_sav_bal_amt  as           sum_sav_bal_amt         
,cust_txt.num_of_deposits  as    num_of_deposits         
,cust_txt.sum_deposit_amt   as    sum_dep_amt      
,cust_txt.num_of_payments  as    num_of_pymt         
,cust_txt.sum_payment_amt as    sum_pymt_amt        
,cust_txt.num_of_withdrawals as  num_of_withdrawal  
,cust_txt.sum_withdrawal_amt as  sum_withdrawal_amt  
,cust_txt.num_of_nsf_txn as      num_of_nsf         
,cust_txt.sum_nsf_amt   as       sum_nsf_amt          
,cust_txt.num_of_odp_txn  as     num_of_odp         
,cust_txt.sum_odp_amt  as         sum_odp_amt         
from """ + cf.CBSDBName + """.cbs_customer_base cust_b  
left join  (select cust_cid as cust_cid
,sum(num_of_deposits) as num_of_deposits      
,sum(sum_deposit_amt) as sum_deposit_amt      
,sum(num_of_payments) as num_of_payments       
,sum(sum_payment_amt) as sum_payment_amt       
,sum(num_of_withdrawals) as num_of_withdrawals    
,sum(sum_withdrawal_amt) as sum_withdrawal_amt    
,sum(num_of_nsf_txn) as num_of_nsf_txn        
,sum(sum_nsf_amt)  as  sum_nsf_amt      
,sum(num_of_odp_txn)  as num_of_odp_txn      
,sum(sum_odp_amt)  as sum_odp_amt
from """ + cf.CBSDBName + """.cbs_acct_base a left join """ + cf.CBSDBName + """.risk_sav_acct_txn_sum_fact b 
on substring(account_num,2,12)=b.acct_num   
and a.date_type=b.date_type
and a.eff_dt = b.eff_dt 
where a.eff_dt= '""" + cf.bdate + """'
and a.date_type='""" + date_type + """'
and trim(product_type)= 'SAV'   --a.src_sys_cd= 'BB'
group by cust_cid) cust_txt 
on cust_b.cust_cid = cust_txt.cust_cid
left join """ + cf.CBSDBName + """.risk_sav_cust_sum_fact cust_snp
on cust_b.cust_cid=cust_snp.cust_cid
and cust_b.eff_dt=cust_snp.eff_dt
and cust_b.date_type=cust_snp.date_type
where cust_b.eff_dt='""" + cf.bdate + """'
and cust_b.date_type='""" + date_type + """'
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
