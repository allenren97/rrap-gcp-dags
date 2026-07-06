#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_cr_bureau_deli_mth_snapshot.py
#
#        USAGE: ./risk_cr_bureau_deli_mth_snapshot.py business_date date_type
#
#  DESCRIPTION: Credit Bureau Monthly Snapshot -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 07/20/2018 16:18:33 ; Updated Oct.4.2018
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



# insert into table risk_cr_bureau_deli_mth_snapshot table with monthly data

SQL1 = """

insert overwrite table """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select 
current_timestamp() as insrt_process_tmstmp
, '""" + os.path.realpath(__file__) + """' as op_field
, cust_cid
, mth_since_oldst_trade_opnd_cnt
, mth_since_last_30_day_dlqnt_cnt
, mth_since_last_60_day_dlqnt_cnt
, colctn_cnt
, tot_bal_tp_bankcard_amt
, trade_never_dlqnt_pc
, tot_pd_amt
, tot_utltn_amt
, highst_actv_utltn
, tot_avl_cr_not_utilized_amt
, tot_utltn_bnk_revlvng_crd_amt
, mth_since_most_recnt_dlqnt_cnt
, max_revlvng_cr_crnt_utltn_amt
, inqry_cnt
, inqry_past_6_mth_cnt
, occ_60_day_pd_within_past_12_mth_cnt
, tm_30_day_pd_last_12_mth_cnt
, trade_90_dpd_last_24_mth_cnt
, oldst_opn_trade_age_line_mth_cnt
from (
select 
 cust_cid
, mth_since_oldst_trade_opnd_cnt
, mth_since_last_30_day_dlqnt_cnt
, mth_since_last_60_day_dlqnt_cnt
, colctn_cnt
, tot_bal_tp_bankcard_amt
, trade_never_dlqnt_pc
, tot_pd_amt
, tot_utltn_amt
, highst_actv_utltn
, tot_avl_cr_not_utilized_amt
, tot_utltn_bnk_revlvng_crd_amt
, mth_since_most_recnt_dlqnt_cnt
, max_revlvng_cr_crnt_utltn_amt
, inqry_cnt
, inqry_past_6_mth_cnt
, occ_60_day_pd_within_past_12_mth_cnt
, tm_30_day_pd_last_12_mth_cnt
, trade_90_dpd_last_24_mth_cnt
, oldst_opn_trade_age_line_mth_cnt
, row_number() over (partition by cust_cid order by SCORE_LAST_RECVD_DT desc) as row_num
from """ + cf.TSZRRAPDBName + """.cca_cr_bureau_deli_mth_snapshot
where businesseffectivedate = '""" + cf.bdate + """')a 
where row_num = 1
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


