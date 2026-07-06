#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_sav_dly_txn_hist_delta.py
#
#        USAGE: ./rrisk_sav_dly_txn_hist_delta.py bdate datetype
#
#  DESCRIPTION: Risk SAV daily transaction history delta -- Daily job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for RISK_SAV_DLY_TXN_HIST_DELTA table load, by Gordana
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
	set hive.execution.engine=tez;
	set hive.vectorized.execution.enabled = true;
	set hive.vectorized.execution.reduce.enabled = true;
	set hive.exec.parallel=true;
	insert overwrite table """ + cf.CBSDBName + """.risk_sav_dly_txn_hist_delta partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select 
ws_data.ws_key.ws_acct as ws_acct
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,ws_data.ws_key.ws_bank as ws_bank
,ws_data.ws_teller_data.ws_teller_branch as ws_teller_branch     
,ws_data.ws_teller_data.ws_teller_number  as ws_teller_number   
,ws_data.ws_sender_id   as ws_sender_id        
,ws_data.ws_originator as ws_originator         
,ws_data.ws_for as ws_for                
,ws_data.ws_amount as ws_amount 
,from_unixtime(unix_timestamp(ws_data.ws_posted_date,'yyyyMMdd'),'yyyy-MM-dd') as ws_posted_date        
,from_unixtime(unix_timestamp(ws_data.ws_entered_date,'yyyyMMdd'),'yyyy-MM-dd') as ws_entered_date       
,ws_data.ws_entered_time as ws_entered_time       
,ws_data.ws_tran_code as ws_tran_code
,ws_data.ws_mnemonic_text as ws_mnemonic_text
,ws_data.ws_sign as ws_sign               
,ws_data.ws_colt_mnemonic_code as ws_colt_mnemonic_code
,ws_data.ws_retracted_ind as ws_retracted_ind      
,ws_data.ws_narrative_code as ws_narrative_code     
,ws_data.ws_appl_area_flag as ws_appl_area_flag     
,ws_data.ws_chq_number as ws_chq_number         
,ws_data.ws_alt_acct_no as ws_alt_acct_no        
,ws_data.ws_cau_ind as ws_cau_ind            
,ws_data.ws_pinned_ind as ws_pinned_ind         
,ws_data.ws_reported_ind as ws_reported_ind       
,ws_data.ws_uniq_txn_id.ws_utid_source as ws_utid_source        
,ws_data.ws_uniq_txn_id.ws_utid_stck as ws_utid_stck          
,ws_data.ws_uniq_txn_id.ws_utid_seq_no as ws_utid_seq_no        
,ws_data.ws_appl_area as ws_appl_area          
,from_unixtime(unix_timestamp(operationalfields.expirydate,'yyyy-MM-dd'),'yyyy-MM-dd') as expirydate                      
from  """ + cf.TSZDBName + """.bb_transaction_dly_1
where businesseffectivedate between add_months(date_add('""" + cf.bdate + """',1),-1) and '""" + cf.bdate + """'
        and FROM_UNIXTIME(UNIX_TIMESTAMP(ws_data.ws_entered_date, 'yyyyMMdd'), 'yyyy-MM-dd') = businesseffectivedate
 """
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
