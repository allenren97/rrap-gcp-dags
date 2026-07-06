#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_sav_acct_txn_sum_fact.py
#
#        USAGE: ./risk_sav_acct_txn_sum_fact.py business_date date_type
#
#  DESCRIPTION: Risk Savings Account Transaction Summary Fact -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/30/2018 16:18:33; Last updated: 09/28/2018
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



# insert into table risk_sav_acct_txn_sum_fact table with monthly data

SQL1 = """

with 
active_dlvy as (
select host_origin_cd, tllr_low, tllr_high, txn_cd_low, txn_cd_high, chnl_active_origin, eff_dt
from """ + cf.CBSDBName + """.risk_active_dlvy_origin_mappng
where eff_dt in (select max(eff_dt) from """ + cf.CBSDBName + """.risk_active_dlvy_origin_mappng))
, 
txn_mappng as (
select dlvy_cd, dlvy_desc, txn_type_cd, txn_grp_cd, eff_dt 
from """ + cf.CBSDBName + """.risk_txn_active_mappng 
where eff_dt in (select max(eff_dt) from """ + cf.CBSDBName + """.risk_txn_active_mappng))
, 

sav_txn as (
select acct_num, ws_utid_source, ws_utid_stck, ws_utid_seq_no, ws_originator, ws_teller_num, ws_txn_cd, ws_mnemonic_txt
, (case when ws_sign = '-' then -1 else 1 end) *  sav_txn_amt as sav_txn_amt, eff_dt, date_type 
from """ + cf.CBSDBName + """.risk_sav_dly_txn_hist_delta
where eff_dt between add_months(date_add('""" + cf.bdate + """',1),-1) and '""" + cf.bdate + """')
, 
sav_txn_dlvy as
(
select acct_num, ws_utid_source, ws_utid_stck, ws_utid_seq_no, sav_txn_amt, a.eff_dt, a.date_type, c.txn_grp_cd
 from  sav_txn  a, active_dlvy b, txn_mappng c
where substr(trim(a.ws_originator),1,1 ) = trim(b.host_origin_cd)
and a.ws_teller_num between b.tllr_low and b.tllr_high  
and a.ws_txn_cd between (cast(b.txn_cd_low as int)) and (cast(b.txn_cd_high as int))
and concat(trim(b.chnl_active_origin),a.ws_txn_cd,trim(a.ws_mnemonic_txt)) = trim(c.dlvy_cd)
),  

sav_txn_mappng as (
select a.acct_num, a.ws_utid_source, a.ws_utid_stck, a.ws_utid_seq_no , a.sav_txn_amt, a.eff_dt, a.date_type,  b.txn_grp_cd
from  sav_txn  a 
left outer join txn_mappng b
on concat(a.ws_txn_cd,trim(a.ws_mnemonic_txt)) =  trim(b.dlvy_cd)
left outer join sav_txn_dlvy c 
on a.acct_num = c.acct_num 
and a.eff_dt = c.eff_dt
and a.ws_utid_source = c.ws_utid_source         
and a.ws_utid_stck = c.ws_utid_stck          
and a.ws_utid_seq_no = c.ws_utid_seq_no
where c.acct_num is null 
and c.eff_dt is null 
and c.ws_utid_source is null
and c.ws_utid_stck is null
and c.ws_utid_seq_no is null
), 

acct_txn_sum as
(
select acct_num
,sum(case when txn_grp_cd in ('D','P') then 1 else null end) as num_of_deposits
,sum(case when txn_grp_cd in ('D','P') then x.sav_txn_amt else null end) as sum_deposit_amt
,sum(case when txn_grp_cd='P' then 1 else null end) as num_of_payments
,sum(case when txn_grp_cd='P' then x.sav_txn_amt else null end) as sum_payment_amt
,sum(case when txn_grp_cd in ('W','N','O') then 1 else null end) as num_of_withdrawals
,sum(case when txn_grp_cd in ('W','N','O') then x.sav_txn_amt else null end) as sum_withdrawal_amt
,sum(case when txn_grp_cd='N' then 1 else null end) as num_of_nsf_txn
,sum(case when txn_grp_cd='N' then x.sav_txn_amt else null end) as sum_nsf_amt
,sum(case when txn_grp_cd='O' then 1 else null end) as num_of_odp_txn 
,sum(case when txn_grp_cd='O' then x.sav_txn_amt else null end) as sum_odp_amt
,sum(case when txn_grp_cd = 'C' or txn_grp_cd is null then 1 else null end) as num_of_oth_txn
,sum(case when txn_grp_cd = 'C' or txn_grp_cd is null then x.sav_txn_amt else null end) as sum_of_oth_txn_amt 
from
(select a.acct_num, a.txn_grp_cd, a.sav_txn_amt      
from sav_txn_dlvy a
union all  
select b.acct_num, b.txn_grp_cd, b.sav_txn_amt
 from sav_txn_mappng b
 ) x
group by acct_num)

insert overwrite table """ + cf.CBSDBName + """.risk_sav_acct_txn_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select acct_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,num_of_deposits       
,sum_deposit_amt       
,num_of_payments       
,sum_payment_amt       
,num_of_withdrawals    
,sum_withdrawal_amt    
,num_of_nsf_txn        
,sum_nsf_amt           
,num_of_odp_txn        
,sum_odp_amt           
,num_of_oth_txn        
,sum_of_oth_txn_amt    
from acct_txn_sum
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


