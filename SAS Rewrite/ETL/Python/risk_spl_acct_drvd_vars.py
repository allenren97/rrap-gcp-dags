#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_spl_acct_drvd_vars.py
#
#        USAGE: ./risk_spl_acct_drvd_vars.py bdate datetype
#
#  DESCRIPTION: Risk SPL Account Derived Variables table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 08/28/2018  
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



# insert into table risk_spl_acct_drvd_vars table with monthly data

SQL1 = """
with 
step1 as (
select a.acct_num, a.eff_dt, a.date_type, a.SUBVENTED_IND, a.prim_cust_cid, STEP_PLN_AGRMNT_NUM
, case
 when (a.recd_stat_cd = 9 or a.recd_stat_cd = 0 or a.recd_stat_cd is null) then 'CLO'
 when (a.recd_stat_cd = 6 or a.recd_stat_cd = 7 or a.recd_stat_cd = 8) then 'CHG'
 when a.chrg_off_dt is not null then 'CHG'   
 when a.days_overdue >= 90 or a.recd_stat_cd = 5 then 'DEF'
 when a.days_overdue < 90 and a.recd_stat_cd = 4 then 'CUR'
 else null end PIT_STATUS
 , case 
when a.SUBVENTED_IND = 'Y' then 'INDIRECT'
when z.prod_id is not null then z.tp
when y.prod_id is not null then y.scrty_type
else NULL end as PROD_TYPE_CD
, z.prod_id as scrty_prps_prod_id
, y.prod_id as scrty_prod_id
, z.prod as scrty_prps_prod_cd
, y.prod_cd as scrty_prod_cd 
, ucase(z.tp) as scrty_prps_prod_tp
, ucase(y.scrty_type) as scrty_prod_tp
, z.sub_prod as scrty_prps_sub_prod
, y.sub_prod as scrty_sub_prod
from """ + cf.CBSDBName + """.risk_spl_acct_snapshot a
left outer join (select * from """ + cf.CBSDBName + """.risk_psnl_loan_scrty_prps_cd_lkp p
where p.eff_dt in (select max(b.eff_dt) from """ + cf.CBSDBName + """.risk_psnl_loan_scrty_prps_cd_lkp b)) z 
ON  a.LOAN_PRPS_CD = z.prps_cd 
and a.SCRTY_CD = z.scrty_cd
left outer join (select * from """ + cf.CBSDBName + """.risk_psnl_loan_scrty_cd_lkp s
where s.eff_dt in (select max(o.eff_dt) from """ + cf.CBSDBName + """.risk_psnl_loan_scrty_cd_lkp o)) y 
ON a.SCRTY_CD = y.scrty_cd
where a.eff_dt = '""" + cf.bdate + """'
and a.date_type = '""" + date_type + """'
),
step2 as (
select a.acct_num, a.eff_dt, a.date_type, a.SUBVENTED_IND, a.prim_cust_cid
, case when trim(ucase(a.PROD_TYPE_CD)) = 'DIRECT' and (a.STEP_PLN_AGRMNT_NUM is null OR a.STEP_PLN_AGRMNT_NUM = '') then 'S08'
when a.SUBVENTED_IND = 'Y' then 'S10'
when a.scrty_prps_prod_id is not null then a.scrty_prps_prod_id
when a.scrty_prod_id is not null then a.scrty_prod_id
else -1 end as PROD_ID
, a.PIT_STATUS
, case when trim(ucase(a.PROD_TYPE_CD)) = 'DIRECT' and (a.STEP_PLN_AGRMNT_NUM is null OR a.STEP_PLN_AGRMNT_NUM = '') then 'SPL under STEP'
when a.SUBVENTED_IND = 'Y' then 'Auto'
when a.scrty_prps_prod_id is not null then a.scrty_prps_prod_cd
when a.scrty_prod_id is not null then a.scrty_prod_cd
else -1 end as PROD_CD
, case 
when a.SUBVENTED_IND = 'Y' then 'INDIRECT'
when a.scrty_prps_prod_id is not null then a.scrty_prps_prod_tp
when a.scrty_prod_id is not null then a.scrty_prod_tp
else NULL end as PROD_TYPE_CD
, case when trim(ucase(a.PROD_TYPE_CD)) = 'DIRECT' and (a.STEP_PLN_AGRMNT_NUM is null OR a.STEP_PLN_AGRMNT_NUM = '') then 'SPL under STEP'
when a.SUBVENTED_IND = 'Y' then 'Rate Subvented'
when a.scrty_prps_prod_id is not null then a.scrty_prps_sub_prod
when a.scrty_prod_id is not null then a.scrty_sub_prod
else -1 end as SUB_PROD_CD
from step1 a)
insert overwrite table """ + cf.CBSDBName + """.risk_spl_acct_drvd_vars partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select m.acct_num
,current_timestamp() as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
, m.PRIM_CUST_CID as PRIM_CUST_CID
, m.PROD_ID
, m.PIT_STATUS
, m.PROD_CD
, m.PROD_TYPE_CD
, m.SUB_PROD_CD
, case
when m.PROD_ID in ('S01','S02','S03','S04','S05','S06','S07','S08')  then 'DIRECT'
when m.PROD_ID in ('S09','S10','S11','S12','S13','S14','S15') then 'INDIRECT'
else NULL 
end as SUB_PORTFL_CD
from step2 m
;
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
