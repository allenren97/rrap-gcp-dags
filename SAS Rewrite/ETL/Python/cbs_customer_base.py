#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_customer_base.py
#
#        USAGE: ./cbs_customer_base.py business_date date_type
#
#  DESCRIPTION: CBS Customer Base table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu 
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
        date_type = 'Dailyly'
elif (args.datetype.upper()  == 'A'):
        date_type = 'Adhoc'
else:
	print("[Fatal]: date load type is incorrect, please make sure the datetype argument should be m/w/d!")
	sys.exit(-1)
	
cf = CBS_Configuration(args.bdate)



# insert into table cbs_customer_base table with monthly data

SQL1 = """
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.exec.parallel=true;
insert overwrite table """ + cf.CBSDBName + """.cbs_customer_base partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select 
cust_cid as CUST_CID,
current_timestamp() as insrt_process_tmstmp,
'""" + os.path.realpath(__file__) + """' as op_field,
cast(cid_num as Varchar(30)) as CID_NUM,
cast(tm_id as Varchar(10)) as TM_ID,
cast (concat(
substring(process_dt, 6, 4),
'-',
case 
when process_dt like '%JAN%' then '01'
when process_dt like '%FEB%' then '02'
when process_dt like '%MAR%' then '03'
when process_dt like '%APR%' then '04'
when process_dt like '%MAY%' then '05'
when process_dt like '%JUN%' then '06'
when process_dt like '%JUL%' then '07'
when process_dt like '%AUG%' then '08'
when process_dt like '%SEP%' then '09'
when process_dt like '%OCT%' then '10'
when process_dt like '%NOV%' then '11'
when process_dt like '%DEC%' then '12'
end, 
'-',
substring(process_dt,1,2)
) as Date) as PROCESS_DT,
cast(num_prods as Int) as NUM_PRODS,
cast(num_lend_prods_cur as Int) as NUM_LEND_PRODS_CUR,
cast(num_lend_prods_clsd as Int) as NUM_LEND_PRODS_CLSD,
cast(num_lend_prods_bnkrpt as Int) as NUM_LEND_PRODS_BNKRPT,
cast(num_lend_prods_def as Int) as NUM_LEND_PRODS_DEF,
cast(num_lend_prods_chrg_off as Int) as NUM_LEND_PRODS_CHRG_OFF,
cast(num_lend_prods_wrt_off as Int) as NUM_LEND_PRODS_WRT_OFF,
cast(num_lend_prods_comm as Int) as NUM_LEND_PRODS_COMM,
cast(num_lend_prods_comm_cur as Int) as NUM_LEND_PRODS_COMM_CUR,
cast(num_lend_prods_comm_clsd as Int) as NUM_LEND_PRODS_COMM_CLSD,
cast(num_lend_prods_comm_chrg_off as Int) as NUM_LEND_PRODS_COMM_CHRG_OFF,
cast(num_lend_prods_comm_def as Int) as NUM_LEND_PRODS_COMM_DEF,
cast(num_lend_prods_comm_wrt_off as Int) as NUM_LEND_PRODS_COMM_WRT_OFF,
private_bank_ind as PRIVATE_BANK_IND,
cast(num_mort as Int) as NUM_MORT,
cast(num_spl as Int) as NUM_SPL,
cast(num_rev as Int) as NUM_REV,
cast(num_ssl as Int) as NUM_SSL,
cast(num_lend_prods as Int) as NUM_LEND_PRODS,
cast(worst_dlq_days as Int) as WORST_DLQ_DAYS,
basel_cust_id as BASEL_CUST_ID,
bureau_exist as BUREAU_EXIST,
cust_type as CUST_TYPE,
cust_status as CUST_STATUS,
deceased_ind as DECEASED_IND,
bnkrptcy_flag as BNKRPTCY_FLAG,
under_18_flag as UNDER_18_FLAG,
retail_ind as RETAIL_IND,
prime_ind as PRIME_IND,
secondary_ind as SECONDARY_IND,
prime_ind_lend as PRIME_IND_LEND,
secondary_ind_lend as SECONDARY_IND_LEND,
cast(num_nonlend_prods_act as Int) as NUM_NONLEND_PRODS_ACT,
cast(num_nonlend_prods_pnd_clsr as Int) as NUM_NONLEND_PRODS_PND_CLSR,
cast(num_nonlend_prods_dor as Int) as NUM_NONLEND_PRODS_DOR,
cast(num_nonlend_prods_inact as Int) as NUM_NONLEND_PRODS_INACT,
cast(num_nonlend_prods_pnd as Int) as NUM_NONLEND_PRODS_PND,
cast(num_nonlend_prods_stoln as Int) as NUM_NONLEND_PRODS_STOLN,
cast(num_nonlend_prods_clsd as Int) as NUM_NONLEND_PRODS_CLSD,
cast(num_nonlend_prods_wrt_off as Int) as NUM_NONLEND_PRODS_WRT_OFF,
cast(num_nonlend_prods as Int) as NUM_NONLEND_PRODS,
dep_only_flg as DEP_ONLY_FLG,
noacct_excl as NOACCT_EXCL,
bankruptcy_excl as BANKRUPTCY_EXCL,
under_age_excl as UNDER_AGE_EXCL,
cust_pit_stat as CUST_PIT_STAT,
cast(min_ltv as double) as MIN_LTV,
cast(max_ltv as double) as MAX_LTV,
cast(avg_ltv as double) as AVG_LTV,
cast(min_ltv_heloc as double) as MIN_LTV_HELOC,
cast(max_ltv_heloc as double) as MAX_LTV_HELOC,
cast(avg_ltv_heloc as double) as AVG_LTV_HELOC,
cast(worst_days_dlq_cust as Int) as WORST_DAYS_DLQ_CUST,
cast(worst_days_dlq_kq_cust as Int) as WORST_DAYS_DLQ_KQ_CUST,
cast(worst_days_dlq_mort_cust as Int) as WORST_DAYS_DLQ_MORT_CUST,
cast(worst_days_dlq_spl_cust as Int) as WORST_DAYS_DLQ_SPL_CUST,
cast(num_delq_acct as Int) as NUM_DELQ_ACCT,
cast(cust_default_date as Date) as CUST_DEFAULT_DATE,
cust_default_ind as CUST_DEFAULT_IND,
corp_comm_excl as CORP_COMM_EXCL,
staff_excl as STAFF_EXCL
from """ + cf.TSZRRAPDBName + """.cbs_customer_base a 
where a.businesseffectivedate = '""" + cf.bdate + """'
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


