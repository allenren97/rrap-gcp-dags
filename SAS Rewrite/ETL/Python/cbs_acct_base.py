#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_cis_data_pop.py
#
#        USAGE: ./cbs_cis_data_pop.py bdate datetype
#
#  DESCRIPTION: Risk KQ account snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Rahim Dobani
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 07/20/2018 16:18:33 
#     REVIEWER: 
#     REVISION: ---
#    SRC_TABLE:  tsz_b9xf_rrap.cbs_cis_data_pop
#    TGT_TABLE:  crz_cust_scorecard.cbs_acct_base
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



# insert into table cbs_acct_base table with monthly data

SQL1 = """
	set hive.execution.engine=tez;
	set hive.vectorized.execution.enabled = true;
	set hive.vectorized.execution.reduce.enabled = true;
	set hive.exec.parallel=true;
	insert overwrite table """ + cf.CBSDBName + """.cbs_acct_base partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
	select
cust_cid as cust_cid,
current_timestamp as insrt_process_tmstmp,
'""" + os.path.realpath(__file__) + """' as op_field,
product_type as product_type,
account_num as account_num,
primary_cust_flag as primary_cust_flag,
relation_code as relation_code,
employee_ind as employee_ind,
mort_num as mort_num,
loan_num as loan_num,
cast (concat(
substring(file_date, 6, 4),
'-',
case 
when file_date like '%JAN%' then '01'
when file_date like '%FEB%' then '02'
when file_date like '%MAR%' then '03'
when file_date like '%APR%' then '04'
when file_date like '%MAY%' then '05'
when file_date like '%JUN%' then '06'
when file_date like '%JUL%' then '07'
when file_date like '%AUG%' then '08'
when file_date like '%SEP%' then '09'
when file_date like '%OCT%' then '10'
when file_date like '%NOV%' then '11'
when file_date like '%DEC%' then '12'
end, 
'-',
substring(file_date,1,2)
) as date) as file_date,
cast (concat(
substring(process_date, 6, 4),
'-',
case 
when process_date like '%JAN%' then '01'
when process_date like '%FEB%' then '02'
when process_date like '%MAR%' then '03'
when process_date like '%APR%' then '04'
when process_date like '%MAY%' then '05'
when process_date like '%JUN%' then '06'
when process_date like '%JUL%' then '07'
when process_date like '%AUG%' then '08'
when process_date like '%SEP%' then '09'
when process_date like '%OCT%' then '10'
when process_date like '%NOV%' then '11'
when process_date like '%DEC%' then '12'
end, 
'-',
substring(process_date,1,2)
) as date) as process_date,
basel_acct_id as basel_acct_id,
prd_id_spl as prd_id_spl,
comm_flag_spl as comm_flag_spl,
cast(os_bal_amt_spl as DECIMAL(18,2)) as os_bal_amt_spl,
recd_stat_cd_spl,
cast (concat(
substring(paid_off_date_mort, 6, 4),
'-',
case 
when paid_off_date_mort like '%JAN%' then '01'
when paid_off_date_mort like '%FEB%' then '02'
when paid_off_date_mort like '%MAR%' then '03'
when paid_off_date_mort like '%APR%' then '04'
when paid_off_date_mort like '%MAY%' then '05'
when paid_off_date_mort like '%JUN%' then '06'
when paid_off_date_mort like '%JUL%' then '07'
when paid_off_date_mort like '%AUG%' then '08'
when paid_off_date_mort like '%SEP%' then '09'
when paid_off_date_mort like '%OCT%' then '10'
when paid_off_date_mort like '%NOV%' then '11'
when paid_off_date_mort like '%DEC%' then '12'
end, 
'-',
substring(paid_off_date_mort,1,2)
) as date) as paid_off_date_mort,
cast(current_bal_mort as DECIMAL(18,2)) current_bal_mort,
comm_tp_cd_mort as comm_tp_cd_mort,
cast(total_suspense_mort as DECIMAL(18,2)) total_suspense_mort,
cast(os_bal_amt_mort as DECIMAL(18,2)) os_bal_amt_mort,
pd_off_f_mort,
cast(mth_in_arrs_cnt_mort as INT) as mth_in_arrs_cnt_mort,
life_insur_cd_mort as life_insur_cd_mort,
source_cd,
acct_stat_cd,
cast(cr_lmt_amt as DECIMAL(18,2)) as cr_lmt_amt,
cast(tot_new_bal_amt as DECIMAL(18,2)) as tot_new_bal_amt,
cast (concat(
substring(non_accrl_dt, 6, 4),
'-',
case 
when non_accrl_dt like '%JAN%' then '01'
when non_accrl_dt like '%FEB%' then '02'
when non_accrl_dt like '%MAR%' then '03'
when non_accrl_dt like '%APR%' then '04'
when non_accrl_dt like '%MAY%' then '05'
when non_accrl_dt like '%JUN%' then '06'
when non_accrl_dt like '%JUL%' then '07'
when non_accrl_dt like '%AUG%' then '08'
when non_accrl_dt like '%SEP%' then '09'
when non_accrl_dt like '%OCT%' then '10'
when non_accrl_dt like '%NOV%' then '11'
when non_accrl_dt like '%DEC%' then '12'
end, 
'-',
substring(non_accrl_dt,1,2)
) as date) as non_accrl_dt,
cast (concat(
substring(write_off_dt, 6, 4),
'-',
case 
when write_off_dt like '%JAN%' then '01'
when write_off_dt like '%FEB%' then '02'
when write_off_dt like '%MAR%' then '03'
when write_off_dt like '%APR%' then '04'
when write_off_dt like '%MAY%' then '05'
when write_off_dt like '%JUN%' then '06'
when write_off_dt like '%JUL%' then '07'
when write_off_dt like '%AUG%' then '08'
when write_off_dt like '%SEP%' then '09'
when write_off_dt like '%OCT%' then '10'
when write_off_dt like '%NOV%' then '11'
when write_off_dt like '%DEC%' then '12'
end, 
'-',
substring(write_off_dt,1,2)
) as date) as write_off_dt,
acct_cls_rsn_cd,
blocked_ind,
deceased_ind,
stolen_ind,
cast(num_of_lend_prods as INT) as num_of_lend_prods,
cid_num,
mort_ind,
spl_ind,
rev_ind,
ssl_ind,
cast(lend_prods_cur as INT) lend_prods_cur,
cast(lend_prods_clsd as INT) lend_prods_clsd,
cast(lend_prods_bnkrpt as INT) lend_prods_bnkrpt,
cast(lend_prods_def as INT) lend_prods_def,
cast(lend_prods_chrg_off as INT) lend_prods_chrg_off,
cast(lend_prods_wrt_off as INT) lend_prods_wrt_off,
cast(lend_prods_comm as INT) lend_prods_comm,
cast(lend_prods_comm_cur as INT) lend_prods_comm_cur,
cast(lend_prods_comm_clsd as INT) lend_prods_comm_clsd,
cast(lend_prods_comm_def as INT) lend_prods_comm_def,
cast(lend_prods_comm_chrg_off as INT) lend_prods_comm_chrg_off,
cast(lend_prods_comm_wrt_off as INT) lend_prods_comm_wrt_off,
private_bank_ind as private_bank_ind,
corp_comm_excl as corp_comm_excl,
staff_excl,
cast (concat(
substring(default_date, 6, 4),
'-',
case 
when default_date like '%JAN%' then '01'
when default_date like '%FEB%' then '02'
when default_date like '%MAR%' then '03'
when default_date like '%APR%' then '04'
when default_date like '%MAY%' then '05'
when default_date like '%JUN%' then '06'
when default_date like '%JUL%' then '07'
when default_date like '%AUG%' then '08'
when default_date like '%SEP%' then '09'
when default_date like '%OCT%' then '10'
when default_date like '%NOV%' then '11'
when default_date like '%DEC%' then '12'
end, 
'-',
substring(default_date,1,2)
) as date) as default_date,
cast(default_bal as DECIMAL(18,2)) as default_bal,
default_ind,
model_excl,
pit_status,
cast(days_dlqnt as INT) as days_dlqnt,
cast(non_lend_prods_act as INT) as non_lend_prods_act,
cast(non_lend_prods_pend_clsd as INT) as non_lend_prods_pend_clsd,
cast(non_lend_prods_dor as INT) as non_lend_prods_dor,
cast(non_lend_prods_inact as INT) non_lend_prods_inact,
cast(non_lend_prods_pnd as INT) non_lend_prods_pnd,
cast(non_lend_prods_stoln as INT) non_lend_prods_stoln,
cast(non_lend_prods_clsd as INT) non_lend_prods_clsd,
cast(non_lend_prods_wrt_off as INT) non_lend_prods_wrt_off,
cust_type,
cust_status,
cust_dep_only_flg,
cust_deceased_ind,
cust_prime_ind,
cust_secondary_ind,
cust_noacct_excl,
cust_bankruptcy_excl,
cust_under_age_excl
from
""" + cf.TSZRRAPDBName + """.cbs_cis_data_pop
where businesseffectivedate = '""" + cf.bdate + """'
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
