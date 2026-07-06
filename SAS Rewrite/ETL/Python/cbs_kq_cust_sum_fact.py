#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_kq_cust_sum_fact.py
#
#        USAGE: ./cbs_kq_cust_sum_fact.py business_date date_type
#
#  DESCRIPTION: CBS KQ Customer Summary Fact table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/30/2018 16:18:33; Last updated: 12/28/2018
#     REVIEWER: 
#     REVISION: --- removed process_dt, max_inact_cd 
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



# insert into table cbs_kq_cust_sum_fact table with monthly data

SQL1 = """
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
with acct_base as (
select cust_cid, product_type, account_num, primary_cust_flag, process_date, eff_dt, date_type 
from """ + cf.CBSDBName + """.cbs_acct_base
where eff_dt = '""" + cf.bdate + """'
AND date_type = '""" + date_type + """'
AND product_type IN (select cis_prod_cd from """ + cf.CBSDBName + """.risk_kq_acct_snapshot where eff_dt = '""" + cf.bdate + """' and date_type = '""" + date_type + """' group by cis_prod_cd)
AND (
(pit_status IN ('CUR', 'DEF', 'CHG') AND (MODEL_EXCL <> 'Y' or MODEL_EXCL IS NULL)) OR 
(pit_status IN ('CUR', 'DEF', 'CHG') AND  MODEL_EXCL = 'Y' AND PRODUCT_TYPE in ('BLV', 'VUS'))
)
), 

kq_snapshot as (
select acct_num, date_type, eff_dt, prod_cd
, majr_prod_grp, cis_prod_cd
, case 
when majr_prod_grp IN ('VAX', 'VIS') then 'Cards' 
when majr_prod_grp = 'SCL' and prod_cd <> 'SSL' then 'LOC'
when majr_prod_grp = 'SCL' and prod_cd = 'SSL' then 'SSL'
else 'OTHER' end as cr_type
, tot_new_bal_amt, cr_lmt_amt, bns_dlqnt_day, orig_chrg_ff_amt, prch_crnt_cycl_bal_amt, prch_1_cycl_ago_bal_amt, prch_2_cycl_ago_bal_amt
, csh_advnc_crnt_cycl_bal_amt, csh_advnc_1_cycl_ago_bal_amt, csh_advnc_2_cycl_ago_bal_amt, inact_cd, scrty_val_amt
from """ + cf.CBSDBName + """.risk_kq_acct_snapshot 
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
),

kq_drvd_vars as (
select acct_num, date_type, eff_dt
, pit_stat_ver_2_cd, prchs_cnt, prchs_intr_chrgd_amt, csh_advnc_intr_chrgd_amt, total_int_chrgd_amt, heloc_flag
from """ + cf.CBSDBName + """.risk_kq_acct_drvd_vars 
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
),

kq_all as (
select a.acct_num, a.date_type, a.eff_dt, 
cr_type, tot_new_bal_amt, cr_lmt_amt, bns_dlqnt_day, orig_chrg_ff_amt, prch_crnt_cycl_bal_amt, prch_1_cycl_ago_bal_amt, prch_2_cycl_ago_bal_amt
, csh_advnc_crnt_cycl_bal_amt, csh_advnc_1_cycl_ago_bal_amt, csh_advnc_2_cycl_ago_bal_amt, inact_cd, scrty_val_amt
,  pit_stat_ver_2_cd, prchs_cnt, prchs_intr_chrgd_amt, csh_advnc_intr_chrgd_amt, total_int_chrgd_amt, heloc_flag
from kq_snapshot a 
inner join kq_drvd_vars b 
ON a.acct_num = b.acct_num
),
 
kq_agg as (
select a.cust_cid
, a.eff_dt 
, a.date_type 
, cr_type
, count(a.account_num) as num_of_acct
, sum(case when pit_stat_ver_2_cd = 'CUR' then 1 else 0 end) as num_of_accts_crnt
, sum(case when pit_stat_ver_2_cd = 'DEF' then 1 else 0 end) as num_of_accts_def
, sum(case when pit_stat_ver_2_cd = 'CHG' then 1 else 0 end) as num_of_accts_chrg_off
, sum(case when pit_stat_ver_2_cd = 'CUR' then tot_new_bal_amt else 0 end) as sum_bal_crnt_amt
, sum(case when pit_stat_ver_2_cd = 'DEF' then tot_new_bal_amt else 0 end) as sum_bal_def_amt
, sum(case when pit_stat_ver_2_cd = 'CHG' then tot_new_bal_amt else 0 end) as sum_bal_chrg_off_amt
, sum(greatest(cast(0 as decimal(18,2)), cr_lmt_amt)) as sum_cr_lmt_amt
, sum(greatest(cast(0 as decimal(18,2)), tot_new_bal_amt)) as sum_tot_new_bal_amt
, max(greatest(0, bns_dlqnt_day - 30)) as worst_dlqnt_days
, sum((case when pit_stat_ver_2_cd = 'CUR' and greatest(bns_dlqnt_day-30,0)>0 then 1 else 0 end)) as num_of_accts_dlqnt
, sum(orig_chrg_ff_amt) as sum_orig_chrg_off_amt
, sum(prch_crnt_cycl_bal_amt) as sum_prch_bal_amt
, sum(prch_1_cycl_ago_bal_amt) as sum_1_cycl_ago_prch_bal_amt
, sum(prch_2_cycl_ago_bal_amt) as sum_2_cycl_ago_prch_bal_amt 
, sum(csh_advnc_crnt_cycl_bal_amt) as sum_csh_advnc_bal_amt
, sum(csh_advnc_1_cycl_ago_bal_amt) as sum_1_cycl_ago_csh_advnc_bal_amt
, sum(csh_advnc_2_cycl_ago_bal_amt) as sum_2_cycl_ago_csh_advnc_bal_amt 
, sum(prchs_cnt) as num_of_prchs
, sum(prchs_intr_chrgd_amt) as sum_prchs_intr_chrgd_amt
, sum(csh_advnc_intr_chrgd_amt) as sum_csh_advnc_intr_chrgd_amt 
, sum(total_int_chrgd_amt) as sum_total_int_chrgd_amt
, max(inact_cd) as max_inact_cd
, max(scrty_val_amt) as max_scrty_val_amt
, max(case when total_int_chrgd_amt > 0 then 1 else 0 end) as max_rev_ind
, sum(case when heloc_flag = 'Y' then 1 else 0 end) as num_of_heloc
from acct_base a 
left outer join kq_all b  
ON lpad(a.account_num, 23, '0') = lpad(b.acct_num, 23, '0')
group by a.cust_cid, a.eff_dt, a.date_type, cr_type)

insert overwrite table """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """', cr_type)
select cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
, num_of_acct
, num_of_accts_crnt
, num_of_accts_def
, num_of_accts_chrg_off
, sum_bal_crnt_amt
, sum_bal_def_amt
, sum_bal_chrg_off_amt
, sum_cr_lmt_amt
, sum_tot_new_bal_amt
, worst_dlqnt_days
, num_of_accts_dlqnt
, sum_orig_chrg_off_amt
, sum_prch_bal_amt
, sum_1_cycl_ago_prch_bal_amt
, sum_2_cycl_ago_prch_bal_amt
, sum_csh_advnc_bal_amt
, sum_1_cycl_ago_csh_advnc_bal_amt
, sum_2_cycl_ago_csh_advnc_bal_amt
, num_of_prchs
, sum_prchs_intr_chrgd_amt
, sum_csh_advnc_intr_chrgd_amt
, sum_total_int_chrgd_amt
, max_scrty_val_amt
, case when cr_type = 'Cards' then max_rev_ind else NULL end as max_rev_ind
, num_of_heloc
, case  
when sum_cr_lmt_amt > 0 then (sum_tot_new_bal_amt/sum_cr_lmt_amt)
when sum_cr_lmt_amt <= 0 and sum_tot_new_bal_amt > 0 then 1 
when sum_cr_lmt_amt <= 0 and sum_tot_new_bal_amt = 0 then 0  
end as util
, cr_type 
from kq_agg 
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


