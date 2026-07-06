#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_sav_acct_snapshot.py
#
#        USAGE: ./risk_sav_acct_snapshot.py bdate datetype
#
#  DESCRIPTION: Risk SAV account snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for RISK_SAV_ACCT_SNAPSHOT table load, by Gordana
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
        date_type = 'Dailyly'
elif (args.datetype.upper()  == 'A'):
        date_type = 'Adhoc'
else:
	print("[Fatal]: date load type is incorrect, please make sure the datetype argument should be m/w/d!")
	sys.exit(-1)
	
cf = CBS_Configuration(args.bdate)



# insert into table cbs_mdm_flags table with monthly data

SQL1 = """
with all_accts as
(select lpad(bbdwbsac_account,12,'0') as acct_num
,max(businesseffectivedate) as b_date
from """ + cf.TSZDBName + """.v_bb_bbbsac 
where businesseffectivedate between add_months(date_add('""" + cf.bdate + """',1),-1) and '""" + cf.bdate + """'
and bbdwbsac_acct_status = 'A'
group by lpad(bbdwbsac_account,12,'0')
)
insert overwrite table """ + cf.CBSDBName + """.risk_sav_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select
lpad(a.bbdwbsac_account,12,'0') as acct_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,trim(bbdwbsac_acct_status) as acct_stat
,bbdwbsac_nsf_history_period1 as nsf_hist_period_1_cnt
,bbdwbsac_nsf_history_period2 as nsf_hist_period_2_cnt
,bbdwbsac_nsf_history_period3 as nsf_hist_period_3_cnt
,bbdwbsac_nsf_history_period4 as nsf_hist_period_4_cnt
,bbdwbsac_nsf_history_period5 as nsf_hist_period_5_cnt
,bbdwbsac_agg_cr_ytd_bal as agg_credit_ytd_bal_amt
,bbdwbsac_agg_db_ytd_bal as agg_debit_ytd_bal_amt
,bbdwbsac_acct_bal* -1 as os_bal_coa_amt
,bbdwbsac_acct_bal as acct_bal_amt
,trim(bbdwbsac_acct_type) as acct_type_cd
,trim(bbdwbsac_frozen_acct) as frzn_acct_ind
,case when bbdwbsac_opened_date='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_opened_date,'yyyyMMdd'),'yyyy-MM-dd')  end  as acct_open_dt
,case when bbdwbsac_last_transfer_date='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_last_transfer_date,'yyyyMMdd'),'yyyy-MM-dd')  end  as last_tfr_dt
,case when bbdwbsac_last_active_date ='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_last_active_date,'yyyyMMdd'),'yyyy-MM-dd')  end  as last_active_dt
,case when bbdwbsac_last_stmt_psbk_date ='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_last_stmt_psbk_date,'yyyyMMdd'),'yyyy-MM-dd')  end  as last_stmt_pssbk_dt
,case when bbdwbsac_odlimit_add_deleted ='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_odlimit_add_deleted,'yyyyMMdd'),'yyyy-MM-dd')  end  as od_lmt_add_del_dt
,bbdwbsac_overdraft_limit as od_lmt_amt
,case when trim(bbdwbsac_step_link) = 'Y' then 'Y' else 'N'  end as step_f
,bbdwbsac_passbook_bal as pssbk_bal_amt
,bbdwbsac_paymt_hist_1_29_days as dlqnt_1_29_day_cnt
,bbdwbsac_paymt_hist_30_59_days as dlqnt_30_59_day_cnt
,bbdwbsac_paymt_hist_60_up_days as dlqnt_60_up_day_cnt
,case when bbdwbsac_last_deposit_date ='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_last_deposit_date,'yyyyMMdd'),'yyyy-MM-dd')  end  as last_deposit_dt
,bbdwbsac_last_deposit_amt as last_deposit_amt
,case when bbdwbsac_last_statement_date='00000000' then null else from_unixtime(unix_timestamp(bbdwbsac_last_statement_date,'yyyyMMdd'),'yyyy-MM-dd')  end  as last_stmt_dt
,cast(bbdwbsac_delinquency_counter as int) as dlqnt_mth_cnt
,bbdwbsac_acct_bal_last_cycle as acct_bal_last_cycl_amt
,bbdwbsac_agg_cr_bal_cur_mth as agg_credit_bal_crnt_mth_amt
,bbdwbsac_agg_cr_days_cur_mth as agg_credit_day_crnt_mth_cnt
,bbdwbsac_agg_db_bal_cur_mth as agg_debit_bal_crnt_mth_amt
,bbdwbsac_agg_db_days_cur_mth as agg_debit_day_crnt_mth_cnt
,bbdwbsac_min_dep_past_due as min_deposit_pst_due_amt
,bbdwbsac_min_dep_required as min_deposit_required_amt
,trim(bbdwbsac_residence) as residence_cd
,trim(bbdwbsac_int_pac_flag) as intrnl_pac_flag
,trim(bbdwbsac_int_pad_flag) as intrnl_pad_flag
,NULL as currency_cd
,NULL as od_occurred_dt
,NULL as gl_acct_num
from """ + cf.TSZDBName + """.v_bb_bbbsac a join all_accts b
on  lpad(a.bbdwbsac_account,12,'0') = acct_num
and a.businesseffectivedate=b_date
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
