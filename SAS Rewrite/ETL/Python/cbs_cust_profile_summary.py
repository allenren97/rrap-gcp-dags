#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_cust_profile_summary.py
#
#        USAGE: ./cbs_cust_profile_summary.py bdate datetype
#
#  DESCRIPTION: CBS Customer Profile Summary table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Rahim Dobani 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 26/02/2018 16:18:33 
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



# insert into table cbs_cust_profile_summary table with monthly data

SQL1 = """
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

with SEG as
(
select 
trim(cust_cid) as cust_cid
,heloc_ind
,mth_since_oldst_trade_opnd_cnt
,trade_never_dlqnt_pc
,avg_tot_sav_inv_bal_12m
,cc_heloc_ind
,loc_heloc_ind
,mort_ind
,spl_ind
,noacct_excl
,seg_nm
,seg_num
,eff_dt
,date_type
from """ + cf.CBSDBName + """.cbs_cust_segmentation where eff_dt = '""" + args.bdate + """' and date_type = '""" + date_type + """'
),

CUST_BASE as
(
select
trim(a.cust_cid) as cust_cid
,a.num_prods
,a.num_mort
,a.num_spl
,a.num_rev
,a.num_ssl
,a.worst_dlq_days
,a.cust_type
,a.cust_status
,a.deceased_ind
,a.retail_ind
,a.dep_only_flg
,a.under_age_excl
,a.cust_pit_stat
,a.worst_days_dlq_cust
,a.worst_days_dlq_kq_cust
,a.worst_days_dlq_mort_cust
,a.worst_days_dlq_spl_cust
,a.num_delq_acct
,a.cust_default_date
,a.cust_default_ind
,a.corp_comm_excl
,a.staff_excl
from """ + cf.CBSDBName + """.cbs_customer_base a, SEG 
where trim(a.cust_cid) = SEG.cust_cid 
and a.eff_dt = SEG.eff_dt 
and a.date_type = SEG.date_type
and a.eff_dt = '""" + args.bdate + """' and a.date_type = '""" + date_type + """'
),

IP as 
(
select
trim(a.cust_cid) as cust_cid
,count(*) as ip_cnt
,sum(coalesce(TOT_BAL_REG_AMT,0)) as TOT_BAL_REG_AMT
,sum(coalesce(TOT_BAL_NON_REG_AMT,0)) as TOT_BAL_NON_REG_AMT
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact a, SEG
where trim(a.cust_cid) = SEG.cust_cid
and a.eff_dt between add_months('""" + args.bdate + """',-11) and '""" + args.bdate + """' and a.date_type = '""" + date_type + """'
and (tot_num_reg_acct>0 or tot_num_non_reg_acct>0)
group by a.cust_cid
),

IP_AVG as 
(
select
cust_cid
,(TOT_BAL_REG_AMT+TOT_BAL_NON_REG_AMT)/ip_cnt as tot_bal_invst_acctavg12m
from IP
),

KQ_CC as 
(
select
trim(a.cust_cid) as cust_cid
,num_of_heloc as cc_num_heloc
,num_of_accts as num_cards
,num_of_accts_crnt as num_cur_cards
,num_of_accts_def as num_def_cards
,'Y' as card_ind
,cr_type
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact a, SEG
where trim(a.cust_cid) = SEG.cust_cid
and a.eff_dt = SEG.eff_dt 
and a.date_type = SEG.date_type
and a.num_of_accts>0
and a.cr_type = 'Cards'
and a.eff_dt = '""" + args.bdate + """' and a.date_type = '""" + date_type + """'
),

KQ_LOC as 
(
select
trim(a.cust_cid) as cust_cid
,num_of_heloc as loc_num_heloc
,num_of_accts as num_loc
,num_of_accts_crnt as num_cur_loc 
,num_of_accts_def as num_def_loc
,'Y' as loc_ind
,cr_type
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact a, SEG
where trim(a.cust_cid) = SEG.cust_cid
and a.eff_dt = SEG.eff_dt and a.date_type = SEG.date_type
and a.num_of_accts>0
and a.cr_type = 'LOC'
and a.eff_dt = '""" + args.bdate + """' and a.date_type = '""" + date_type + """'
),

MDM as 
(
select
trim(a.party_id) as cust_cid
,a.cust_age
,time_on_books
,marital_status
from """ + cf.CBSDBName + """.cbs_mdm_flags a, SEG
where trim(a.party_id) = SEG.cust_cid
and a.eff_dt = SEG.eff_dt and a.date_type =SEG.date_type
and a.eff_dt = '""" + args.bdate + """' and a.date_type = '""" + date_type + """'
),

CR_BR as 
(
select 
trim(a.cust_cid) as cust_cid
,oldst_opn_trade_age_line_mth_cnt
from """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT a, SEG
where trim(a.cust_cid) = SEG.cust_cid
and a.eff_dt = SEG.eff_dt
and a.date_type = SEG.date_type
and a.eff_dt = '""" + args.bdate + """' and a.date_type = '""" + date_type + """'
)

insert overwrite table """ + cf.CBSDBName + """.cbs_cust_profile_summary partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """', seg_num)
select 
SEG.cust_cid as cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,a.num_prods as num_prod
,a.num_mort as num_mor
,a.num_spl as num_spl
,a.num_rev as num_rev
,a.num_ssl as num_ssl
,a.worst_dlq_days as worst_dlqnt_days
,e.marital_status as marital_status
,a.cust_type as cust_type
,a.cust_status as cust_stat
,a.deceased_ind as deceased_ind
,a.retail_ind as retail_ind
,e.cust_age as age
,e.time_on_books as time_on_books
,a.dep_only_flg as dep_only_flg
,SEG.noacct_excl as noacct_excl
,a.under_age_excl as under_age_excl
,a.cust_pit_stat as cust_pit_stat
,a.worst_days_dlq_cust as worst_dlqnt_days_cust
,a.worst_days_dlq_kq_cust as worst_dlqnt_days_cust_kq
,a.worst_days_dlq_mort_cust as worst_dlqnt_days_cust_mor
,a.worst_days_dlq_spl_cust as worst_dlqnt_days_cust_spl
,a.num_delq_acct as num_of_accts_dlqnt
,a.cust_default_date as cust_default_date
,a.cust_default_ind as cust_default_ind
,a.corp_comm_excl as corp_comm_excl
,a.staff_excl as staff_excl
,c.cc_num_heloc as cc_num_heloc
,d.loc_num_heloc as loc_num_heloc
,SEG.heloc_ind as heloc_ind
,SEG.mth_since_oldst_trade_opnd_cnt as mth_since_oldst_trade_opnd_cnt
,f.oldst_opn_trade_age_line_mth_cnt as oldst_opn_trade_age_line_mth_cnt
,SEG.trade_never_dlqnt_pc as trade_never_dlqnt_pc
,b.tot_bal_invst_acctavg12m as tot_bal_invst_acctavg12m
,SEG.avg_tot_sav_inv_bal_12m as tot_sav_inv_bal_amtavg12m
,SEG.cc_heloc_ind as cc_heloc_ind
,SEG.loc_heloc_ind as loc_heloc_ind
,SEG.mort_ind as mor_ind
,c.card_ind as card_ind
,d.loc_ind as loc_ind
,SEG.seg_nm as seg_nm
,c.num_cards as num_cards
,c.num_cur_cards as num_cur_cards
,c.num_def_cards as num_def_cards
,d.num_loc as num_loc
,d.num_cur_loc as num_cur_loc
,d.num_def_loc as num_def_loc
,SEG.seg_num
from
SEG
left outer join CUST_BASE a on a.cust_cid = SEG.cust_cid
left outer join IP_AVG b on b.cust_cid = SEG.cust_cid
left outer join KQ_CC c on c.cust_cid = SEG.cust_cid
left outer join KQ_LOC d on d.cust_cid = SEG.cust_cid
left outer join MDM e on e.cust_cid = SEG.cust_cid
left outer join CR_BR f on f.cust_cid = SEG.cust_cid
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
