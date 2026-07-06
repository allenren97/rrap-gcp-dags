#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_cust_segmentation.py
#
#        USAGE: ./cbs_cust_segmentation.py business_date date_type
#
#  DESCRIPTION: CBS Customer Segmentation table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 12/10/2018 16:18:33; Last updated: 02/12/2019
#     REVIEWER: 
#     REVISION: 
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



# insert into table cbs_cust_segmentation table with monthly data

SQL1 = """
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

with cust_base as (
select cust_cid, eff_dt, date_type, cust_type, cust_status, noacct_excl, dep_only_flg, worst_dlq_days
, case when (worst_dlq_days <= 0 or worst_dlq_days is null) then 'Non-Delinquent'
when worst_dlq_days > 0 and worst_dlq_days <= 29 then 'Delinquent - Cycle I'
when worst_dlq_days > 29 and worst_dlq_days <= 59 then 'Delinquent - Cycle II'
when worst_dlq_days > 59 and worst_dlq_days <= 89 then 'Delinquent - Cycle III'
when worst_dlq_days >= 90 then 'Default'
end as delq_cat
from """ + cf.CBSDBName + """.cbs_customer_base
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
--and cust_status = 'Open'
)
, mdm_flags as (
select party_id, time_on_books
, case when time_on_books between 0 and 6 then 'Y' else 'N' end as new_cust_ind
from """ + cf.CBSDBName + """.cbs_mdm_flags
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
) 
, acct_base as (
select cust_cid
, if(sum(case when mort_ind = 1 then 1 else 0 end)>0,1,0) as mort_ind 
, if(sum(case when spl_ind = 1 then 1 else 0 end)>0,1,0) as spl_ind
from """ + cf.CBSDBName + """.cbs_acct_base 
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
group by cust_cid 
)
, credit_bureau as ( 
select trim(cust_cid) as cust_cid, mth_since_oldst_trade_opnd_cnt, trade_never_dlqnt_pc
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
)
, kq_sum as (
select cust_cid
, if(sum(case when cr_type = 'LOC' then num_of_heloc else 0 end)>0,1, 0) as loc_heloc_ind
, if(sum(case when cr_type = 'Cards' then num_of_heloc else 0 end)>0,1,0) as cc_heloc_ind
, case
when if(sum(case when cr_type = 'LOC' then num_of_heloc else 0 end)>0,1, 0)=1 
OR if(sum(case when cr_type = 'Cards' then num_of_heloc else 0 end)>0,1,0)=1
then 1 else 0 end as heloc_ind
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact 
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
group by cust_cid 
)
, sav_bal as (
select cust_cid, eff_dt, greatest(cast(0 as decimal(18,2)),sum_sav_bal_prim_amt) as sum_bal  
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact 
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
--and (num_sav_acct_prim > 0 or num_sav_acct_sec > 0)
)
, inv_bal as (
select cust_cid, eff_dt
,greatest(cast (0 as decimal(18,2)), cast((coalesce(TOT_BAL_REG_AMT,0)+ coalesce(TOT_BAL_NON_REG_AMT,0)) as decimal(18,2))) as sum_bal
--,greatest(cast(0 as decimal(18,2)),coalesce(tot_bal_reg_amt,0)) + greatest(cast(0 as decimal(18,2)),coalesce(tot_bal_non_reg_amt,0)) as sum_bal
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
--and (TOT_NUM_REG_ACCT >0 or TOT_NUM_NON_REG_ACCT >0)
)

, sav_inv_bal as (
select cust_cid, eff_dt, sum(sum_bal) as sum_bal
, row_number() over (partition by cust_cid order by eff_dt desc) row_num 
from 
(select trim(cust_cid) as cust_cid, eff_dt, sum_bal 
from sav_bal
union all   
select trim(cust_cid) as cust_cid, eff_dt, sum_bal 
from inv_bal) a 
group by cust_cid, eff_dt 
)

, sav_inv_avg as (
select cust_cid, avg(sum_bal) as avg_tot_sav_inv_bal_12m
, sum(sum_bal)/count(distinct eff_dt) as avg_tot_sav_inv_bal_12m1
from sav_inv_bal 
where row_num <= 12
group by cust_cid
) 

, cust_seg as (
select a.cust_cid
, a.cust_type
, a.cust_status 
, a.noacct_excl 
, a.dep_only_flg 
, a.worst_dlq_days
, case 
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '1' then 'Deposit Only - Not Applicable' 
else a.delq_cat end as delq_cat
, b.time_on_books
, b.new_cust_ind
, f.avg_tot_sav_inv_bal_12m
, case when f.avg_tot_sav_inv_bal_12m >= 50000 then 'Y' else 'N' end as high_val_cust_ind
, e.loc_heloc_ind
, e.cc_heloc_ind
, e.heloc_ind
, c.mort_ind
, c.spl_ind 
, d.mth_since_oldst_trade_opnd_cnt
, d.trade_never_dlqnt_pc
, 
case when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '1' then 10
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and a.worst_dlq_days > 0 and a.worst_dlq_days <= 29 then 2
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and a.worst_dlq_days > 29 and a.worst_dlq_days <= 89 then 1
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and a.worst_dlq_days >= 90 then 11
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) and b.time_on_books >=0 and b.time_on_books < 7 then 3
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) 
 and (b.time_on_books >= 7 or b.time_on_books is null) and f.avg_tot_sav_inv_bal_12m >= 50000 then 4
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) 
 and (b.time_on_books >= 7 or b.time_on_books is null) and (f.avg_tot_sav_inv_bal_12m < 50000 or f.avg_tot_sav_inv_bal_12m is null) and (e.heloc_ind = 1 or c.mort_ind = '1') then 5
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) 
 and (b.time_on_books >= 7 or b.time_on_books is null) and (f.avg_tot_sav_inv_bal_12m < 50000 or f.avg_tot_sav_inv_bal_12m is null) and (e.heloc_ind = 0 or e.heloc_ind is null) and c.mort_ind = '0' and c.spl_ind ='1' then 6
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) 
 and (b.time_on_books >= 7 or b.time_on_books is null) and (f.avg_tot_sav_inv_bal_12m < 50000 or f.avg_tot_sav_inv_bal_12m is null) and (e.heloc_ind = 0 or e.heloc_ind is null) and c.mort_ind = '0' and c.spl_ind = '0' 
 and (d.mth_since_oldst_trade_opnd_cnt < 25 or d.mth_since_oldst_trade_opnd_cnt is null) then 7
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) 
 and (b.time_on_books >= 7 or b.time_on_books is null) and (f.avg_tot_sav_inv_bal_12m < 50000 or f.avg_tot_sav_inv_bal_12m is null) and (e.heloc_ind = 0 or e.heloc_ind is null) and c.mort_ind = '0' and c.spl_ind = '0' and d.mth_since_oldst_trade_opnd_cnt >= 25 
 and d.trade_never_dlqnt_pc = 100 then 8
when a.cust_type = 'Retail' and a.cust_status = 'Open' and a.noacct_excl = 'N' and a.dep_only_flg = '0' and (a.worst_dlq_days <= 0 or a.worst_dlq_days is null) 
 and (b.time_on_books >= 7 or b.time_on_books is null) and (f.avg_tot_sav_inv_bal_12m < 50000 or f.avg_tot_sav_inv_bal_12m is null) and (e.heloc_ind = 0 or e.heloc_ind is null) and c.mort_ind = '0' and c.spl_ind = '0' and d.mth_since_oldst_trade_opnd_cnt >= 25 
 and (d.trade_never_dlqnt_pc < 100 or d.trade_never_dlqnt_pc is null) then 9
else -1  
end as seg_num
, a.eff_dt
, a.date_type 

from cust_base a 
left outer join mdm_flags b 
ON trim(a.cust_cid) = trim(b.party_id) 

left outer join acct_base c 
ON trim(a.cust_cid) = trim(c.cust_cid) 

left outer join credit_bureau d 
ON trim(a.cust_cid) = trim(d.cust_cid) 

left outer join kq_sum e 
ON trim(a.cust_cid) = trim(e.cust_cid) 

left outer join sav_inv_avg f 
ON trim(a.cust_cid) = trim(f.cust_cid) 

)

insert overwrite table """ + cf.CBSDBName + """.cbs_cust_segmentation partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """', seg_num)
select cust_cid
, current_timestamp as insrt_process_tmstmp
, '""" + os.path.realpath(__file__) + """' as op_field
, cust_type
, cust_status 
, noacct_excl 
, dep_only_flg 
, worst_dlq_days
, delq_cat
, time_on_books
, new_cust_ind
, avg_tot_sav_inv_bal_12m
, high_val_cust_ind
, loc_heloc_ind
, cc_heloc_ind
, heloc_ind
, mort_ind
, spl_ind 
, mth_since_oldst_trade_opnd_cnt
, trade_never_dlqnt_pc
, case when seg_num = 1 then 'Delinquent Cycle II or Delinquent Cycle III'
when seg_num = 2 then 'Delinquent Cycle I'
when seg_num = 3 then 'New Customer'
when seg_num = 4 then 'High Value Customer'
when seg_num = 5 then 'Real-Estate Secured'
when seg_num = 6 then 'Other Secured'
when seg_num = 7 then 'Unsecured Shallow/Thin Customers'
when seg_num = 8 then 'Unsecured Clean'
when seg_num = 9 then 'Unsecured Dirty'
when seg_num = 10 then 'Deposit Only Customers'
when seg_num = 11 then 'Default Customers'
else 'Other' 
end as seg_nm 
, case when seg_num = 1 then 'Segment 1 - Delinquent Cycle II or Delinquent Cycle III'
when seg_num = 2 then 'Segment 2 - Delinquent Cycle I'
when seg_num = 3 then 'Segment 3 - New Customer'
when seg_num = 4 then 'Segment 4 - High Value Customer'
when seg_num = 5 then 'Segment 5 - Real-Estate Secured'
when seg_num = 6 then 'Segment 6 - Other Secured'
when seg_num = 7 then 'Segment 7 - Unsecured Shallow/Thin Customers'
when seg_num = 8 then 'Segment 8 - Unsecured Clean'
when seg_num = 9 then 'Segment 9 - Unsecured Dirty'
when seg_num = 10 then 'Segment 10 - Deposit Only Customers'
when seg_num = 11 then 'Segment 11 - Default Customers'
else 'Other' 
end as seg_desc 
, seg_num
from cust_seg 
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


