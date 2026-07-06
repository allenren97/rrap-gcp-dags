#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_var_seg7.py
#
#        USAGE: ./cbs_model_scorecrd_var_seg7.py business_date date_type
#
#  DESCRIPTION: CBS Model Scorecard Variable Segment 7 table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 12/24/2018 16:18:33; Last updated: 03/20/2019
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



# insert into table cbs_model_scorecrd_var_seg7 table with monthly data

SQL1 = """
with cust_seg7 as (
select trim(cust_cid) as cust_cid, eff_dt, date_type 
from """ + cf.CBSDBName + """.cbs_cust_segmentation
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and seg_num = 7
) 

, cust_base_partitioned as (
select trim(cust_cid) as cust_cid, worst_dlq_days
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_customer_base a
where eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
)

, cust_base_var_6m  as (
select cust_cid, max(worst_dlq_days) as worst_dlq_days_max6m
from cust_base_partitioned
where row_num <= 6 
group by cust_cid 
)

, cust_base_sc_var as (
select a.cust_cid, b.worst_dlq_days_max6m
from cust_seg7 a 
left outer join cust_base_var_6m b 
ON a.cust_cid = b.cust_cid 
)

, tu_partitioned as (
select trim(a.cust_cid) as cust_cid, eff_dt, colctn_cnt, inqry_cnt, mth_since_last_30_day_dlqnt_cnt
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot  a
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
)

, tu_var_6m as (
select cust_cid, max(colctn_cnt) as COLCTN_Cmax6m
from tu_partitioned
where row_num <=6
group by cust_cid 
)

, tu_direct as (
select trim(cust_cid) as cust_cid, inqry_cnt, mth_since_last_30_day_dlqnt_cnt
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot  a 
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
)

, tu_sc_var as (
select a.cust_cid, b.COLCTN_Cmax6m, c.inqry_cnt, c.mth_since_last_30_day_dlqnt_cnt
from cust_seg7 a 
left outer join tu_var_6m b 
ON a.cust_cid = b.cust_cid 
left outer join tu_direct c 
ON a.cust_cid = c.cust_cid 
)

, sav_direct as (
select trim(cust_cid) as cust_cid, sum_pymt_amt as SUM_OF_PAYAMT, sum_sav_bal_amt as CUST_TOT_SAV_BAL
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact a  
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
) 

, sav_sc_var as (
select a.cust_cid, b.SUM_OF_PAYAMT, b.CUST_TOT_SAV_BAL
from cust_seg7 a 
left outer join sav_direct b
ON a.cust_cid = b.cust_cid 
)

, ip_partitioned as (
select trim(a.cust_cid) as cust_cid, eff_dt, tot_bal_reg_amt, tot_bal_non_reg_amt
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact a
where eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and (tot_num_reg_acct >0 or tot_num_non_reg_acct >0)
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
)

, ip_var_3m as (
select cust_cid, min(coalesce(tot_bal_reg_amt,0)+coalesce(tot_bal_non_reg_amt,0)) as TOT_BAL_INVST_ACCTmin3m
from ip_partitioned 
where row_num <= 3
group by cust_cid 
)

, ip_direct as (
select trim(cust_cid) as cust_cid, tot_bal_reg_amt as TOT_BAL_REG_ACCT
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact a
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
)

, ip_sc_var as (
select a.cust_cid, b.TOT_BAL_INVST_ACCTmin3m, c.TOT_BAL_REG_ACCT
from cust_seg7 a 
left outer join ip_var_3m b 
ON a.cust_cid = b.cust_cid 
left outer join ip_direct c 
ON a.cust_cid = c.cust_cid 
)

, kq_partition as (
select trim(a.cust_cid) as cust_cid, cr_type, eff_dt, sum_csh_advnc_bal_amt, sum_csh_advnc_intr_chrgd_amt, sum_2_cycl_ago_prch_bal_amt as sum_prch_bal_amt2, sum_total_int_chrgd_amt,num_of_accts_dlqnt
, sum_tot_new_bal_amt, sum_cr_lmt_amt
, row_number() over (partition by a.cust_cid, cr_type order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact  a
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and upper(cr_type) IN ('CARDS', 'LOC')
and num_of_accts > 0 
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
)

, kq_var_3m as (
select cust_cid, avg(num_of_accts_dlqnt) as num_cc_dlqavg3m
from kq_partition
where row_num <= 3
and upper(cr_type) = 'CARDS'
group by cust_cid
)

, kq_var_6m as (
select cust_cid, avg(sum_prch_bal_amt2) as cc_PRCH_BALAMT2avg6m
from kq_partition
where row_num <= 6 
and upper(cr_type) = 'CARDS'
group by cust_cid
) 

, kq_var_12m as (
select cust_cid
, avg(sum_csh_advnc_bal_amt) as cc_CSH_ADVNC_BALAMTavg12m
, avg(sum_csh_advnc_intr_chrgd_amt) as cc_CSH_ADV_INT_CHGAMTavg12m
, avg(sum_total_int_chrgd_amt) as cc_TOT_INT_CHGavg12m
from kq_partition
where row_num <= 12 
and upper(cr_type) = 'CARDS'
group by cust_cid
)

, kq_util as (
select trim(cust_cid) as cust_cid 
, sum(case when cr_type = 'LOC' then sum_tot_new_bal_amt end) as loc_total_new_bal_amt
, sum(case when cr_type = 'LOC' then sum_cr_lmt_amt end) as loc_cr_lmt_amt
, sum(case when upper(cr_type) = 'CARDS' then sum_tot_new_bal_amt end) as cc_total_new_bal_amt
, sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt end) as cc_cr_lmt_amt
, cast(
case 
when sum(case when cr_type = 'LOC' then sum_tot_new_bal_amt end) is null 
and sum(case when cr_type = 'LOC' then sum_cr_lmt_amt end) is null
and sum(case when upper(cr_type) = 'CARDS' then sum_tot_new_bal_amt end) is null 
and sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt end) is null 
then NULL 

when sum(case when cr_type = 'LOC' then sum_tot_new_bal_amt end) is null 
and sum(case when upper(cr_type) = 'CARDS' then sum_tot_new_bal_amt end) is null 
and (
sum(case when cr_type = 'LOC' then sum_cr_lmt_amt end) is not null OR
sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt end) is not null
) then 0 

when (
sum(case when cr_type = 'LOC' then sum_tot_new_bal_amt else 0 end) + sum(case when upper(cr_type) = 'CARDS' then sum_tot_new_bal_amt else 0 end)
) > 0 
and (
(
sum(case when cr_type = 'LOC' then sum_cr_lmt_amt else 0 end) + sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt else 0 end)
) <= 0 
 OR 
(
sum(case when cr_type = 'LOC' then sum_cr_lmt_amt end) is null and sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt end) is null
) 
) then 1 

when (
sum(case when cr_type = 'LOC' then sum_tot_new_bal_amt else 0 end) + sum(case when upper(cr_type) = 'CARDS' then sum_tot_new_bal_amt else 0 end)
) = 0 
and (
(
sum(case when cr_type = 'LOC' then sum_cr_lmt_amt else 0 end) + sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt else 0 end)
) <= 0 
OR 
(
sum(case when cr_type = 'LOC' then sum_cr_lmt_amt end) is null and sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt end) is null
) 
) then 0

else 
(
coalesce(sum(case when cr_type = 'LOC' then sum_tot_new_bal_amt end),0) + coalesce(sum(case when upper(cr_type) = 'CARDS' then sum_tot_new_bal_amt end),0)
)
/ (
coalesce(sum(case when cr_type = 'LOC' then sum_cr_lmt_amt end),0) + coalesce(sum(case when upper(cr_type) = 'CARDS' then sum_cr_lmt_amt end),0)
) end as decimal(17,3)) as util 
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact  a
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and upper(cr_type) IN ('CARDS', 'LOC')
and trim(a.cust_cid) IN (select cust_cid from cust_seg7)
group by cust_cid
)


, kq_sc_var as (
select a.cust_cid
, b.num_cc_dlqavg3m
, c.cc_CSH_ADVNC_BALAMTavg12m
, c.cc_CSH_ADV_INT_CHGAMTavg12m
, c.cc_TOT_INT_CHGavg12m
, d.cc_PRCH_BALAMT2avg6m 
, f.util 
from cust_seg7 a 
left outer join kq_var_3m b
ON a.cust_cid = b.cust_cid 
left outer join kq_var_12m c 
ON a.cust_cid = c.cust_cid 
left outer join kq_var_6m d
ON a.cust_cid = d.cust_cid 
left outer join kq_util f 
ON a.cust_cid = f.cust_cid 
)


, sc_vars_seg7 as (
select seg.cust_cid
, seg.eff_dt
, seg.date_type
, tu.COLCTN_Cmax6m
, tu.inqry_cnt as INQRY_C
, tu.mth_since_last_30_day_dlqnt_cnt as MTH_SNC_LST_30D_DLQ_CN
, sav.SUM_OF_PAYAMT
, sav.CUST_TOT_SAV_BAL
, ip.TOT_BAL_INVST_ACCTmin3m
, ip.TOT_BAL_REG_ACCT
, kq.cc_CSH_ADVNC_BALAMTavg12m
, kq.cc_CSH_ADV_INT_CHGAMTavg12m
, kq.cc_PRCH_BALAMT2avg6m
, kq.cc_TOT_INT_CHGavg12m
, kq.num_cc_dlqavg3m
, kq.util
, cast(cu.worst_dlq_days_max6m as decimal(17,3)) as worst_days_dlq_max6m

from cust_seg7 seg 

left outer join tu_sc_var tu 
ON seg.cust_cid = trim(tu.cust_cid) 

left outer join sav_sc_var sav
ON seg.cust_cid = sav.cust_cid

left outer join ip_sc_var ip 
ON seg.cust_cid = ip.cust_cid 

left outer join kq_sc_var kq
ON seg.cust_cid = kq.cust_cid 

left outer join cust_base_sc_var cu 
ON seg.cust_cid = cu.cust_cid 

)

, sc_vars_trans_seg7 as (
select cust_cid, eff_dt, date_type, sc_var, sc_var_val
from (
select cust_cid, eff_dt, date_type, map(
            'COLCTN_Cmax6m', COLCTN_Cmax6m,
            'INQRY_C', INQRY_C,
            'MTH_SNC_LST_30D_DLQ_CN', MTH_SNC_LST_30D_DLQ_CN,
            'SUM_OF_PAYAMT', SUM_OF_PAYAMT,
            'TOT_BAL_INVST_ACCTmin3m',TOT_BAL_INVST_ACCTmin3m,
            'TOT_BAL_REG_ACCT', TOT_BAL_REG_ACCT,
            'cc_CSH_ADVNC_BALAMTavg12m',cc_CSH_ADVNC_BALAMTavg12m,
            'cc_CSH_ADV_INT_CHGAMTavg12m',cc_CSH_ADV_INT_CHGAMTavg12m,
            'cc_PRCH_BALAMT2avg6m',cc_PRCH_BALAMT2avg6m,
            'cc_TOTAL_INT_CHGavg12m', cc_TOT_INT_CHGavg12m,
            'CUST_TOT_SAV_BAL',CUST_TOT_SAV_BAL,
            'num_cc_dlqavg3m',num_cc_dlqavg3m,
            'util',util,
            'worst_days_dlq_max6m', worst_days_dlq_max6m) as map_sc
from sc_vars_seg7 ) a 
lateral view explode(map_sc) expsc as sc_var, sc_var_val
)

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg7 partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_cid
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
, 7 as seg_num
, sc_var
, sc_var_val 
from sc_vars_trans_seg7
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


