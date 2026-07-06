#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_var_seg3.py
#
#        USAGE: ./cbs_model_scorecrd_var_seg3.py business_date date_type
#
#  DESCRIPTION: CBS Model Scorecard Variable Segment 3 table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/30/2018 16:18:33; Last updated: 03/22/2019
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



# insert into table cbs_model_scorecrd_var_seg3 table with monthly data

SQL1 = """
with cust_seg3 as (
select trim(cust_cid) as cust_cid, eff_dt, date_type 
from """ + cf.CBSDBName + """.cbs_cust_segmentation
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and seg_num = 3
) 

, cust_base_partitioned as (
select trim(cust_cid) as cust_cid, worst_dlq_days
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_customer_base a
where eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, cust_base_var_6m  as (
select cust_cid, max(worst_dlq_days) as worst_dlq_days_max6m
from cust_base_partitioned
where row_num <= 6 
group by cust_cid 
)

, cust_base_sc_var as (
select a.cust_cid, b.worst_dlq_days_max6m
from cust_seg3 a 
left outer join cust_base_var_6m b 
ON a.cust_cid = b.cust_cid 
)

, spl_partitioned as (
select trim(a.cust_cid) as cust_cid, eff_dt, sum_accr_intr, direct_ind, subvented_ind
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.cbs_spl_cust_sum_fact a
where eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and num_of_spl_acct > 0
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, spl_var_6m as (
select cust_cid, avg(sum_accr_intr) as ACCR_INT_SPLavg6m
from spl_partitioned 
where row_num <= 6
group by cust_cid
)

, spl_direct as (
select trim(cust_cid) as cust_cid, direct_ind, subvented_ind
from """ + cf.CBSDBName + """.cbs_spl_cust_sum_fact a
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, spl_sc_var as (
select a.cust_cid, b.ACCR_INT_SPLavg6m, c.direct_ind, c.subvented_ind
from cust_seg3 a 
left outer join spl_var_6m b 
ON a.cust_cid = b.cust_cid 
left outer join spl_direct c 
ON a.cust_cid = c.cust_cid 
)

, sav_partitioned as (
select trim(a.cust_cid) cust_cid, eff_dt, num_of_nsf, sum_sav_bal_amt
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact a  
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and (num_sav_acct_prim > 0 or num_sav_acct_sec > 0)
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, sav_var_3m as (
select cust_cid, max(num_of_nsf) as SUM_OF_NSF_NUMmax3m
from sav_partitioned
where row_num <= 3 
group by cust_cid
)

, sav_direct as (
select trim(cust_cid) as cust_cid, sum_sav_bal_amt as CUST_TOT_SAV_BAL
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact a  
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, sav_sc_var as (
select a.cust_cid, b.SUM_OF_NSF_NUMmax3m, c.CUST_TOT_SAV_BAL
from cust_seg3 a 
left outer join sav_var_3m b 
ON a.cust_cid = b.cust_cid 
left outer join sav_direct c 
ON a.cust_cid = c.cust_cid 
)

, tu_partitioned as (
select trim(a.cust_cid) as cust_cid, eff_dt, tot_utltn_amt, inqry_past_6_mth_cnt, mth_since_most_recnt_dlqnt_cnt, mth_since_oldst_trade_opnd_cnt, tm_30_day_pd_last_12_mth_cnt
, row_number() over (partition by a.cust_cid order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot  a
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, tu_var_6m as (
select cust_cid, max(tot_utltn_amt) as TOT_UTLAMTmax6m
from tu_partitioned
where row_num <=6
group by cust_cid 
)

, tu_direct as (
select trim(cust_cid) as cust_cid, inqry_past_6_mth_cnt, mth_since_most_recnt_dlqnt_cnt, mth_since_oldst_trade_opnd_cnt, tm_30_day_pd_last_12_mth_cnt
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot  a
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, tu_sc_var as (
select a.cust_cid, b.TOT_UTLAMTmax6m, c.inqry_past_6_mth_cnt, c.mth_since_most_recnt_dlqnt_cnt, c.mth_since_oldst_trade_opnd_cnt, c.tm_30_day_pd_last_12_mth_cnt
from cust_seg3 a 
left outer join tu_var_6m b 
ON a.cust_cid = b.cust_cid 
left outer join tu_direct c 
ON a.cust_cid = c.cust_cid 
)

, kq_partition as (
select trim(a.cust_cid) as cust_cid, cr_type, eff_dt, sum_csh_advnc_bal_amt, sum_tot_new_bal_amt, sum_cr_lmt_amt, num_of_heloc
, row_number() over (partition by a.cust_cid, cr_type order by eff_dt desc) row_num 
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact  a
where eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and upper(cr_type) IN ('CARDS', 'LOC')
and num_of_accts > 0 
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)

, kq_var_6m as (
select cust_cid ,avg(sum_csh_advnc_bal_amt) as CSH_ADVNC_BALAMTavg6m
from kq_partition
where row_num <= 6 
and upper(cr_type) = 'CARDS'
group by cust_cid
) 

, kq_direct as (
select trim(cust_cid) as cust_cid 
, sum(case when upper(cr_type) = 'CARDS' and num_of_heloc > 0 then 1 else 0 end) as cc_heloc_ind
, sum(case when upper(cr_type) = 'LOC' and num_of_heloc > 0 then 1 else 0 end) as loc_heloc_ind
, sum(case when upper(cr_type) = 'CARDS' and sum_tot_new_bal_amt is not null and sum_cr_lmt_amt is not null then 1 else 0 end) as card_ind 
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact  a
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and upper(cr_type) IN ('CARDS', 'LOC')
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
group by cust_cid 
)

, kq_sc_var as (
select a.cust_cid, b.CSH_ADVNC_BALAMTavg6m, c.cc_heloc_ind, c.loc_heloc_ind, c.card_ind
from cust_seg3 a 
left outer join kq_var_6m b 
ON a.cust_cid = b.cust_cid 
left outer join kq_direct c
ON a.cust_cid = c.cust_cid 
)

, kq_util as (
select trim(cust_cid) as cust_cid 
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
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
group by cust_cid
)


, mort_sc_var as (
select trim(a.cust_cid) as cust_cid
from """ + cf.CBSDBName + """.cbs_mort_cust_sum_fact  a
where eff_dt = '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and trim(a.cust_cid) IN (select cust_cid from cust_seg3)
)


, sc_vars_seg3 as (
select seg.cust_cid
, seg.eff_dt
, seg.date_type 
, spl.ACCR_INT_SPLavg6m                                                              
, cast(spl.direct_ind as decimal(17,3)) as direct_ind                                                                     
, cast(spl.subvented_ind as decimal(17,3)) as subvented_ind                                                                   
, sav.SUM_OF_NSF_NUMmax3m                                                             
, sav.CUST_TOT_SAV_BAL                                                                
, tu.inqry_past_6_mth_cnt                                                            
, tu.mth_since_most_recnt_dlqnt_cnt                                                  
, tu.mth_since_oldst_trade_opnd_cnt                                                  
, tu.tm_30_day_pd_last_12_mth_cnt                                                    
, tu.TOT_UTLAMTmax6m                                                                 
, kqcc.CSH_ADVNC_BALAMTavg6m as cc_CSH_ADVNC_BALAMTavg6m                               
, cast(case 
when mor.cust_cid is not null  or kqcc.cc_heloc_ind = 1  or kqcc.loc_heloc_ind = 1 then 3
when kqcc.card_ind = 1 then 1
else 2 end as decimal(17,3)) as prod_mix                                                               
, kq.util                                                                           
, cast(cu.worst_dlq_days_max6m as decimal(17,3)) as worst_days_dlq_max6m 

from cust_seg3 seg 

left outer join spl_sc_var spl
ON seg.cust_cid = spl.cust_cid 

left outer join sav_sc_var sav
ON seg.cust_cid = sav.cust_cid

left outer join tu_sc_var tu 
ON seg.cust_cid = trim(tu.cust_cid) 

left outer join kq_sc_var kqcc
ON seg.cust_cid = kqcc.cust_cid 

left outer join kq_util kq 
ON seg.cust_cid = kq.cust_cid 

left outer join mort_sc_var mor
ON seg.cust_cid = mor.cust_cid 

left outer join cust_base_sc_var cu 
ON seg.cust_cid = cu.cust_cid 

)

, sc_vars_trans_seg3 as (
select cust_cid, eff_dt, date_type, sc_var, sc_var_val
from (
select cust_cid, eff_dt, date_type, map(
            'ACCR_INT_SPLavg6m', ACCR_INT_SPLavg6m,
            'CUST_TOT_SAV_BAL',CUST_TOT_SAV_BAL,
            'INQRY_PAST_6M_C', inqry_past_6_mth_cnt,
            'MTH_SNC_MST_RC_DLQ_C',mth_since_most_recnt_dlqnt_cnt,            
            'MTH_SNC_OLD_TRD_OPND_C',mth_since_oldst_trade_opnd_cnt,
            'SUM_OF_NSF_NUMmax3m', SUM_OF_NSF_NUMmax3m,
            'TM_30D_PD_LST_12M_C',tm_30_day_pd_last_12_mth_cnt,
            'TOT_UTLAMTmax6m', TOT_UTLAMTmax6m,
            'cc_CSH_ADVNC_BALAMTavg6m',cc_CSH_ADVNC_BALAMTavg6m,
            'direct_ind', direct_ind,
            'prod_mix',prod_mix,
            'subvented_ind', subvented_ind,
            'util',util, 
            'worst_days_dlq_max6m', worst_days_dlq_max6m) as map_sc
from sc_vars_seg3 ) a 
lateral view explode(map_sc) expsc as sc_var, sc_var_val
)

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg3 partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select cust_cid
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
, 3 as seg_num
, sc_var
, sc_var_val 
from sc_vars_trans_seg3 
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


