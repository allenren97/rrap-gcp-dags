#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_var_seg9.py
#
#        USAGE: ./cbs_model_scorecrd_var_seg9.py business_date date_type
#
#  DESCRIPTION: CBS Model Scorecard Variable Segment 9 table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Gordana Z (SQL), Suhel 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/30/2018 16:18:33; Last updated: 02/11/2019
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



# insert into table cbs_model_scorecrd_var_seg9 table with monthly data

SQL1 = """
with cust_seg_9 as
(select trim(cust_cid) as cust_cid, eff_dt, date_type
from """ + cf.CBSDBName + """.cbs_cust_segmentation
where eff_dt='""" + cf.bdate + """'
and date_type='""" + date_type + """'
and seg_num=9) 
,
Full_Deli as 
(select
trim(deli.cust_cid) as cust_cid
,deli.eff_dt
,inqry_past_6_mth_cnt as INQRY_PAST_6M_C
,mth_since_last_60_day_dlqnt_cnt  as MTH_SNC_LST_60D_DLQ_CN
,mth_since_most_recnt_dlqnt_cnt as MTH_SNC_MST_RC_DLQ_C
,tot_avl_cr_not_utilized_amt as TOT_AVL_CR_NOT_UTLAMT
,trade_90_dpd_last_24_mth_cnt as TRD_90D_LST_24M_C
,max_revlvng_cr_crnt_utltn_amt  as MAX_REV_CRNT_UTLAMT
,tot_bal_tp_bankcard_amt  as TOT_BAL_TP_BCARDAMT
,trade_never_dlqnt_pc as TRD_NEVER_DLQNT_PC
,row_number() over (partition by trim(deli.cust_cid) order by deli.eff_dt desc) row_num
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot deli
, cust_seg_9 cs
where deli.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and deli.date_type='""" + date_type + """'
and trim(deli.cust_cid)= cs.cust_cid
) 
,
Var_INQRY_PAST_6M_C as
(select cust_cid
       ,INQRY_PAST_6M_C
from Full_Deli
where eff_dt = '""" + cf.bdate + """')
,
Var_MTH_SNC_LST_60D_DLQ_CN as
(select cust_cid
       ,MTH_SNC_LST_60D_DLQ_CN
from Full_Deli
where eff_dt = '""" + cf.bdate + """')
,
Var_MTH_SNC_MST_RC_DLQ_C as
(select cust_cid
       ,MTH_SNC_MST_RC_DLQ_C
from Full_Deli
where eff_dt = '""" + cf.bdate + """')
,
Var_TOT_AVL_CR_NOT_UTLAMT as
(select cust_cid
       ,TOT_AVL_CR_NOT_UTLAMT
from Full_Deli
where eff_dt = '""" + cf.bdate + """')
,
Var_TRD_90D_LST_24M_C as
(select cust_cid
       ,TRD_90D_LST_24M_C
from Full_Deli
where eff_dt = '""" + cf.bdate + """')
,
Var_MAX_REV_CRNT_UTLAMTmax12m as
(select cust_cid
       ,max(MAX_REV_CRNT_UTLAMT) as MAX_REV_CRNT_UTLAMTmax12m
 from Full_Deli
 group by cust_cid
), 
Var_TOT_BAL_TP_BCARDAMTmin3m as
(select cust_cid
       ,min(TOT_BAL_TP_BCARDAMT) as TOT_BAL_TP_BCARDAMTmin3m
 from Full_Deli
 where row_num <=3
 group by cust_cid
)
--,
--Var_TRD_NEVER_DLQNT_PCchg1m as -->>> removed due to requirements change in March 2019
--(select a.cust_cid
--  ,a.TRD_NEVER_DLQNT_PC as TRD_NEVER_DLQNT_PC_1
--  ,b.TRD_NEVER_DLQNT_PC as TRD_NEVER_DLQNT_PC_2
--  ,case when (a.TRD_NEVER_DLQNT_PC <> 0 and a.TRD_NEVER_DLQNT_PC is not null) and (b.TRD_NEVER_DLQNT_PC is null or b.TRD_NEVER_DLQNT_PC = 0 ) then 1
--        when (a.TRD_NEVER_DLQNT_PC is null or a.TRD_NEVER_DLQNT_PC = 0 ) and (b.TRD_NEVER_DLQNT_PC <> 0 or b.TRD_NEVER_DLQNT_PC  is not null) then -1
--        when a.TRD_NEVER_DLQNT_PC = 0 and b.TRD_NEVER_DLQNT_PC = 0 then 0
--        when a.TRD_NEVER_DLQNT_PC is null and b.TRD_NEVER_DLQNT_PC is null then null
--        else a.TRD_NEVER_DLQNT_PC/b.TRD_NEVER_DLQNT_PC - 1 end
--        as TRD_NEVER_DLQNT_PCchg1m
-- from Full_Deli a,
--      Full_Deli b
--where a.cust_cid=b.cust_cid
--and a.row_num=1
--and b.row_num=2
--)
,
Var_time_on_books as
(select  x.cust_cid, time_on_books
from
(select
 trim(mdm.party_id) as cust_cid
,time_on_books 
,row_number() over (partition by trim(mdm.party_id) order by mdm.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_mdm_flags mdm
    ,cust_seg_9 cs
where mdm.eff_dt = '""" + cf.bdate + """'
and mdm.date_type='""" + date_type + """'
and trim(mdm.party_id)= cs.cust_cid
) x
)
,
Full_KQ_P2 as
(select
trim(kq.cust_cid) as cust_cid
,kq.eff_dt
,cr_type
,worst_dlqnt_days
,max_rev_ind  as rev_ind
,row_number() over (partition by kq.cust_cid, cr_type order by kq.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact kq   -- CHANGE TABLE NAME !!!!
     ,cust_seg_9 cs
where trim(kq.cust_cid)= cs.cust_cid
and kq.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and kq.date_type='""" + date_type + """'
and num_of_accts  > 0
and cr_type in ('Cards','LOC')
)
,
Var_rev_indsum3m as -- check if only CC, as it's not in the requirements
(select cust_cid
       ,sum(rev_ind) as rev_indsum3m
from Full_KQ_P2
where cr_type = 'Cards'
  and row_num <=3
group by cust_cid
)
,
Full_BB as
(select
trim(bb.cust_cid) as cust_cid
,bb.eff_dt
,sum_sav_bal_prim_amt 
,row_number() over (partition by trim(bb.cust_cid) order by bb.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact bb
    ,cust_seg_9 cs
where bb.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and bb.date_type='""" + date_type + """'
and (num_sav_acct_prim > 0 ) -- no need: or num_sav_acct_sec > 0)
and trim(bb.cust_cid)= cs.cust_cid
) 
,
Full_IP as
(select
trim(ip.cust_cid) as cust_cid
,ip.eff_dt
,tot_bal_reg_amt 
,tot_bal_non_reg_amt 
,row_number() over (partition by trim(ip.cust_cid) order by ip.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact ip
    ,cust_seg_9 cs
where ip.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and ip.date_type='""" + date_type + """'
and (tot_num_reg_acct >0 or tot_num_non_reg_acct >0)
and trim(ip.cust_cid)= cs.cust_cid
) 
,
Full_BB_sum as
(select cust_cid
,sum(sum_sav_bal_prim_amt) as sav_amt
,count(*) total_bb
from Full_BB
group by cust_cid)
,
Full_IP_sum as
(select cust_cid
,sum(tot_bal_reg_amt) as reg_amt
,sum(tot_bal_non_reg_amt) non_reg_amt
,count(*) totalip
from Full_IP
group by cust_cid)
,
Periods_BB_IP as  -- it should be only periods that customers have eather of one of the accounts
(select cust_cid, count(*) total_p from 
(Select cust_cid, eff_dt from Full_IP
union
select cust_cid, eff_dt from Full_BB
) x
group by cust_cid 
)
,
Var_tot_sav_inv_bal_amtavg12m as
(select cust.cust_cid, (coalesce(sav_amt,0)+coalesce(reg_amt,0) + coalesce(non_reg_amt,0))/total_p as
tot_sav_inv_bal_amtavg12m
,total_p
from Periods_BB_IP  cust left join Full_BB_sum bb on
cust.cust_cid=bb.cust_cid
left join Full_IP_sum ip on
cust.cust_cid=ip.cust_cid)
,
FULL_KQ_SUM_P1 as
(select kq.cust_cid
,kq.eff_dt
,sum(case when cr_type='LOC' then sum_tot_new_bal_amt end) as loc_amt
,sum(case when cr_type='Cards' then sum_tot_new_bal_amt end) as cc_amt
,sum(case when cr_type='LOC' then sum_cr_lmt_amt end) as loc_lmt
,sum(case when cr_type='Cards' then sum_cr_lmt_amt end) as cc_lmt
,row_number() over (partition by kq.cust_cid order by kq.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact kq, -- sd is only Suhels temp table
cust_seg_9 cust9
where num_of_accts > 0
and kq.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and kq.date_type='""" + date_type + """'
and kq.cust_cid = cust9.cust_cid
and kq.cr_type in ('LOC','Cards')
group by kq.cust_cid, kq.eff_dt
)
,
Var_util as
(select cust_cid, case when (nvl(loc_amt,0)+nvl(cc_amt,0)) > 0 and ((nvl(loc_lmt,0)+nvl(cc_lmt,0)) <=0 or  (loc_lmt is null and cc_lmt is null)) then 1
                         when (nvl(loc_amt,0)+nvl(cc_amt,0)) = 0 and ((nvl(loc_lmt,0)+nvl(cc_lmt,0)) <=0 or (loc_lmt is null and cc_lmt is null)) then 0 
                         when (loc_amt) is null and (cc_amt) is null and (loc_lmt is not null or cc_lmt is not null) then 0
                         when (loc_amt) is null and (cc_amt) is null and (loc_lmt) is null and (cc_lmt) is null then null -- no such case
                         else (coalesce(loc_amt,0)+coalesce(cc_amt,0))/(coalesce(loc_lmt,0)+coalesce(cc_lmt,0)) 
 end as util
from FULL_KQ_SUM_P1 
where eff_dt = '""" + cf.bdate + """'
)
,
Full_Cust_Direct_Move as
(select
 trim(cust.cust_cid) as cust_cid
,worst_dlq_days
,row_number() over (partition by cust.cust_cid order by cust.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_customer_base cust
    ,cust_seg_9 cs
where cust.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and cust.date_type='""" + date_type + """'
and trim(cust.cust_cid)= cs.cust_cid
)
,
Var_worst_days_dlq_max6m as
(select x.cust_cid, max(worst_dlq_days) as worst_days_dlq_max6m
from
(select cust_cid, worst_dlq_days
from Full_Cust_Direct_Move
where row_num <= 6 
) x
group by x.cust_cid
)

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg9 partition (eff_dt='""" + cf.bdate + """', date_type='""" + date_type + """') 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9,'INQRY_PAST_6M_C',INQRY_PAST_6M_C
from cust_seg_9 cs left join Var_INQRY_PAST_6M_C var1
on cs.cust_cid=var1.cust_cid

union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'MAX_REV_CRNT_UTLAMTmax12m', MAX_REV_CRNT_UTLAMTmax12m
from cust_seg_9 cs left join Var_MAX_REV_CRNT_UTLAMTmax12m var2
on cs.cust_cid=var2.cust_cid

union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9,'MTH_SNC_LST_60D_DLQ_CN',MTH_SNC_LST_60D_DLQ_CN
from cust_seg_9 cs left join Var_MTH_SNC_LST_60D_DLQ_CN var3
on cs.cust_cid=var3.cust_cid

union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9,'MTH_SNC_MST_RC_DLQ_C', MTH_SNC_MST_RC_DLQ_C
from cust_seg_9 cs left join Var_MTH_SNC_MST_RC_DLQ_C var4
on cs.cust_cid=var4.cust_cid

union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'TOT_AVL_CR_NOT_UTLAMT', TOT_AVL_CR_NOT_UTLAMT
from cust_seg_9 cs left join Var_TOT_AVL_CR_NOT_UTLAMT var5
on cs.cust_cid=var5.cust_cid

union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'TOT_BAL_TP_BCARDAMTmin3m', TOT_BAL_TP_BCARDAMTmin3m
from cust_seg_9 cs left join Var_TOT_BAL_TP_BCARDAMTmin3m var6
on cs.cust_cid=var6.cust_cid

union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'TRD_90D_LST_24M_C', TRD_90D_LST_24M_C
from cust_seg_9 cs left join Var_TRD_90D_LST_24M_C var7
on cs.cust_cid=var7.cust_cid  

--union all -->>> removed due to requirements change in March 2019
--select cs.cust_cid 
--,'""" + os.path.realpath(__file__) + """' as op_field
--,current_timestamp as insrt_process_tmstmp
--,9, 'TRD_NEVER_DLQNT_PCchg1m', TRD_NEVER_DLQNT_PCchg1m
--from cust_seg_9 cs left join Var_TRD_NEVER_DLQNT_PCchg1m var8
--on cs.cust_cid=var8.cust_cid

union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'rev_indsum3m', rev_indsum3m
from cust_seg_9 cs left join Var_rev_indsum3m var9
on cs.cust_cid=var9.cust_cid 

union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9,'time_on_books', time_on_books
from cust_seg_9 cs left join Var_time_on_books var10
on cs.cust_cid=var10.cust_cid

union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'tot_sav_inv_bal_amtavg12m', case when total_p is null then null else tot_sav_inv_bal_amtavg12m end 
from cust_seg_9 cs left join Var_tot_sav_inv_bal_amtavg12m var11
on cs.cust_cid=var11.cust_cid

union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9, 'util', util
from cust_seg_9 cs left join Var_util var12
on cs.cust_cid=var12.cust_cid

union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,9,'worst_days_dlq_max6m', worst_days_dlq_max6m
from cust_seg_9 cs left join Var_worst_days_dlq_max6m var13
on cs.cust_cid=var13.cust_cid
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


