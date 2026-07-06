#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_var_seg5.py
#
#        USAGE: ./cbs_model_scorecrd_var_seg5.py business_date date_type
#
#  DESCRIPTION: CBS Model Scorecard Variable Segment 5 table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Gordana Z (SQL), Suhel 
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



# insert into table cbs_model_scorecrd_var_seg5 table with monthly data

SQL1 = """
with cust_seg_5 as
(select trim(cust_cid) as cust_cid, eff_dt, date_type
from """ + cf.CBSDBName + """.cbs_cust_segmentation
where eff_dt='""" + cf.bdate + """'
and date_type='""" + date_type + """'
and seg_num=5)  
,
Full_Deli_Direct_Move as 
(select
trim(d.cust_cid) as cust_cid
,d.eff_dt
,highst_actv_utltn as HIGHST_ACTV_UTL 
,row_number() over (partition by trim(d.cust_cid) order by d.eff_dt desc) row_num
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot d
,cust_seg_5 cs
where d.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and d.date_type='""" + date_type + """'
and trim(d.cust_cid)= cs.cust_cid
) 

, tu_direct as (
select
trim(d.cust_cid) as cust_cid
,d.eff_dt
,highst_actv_utltn as HIGHST_ACTV_UTL 
,inqry_past_6_mth_cnt as INQRY_PAST_6M_C
,mth_since_last_60_day_dlqnt_cnt as MTH_SNC_LST_60D_DLQ_CN
,mth_since_most_recnt_dlqnt_cnt as MTH_SNC_MST_RC_DLQ_C
,tot_utltn_amt as TOT_UTLAMT
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot d
,cust_seg_5 cs
where d.eff_dt = '""" + cf.bdate + """'
and d.date_type='""" + date_type + """'
and trim(d.cust_cid)= cs.cust_cid
)
,
Var_HIGHST_ACTV_UTL as
(
select cust_cid, HIGHST_ACTV_UTL
from tu_direct
 )
,
Var_INQRY_PAST_6M_C as
(
select cust_cid, INQRY_PAST_6M_C
from tu_direct
 )
,
Var_MTH_SNC_LST_60D_DLQ_CN as
(
select cust_cid, MTH_SNC_LST_60D_DLQ_CN
from tu_direct
 )
,
Var_MTH_SNC_MST_RC_DLQ_C as
(select cust_cid, MTH_SNC_MST_RC_DLQ_C
 from tu_direct
)
,
Var_TOT_UTLAMT as
(select cust_cid, TOT_UTLAMT
from tu_direct
 )
,
Var_HIGHST_ACTV_UTLmax12m as
(select cust_cid, max(HIGHST_ACTV_UTL) as HIGHST_ACTV_UTLmax12m
from Full_Deli_Direct_Move
group by cust_cid
)
,
Full_Sav_Direct_Move as 
(select
trim(bb.cust_cid) as cust_cid
,bb.eff_dt
,sum_dep_amt
,row_number() over (partition by trim(bb.cust_cid) order by bb.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact bb
    ,cust_seg_5 cs
where bb.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and bb.date_type='""" + date_type + """'
and (num_sav_acct_prim > 0 or num_sav_acct_sec > 0)
and trim(bb.cust_cid)= cs.cust_cid
) 

, sav_direct as (
select
trim(bb.cust_cid) as cust_cid
,bb.eff_dt
,num_of_nsf
,sum_pymt_amt 
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact bb
,cust_seg_5 cs
where bb.eff_dt = '""" + cf.bdate + """'
and bb.date_type='""" + date_type + """'
and trim(bb.cust_cid)= cs.cust_cid
)
,
Var_SUM_OF_NSF_NUM as
(select cust_cid,
        num_of_nsf  as SUM_OF_NSF_NUM
 from sav_direct 
)
,
Var_SUM_OF_PAYAMT as
(select cust_cid,
        sum_pymt_amt as SUM_OF_PAYAMT
 from sav_direct 
)
,
Var_SUM_OF_DEPAMTmin6m as
(select cust_cid, min(sum_dep_amt) as SUM_OF_DEPAMTmin6m 
 from Full_Sav_Direct_Move 
where row_num <=6
group by cust_cid)
,
Var_TOT_BAL_REG_ACCT as
(
select
trim(ip.cust_cid) as cust_cid
,ip.eff_dt
,tot_bal_reg_amt as TOT_BAL_REG_ACCT
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact ip
,cust_seg_5 cs
where ip.eff_dt = '""" + cf.bdate + """'
and ip.date_type='""" + date_type + """'
and trim(ip.cust_cid)= cs.cust_cid
)
,
Full_Cust_Direct_Move as
(select
 trim(cust.cust_cid) as cust_cid
,avg_ltv as avg_LTV
,avg_ltv_heloc as avg_LTV_heloc
,worst_dlq_days
,worst_days_dlq_mort_cust
,row_number() over (partition by cust.cust_cid order by cust.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_customer_base cust
    ,cust_seg_5 cs
where cust.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and cust.date_type='""" + date_type + """'
and trim(cust.cust_cid)= cs.cust_cid
)
,
Var_avg_LTV as
(select cust_cid,
        avg_LTV
 from Full_Cust_Direct_Move 
 where row_num = 1)
,
Var_avg_LTV_heloc as
(select cust_cid,
        avg_LTV_heloc
 from Full_Cust_Direct_Move 
 where row_num = 1
 )
,
Var_worst_mor_dlq_daysmax3m as
(select cust_cid, 
        max(worst_days_dlq_mort_cust) as worst_mor_dlq_daysmax3m
from Full_Cust_Direct_Move
where row_num <= 3
group by cust_cid
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

,
Full_KQ_Direct_Move_P2 as
(select
trim(kq.cust_cid) as cust_cid 
,cr_type
,kq.eff_dt
,num_of_accts_dlqnt
,row_number() over (partition by kq.cust_cid, cr_type order by kq.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact kq   
,cust_seg_5 cs
where trim(kq.cust_cid)= cs.cust_cid
and kq.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
and kq.date_type='""" + date_type + """'
and num_of_accts  > 0
and cr_type = 'Cards'
)

,
Var_num_cc_dlqmax12m as
(select cust_cid, max(num_of_accts_dlqnt) as num_cc_dlqmax12m
from Full_KQ_Direct_Move_P2
group by cust_cid
)

,
Sum_KQ_P2 as
(select trim(kq.cust_cid) as cust_cid 
,kq.eff_dt
,sum(case when cr_type='LOC' then sum_tot_new_bal_amt end) as loc_amt
,sum(case when cr_type='Cards' then sum_tot_new_bal_amt end) as cc_amt
,sum(case when cr_type='LOC' then sum_cr_lmt_amt end) as loc_lmt
,sum(case when cr_type='Cards' then sum_cr_lmt_amt end) as cc_lmt
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact kq, 
cust_seg_5 cust5
where kq.eff_dt = '""" + cf.bdate + """'
and kq.date_type='""" + date_type + """'
and trim(kq.cust_cid) = cust5.cust_cid
and kq.cr_type in ('LOC','Cards') 
group by kq.cust_cid, kq.eff_dt
)

,
Var_cc_TOT_NEW_BAL_AMT as
(select cust_cid, cc_amt as cc_TOT_NEW_BAL_AMT
 from Sum_KQ_P2
)
,
Var_util as
(select cust_cid, case when (nvl(loc_amt,0)+nvl(cc_amt,0)) > 0 and ((nvl(loc_lmt,0)+nvl(cc_lmt,0)) <=0 or  (loc_lmt is null and cc_lmt is null)) then 1
                         when (nvl(loc_amt,0)+nvl(cc_amt,0)) = 0 and ((nvl(loc_lmt,0)+nvl(cc_lmt,0)) <=0 or (loc_lmt is null and cc_lmt is null)) then 0 
                         when (loc_amt) is null and (cc_amt) is null and (loc_lmt is not null or cc_lmt is not null) then 0
                         when (loc_amt) is null and (cc_amt) is null and (loc_lmt) is null and (cc_lmt) is null then null -- no such case
                         else (coalesce(loc_amt,0)+coalesce(cc_amt,0))/(coalesce(loc_lmt,0)+coalesce(cc_lmt,0)) 
 end as util
from Sum_KQ_P2
)

,
Full_MO_Direct_Move as
(select
 trim(mo.cust_cid) as cust_cid 
,mo.eff_dt
,avg_prpty_val
from """ + cf.CBSDBName + """.cbs_mort_cust_sum_fact mo
 ,cust_seg_5 cs
where mo.eff_dt = '""" + cf.bdate + """'
and mo.date_type='""" + date_type + """'
and trim(mo.cust_cid)= cs.cust_cid
)
,
Var_avg_prop_value as
(select  cust_cid
        ,avg_prpty_val as avg_prop_value
 from Full_MO_Direct_Move
)


insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg5 partition (eff_dt='""" + cf.bdate + """', date_type='""" + date_type + """') 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5,'HIGHST_ACTV_UTL',HIGHST_ACTV_UTL
from cust_seg_5 cs left join Var_HIGHST_ACTV_UTL var1
on cs.cust_cid=var1.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'HIGHST_ACTV_UTLmax12m',HIGHST_ACTV_UTLmax12m
from cust_seg_5 cs left join Var_HIGHST_ACTV_UTLmax12m var2
on cs.cust_cid=var2.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5,'INQRY_PAST_6M_C',INQRY_PAST_6M_C
from cust_seg_5 cs left join Var_INQRY_PAST_6M_C var3
on cs.cust_cid=var3.cust_cid
union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5,'MTH_SNC_LST_60D_DLQ_CN', MTH_SNC_LST_60D_DLQ_CN
from cust_seg_5 cs left join Var_MTH_SNC_LST_60D_DLQ_CN var4
on cs.cust_cid=var4.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'MTH_SNC_MST_RC_DLQ_C', MTH_SNC_MST_RC_DLQ_C
from cust_seg_5 cs left join Var_MTH_SNC_MST_RC_DLQ_C var5
on cs.cust_cid=var5.cust_cid
union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'SUM_OF_DEPAMTmin6m', SUM_OF_DEPAMTmin6m
from cust_seg_5 cs left join Var_SUM_OF_DEPAMTmin6m var6
on cs.cust_cid=var6.cust_cid
union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'SUM_OF_NSF_NUM', SUM_OF_NSF_NUM
from cust_seg_5 cs left join Var_SUM_OF_NSF_NUM var7
on cs.cust_cid=var7.cust_cid
union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'SUM_OF_PAYAMT', SUM_OF_PAYAMT
from cust_seg_5 cs left join Var_SUM_OF_PAYAMT var8
on cs.cust_cid=var8.cust_cid
union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'TOT_BAL_REG_ACCT', cast(TOT_BAL_REG_ACCT as decimal(17,2)) as TOT_BAL_REG_ACCT
from cust_seg_5 cs left join Var_TOT_BAL_REG_ACCT var9
on cs.cust_cid=var9.cust_cid 
union all 
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5,'TOT_UTLAMT', TOT_UTLAMT
from cust_seg_5 cs left join Var_TOT_UTLAMT var10
on cs.cust_cid=var10.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'avg_LTV', avg_LTV
from cust_seg_5 cs left join Var_avg_LTV var11
on cs.cust_cid=var11.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'avg_LTV_heloc', avg_LTV_heloc
from cust_seg_5 cs left join Var_avg_LTV_heloc var12
on cs.cust_cid=var12.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5,'avg_prop_value', avg_prop_value
from cust_seg_5 cs left join Var_avg_prop_value var13
on cs.cust_cid=var13.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'cc_TOT_NEW_BAL_AMT', cc_TOT_NEW_BAL_AMT
from cust_seg_5 cs left join Var_cc_TOT_NEW_BAL_AMT var14
on cs.cust_cid=var14.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'num_cc_dlqmax12m', num_cc_dlqmax12m
from cust_seg_5 cs left join  Var_num_cc_dlqmax12m var16
on cs.cust_cid=var16.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5,  'util', util
from cust_seg_5 cs left join Var_util var17
on cs.cust_cid=var17.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'worst_days_dlq_max6m', worst_days_dlq_max6m
from cust_seg_5 cs left join Var_worst_days_dlq_max6m var18
on cs.cust_cid=var18.cust_cid
union all
select cs.cust_cid 
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,5, 'worst_mor_dlq_daysmax3m', worst_mor_dlq_daysmax3m
from cust_seg_5 cs left join  Var_worst_mor_dlq_daysmax3m var19
on cs.cust_cid=var19.cust_cid
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


