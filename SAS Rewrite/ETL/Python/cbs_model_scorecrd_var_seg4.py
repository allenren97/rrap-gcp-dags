#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/CBS_MODEL_SCORECRD_VAR_SEG4.py
#
#        USAGE: ./CBS_MODEL_SCORECRD_VAR_SEG4.py bdate datetype
#
#  DESCRIPTION: Risk Mortgage Account Derived Variables table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Rahim Dobani
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 12/21/2018  (MM/DD/YYYY)
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



# insert into table CBS_MODEL_SCORECRD_VAR_SEG4 table with monthly data

SQL1 = """

with
seg_4_cust as 
(select trim(cust_cid) as cust_cid, eff_dt, date_type from
""" + cf.CBSDBName + """.cbs_cust_segmentation a where eff_dt = '""" + cf.bdate + """' and date_type = '""" + date_type + """' and seg_num = 4
),


cbr as (
select trim(cbr_dl.cust_cid) as cust_cid, 
cbr_dl.highst_actv_utltn, 
cbr_dl.inqry_past_6_mth_cnt, 
mth_since_most_recnt_dlqnt_cnt, 
cbr_dl.eff_dt, 
tot_avl_cr_not_utilized_amt,
row_number() over (partition by trim(cbr_dl.cust_cid) order by cbr_dl.eff_dt desc) as row_num
from """ + cf.CBSDBName + """.risk_cr_bureau_deli_mth_snapshot cbr_dl, seg_4_cust sg4
where trim(cbr_dl.cust_cid) = sg4.cust_cid
and cbr_dl.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """' 
),

KQ as
( 
select trim(kq.cust_cid) cust_cid, 
kq.eff_dt, 
kq.cr_type, 
kq.sum_prchs_intr_chrgd_amt, 
kq.num_of_heloc, 
kq.num_of_accts,
kq.sum_tot_new_bal_amt,
kq.sum_cr_lmt_amt,
kq.worst_dlqnt_days,
row_number() over (partition by kq.cust_cid, cr_type order by kq.eff_dt desc) as row_num,
dense_rank() over (partition by kq.cust_cid  order by kq.eff_dt desc) as rnk
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact kq, seg_4_cust sg4 
where trim(kq.cust_cid) = sg4.cust_cid and kq.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """' and kq.num_of_accts >0
and kq.cr_type in ('Cards','LOC')
),


MORT as
(
select a.cust_cid, a.worst_mort_dlqnt_days, row_number() over (partition by a.cust_cid order by a.eff_dt desc) as row_num from 
""" + cf.CBSDBName + """.cbs_mort_cust_sum_fact a, seg_4_cust sg4
where a.cust_cid = sg4.cust_cid and a.num_mort > 0
and a.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
),


SPL as (
select a.cust_cid, a.spl_worst_dlqnt_days, row_number() over (partition by a.cust_cid order by a.eff_dt desc) as row_num from 
""" + cf.CBSDBName + """.cbs_spl_cust_sum_fact a, seg_4_cust sg4
where a.cust_cid = sg4.cust_cid and a.num_of_spl_acct > 0
and a.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
),




cust_tot_sav as (
select a.cust_cid, a.cust_tot_sav_bal from 
(
select trim(csf.cust_cid) as cust_cid, sum_sav_bal_amt as cust_tot_sav_bal, csf.eff_dt, row_number() over (partition by csf.cust_cid order by csf.eff_dt desc) as row_num
from """ + cf.CBSDBName + """.cbs_sav_cust_sum_fact csf, seg_4_cust sg4
where trim(csf.cust_cid) = sg4.cust_cid
and (csf.num_sav_acct_prim > 0 or num_sav_acct_sec > 0)
and csf.eff_dt = '""" + cf.bdate + """' 
) a
),


ACTV_UTLavg3m as 
(
select cust_cid, avg(highst_actv_utltn) as HIGHST_ACTV_UTLavg3m from
cbr 
where row_num <= 3
group by cust_cid 
),


INQRY_6M as 
(
select cust_cid, inqry_past_6_mth_cnt as INQRY_PAST_6M_C from
cbr 
where eff_dt = '""" + cf.bdate + """'
),


MST_RC_DLQ_C as 
(
select cust_cid, mth_since_most_recnt_dlqnt_cnt as MTH_SNC_MST_RC_DLQ_C from
cbr 
where eff_dt = '""" + cf.bdate + """'
),


CR_NOT_UTL as (
select cust_cid, tot_avl_cr_not_utilized_amt as TOT_AVL_CR_NOT_UTLAMT from
cbr 
where eff_dt = '""" + cf.bdate + """'
),


NONREG_ACC as 
(
select trim(c.cust_cid) as cust_cid, avg(c.tot_bal_non_reg_amt) as TOT_BAL_NONREG_ACCTavg3m from 
(select ip.cust_cid, ip.eff_dt, ip.tot_bal_non_reg_amt, row_number() over (partition by ip.cust_cid order by ip.eff_dt desc) as row_num
from """ + cf.CBSDBName + """.cbs_ip_cust_sum_fact ip, seg_4_cust sg4 
where trim(ip.cust_cid) = sg4.cust_cid and tot_num_non_reg_acct >0 and ip.eff_dt between add_months('""" + cf.bdate + """',-11) and '""" + cf.bdate + """'
)c where c.row_num <= 3
group by c.cust_cid
),


CC_INT as (
select cust_cid as cust_cid, max(sum_prchs_intr_chrgd_amt) as cc_PRCH_INT_CHGAMTmax3m from 
KQ where cr_type ='Cards' and row_num<=3
group by cust_cid
),


--HELOC as -->>>Removed from Revised Requirements in March 2019
--(
--select cust_cid, sum(case when num_of_heloc >0 then 1 else 0 end) as heloc_ind
--from KQ where cr_type in ('Cards','LOC') and rnk = 1
--group by cust_cid
--),


NUM_CARDS as
(
select cust_cid, num_of_accts as num_cards
from KQ where cr_type ='Cards' and eff_dt = '""" + cf.bdate + """'
),


UTIL as
(
select x.cust_cid, case when (nvl(loc_amt,0)+nvl(cc_amt,0)) > 0 and ((nvl(loc_lmt,0)+nvl(cc_lmt,0)) <=0 or  (loc_lmt is null and cc_lmt is null)) then 1
                         when (nvl(loc_amt,0)+nvl(cc_amt,0)) = 0 and ((nvl(loc_lmt,0)+nvl(cc_lmt,0)) <=0 or (loc_lmt is null and cc_lmt is null)) then 0 
                         when (loc_amt) is null and (cc_amt) is null and (loc_lmt is not null or cc_lmt is not null) then 0
                         when (loc_amt) is null and (cc_amt) is null and (loc_lmt) is null and (cc_lmt) is null then null -- no such case
                         else (coalesce(loc_amt,0)+coalesce(cc_amt,0))/(coalesce(loc_lmt,0)+coalesce(cc_lmt,0)) 
 end as util
from
(select kq.cust_cid
,kq.eff_dt
,sum(case when cr_type='LOC' then sum_tot_new_bal_amt end) as loc_amt
,sum(case when cr_type='Cards' then sum_tot_new_bal_amt end) as cc_amt
,sum(case when cr_type='LOC' then sum_cr_lmt_amt end) as loc_lmt
,sum(case when cr_type='Cards' then sum_cr_lmt_amt end) as cc_lmt
,row_number() over (partition by kq.cust_cid order by kq.eff_dt desc) row_num
from """ + cf.CBSDBName + """.cbs_kq_cust_sum_fact kq,
seg_4_cust sg4
where num_of_accts > 0
and kq.cr_type in ('LOC','Cards')
and kq.eff_dt = '""" + cf.bdate + """'  --- processing month
and kq.date_type='""" + date_type + """'
and kq.cust_cid = sg4.cust_cid
group by kq.cust_cid, kq.eff_dt
)
x
),

mx_dlq_6m as (
select cust_cid, max(worst_dlq_days) as worst_days_dlq_max6m from 
(
select a.cust_cid, a.worst_dlq_days, a.eff_dt, row_number ()over(partition by a.cust_cid order by a.eff_dt desc) as row_num
from """ + cf.CBSDBName + """.cbs_customer_base a, seg_4_cust sg4
where a.cust_cid = sg4.cust_cid
and sg4.eff_dt = '""" + cf.bdate + """' 
and a.eff_dt between add_months('""" + cf.bdate + """', -11) and '""" + cf.bdate + """'
)x where row_num <=6 
group by cust_cid
)

insert overwrite table """ + cf.CBSDBName + """.CBS_MODEL_SCORECRD_VAR_SEG4 partition (eff_dt = '""" + cf.bdate + """' , date_type = '""" + date_type + """')

select 
sg4.cust_cid
,'""" + os.path.realpath(__file__) + """' as op_field
,current_timestamp as insrt_process_tmstmp
,4 as seg_num
,vars.sc_var
,vars.sc_var_val
from seg_4_cust sg4 
left outer join
(

select sg4.cust_cid, 'CUST_TOT_SAV_BAL' as sc_var, b.cust_tot_sav_bal as sc_var_val
from seg_4_cust sg4 left outer join cust_tot_sav b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'HIGHST_ACTV_UTLavg3m' as sc_var, b.HIGHST_ACTV_UTLavg3m as sc_var_val
from seg_4_cust sg4 left outer join ACTV_UTLavg3m b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'INQRY_PAST_6M_C' as sc_var, b.INQRY_PAST_6M_C as sc_var_val
from seg_4_cust sg4 left outer join INQRY_6M b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'MTH_SNC_MST_RC_DLQ_C' as sc_var, b.MTH_SNC_MST_RC_DLQ_C as sc_var_val
from seg_4_cust sg4 left outer join MST_RC_DLQ_C b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'TOT_AVL_CR_NOT_UTLAMT' as sc_var, b.TOT_AVL_CR_NOT_UTLAMT as sc_var_val
from seg_4_cust sg4 left outer join CR_NOT_UTL b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'TOT_BAL_NONREG_ACCTavg3m' as sc_var, b.TOT_BAL_NONREG_ACCTavg3m as sc_var_val
from seg_4_cust sg4 left outer join NONREG_ACC b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'cc_PRCH_INT_CHGAMTmax3m' as sc_var, b.cc_PRCH_INT_CHGAMTmax3m as sc_var_val
from seg_4_cust sg4 left outer join CC_INT b on sg4.cust_cid = b.cust_cid

--UNION ALL
---->>>Removed from Revised Requirements in March 2019
--
--select sg4.cust_cid, 'heloc_ind' as sc_var, if(b.heloc_ind >0,1,b.heloc_ind) as sc_var_val
--from seg_4_cust sg4 left outer join HELOC b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'num_cards' as sc_var, b.num_cards as sc_var_val
from seg_4_cust sg4 left outer join NUM_CARDS b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'util' as sc_var, b.util as sc_var_val
from seg_4_cust sg4 left outer join UTIL b on sg4.cust_cid = b.cust_cid

UNION ALL


select sg4.cust_cid, 'worst_days_dlq_max6m' as sc_var, b.worst_days_dlq_max6m as sc_var_val
from seg_4_cust sg4 left outer join mx_dlq_6m b on sg4.cust_cid = b.cust_cid
) vars on sg4.cust_cid = vars.cust_cid 
where sg4.eff_dt = '""" + cf.bdate + """'

"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
