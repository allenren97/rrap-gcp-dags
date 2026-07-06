#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_var_seg6.py
#
#        USAGE: ./cbs_model_scorecrd_var_seg6.py business_date date_type
#
#  DESCRIPTION: Scorecard Model execution -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Hadi
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 01/10/2019 
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



# insert into table cbs_model_scorecrd_var_seg6 table with monthly data
#-- date_type = '""" + date_type + """')
SQL1 = """


with 

TOT_UTL_BNK_REVAMTmax12m as 
(
SELECT tw.cust_cid, max(tw.tot_utltn_bnk_revlvng_crd_amt) as TOT_UTL_BNK_REVAMTmax12m FROM
(select 
	b.eff_dt, a.cust_cid, b.tot_utltn_bnk_revlvng_crd_amt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=12
GROUP BY CUST_CID
)
,
ACCR_INT_SPLavg6m as 
(
SELECT tw.cust_cid, avg(tw.sum_accr_intr) as ACCR_INT_SPLavg6m FROM
(select 
	b.eff_dt, a.cust_cid, b.sum_accr_intr, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_SPL_CUST_SUM_FACT b 
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and b.num_of_spl_acct > 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=6 
GROUP BY CUST_CID
)
,
cc_CSH_ADVNC_BALAMT1avg12m as 
(
SELECT tw.cust_cid, avg(tw.sum_1_cycl_ago_csh_advnc_bal_amt) as cc_CSH_ADVNC_BALAMT1avg12m FROM
(select 
	b.eff_dt, a.cust_cid, b.sum_1_cycl_ago_csh_advnc_bal_amt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and upper(b.CR_TYPE)='CARDS' and b.num_of_accts> 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=12
GROUP BY CUST_CID
)
,
HIGHST_ACTV_UTLmax6m as 
(
SELECT tw.cust_cid, max(tw.highst_actv_utltn) as HIGHST_ACTV_UTLmax6m FROM
(select 
	b.eff_dt, a.cust_cid, b.highst_actv_utltn, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=6 
GROUP BY CUST_CID
)
,
PRI_TOTAL_SAV_BALmin6m as 
(
SELECT tw.cust_cid, min(tw.sum_sav_bal_prim_amt) as PRI_TOTAL_SAV_BALmin6m FROM
(select 
	b.eff_dt, a.cust_cid, b.sum_sav_bal_prim_amt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_SAV_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and (b.num_sav_acct_prim > 0)
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=6 
GROUP BY CUST_CID
)
,
TOT_PDAMTavg3m as 
(
SELECT tw.cust_cid, avg(tw.tot_pd_amt) as TOT_PDAMTavg3m FROM
(select 
	b.eff_dt, a.cust_cid, b.tot_pd_amt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """'
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=3
GROUP BY CUST_CID
)
,
worst_days_dlq_max3m as 
(
SELECT tw.cust_cid, max(tw.worst_dlq_days) as worst_days_dlq_max3m FROM
(select 
	b.eff_dt, a.cust_cid, b.worst_dlq_days, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.cbs_customer_base b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'

) TW
WHERE row_num <=3
GROUP BY CUST_CID
)
,
direct_moves_KQ_CC_CUST as 
(
select 
	 a.eff_dt, a.cust_cid
	,b.sum_tot_new_bal_amt as sum_tot_new_bal_amt_cc
	,b.sum_cr_lmt_amt as sum_cr_lmt_amt_cc
	,case when 
		(b.sum_tot_new_bal_amt is not null) AND (b.sum_cr_lmt_amt is not null) then 1
		else 0 
	end as CARD_IND
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt = a.eff_dt
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and upper(b.CR_TYPE)='CARDS' and b.num_of_accts> 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
util_fields as 
(
SELECT DM.CUST_CID, DM.EFF_DT
		,sum(case when upper(DM.cr_type) = 'LOC'   then DM.sum_tot_new_bal_amt  end) as sum_tot_new_bal_amt_loc
		,sum(case when upper(DM.cr_type) = 'LOC'   then DM.sum_cr_lmt_amt 		end) as sum_cr_lmt_amt_loc
		,sum(case when upper(DM.cr_type) = 'CARDS' then DM.sum_tot_new_bal_amt  end) as sum_tot_new_bal_amt_cc
		,sum(case when upper(DM.cr_type) = 'CARDS' then DM.sum_cr_lmt_amt 		end) as sum_cr_lmt_amt_cc
FROM
	(
	select 
		  a.cust_cid, b.cr_type, b.eff_dt, b.sum_tot_new_bal_amt, b.sum_cr_lmt_amt 
		 ,dense_rank() over (partition by a.cust_cid order by b.eff_dt desc) as c_row_num
	from 
		 """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """'  and upper(b.CR_TYPE) in ('LOC','CARDS') and b.num_of_accts > 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
	) DM
WHERE c_row_num=1
GROUP BY CUST_CID, EFF_DT
)
,
direct_moves_SAV_CUST as 
(
select 
	 a.eff_dt, a.cust_cid
	,b.num_of_nsf
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_SAV_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt = a.eff_dt 
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and (b.num_sav_acct_prim > 0 or b.num_sav_acct_sec > 0)
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
direct_moves_SPL_CUST as 
(
select 
	 a.eff_dt, a.cust_cid
	,b.subvented_ind
	,b.sum_crnt_bal
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_SPL_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt = a.eff_dt 
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and b.num_of_spl_acct > 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
direct_moves_BUREAU as 
(
select 
	 a.eff_dt, a.cust_cid
	,b.inqry_cnt
	,b.inqry_past_6_mth_cnt
	,b.mth_since_most_recnt_dlqnt_cnt
	,b.occ_60_day_pd_within_past_12_mth_cnt
	,b.tot_avl_cr_not_utilized_amt
	,b.trade_90_dpd_last_24_mth_cnt
	,b.trade_never_dlqnt_pc
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt = a.eff_dt
			
	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
trade_never_dlqnt_pc_1 as 
(
SELECT tw.cust_cid, tw.trade_never_dlqnt_pc as trade_never_dlqnt_pc_1 FROM
(select 
	 b.eff_dt
	,a.cust_cid
	,b.trade_never_dlqnt_pc
	,row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num = 2
)
,
cur_bal_spl_3 as 
(
SELECT tw.cust_cid, tw.sum_crnt_bal as sum_crnt_bal_3 FROM
(select 
	 b.eff_dt
	,a.cust_cid
	,b.sum_crnt_bal
	,row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_SPL_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 

	 where a.seg_num=6 and a.eff_dt='""" + args.bdate + """' and b.num_of_spl_acct > 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num = 3
)
,
PRE_FINAL as
(
SELECT 
	 trim(a.cust_cid) as cust_cid
	,a.eff_dt
	,a.date_type
	,b.ACCR_INT_SPLavg6m
	,case when (coalesce(m.card_ind) >= 0.5 and coalesce(m.card_ind) is not null) AND (c.cc_CSH_ADVNC_BALAMT1avg12m >=0.083333 and c.cc_CSH_ADVNC_BALAMT1avg12m is not null) then 3
		  when (coalesce(m.card_ind) >= 0.5 and coalesce(m.card_ind) is not null) then 2
		  else 1
	end as CASH_ADV_CAT
	,d.HIGHST_ACTV_UTLmax6m
	,e.inqry_cnt as INQRY_C
	,e.inqry_past_6_mth_cnt as INQRY_PAST_6M_C
	,e.mth_since_most_recnt_dlqnt_cnt as MTH_SNC_MST_RC_DLQ_C
	,e.occ_60_day_pd_within_past_12_mth_cnt as OCC_60D_PD_IN_12_M
	,f.PRI_TOTAL_SAV_BALmin6m
	,g.num_of_nsf as SUM_OF_NSF_NUM
	,e.tot_avl_cr_not_utilized_amt as TOT_AVL_CR_NOT_UTLAMT
	,h.TOT_PDAMTavg3m 
	,i.TOT_UTL_BNK_REVAMTmax12m
	,e.trade_90_dpd_last_24_mth_cnt as TRD_90D_LST_24M_C
	,case 
    when (e.trade_never_dlqnt_pc is not null and e.trade_never_dlqnt_pc <> 0) AND (j.trade_never_dlqnt_pc_1 is null or j.trade_never_dlqnt_pc_1 = 0) then 1
    when (e.trade_never_dlqnt_pc is null or e.trade_never_dlqnt_pc = 0) AND (j.trade_never_dlqnt_pc_1 is not null and j.trade_never_dlqnt_pc_1 <> 0) then -1
    when e.trade_never_dlqnt_pc = 0 and j.trade_never_dlqnt_pc_1 = 0 then 0
    when e.trade_never_dlqnt_pc is null and j.trade_never_dlqnt_pc_1 is null then NULL
    else ((e.trade_never_dlqnt_pc/j.trade_never_dlqnt_pc_1) - 1) 
   end as TRD_NEVER_DLQNT_PCchg1m
  ,case
    when (k.sum_crnt_bal is not null and k.sum_crnt_bal <> 0) AND (l.sum_crnt_bal_3 is null or l.sum_crnt_bal_3 = 0) then 1
    when (k.sum_crnt_bal is null or k.sum_crnt_bal = 0) AND (l.sum_crnt_bal_3 is not null and l.sum_crnt_bal_3 <> 0) then -1
    when k.sum_crnt_bal = 0 and l.sum_crnt_bal_3 = 0 then 0
    when k.sum_crnt_bal is null and l.sum_crnt_bal_3 is null then null
    else ((k.sum_crnt_bal/l.sum_crnt_bal_3) -1) 
   end as cur_bal_splchg3m
	,cast(k.subvented_ind as int) as subvented_ind
	,case 
		when (n.sum_tot_new_bal_amt_cc is null and n.sum_tot_new_bal_amt_loc is null) and (n.sum_cr_lmt_amt_cc is null and  n.sum_cr_lmt_amt_loc is null) then null
		when (n.sum_tot_new_bal_amt_cc is null and n.sum_tot_new_bal_amt_loc is null) then 0
		when (n.sum_tot_new_bal_amt_cc + n.sum_tot_new_bal_amt_loc) > 0 and ((n.sum_cr_lmt_amt_cc + n.sum_cr_lmt_amt_loc) <=0 or (n.sum_cr_lmt_amt_cc + n.sum_cr_lmt_amt_loc) is null) then 1
		when (n.sum_tot_new_bal_amt_cc + n.sum_tot_new_bal_amt_loc) = 0 and ((n.sum_cr_lmt_amt_cc + n.sum_cr_lmt_amt_loc) <=0 or (n.sum_cr_lmt_amt_cc + n.sum_cr_lmt_amt_loc) is null) then 0
	    else (coalesce(n.sum_tot_new_bal_amt_cc,0) + coalesce(n.sum_tot_new_bal_amt_loc,0)) / (coalesce(n.sum_cr_lmt_amt_cc,0) + coalesce(n.sum_cr_lmt_amt_loc,0)) 
	end as UTIL
	,r.worst_days_dlq_max3m
FROM 
		""" + cf.CBSDBName + """.cbs_cust_segmentation a
	LEFT JOIN ACCR_INT_SPLavg6m b 				on trim(a.cust_cid)=trim(b.cust_cid)
	LEFT JOIN cc_CSH_ADVNC_BALAMT1avg12m c		on trim(a.cust_cid)=trim(c.cust_cid)
	LEFT JOIN HIGHST_ACTV_UTLmax6m d			on trim(a.cust_cid)=trim(d.cust_cid)
	LEFT JOIN direct_moves_BUREAU e 			on trim(a.cust_cid)=trim(e.cust_cid)
	LEFT JOIN PRI_TOTAL_SAV_BALmin6m f 			on trim(a.cust_cid)=trim(f.cust_cid)
	LEFT JOIN direct_moves_SAV_CUST g			on trim(a.cust_cid)=trim(g.cust_cid)
	LEFT JOIN TOT_PDAMTavg3m h 					on trim(a.cust_cid)=trim(h.cust_cid)
	LEFT JOIN TOT_UTL_BNK_REVAMTmax12m i 		on trim(a.cust_cid)=trim(i.cust_cid)
	LEFT JOIN trade_never_dlqnt_pc_1 j 			on trim(a.cust_cid)=trim(j.cust_cid)
	LEFT JOIN direct_moves_SPL_CUST k 			on trim(a.cust_cid)=trim(k.cust_cid)
	LEFT JOIN cur_bal_spl_3 l 					on trim(a.cust_cid)=trim(l.cust_cid)
	LEFT JOIN direct_moves_KQ_CC_CUST m 		on trim(a.cust_cid)=trim(m.cust_cid)
	LEFT JOIN util_fields n 					on trim(a.cust_cid)=trim(n.cust_cid)
	LEFT JOIN worst_days_dlq_max3m r			on trim(a.cust_cid)=trim(r.cust_cid)
	
WHERE a.eff_dt='""" + args.bdate + """' and a.seg_num = 6 and a.date_type= '""" + date_type + """' 
)
  
insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg6 partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """')

			SELECT  cust_cid 
					,'""" + os.path.realpath(__file__) + """' as op_field
					,current_timestamp as insrt_process_tmstmp
					,6 as seg_num
					,sc_var
					,sc_var_val
					
			FROM 
				(SELECT CUST_CID, EFF_DT, DATE_TYPE
				,MAP(
					'ACCR_INT_SPLavg6m' , ACCR_INT_SPLavg6m,
					'HIGHST_ACTV_UTLmax6m' , HIGHST_ACTV_UTLmax6m,
					'INQRY_C' , INQRY_C,
					'INQRY_PAST_6M_C' , INQRY_PAST_6M_C,
					'MTH_SNC_MST_RC_DLQ_C' , MTH_SNC_MST_RC_DLQ_C,
					'OCC_60D_PD_IN_12_M' , OCC_60D_PD_IN_12_M,
					'PRI_TOTAL_SAV_BALmin6m' , PRI_TOTAL_SAV_BALmin6m,
					'SUM_OF_NSF_NUM' , SUM_OF_NSF_NUM,
					'TOT_AVL_CR_NOT_UTLAMT' , TOT_AVL_CR_NOT_UTLAMT,
					'TOT_PDAMTavg3m' , TOT_PDAMTavg3m,
					'TOT_UTL_BNK_REVAMTmax12m' , TOT_UTL_BNK_REVAMTmax12m,
					'TRD_90D_LST_24M_C' , TRD_90D_LST_24M_C,
					'cur_bal_splchg3m' , cur_bal_splchg3m,
					'subvented_ind' , subvented_ind,
					'util' , util,
					'worst_days_dlq_max3m', worst_days_dlq_max3m
					) 
				AS TMP 
					FROM PRE_FINAL ) X
			LATERAL VIEW EXPLODE(TMP) EXPLODE_TABLE AS SC_VAR, SC_VAR_VAL
"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

        for sql in sqls:
                print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
                cf.hive_exec(sql)
                print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")



if __name__ == '__main__':
        main()
