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
#      CREATED: 12/24/2018 
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

cc_CSH_ADVNC_BALAMT1avg6m as 
(
SELECT tw.cust_cid, avg(tw.sum_1_cycl_ago_csh_advnc_bal_amt) as cc_CSH_ADVNC_BALAMT1avg6m FROM
(select 
	b.eff_dt, a.cust_cid, b.sum_1_cycl_ago_csh_advnc_bal_amt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' and upper(b.CR_TYPE)='CARDS' and b.num_of_accts> 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=6
GROUP BY CUST_CID
)
,
INQRY_PAST_6M_Cmax12m as 
(
SELECT tw.cust_cid, max(tw.inqry_past_6_mth_cnt) as INQRY_PAST_6M_Cmax12m FROM
(select 
	b.eff_dt, a.cust_cid, b.inqry_past_6_mth_cnt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=12
GROUP BY CUST_CID
)
,
SUM_OF_NSF_NUMmax12m as 
(
SELECT cust_cid, max(dm.num_of_nsf) as SUM_OF_NSF_NUMmax12m FROM
(select 
	 a.eff_dt, a.cust_cid
	,b.num_of_nsf
	,row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_SAV_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 

	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' and (b.num_sav_acct_prim > 0 or b.num_sav_acct_sec > 0)
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) DM
WHERE row_num <=12
GROUP BY CUST_CID
)
,
TOT_AVL_CR_NOT_UTLAMTavg6m as 
(
SELECT cust_cid, avg(dm.tot_avl_cr_not_utilized_amt) as TOT_AVL_CR_NOT_UTLAMTavg6m FROM
(select 
	 a.eff_dt, a.cust_cid
	,b.tot_avl_cr_not_utilized_amt
	,row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) DM
WHERE row_num <=6
GROUP BY CUST_CID
)
,
TOT_BAL_INVST_ACCT as 
(
select 
	 a.eff_dt, a.cust_cid
	,coalesce(b.tot_bal_reg_amt,0) + coalesce(b.tot_bal_non_reg_amt,0) as TOT_BAL_INVST_ACCT
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_IP_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt = a.eff_dt 
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' and (b.tot_num_reg_acct >0 or b.tot_num_non_reg_acct >0)
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
TOT_BAL_TP_BCARDAMTmax3m as 
(
SELECT cust_cid, max(dm.tot_bal_tp_bankcard_amt) as TOT_BAL_TP_BCARDAMTmax3m FROM
(select 
	 a.eff_dt, a.cust_cid
	,b.tot_bal_tp_bankcard_amt
	,row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0)

	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) DM
WHERE row_num <=3
GROUP BY CUST_CID
)
,
cc_CSH_ADV_INT_CHGAMTmax12m as 
(
SELECT tw.cust_cid, max(tw.sum_csh_advnc_intr_chrgd_amt) as cc_CSH_ADV_INT_CHGAMTmax12m FROM
(select 
	b.eff_dt, a.cust_cid, b.sum_csh_advnc_intr_chrgd_amt, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' and upper(b.CR_TYPE)='CARDS' and b.num_of_accts> 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
) TW
WHERE row_num <=12
GROUP BY CUST_CID
)
,
rev_indsum3m as 
(
SELECT tw2.cust_cid, sum(tw2.max_rev_ind) as rev_indsum3m FROM
(select b.eff_dt, a.cust_cid, b.max_rev_ind, row_number() over (partition by a.cust_cid order by b.eff_dt desc) row_num 
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt >= add_months('""" + args.bdate + """',-11) and b.eff_dt <= add_months('""" + args.bdate + """',0) 
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' and (upper(b.CR_TYPE)='CARDS' ) and b.num_of_accts> 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
	 
) TW2
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
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' and upper(b.CR_TYPE)='CARDS' and b.num_of_accts> 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
direct_moves_KQ_LOC_CUST as 
(
select 
	 a.eff_dt, a.cust_cid
	,b.sum_tot_new_bal_amt as sum_tot_new_bal_amt_loc
	,b.sum_cr_lmt_amt as sum_cr_lmt_amt_loc
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.CBS_KQ_CUST_SUM_FACT b
			on trim(a.cust_cid)=trim(b.cust_cid)  and b.eff_dt = a.eff_dt
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """'  and upper(b.CR_TYPE)='LOC' and b.num_of_accts > 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
util_fields_1 as 
(
select m.cust_cid, m.eff_dt,
case 
		when (m.sum_tot_new_bal_amt_cc is null and m.sum_tot_new_bal_amt_loc is null) and (m.sum_cr_lmt_amt_cc is null and  m.sum_cr_lmt_amt_loc is null) then null
		when (m.sum_tot_new_bal_amt_cc is null and m.sum_tot_new_bal_amt_loc is null) then 0
		when (m.sum_tot_new_bal_amt_cc + m.sum_tot_new_bal_amt_loc) > 0 and ((m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) <=0 or (m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) is null) then 1
		when (m.sum_tot_new_bal_amt_cc + m.sum_tot_new_bal_amt_loc) = 0 and ((m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) <=0 or (m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) is null) then 0
		else (coalesce(m.sum_tot_new_bal_amt_cc,0) + coalesce(m.sum_tot_new_bal_amt_loc,0)) / (coalesce(m.sum_cr_lmt_amt_cc,0) + coalesce(m.sum_cr_lmt_amt_loc,0)) 
	end as util_1
from
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
				
		 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """'  and upper(b.CR_TYPE) in ('LOC','CARDS') and b.num_of_accts > 0 
		 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
		) DM
	WHERE c_row_num=1
	GROUP BY CUST_CID, EFF_DT
	) m
)
,
util_fields_2 as 
(
select m.cust_cid, m.eff_dt,
case 
		when (m.sum_tot_new_bal_amt_cc is null and m.sum_tot_new_bal_amt_loc is null) and (m.sum_cr_lmt_amt_cc is null and  m.sum_cr_lmt_amt_loc is null) then null
		when (m.sum_tot_new_bal_amt_cc is null and m.sum_tot_new_bal_amt_loc is null) then 0
		when (m.sum_tot_new_bal_amt_cc + m.sum_tot_new_bal_amt_loc) > 0 and ((m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) <=0 or (m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) is null) then 1
		when (m.sum_tot_new_bal_amt_cc + m.sum_tot_new_bal_amt_loc) = 0 and ((m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) <=0 or (m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) is null) then 0
		else (coalesce(m.sum_tot_new_bal_amt_cc,0) + coalesce(m.sum_tot_new_bal_amt_loc,0)) / (coalesce(m.sum_cr_lmt_amt_cc,0) + coalesce(m.sum_cr_lmt_amt_loc,0)) 
	end as util_2
from
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
				
		 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """'  and upper(b.CR_TYPE) in ('LOC','CARDS') and b.num_of_accts > 0 
		 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
		) DM
	WHERE c_row_num=2
	GROUP BY CUST_CID, EFF_DT
	) m
)
,
util_fields_3 as 
(
select m.cust_cid, m.eff_dt,
case 
		when (m.sum_tot_new_bal_amt_cc is null and m.sum_tot_new_bal_amt_loc is null) and (m.sum_cr_lmt_amt_cc is null and  m.sum_cr_lmt_amt_loc is null) then null
		when (m.sum_tot_new_bal_amt_cc is null and m.sum_tot_new_bal_amt_loc is null) then 0
		when (m.sum_tot_new_bal_amt_cc + m.sum_tot_new_bal_amt_loc) > 0 and ((m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) <=0 or (m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) is null) then 1
		when (m.sum_tot_new_bal_amt_cc + m.sum_tot_new_bal_amt_loc) = 0 and ((m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) <=0 or (m.sum_cr_lmt_amt_cc + m.sum_cr_lmt_amt_loc) is null) then 0
		else (coalesce(m.sum_tot_new_bal_amt_cc,0) + coalesce(m.sum_tot_new_bal_amt_loc,0)) / (coalesce(m.sum_cr_lmt_amt_cc,0) + coalesce(m.sum_cr_lmt_amt_loc,0)) 
	end as util_3
from
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
				
		 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """'  and upper(b.CR_TYPE) in ('LOC','CARDS') and b.num_of_accts > 0 
		 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
		) DM
	WHERE c_row_num=3
	GROUP BY CUST_CID, EFF_DT
	) m
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
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """'  and upper(b.CR_TYPE) in ('LOC','CARDS') and b.num_of_accts > 0 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
	) DM
WHERE c_row_num=1
GROUP BY CUST_CID, EFF_DT
)
,
direct_moves_BUREAU as 
(
select 
	 a.eff_dt, a.cust_cid, a.time_on_books
	,b.inqry_cnt
	,b.oldst_opn_trade_age_line_mth_cnt as OLDST_OPN_TRD_LINEM_C
	,b.tot_utltn_bnk_revlvng_crd_amt as TOT_UTL_BNK_REVAMT
	
from 
			   """ + cf.CBSDBName + """.cbs_cust_segmentation a 

	 left join """ + cf.CBSDBName + """.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT b
			on trim(a.cust_cid)=trim(b.cust_cid) and b.eff_dt = a.eff_dt
			
	 where a.seg_num=8 and a.eff_dt='""" + args.bdate + """' 
	 and a.date_type= '""" + date_type + """' and b.date_type = '""" + date_type + """'
)
,
PRE_FINAL as
(
SELECT 
	 trim(a.cust_cid) as cust_cid
	,a.eff_dt
	,a.date_type
	,case when (coalesce(k.card_ind) < 0.5 OR k.card_ind is null)  then 1
		  when (b.cc_CSH_ADVNC_BALAMT1avg6m >=115 and b.cc_CSH_ADVNC_BALAMT1avg6m is not null) then 3
		  else 2
	end as CASH_ADV_CAT

	,c.inqry_cnt as INQRY_C
	,d.INQRY_PAST_6M_Cmax12m
	,c.OLDST_OPN_TRD_LINEM_C
	,e.SUM_OF_NSF_NUMmax12m
	,f.TOT_AVL_CR_NOT_UTLAMTavg6m
	,g.TOT_BAL_INVST_ACCT
	,h.TOT_BAL_TP_BCARDAMTmax3m
	,c.TOT_UTL_BNK_REVAMT
	,cc_CSH_ADV_INT_CHGAMTmax12m
	,j.rev_indsum3m
	,c.time_on_books
	
	
	,CASE
         WHEN COALESCE(m.util_1, n.util_2, o.util_3) IS NOT NULL THEN 
			 (COALESCE(m.util_1, 0) + COALESCE(n.util_2, 0) + COALESCE(o.util_3, 0)) / 
				(CASE WHEN m.util_1 IS NULL THEN 0 ELSE 1 END + 
				 CASE WHEN n.util_2 IS NULL THEN 0 ELSE 1 END + 
				 CASE WHEN o.util_3 IS NULL THEN 0 ELSE 1 END)
		 ELSE NULL
     END as utilavg3m
	,case 
		when (p.sum_tot_new_bal_amt_cc is null and p.sum_tot_new_bal_amt_loc is null) and (p.sum_cr_lmt_amt_cc is null and  p.sum_cr_lmt_amt_loc is null) then null
		when (p.sum_tot_new_bal_amt_cc is null and p.sum_tot_new_bal_amt_loc is null) then 0
		when (p.sum_tot_new_bal_amt_cc + p.sum_tot_new_bal_amt_loc) > 0 and ((p.sum_cr_lmt_amt_cc + p.sum_cr_lmt_amt_loc) <=0 or (p.sum_cr_lmt_amt_cc + p.sum_cr_lmt_amt_loc) is null) then 1
		when (p.sum_tot_new_bal_amt_cc + p.sum_tot_new_bal_amt_loc) = 0 and ((p.sum_cr_lmt_amt_cc + p.sum_cr_lmt_amt_loc) <=0 or (p.sum_cr_lmt_amt_cc + p.sum_cr_lmt_amt_loc) is null) then 0
	    else (coalesce(p.sum_tot_new_bal_amt_cc,0) + coalesce(p.sum_tot_new_bal_amt_loc,0)) / (coalesce(p.sum_cr_lmt_amt_cc,0) + coalesce(p.sum_cr_lmt_amt_loc,0)) 
	end as util

FROM 
		""" + cf.CBSDBName + """.cbs_cust_segmentation a
	LEFT JOIN cc_CSH_ADVNC_BALAMT1avg6m   b		on trim(a.cust_cid)=trim(b.cust_cid)
	LEFT JOIN direct_moves_BUREAU         c 	on trim(a.cust_cid)=trim(c.cust_cid)
	LEFT JOIN INQRY_PAST_6M_Cmax12m       d		on trim(a.cust_cid)=trim(d.cust_cid)
	LEFT JOIN SUM_OF_NSF_NUMmax12m        e		on trim(a.cust_cid)=trim(e.cust_cid)
	LEFT JOIN TOT_AVL_CR_NOT_UTLAMTavg6m  f 	on trim(a.cust_cid)=trim(f.cust_cid)
	LEFT JOIN TOT_BAL_INVST_ACCT          g 	on trim(a.cust_cid)=trim(g.cust_cid)
	LEFT JOIN TOT_BAL_TP_BCARDAMTmax3m    h		on trim(a.cust_cid)=trim(h.cust_cid)
	LEFT JOIN cc_CSH_ADV_INT_CHGAMTmax12m i 	on trim(a.cust_cid)=trim(i.cust_cid)
	LEFT JOIN rev_indsum3m                j		on trim(a.cust_cid)=trim(j.cust_cid)
	LEFT JOIN direct_moves_KQ_CC_CUST     k		on trim(a.cust_cid)=trim(k.cust_cid)
	LEFT JOIN direct_moves_KQ_LOC_CUST    l 	on trim(a.cust_cid)=trim(l.cust_cid)
 	LEFT JOIN util_fields_1               m 	on trim(a.cust_cid)=trim(m.cust_cid)
 	LEFT JOIN util_fields_2               n 	on trim(a.cust_cid)=trim(n.cust_cid)
 	LEFT JOIN util_fields_3               o 	on trim(a.cust_cid)=trim(o.cust_cid)
 	LEFT JOIN util_fields                 p 	on trim(a.cust_cid)=trim(p.cust_cid)
	
	
WHERE a.eff_dt='""" + args.bdate + """' and a.seg_num = 8 and a.date_type= '""" + date_type + """' 
) 

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg8 partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """')

			SELECT  cust_cid 
					,'""" + os.path.realpath(__file__) + """' as op_field
					,current_timestamp as insrt_process_tmstmp
					,8 as seg_num
					,sc_var
					,sc_var_val
					
			FROM 
				(SELECT CUST_CID, EFF_DT, DATE_TYPE
				,MAP(
					 'CASH_ADV_CAT',  CASH_ADV_CAT
					,'INQRY_C',  INQRY_C
					,'INQRY_PAST_6M_Cmax12m',  INQRY_PAST_6M_Cmax12m
					,'OLDST_OPN_TRD_LINEM_C',  OLDST_OPN_TRD_LINEM_C
					,'SUM_OF_NSF_NUMmax12m',  SUM_OF_NSF_NUMmax12m
					,'TOT_AVL_CR_NOT_UTLAMTavg6m',  TOT_AVL_CR_NOT_UTLAMTavg6m
					,'TOT_BAL_INVST_ACCT',  TOT_BAL_INVST_ACCT
					,'TOT_BAL_TP_BCARDAMTmax3m',  TOT_BAL_TP_BCARDAMTmax3m
					,'TOT_UTL_BNK_REVAMT',  TOT_UTL_BNK_REVAMT
					,'cc_CSH_ADV_INT_CHGAMTmax12m',  cc_CSH_ADV_INT_CHGAMTmax12m
					,'rev_indsum3m',  rev_indsum3m
					,'time_on_books',  time_on_books
					,'util',  util
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
