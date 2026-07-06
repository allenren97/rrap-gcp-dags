#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_mort_acct_snapshot.py
#
#        USAGE: ./risk_mort_acct_snapshot.py bdate datetype
#
#  DESCRIPTION: Risk Mortgage Account Snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 08/28/2018  
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



# insert into table risk_mort_acct_snapshot table with monthly data

SQL1 = """
	set hive.execution.engine=tez;
	set hive.vectorized.execution.enabled = true;
	set hive.vectorized.execution.reduce.enabled = true;
	set hive.exec.parallel=true;
	insert overwrite table """ + cf.CBSDBName + """.risk_mort_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select a.mort_num as mort_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
, a.state_loan_auth_dt as loan_auth_dt
, a.cls_acct_cls_dt as pd_off_dt
, a.rnwl_dt as last_rnew_dt
, a.state_acct_mtur_dt as crnt_term_mat_dt
, a.rnwl_dt as rnewl_dt
, a.state_orig_disburs_dt as int_adj_dt
, substring(a.STATE_DISTR_1_FREQ_NM, 1,1) as float_cd
, case when a.ACCT_STAT_CD = 'CL' then 'Y' else 'N' end as pd_off_flag
, case when a.FRCLS_FORECL_DT is not null then 'Y' else NULL end as frclsr_flag
, CASE WHEN a.DELQ_DAY_CNT <= 0 or a.DELQ_DAY_CNT is null THEN 0
WHEN a.DELQ_DAY_CNT > 0 THEN cast((a.DELQ_DAY_CNT/30) as int) 
END as mth_in_arrs_cnt
, a.cond_acct_sub_type_cd as ins_class_cd
, case when a.REF_PRPTY_TYPE_DESC is null then NULL 
else substring(a.REF_PRPTY_TYPE_DESC, 1, 6) end as prpty_type_cd
, a.ref_prpty_type_desc as prpty_type_desc
, CAST(CAST( a.GLMAP_INVESTOR_CD  AS INT) as char(4)) as fund_cd
, a.os_bal_coa_amt as crnt_bal_amt
, a.cond_ownshp_branch_transit as serv_br_trnst_num
, a.int_tot_int_due_amt as intr_due_amt
, a.cond_orig_branch_transit as proc_br_trnst_num
, a.state_loan_auth_dt as mort_auth_dt
, a.cond_orig_loan_amt as auth_amt
, a.coll_coll_val_amt as lend_val
, a.escr_tot_escr_bal_amt as TAX_CRNT_BAL_AMT
, a.int_accr_int_amt as intr_accr_amt
, a.state_tot_disburs_amt as tot_advnc_amt
, case when a.COND_PROD_GRP_CD = 'COM' then 'NP'
when a.COND_PROD_GRP_CD = 'MTG' then 'P'  else 'P'  end as brwr_cd
, case when substring(a.STATE_DISTR_1_FREQ_NM, 1,1) IN ('W', 'B', 'S')
then a.STATE_DISTR_1_NEXT_DUE_DT else NULL end as week_frst_unpaid_dt
, concat(
case when a.coll_prpty_type_cd IN  (160, 162, 163, 164, 167, 168) then substr(cast(a.coll_prpty_type_cd as varchar(3)),-2) 
when a.coll_prpty_type_cd IN (261, 361, 461, 561, 661, 761) then If(a.coll_prpty_age_val = 0, '65',substr(cast(a.coll_prpty_type_cd as varchar(3)),-2))
when a.coll_prpty_type_cd = 165 then '66'
when a.coll_prpty_type_cd = 211 then '31'
when a.coll_prpty_type_cd = 900 then '09'
when a.coll_prpty_type_cd = 101 then if(a.coll_prpty_age_val = 0, '05', '01')
when a.coll_prpty_type_cd = 111 then if(a.coll_prpty_age_val = 0, '15', '11')
when a.coll_prpty_type_cd = 191 then if(a.coll_prpty_age_val = 0, '95', '91')
when a.coll_prpty_type_cd = 221 then if(a.coll_prpty_age_val = 0, '25', '21')
when a.coll_prpty_type_cd = 0 or a.coll_prpty_type_cd is null then '00' 
else '00'
end, lpad(COLL_UNIT_CNT,3,'0')) as scrty_tp_2
, a.COLL_BUILDNG_VAL_AMT as aprsd_last_bldgn_val_amt
,a.COLL_LAND_VAL_AMT as aprsd_land_val_amt
,NULL as aprsd_last_land_val_amt
,NULL as aprsd_bldng_val_amt
,a.COLL_ORIG_APPR_LAND_VAL_AMT as aprsd_orig_land_val_amt
,a.COLL_ORIG_APPR_BUILDNG_VAL_AMT aprsd_orig_bldng_val_amt
,a.state_last_loan_adv_dt as final_advnc_dt
,a.cond_bus_src_cd as bus_src_cd
,case when substring(a.STATE_DISTR_1_FREQ_NM, 1,1) = 'M'
then a.STATE_DISTR_1_NEXT_DUE_DT else NULL end as frst_unpd_dt
, a.coll_sls_prc_amt as sale_dt_amt
, a.state_cust_risk_cd as cri_cd
, a.state_acct_risk_f as ari_cd
, if(d.agreement_no is not null, 'Y', 'N') as step_flag
, d.agreement_no as STEP_PLN_AGRMNT_NUM
, c.suspense_bal_amt as tot_susp_bal_amt
, a.cond_prod_type_cd as acct_type_cd
, a.cond_prod_type_cd as prod_type_cd
, a.ref_prod_type_desc as prod_type_desc
, a.coll_prov_state_cd as prpty_prov_cd
, case when a.cond_pymt_term_cd like '%M' then regexp_replace(a.cond_pymt_term_cd, 'M', '')
when a.cond_pymt_term_cd like '%D' then NULL end as mort_term_mth 
, a.int_int_rt_max_pct as max_intr_rt 
, cst.cust_cid as prim_cust_cid
, case when a.prpay_ytd_amt is NULL then 0 else a.PRPAY_YTD_AMT end as ytd_prepay_amt
, a.state_remain_amort_mm_cnt as amort_mths
, a.gl_acct_num as gl_acct_num 
, a.gl_acctng_transit as gl_trnst_num
, a.cond_prod_grp_cd as prod_grp_cd
, a.coll_unit_cnt unit_cnt
, case when a.COND_PROD_GRP_CD = 'COM' OR a.COLL_UNIT_CNT >= 5 then 'Commercial' else 'Residential' end as comm_type
from """ + cf.RCRRDBName + """.mortgage_mth_snapshot a
left outer join """ + cf.CBSDBName + """.risk_step_xref d
ON a.mth_end_dt = d.eff_dt
and a.mort_num = cast(d.account_no as bigint)
and a.src_sys_cd = d.source_sys_cd
left outer join """ + cf.CBSDBName + """.risk_cust_acct_rltnp cst
ON a.mort_num = cst.acct_num
AND a.mth_end_dt = cst.eff_dt
AND a.src_sys_cd = cst.src_sys_cd
AND cst.primary_acct_holder_f = 'Y'
left outer join (select mort_num, businesseffectivedate, suspense_bal_amt
from """ + cf.TSZDBName + """.gz_tgz_suspense_bal yy
where businesseffectivedate IN (select max(xx.businesseffectivedate)
from """ + cf.TSZDBName + """.gz_tgz_suspense_bal xx
where month(xx.businesseffectivedate) = month('""" + cf.bdate + """')
and year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))
) c 
ON a.mort_num = c.mort_num 
where a.mth_end_dt = '""" + cf.bdate + """'
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
