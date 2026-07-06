#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_cis_data_pop.py
#
#        USAGE: ./cbs_cis_data_pop.py bdate datetype
#
#  DESCRIPTION: Risk KQ account snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 07/20/2018 16:18:33 
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



# insert into table risk_spl_acct_snapshot table with monthly data

SQL1 = """
	set hive.execution.engine=tez;
	set hive.vectorized.execution.enabled = true;
	set hive.vectorized.execution.reduce.enabled = true;
	set hive.exec.parallel=true;
	insert overwrite table """ + cf.CBSDBName + """.risk_spl_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
	select
cast(a.acct_num as VARCHAR(30)) as ACCT_NUM,
current_timestamp as insrt_process_tmstmp,
'""" + os.path.realpath(__file__) + """' as op_field,
cast(d.agreement_no as bigint) as STEP_PLN_AGRMNT_NUM,
cast(a.branch_loctn_transit as INT) as TRNST_NUM,
cast(a.loan_num as VARCHAR(30)) as LOAN_NUM,
cast(a.rating_cd as VARCHAR(10)) as RT_CD,
cast(a.recd_stat_cd as VARCHAR(10)) as RECD_STAT_CD,
a.recd_stat_dt as RECD_STAT_DT,
cast(a.cust_residence_cd as VARCHAR(10)) as CUST_RES_CD,
cast(a.type_src_cd as VARCHAR(10)) as TYPE_SRC_CD,
cast(a.loan_purpose_cd as VARCHAR(10)) as LOAN_PRPS_CD,
cast(a.scrty_cd as VARCHAR(10)) as SCRTY_CD,
a.promissors_cnt as PROMISSORS_CNT,
a.guar_cnt as GUARNTY_CNT,
cast(a.commercial_loan_cd as VARCHAR(10)) as COMM_LOAN_CD,
a.note_dt as NOTE_DT,
a.first_rgl_pymt_dt as FRST_RGL_PYMNT_DT,
a.last_rgl_pymt_dt as LAST_RGL_PYMT_DT,
cast(a.orig_loan_amt as DECIMAL(18, 2)) as ORIG_LOAN_AMT,
cast(a.add_on_bal_amt as DECIMAL(18, 2)) as ADD_ON_BAL_AMT,
cast(a.add_on_intr_amt as DECIMAL(18, 3)) as ADD_ON_INTR_AMT,
a.day_overdue as DAYS_OVERDUE,
cast(a.accr_intr_amt as DECIMAL(18, 3)) as ACCR_INTR_AMT,
a.early_maturity_dt as EARLY_MAR_DT,
a.last_pymt_dt as LAST_PYMT_DT,
cast(a.prncpl_bal_amt as DECIMAL(18, 2)) as PRINCIPAL_BAL_AMT,
if (a.scrty_vehcl_val>0,a.scrty_vehcl_val,a.marketable_scrty_val) as MOTOR_VEHCL_VAL,
a.scrty_household_credit_score as SCRTY_HOUSHLD_CR_SCORE,
cast(a.scrty_oth_val as DECIMAL(18, 2)) as SCRTY_OTH_VAL,
cast(a.pls_credit_score as INT) as PLS_CR_SCORE_OVRD_CD,
cast(a.orig_cab_transit as VARCHAR(5)) as BR_LOCTN_TRNST,
cast(a.earned_mth_intr_amt as DECIMAL(18, 3)) as EARNED_MTH_INTR_AMT,
a.orig_note_dt as ORIG_NOT_DT,
a.chrg_off_dt as CHRG_OFF_DT,
cast(a.chrg_off_amt as DECIMAL(18, 2)) as CHRG_OFF_AMT,
cast(a.securitization_cd as VARCHAR(10)) as SECURITIZATION_CD,
a.loan_term as LOAN_TERM,
cast(a.early_maturity_term as INT) as EARLY_MAT_TERM,
cast(a.early_maturity_stat_cd as VARCHAR(10)) as EARLY_MAT_STAT_CD,
cast(a.rgl_pymt_amt as DECIMAL(18, 2)) as RGL_PYMT_AMT,
cast(a.pre_auth_debit_pymt_freq_cd as VARCHAR(10)) as PRE_AUTH_DR_PYMNT_FREQ_CD,
cast(a.intr_rate as DOUBLE) as INTR_RT,
b.cust_cid as PRIM_CUST_CID,
a.cif_company_id as CIF_COMPANY_ID,
cast(a.cif_cust_id as VARCHAR(10)) as CIF_CUST_ID,
cast(a.gl_acctng_transit as VARCHAR(5)) as GL_TRNST_NUM,
cast(a.currency_cd as VARCHAR(3)) as CRNCY_CD,
a.cif_cust_id_tie_breaker as CIF_CUST_ID_TIE_BRKR,
cast(a.booked_amt as DECIMAL(18, 2)) as BOOKED_AMT,
cast(a.gl_acct_num as VARCHAR(10)) as GL_ACCT_NUM,
if(c.orig_cab_transit is not null, 'Y', 'N') as SUBVENTED_IND
FROM """ + cf.RCRRDBName + """.psnl_loan_mth_snapshot a 
left outer join """ + cf.CBSDBName + """.risk_cust_acct_rltnp b
ON a.acct_num = b.acct_num
AND a.mth_end_dt = b.eff_dt
AND a.src_sys_cd = b.src_sys_cd
AND b.primary_acct_holder_f = 'Y'
left outer join (select orig_cab_transit, loan_num, mth_end_dt, src_sys_cd from """ + cf.RCRRDBName + """.psnl_loan_subv_mth_snapshot
group by orig_cab_transit, loan_num, mth_end_dt, src_sys_cd )c
ON a.mth_end_dt = c.mth_end_dt 
and a.acct_num = concat(c.orig_cab_transit, c.loan_num)
left outer join """ + cf.CBSDBName + """.risk_step_xref d
ON a.mth_end_dt = d.eff_dt 
and a.acct_num = substring(d.account_no, 7,12)
and a.src_sys_cd = d.source_sys_cd
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
