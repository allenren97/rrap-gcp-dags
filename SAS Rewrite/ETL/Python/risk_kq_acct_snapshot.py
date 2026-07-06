#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_kq_acct_snapshot.py
#
#        USAGE: ./risk_kq_acct_snapshot.py business_date date_type
#
#  DESCRIPTION: Risk KQ account snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 07/20/2018 16:18:33; Last updated: 09/28/2018
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



# insert into table risk_kq_acct_snapshot table with monthly data

SQL1 = """
insert overwrite table """ + cf.CBSDBName + """.risk_kq_acct_snapshot partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select a.acct_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,y.agreement_no
,cst.cust_cid as prim_cust_cid
,a.acct1_prd_cd as prod_cd
,a.acct1_sub_prd_cd as sub_prod_cd
,prd.cis_product_code
,prd.major_product_group
,a.acct2_transit_number trnst_num
,concat(a.acct1_block_cd,a.acct1_recl_cd) as block_recl_cd
,a.acct1_acct_stat_cd as acct_stat_cd
,a.acct1_open_dt as acct_open_dt
,a.acct1_src as src_cd
,a.acct1_final_score as final_cr_score
,a.acct1_last_prch_dt as last_purch_dt
,a.acct1_last_active_dt as last_acty_dt
,a.acct1_credit_lmt as cr_lmt_amt
,a.delq_mth_dlqnt as mth_dlqnt_cnt 
,a.delq_orig_chng_off_amt as orig_chrg_off_amt
,a.delq_chrg_off_dt as chrg_off_dt
,a.delq_chrg_off_cd as chrg_off_cd
,a.fin1_last_pymt_dt as last_paymnt_dt
,a.os_bal_coa_amt as tot_new_bal_amt
,a.fin2_nal_dt as non_accrl_dt
,a.fin2_wor_dt as write_off_dt
,a.acct2_cls_reason_cd as acct_cls_rsn_cd
,a.delq_bns_dlqnt_day as bns_dlqnt_day
,a.acct1_last_blocked_dt as last_blocked_dt
,a.acct1_scrd_ind as scrd_type_cd
,a.acct1_switch_ind as switch_cd
,a.acct1_switch_dt as switch_dt
,a.acct1_switch_xref as switch_xref
,case when acct1_next_renew_fee_dt_mth_yr = 0 then NULL
when a.acct1_next_renew_fee_dt_mth_yr <= 999 then 
case when substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),2,2) >= 80 
then concat(concat('19', substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),2,2)), '-', concat('0',substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),1,1)),'-','01')
else concat(concat('20', substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),2,2)), '-',concat('0',substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),1,1)),'-','01')
end 
when a.acct1_next_renew_fee_dt_mth_yr > 999 then 
case when substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),3,2) >= 80
then concat(concat('19', substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),3,2)), '-',concat(substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),1,2)),'-','01')
else concat(concat('20', substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),3,2)), '-',concat(substring(cast(a.acct1_next_renew_fee_dt_mth_yr as varchar(5)),1,2)),'-','01')
end else a.acct1_next_renew_fee_dt_mth_yr end
,a.acct2_inact_ind as inact_cd
,a.acct1_scrty_type_cd as scrty_tp_cd
,a.acct1_scrty_val as scrty_val_amt
,c.bcm_corporate_retail_ind as corp_rtl_flag
,d.bcm_current_bill_code as crnt_bill_cd
,e.bcm_purchase_curr_cyc_bal
,e.bcm_purchase_1cyc_ago_bal 
,e.bcm_purchase_2cyc_ago_bal 
,f.bcm_cash_adv_curr_cyc_bal
,f.bcm_cash_adv_1cyc_ago_bal
,f.bcm_cash_adv_2cyc_ago_bal
,c.bcm_tot_unpaid_finance_chg
,e.bcm_ytd_purchases_num 
,e.bcm_ytd_purchase_int_chged 
,e.bcm_ytd_purchase_int_paid
,e.bcm_ytd_cash_adv_int_chged
,e.bcm_ytd_cash_adv_int_paid 
,c.bcm_tot_recovery_int
,e.bcm_purchase_recovery_int 
,f.bcm_cash_adv_recovery_int 
,g.bcm_last_year_credit_int_paid
,g.bcm_tot_ytd_credit_int_paid
,g.bcm_bal_hist_purchases_1
,g.bcm_bal_hist_cash_adv_1
,g.bcm_bal_hist_new_balance_1
,d.bcm_full_payment_ind 
,c.bcm_prev_sub_product
,h.bcm_dlq_history_13_24
,h.bcm_dlq_history_01_12
,e.bcm_amt_last_payment
,a.gl_acct_num
,a.gl_acctng_transit
,a.currency_cd
from """ + cf.RCRRDBName + """.revlvng_credit_mth_snapshot a
left outer join """ + cf.CBSDBName + """.risk_step_xref y
ON a.mth_end_dt = y.eff_dt 
and trim(a.acct_num) = substring(y.account_no,6,13)
and a.src_sys_cd = y.source_sys_cd
left outer join 
(select bcm_account_num, businesseffectivedate, bcm_corporate_retail_ind, bcm_tot_unpaid_finance_chg, bcm_tot_recovery_int, bcm_prev_sub_product 
from """ + cf.TSZDBName + """.kq_tkq_account1 yy
where businesseffectivedate IN (select max(businesseffectivedate)
from """ + cf.TSZDBName + """.kq_tkq_account1 xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """')))  c
ON (trim(a.acct_num) = trim(c.bcm_account_num) )
left outer join 
(select bcm_account_num, businesseffectivedate, bcm_current_bill_code, bcm_full_payment_ind 
from """ + cf.TSZDBName + """.kq_tkq_bill_statements yy
where businesseffectivedate IN (select max(businesseffectivedate)
from """ + cf.TSZDBName + """.kq_tkq_bill_statements xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))) d
ON (trim(a.acct_num) = trim(d.bcm_account_num) )
left outer join 
(select bcm_account_num, businesseffectivedate, bcm_purchase_curr_cyc_bal, bcm_purchase_1cyc_ago_bal, bcm_purchase_2cyc_ago_bal,
bcm_ytd_purchases_num, bcm_ytd_purchase_int_chged, bcm_ytd_purchase_int_paid, bcm_ytd_cash_adv_int_chged,bcm_ytd_cash_adv_int_paid, bcm_purchase_recovery_int,
bcm_amt_last_payment 
from """ + cf.TSZDBName + """.kq_tkq_financial1 yy
where businesseffectivedate IN (select max(businesseffectivedate)
from """ + cf.TSZDBName + """.kq_tkq_financial1 xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))) e 
ON (trim(a.acct_num) = trim(e.bcm_account_num) )
left outer join 
(select bcm_account_num, businesseffectivedate, bcm_cash_adv_curr_cyc_bal, bcm_cash_adv_1cyc_ago_bal, bcm_cash_adv_2cyc_ago_bal,
bcm_cash_adv_recovery_int 
from """ + cf.TSZDBName + """.kq_tkq_financial2 yy
where businesseffectivedate IN (select max(businesseffectivedate)
from """ + cf.TSZDBName + """.kq_tkq_financial2 xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))) f
ON (trim(a.acct_num) = trim(f.bcm_account_num) )
left outer join 
(select bcm_account_num,businesseffectivedate,bcm_last_year_credit_int_paid, bcm_tot_ytd_credit_int_paid,bcm_bal_hist_purchases_1,bcm_bal_hist_cash_adv_1
,bcm_bal_hist_new_balance_1 
from """ + cf.TSZDBName + """.kq_tkq_financial_history yy 
where businesseffectivedate IN (select max(businesseffectivedate)
from """ + cf.TSZDBName + """.kq_tkq_financial_history xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))) g
ON (trim(a.acct_num) = trim(g.bcm_account_num))
left outer join 
(select bcm_account_num, businesseffectivedate, bcm_dlq_history_13_24, bcm_dlq_history_01_12 
from """ + cf.TSZDBName + """.kq_tkq_delinquency yy
where businesseffectivedate IN (select max(businesseffectivedate)
from """ + cf.TSZDBName + """.kq_tkq_delinquency xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))) h
ON (trim(a.acct_num) = trim(h.bcm_account_num) )
left outer join 
(select ks_product_code, ks_sub_product_code, major_product_group, cis_product_code
from """ + cf.TSZDBName + """.kq_tkq_prod_subprod
where businesseffectivedate IN (select max(businesseffectivedate) 
from """ + cf.TSZDBName + """.kq_tkq_prod_subprod xx where month(xx.businesseffectivedate) = month('""" + cf.bdate + """') AND year(xx.businesseffectivedate) = year('""" + cf.bdate + """'))
group by ks_product_code, ks_sub_product_code, major_product_group, cis_product_code) prd 
ON trim(a.acct1_prd_cd) = trim(prd.ks_product_code)
and trim(a.acct1_sub_prd_cd) = trim(prd.ks_sub_product_code)
left outer join (select acct_num, eff_dt, src_sys_cd, primary_acct_holder_f, cust_cid from """ + cf.CBSDBName + """.risk_cust_acct_rltnp) cst
ON (lpad(trim(a.acct_num),23,'0') = lpad(trim(cst.acct_num),23,'0') AND a.mth_end_dt = cst.eff_dt AND a.src_sys_cd = cst.src_sys_cd AND cst.primary_acct_holder_f = 'Y')
where a.mth_end_dt = '""" + args.bdate + """'
"""
 
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


