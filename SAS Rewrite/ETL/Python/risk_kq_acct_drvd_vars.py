#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_kq_acct_drvd_vars.py
#
#        USAGE: ./risk_kq_acct_drvd_vars.py bdate datetype
#
#  DESCRIPTION: Risk Revolving Credit Account Derived Variables table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, AJ
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/17/2018  
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



# insert into table risk_kq_acct_drvd_vars table with monthly data

SQL1 = """
with kq_snapshot as 
(
select curr.acct_num as curr_acct_num, curr.prim_cust_cid as curr_prim_cust_cid, prev.acct_num as prev_acct_num, curr.prod_cd as curr_prod_cd, curr.sub_prod_cd as curr_sub_prod_cd, curr.step_pln_agrmnt_num as curr_step_pln_agrmnt_num, curr.scrd_type_cd as curr_scrd_tp_cd, curr.crnt_bill_cd as curr_crnt_bill_cd, curr.chrg_off_cd as curr_chrg_off_cd, prev.chrg_off_cd as prev_chrg_off_cd, curr.block_recl_cd as curr_block_recl_cd, curr.tot_new_bal_amt as curr_tot_new_bal_amt, prev.block_recl_cd as prev_block_recl_cd, prev.tot_new_bal_amt as prev_tot_new_bal_amt, curr.tot_unpaid_fncl_chrg_amt as curr_tot_unpaid_fncl_chrg_amt, prev.tot_unpaid_fncl_chrg_amt as prev_tot_unpaid_fncl_chrg_amt, curr.acct_cls_rsn_cd as curr_acct_cls_rsn_cd, curr.cr_lmt_amt as curr_cr_lmt_amt, curr.ytd_prchs_intr_chrgd_amt as curr_ytd_prchs_intr_chrgd_amt, prev.ytd_prchs_intr_chrgd_amt as prev_ytd_prchs_intr_chrgd_amt, curr.ytd_prchs_cnt as curr_ytd_prchs_cnt, prev.ytd_prchs_cnt as prev_ytd_prchs_cnt, curr.ytd_csh_advnc_intr_chrgd_amt as curr_ytd_csh_advnc_intr_chrgd_amt, prev.ytd_csh_advnc_intr_chrgd_amt as prev_ytd_csh_advnc_intr_chrgd_amt, curr.bns_dlqnt_day as curr_bns_dlqnt_day, curr.eff_dt as curr_mth_end_dt, curr.date_type as curr_date_type
from """ + cf.CBSDBName + """.risk_kq_acct_snapshot curr
left outer join """ + cf.CBSDBName + """.risk_kq_acct_snapshot prev
on curr.acct_num=prev.acct_num
and curr.eff_dt = '""" + cf.bdate + """'
and curr.date_type = prev.date_type
and prev.eff_dt =  last_day(add_months(curr.eff_dt, -1))
where curr.eff_dt = '""" + cf.bdate + """'
and curr.date_type = '""" + date_type + """'
)

,risk_chrg_off_lkp as 
(
select trim(chrg_off_cd) as chrg_off_cd from """ + cf.CBSDBName + """.risk_chrg_off_lkp chg_lkp where upper(trim(chrg_off_stat_flag)) = 'Y' 
and eff_dt IN (select max(eff_dt) from """ + cf.CBSDBName + """.risk_chrg_off_lkp chg_lkp)
)

,risk_block_recl_lkp as 
(
select trim(block_recl_cd) as block_recl_cd from """ + cf.CBSDBName + """.risk_block_recl_lkp where upper(trim(bnkrpcy_flag))='Y' 
and eff_dt IN (select max(eff_dt) from """ + cf.CBSDBName + """.risk_block_recl_lkp)
)

,risk_block_recl_lkp_consmr as 
(
select trim(consm_scorecrd_exclsn_flag) as consm_scorecrd_exclsn_flag, trim(block_recl_cd) as block_recl_cd from """ + cf.CBSDBName + """.risk_block_recl_lkp where upper(trim(bnkrpcy_flag))='Y' and eff_dt IN (select max(eff_dt) from """ + cf.CBSDBName + """.risk_block_recl_lkp)
)

,risk_block_recl_cls_rsn_lkp as 
(
select trim(consm_scorecrd_exclsn_flag) as consm_scorecrd_exclsn_flag, trim(block_recl_cd) as block_recl_cd, trim(cls_rsn_cd) as cls_rsn_cd from """ + cf.CBSDBName + """.risk_block_recl_cls_rsn_lkp where eff_dt IN (select max(eff_dt) from """ + cf.CBSDBName + """.risk_block_recl_cls_rsn_lkp)
)

,risk_src_prd_lkp as 
(
select trim(consm_scorecrd_exclsn_f) as consm_scorecrd_exclsn_f, trim(consm_prd_treatmnt_cd) as consm_prd_treatmnt_cd, trim(src_prd_cd) as prod_cd, trim(src_sub_prd_cd) as sub_prod_cd from """ + cf.CBSDBName + """.risk_src_prd_lkp where trim(prd_sys_cd)='KS' 
and eff_dt IN (select max(eff_dt) from """ + cf.CBSDBName + """.risk_src_prd_lkp)
)

,derive_RSF_STEP as 
(
select curr_acct_num
, (case when trim(curr_sub_prod_cd)='RS' then 'Y' else 'N' end) as rs_flag
, (case when curr_step_pln_agrmnt_num not in ('-1','-2') then 'Y' 
        when trim(curr_prod_cd) in ('SCL', 'VIC') and trim(curr_scrd_tp_cd)='U' then 'U' 
        when trim(curr_prod_cd) in ('SCL', 'VIC') and substr(trim(curr_crnt_bill_cd),1,1)='U' then 'U'
        when trim(curr_prod_cd) in ('SCL', 'VIC') and substr(trim(curr_crnt_bill_cd),1,2) in ('11','SB','SN','SP','SR','ST') then 'R'
        when trim(curr_prod_cd) in ('SCL', 'VIC') and trim(curr_scrd_tp_cd)='S' then 'O'
        when trim(curr_prod_cd) in ('SCL', 'VIC') then 'O'
   else 'N'
   END
   ) as step_cd

from kq_snapshot
)

,derive_heloc_f as
(
select 
derive_RSF_STEP.curr_acct_num as curr_acct_num
,(case when (derive_RSF_STEP.rs_flag='Y' or derive_RSF_STEP.step_cd in ('Y','R')) then 'Y' else 'N' end) as heloc_flag
from
derive_RSF_STEP
)

,derive_PIT as 
(
select kq.curr_acct_num
, 
  if (der_heloc.heloc_flag='N', if (trim(kq.curr_chrg_off_cd)='1', 'CHG' 
            , if ((kq.curr_bns_dlqnt_day<120 and not (kq.curr_tot_new_bal_amt>0 and trim(kq.curr_chrg_off_cd) in ('N','Q')) and blk_lkp_curr.block_recl_cd is null) , 'CUR'
            , if ((kq.curr_tot_new_bal_amt>0 and kq.curr_tot_new_bal_amt=kq.curr_tot_unpaid_fncl_chrg_amt and chg_lkp.chrg_off_cd is null 
                 and  not ( kq.prev_tot_new_bal_amt>0 and  trim(kq.prev_chrg_off_cd) in ('N','Q'))
                 and (kq.prev_tot_new_bal_amt>0 and blk_lkp_prev.block_recl_cd is null and kq.prev_tot_new_bal_amt>0)) , 'CUR' 
            , if ((kq.curr_tot_new_bal_amt=0 and kq.prev_tot_new_bal_amt=kq.prev_tot_unpaid_fncl_chrg_amt and chg_lkp.chrg_off_cd is null
                 and  not ( kq.prev_tot_new_bal_amt>0 and  trim(kq.prev_chrg_off_cd) in ('N','Q')) 
                 and (kq.prev_tot_new_bal_amt>0 and blk_lkp_prev.block_recl_cd is null and kq.prev_tot_new_bal_amt>0)) , 'CUR' , 'DEF' ))))
   , if (der_heloc.heloc_flag='Y', if (trim(kq.curr_chrg_off_cd)='1' , 'CHG'
            , if (kq.curr_bns_dlqnt_day<120 and not (kq.curr_tot_new_bal_amt>0 and trim(kq.curr_chrg_off_cd) in ('N','Q')) , 'CUR'
            , if (kq.curr_tot_new_bal_amt>0 and kq.curr_tot_new_bal_amt = kq.curr_tot_unpaid_fncl_chrg_amt and trim(kq.curr_chrg_off_cd)<>'1'
                 and not (kq.prev_tot_new_bal_amt>0 and trim(kq.prev_chrg_off_cd) in ('N','Q')) and kq.prev_tot_new_bal_amt>0 , 'CUR'
            , if ((kq.curr_tot_new_bal_amt=0 and kq.prev_tot_new_bal_amt=kq.prev_tot_unpaid_fncl_chrg_amt and trim(kq.prev_chrg_off_cd)<>'1'
                 and  not ( kq.prev_tot_new_bal_amt>0 and trim(kq.prev_chrg_off_cd) in ('N','Q')) and kq.prev_tot_new_bal_amt>0) , 'CUR' , 'DEF'))))
            , null))
   as pit_stat_ver_2_cd

from kq_snapshot kq

left outer join risk_block_recl_lkp blk_lkp_curr
on blk_lkp_curr.block_recl_cd=kq.curr_block_recl_cd

left outer join risk_chrg_off_lkp chg_lkp
on chg_lkp.chrg_off_cd=kq.prev_chrg_off_cd

left outer join risk_block_recl_lkp blk_lkp_prev
on blk_lkp_prev.block_recl_cd=kq.prev_block_recl_cd

left outer join derive_heloc_f der_heloc
on der_heloc.curr_acct_num=kq.curr_acct_num
)
insert overwrite table """ + cf.CBSDBName + """.risk_kq_acct_drvd_vars partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select 
ss.curr_acct_num as acct_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,ss.curr_prim_cust_cid as prim_cust_cid
,ss.prev_chrg_off_cd as prev_chrg_off_cd
,ss.prev_block_recl_cd as prev_block_recl_cd
,ss.prev_tot_new_bal_amt as prev_tot_new_bal_amt
,ss.prev_tot_unpaid_fncl_chrg_amt as prev_tot_unpaid_fncl_chrg_amt
,der.rs_flag as rs_flag
,der.step_cd as step_cd
, der_heloc_flag.heloc_flag as heloc_flag
, der_pit.pit_stat_ver_2_cd as pit_stat_ver_2_cd
, if((ss.curr_cr_lmt_amt<=0 and ss.curr_tot_new_bal_amt<=0), 'Z', src_prd_lkp.consm_prd_treatmnt_cd) as consmr_prod_treatmnt_cd
, case when blk_lkp_consm.consm_scorecrd_exclsn_flag='Y' then 'Y'
     when blk_cls_rsn_lkp_consm.consm_scorecrd_exclsn_flag='Y' then 'Y'
     when src_prd_lkp.consm_scorecrd_exclsn_f='Y' then 'Y'
     when (ss.curr_tot_new_bal_amt<=0 AND ss.curr_cr_lmt_amt<=0) then 'Y' 
     when der_pit.pit_stat_ver_2_cd='CHG' then 'Y' else 'N'
  end as consmr_scorecrd_exclsn_flag
,If(ISNULL(ss.prev_acct_num), 0, greatest(cast(0 as decimal(19,2)),ss.curr_ytd_prchs_intr_chrgd_amt-ss.prev_ytd_prchs_intr_chrgd_amt)) as prchs_intr_chrgd_amt
,If(ISNULL(ss.prev_acct_num), 0, greatest(cast(0 as decimal(19,2)),cast(ss.curr_ytd_prchs_cnt-ss.prev_ytd_prchs_cnt as decimal(19,2)))) as prchs_cnt
,If(ISNULL(ss.prev_acct_num), 0, greatest(cast(0 as decimal(19,2)),ss.curr_ytd_csh_advnc_intr_chrgd_amt-ss.prev_ytd_csh_advnc_intr_chrgd_amt)) as csh_advnc_intr_chrgd_amt
,If(ISNULL(ss.prev_acct_num), 0, (greatest(cast(0 as decimal(19,2)),ss.curr_ytd_prchs_intr_chrgd_amt-ss.prev_ytd_prchs_intr_chrgd_amt) + greatest(cast(0 as decimal(19,2)),ss.curr_ytd_csh_advnc_intr_chrgd_amt-ss.prev_ytd_csh_advnc_intr_chrgd_amt))) as total_int_chrgd_amt
  
from kq_snapshot ss

left outer join derive_RSF_STEP der
on der.curr_acct_num=ss.curr_acct_num

left outer join derive_PIT der_pit
on der_pit.curr_acct_num=ss.curr_acct_num

left outer join risk_src_prd_lkp src_prd_lkp
on src_prd_lkp.prod_cd=ss.curr_prod_cd
and src_prd_lkp.sub_prod_cd=ss.curr_sub_prod_cd

left outer join risk_block_recl_lkp_consmr blk_lkp_consm
on blk_lkp_consm.block_recl_cd = ss.curr_block_recl_cd

left outer join risk_block_recl_cls_rsn_lkp blk_cls_rsn_lkp_consm
on blk_cls_rsn_lkp_consm.cls_rsn_cd = ss.curr_acct_cls_rsn_cd
and blk_cls_rsn_lkp_consm.block_recl_cd = ss.curr_block_recl_cd

left outer join derive_heloc_f der_heloc_flag
on der_heloc_flag.curr_acct_num=ss.curr_acct_num
"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

    for sql in sqls:
        print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
        cf.hive_exec(sql)
        print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")



if __name__ == '__main__':
    main()