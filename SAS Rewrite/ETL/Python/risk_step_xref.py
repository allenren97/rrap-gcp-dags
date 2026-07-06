#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_step_xref.py
#
#        USAGE: ./risk_step_xref.py bdate datetype
#
#  DESCRIPTION: Risk STEP Xref table -- Monthly job
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



# insert into table risk_step_xref table with monthly data

SQL1 = """
insert overwrite table """ + cf.CBSDBName + """.risk_step_xref partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')

select
cast(b.account_no as varchar(30)) as account_no, 
current_timestamp as insrt_process_tmstmp,
'""" + os.path.realpath(__file__) + """' as op_field,
c.src_sys_cd as bor_src_sys_cd,
c.non_bor_prd_cd,
c.bor_prd_src_sys_cd,
c.eff_from_dt as bor_eff_from_dt,
c.eff_to_dt as bor_eff_to_dt,
c.crnt_f, 
b.cab_transit,
b.product_cde,
b.agreement_no,
coalesce(c.bor_prd_src_sys_cd, b.product_cde) as source_sys_cd
from
(select uxactdtl_rec.account_no as account_no
, uxactdtl_rec.cab_transit as CAB_TRANSIT
, uxactdtl_rec.product_cde as PRODUCT_CDE
, businesseffectivedate
, max(uxactdtl_rec.agreement_no) as agreement_no 
from """ + cf.TSZDBName + """.ux_ux300u2 a 
group by uxactdtl_rec.account_no
, uxactdtl_rec.cab_transit
, uxactdtl_rec.product_cde
, businesseffectivedate) b
left outer join """ + cf.RCRRDBName + """.non_bor_prd_mappng c
ON c.non_bor_prd_cd = b.product_cde 
AND c.src_sys_cd = 'UX' 
WHERE b.businesseffectivedate between c.eff_from_dt and c.eff_to_dt
AND b.businesseffectivedate IN (select max(businesseffectivedate) 
from """ + cf.TSZDBName + """.ux_ux300u2
where businesseffectivedate <= '""" + args.bdate + """')
"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
