#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_psnl_loan_scrty_cd_lkp.py
#
#        USAGE: ./risk_psnl_loan_scrty_cd_lkp.py 
#
#  DESCRIPTION: ingestion job via ingestion framework, lookup table ingested by adhoc
#
#      OPTIONS: ---
# REQUIREMENTS: No arguments required
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 08/10/2018 16:32:33 
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


	
cf = CBS_Configuration()



# insert into table cbs_psnl_loan_scrty_cd_lkp table by adhoc

SQL1 = """
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table """ + cf.CBSDBName + """.risk_psnl_loan_scrty_cd_lkp partition(eff_dt)
select 
  scrty_cd
  ,current_timestamp() as insrt_process_tmstmp
  ,'""" + os.path.realpath(__file__) + """' as op_field
  ,scrty_desc
  ,type
  ,prod
  ,sub_prod
  ,step_flag
  ,basel_prod
  ,basel_sub_prod
  ,basel_prod_abbr
  ,prod_id
  ,businesseffectivedate as eff_dt 
from """ + cf.TSZRMADBName + """.cbs_psnl_loan_scrty_cd_lkp
where businesseffectivedate in (select max(businesseffectivedate) from  """ + cf.TSZRMADBName + """.cbs_psnl_loan_scrty_cd_lkp )
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


