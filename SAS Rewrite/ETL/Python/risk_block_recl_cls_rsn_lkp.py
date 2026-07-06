#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_block_recl_cls_rsn_lkp.py
#
#        USAGE: ./risk_block_recl_cls_rsn_lkp.py 
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



# insert into table risk_block_recl_cls_rsn_lkp table by adhoc

SQL1 = """
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table """ + cf.CBSDBName + """.risk_block_recl_cls_rsn_lkp partition(eff_dt)
select 
  block_recl_cd
  ,current_timestamp() as insrt_process_tmstmp
  ,'""" + os.path.realpath(__file__) + """' as op_field
  ,cls_rsn_cd
  ,consm_scorecrd_exclsn_f
  ,businesseffectivedate as eff_dt 
from """ + cf.TSZRMADBName + """.airb_block_recl_cls_rsn_lkp
where businesseffectivedate in (select max(businesseffectivedate) from  """ + cf.TSZRMADBName + """.airb_block_recl_cls_rsn_lkp )
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


