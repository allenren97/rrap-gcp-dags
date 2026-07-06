#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_chrg_off_lkp.py
#
#        USAGE: ./cbs_chrg_off_lkp.py business_date date_type
#
#  DESCRIPTION: ingestion job via ingestion framework, lookup table ingested by adhoc
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


	
cf = CBS_Configuration()



# insert into table cbs_chrg_off_lkp table by adhoc

SQL1 = """
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table """ + cf.CBSDBName + """.risk_chrg_off_lkp partition (eff_dt)
select 
  chrg_off_cd
  ,current_timestamp() as insrt_process_tmstmp
  ,'""" + os.path.realpath(__file__) + """' as op_field
  ,chrg_off_stat_flag
  ,chrg_off_stat_desc
  ,accrl_stat_flag
  ,accrl_stat_desc
  ,businesseffectivedate
from """ + cf.TSZRMADBName + """.cbs_chrg_off_lkp
where businesseffectivedate in (select max(businesseffectivedate) from  """ + cf.TSZRMADBName + """.cbs_chrg_off_lkp )
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


