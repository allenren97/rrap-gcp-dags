#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_lkp.py
#
#        USAGE: ./cbs_model_scorecrd_lkp.py 
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
#      CREATED: 08/13/2018 16:32:33 
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



# insert into table cbs_model_scorecrd_lkp table by adhoc

SQL1 = """
set hive.exec.dynamic.partition.mode=nonstrict;
with init as (
select count(1) as cnt, max(ver) as mver from """ + cf.CBSDBName + """.cbs_model_scorecrd_lkp
)
insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_lkp partition(eff_dt)
select current_timestamp() as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
, a.seg_num
, trim(a.scorecrd_var) as sc_var
, a.bin_cond 
, a.score 
, a.seq_num 
, case when c.cnt = 0 then 1 else (c.mver + 1) end as ver
, a.businesseffectivedate as eff_dt 
from """ + cf.TSZRMADBName + """.cbs_model_scorecrd_lkp a
left outer join init c ON 1=1
where a.businesseffectivedate in (select max(b.businesseffectivedate) from """ + cf.TSZRMADBName + """.cbs_model_scorecrd_lkp b)
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()


