#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_probe_fxd_len_output.py
#
#        USAGE: ./cbs_model_scorecrd_probe_fxd_len_output.py bdate datetype
#
#  DESCRIPTION: CBS Model Scorecard Output Fixed Length -- """ + date_type + """ job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: 
#       AUTHOR: Justin Liu, Suhel D
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 01/25/2019  
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



# insert into table cbs_model_scorecrd_probe_fxd_len_output table with """ + date_type + """ data

SQL1 = """
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_probe_fxd_len_output partition(eff_dt='""" + args.bdate + """',date_type='""" + date_type + """', seg_num)
select current_timestamp() as insrt_process_tmstmp
, '""" + os.path.realpath(__file__) + """' as op_field
, '2' as recrd_type
, regexp_replace(cast('""" + args.bdate + """' as varchar(10)), '-','') as proc_dt 
, 'm' as period_ind
, lpad(cust_cid,20,'0') as cust_cid 
, case when seg_num IN (1,2,10,11) then rpad(score,10,' ')
when seg_num IN (3,4,5,6,7,8,9) then lpad(score,10,'0')
end as score
, repeat(' ', 55) as buffer
, lpad(cast(seg_num as varchar(5)),5,'0') as seg_num 
from """ + cf.CBSDBName + """.cbs_model_scorecrd_probe_output
where eff_dt = '""" + args.bdate + """'
and date_type = '""" + date_type + """'
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
