#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_qry.py
#
#        USAGE: ./cbs_model_scorecrd_qry.py business_date date_type
#
#  DESCRIPTION: Scorecard Model execution -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, AJ
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/30/2018 16:18:33; Last updated: 09/28/2018
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
filename="all_queries.txt"
SQL1="""
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=200;
set hive.cbo.enable=true;
set hive.optimize.ppd=true;
set hive.cbo.enable=true;
set hive.optimize.ppd=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=200;
set hive.support.quoted.identifiers=none;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.execution.engine=tez;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.exec.dynamic.partition=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.enforce.bucketing=true;
set hive.optimize.bucketmapjoin=true;
set hive.support.concurrency=true;

select sc_qry from """ + cf.CBSDBName + """.cbs_model_scorecrd_qry where eff_dt = '""" + args.bdate + """' and date_type = '""" + date_type + """';
"""

ins_ow_qry="""insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """')"""
ins_qry="""insert into table """ + cf.CBSDBName + """.cbs_model_scorecrd partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """')"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

        for sql in sqls:
                print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
                cf.hive_exec2(sql,filename)
                with open(filename, 'r') as f:
                        data=f.read().replace('"""+ os.path.realpath(__file__) +"""', """"""+ os.path.realpath(__file__) +"""""")
                        f.close()
                with open(filename, 'w') as f:
                        f.write(data)
                        f.close()
                myList = []
                qcount=0
                with open(filename, 'r') as f:
                        for line in f:
                                myList.append(line)
                        for element in myList:
                                if qcount==0:
                                        cf.hive_exec(ins_ow_qry +' '+ element)
                                else:
                                        cf.hive_exec(ins_qry +' '+ element)
                                qcount+=1
                f.close()
                print ("[Info]: " + str(datetime.now()) + " Scoring Job completed successfully. Total variables populated are : " + str(qcount))


if __name__ == '__main__':
        main()