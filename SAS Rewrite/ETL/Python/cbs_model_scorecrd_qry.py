#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_qry.py
#
#        USAGE: ./cbs_model_scorecrd_qry.py business_date date_type
#
#  DESCRIPTION: this job will generate the SQL to get model score 
#                 based on the business rules defined in the configuration table(cbs_model_scorecrd_lkp)
#                 the SQL will be saved into table cbs_model_scorecrd_qry) .
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, AJ
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 10/30/2018 16:18:33; Last updated: 02/06/2019 by Justin
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



# insert into table cbs_model_scorecrd_qry table with monthly data
#-- date_type = '""" + date_type + """')
SQL1 = """
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
set hive.server2.logging.operation.level=NONE;

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_qry partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """')
select 
ver
,0 as seg_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,'v_cbs_model_scorecrd_vars' as seg_table
,'' as sc_var
,concat('SELECT ', 'current_timestamp', ' as insrt_process_tmstmp', ', ', decode(unhex(hex(39)), 'US-ASCII'), decode(unhex(hex(34)), 'US-ASCII'), decode(unhex(hex(34)), 'US-ASCII'), decode(unhex(hex(34)), 'US-ASCII'), '+ os.path.realpath(__file__) +', decode(unhex(hex(34)), 'US-ASCII'), decode(unhex(hex(34)), 'US-ASCII'), decode(unhex(hex(34)), 'US-ASCII'), decode(unhex(hex(39)), 'US-ASCII'), ' as op_field', ', ', decode(unhex(hex(39)), 'US-ASCII'),  ver, decode(unhex(hex(39)), 'US-ASCII'), ' as ver',', ','cust_cid, split(str_score,',decode(unhex(hex(39)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(39)), 'US-ASCII'),')[0] as seg_num, split(str_score,',decode(unhex(hex(39)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(39)), 'US-ASCII'),')[1]  as sc_var, cast( split(str_score,',decode(unhex(hex(39)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),decode(unhex(hex(92)), 'US-ASCII'),'|',decode(unhex(hex(39)), 'US-ASCII'),')[2] as int) as score from (  select  cust_cid, case ' , concat_ws(' ', collect_list( sc_qry) ), ' end as str_score    from (   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg3   union all   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg4   union all   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg5   union all   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg6   union all   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg7   union all   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg8   union all   select * from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg9  ) t  where  eff_dt = ', decode(unhex(hex(39)), 'US-ASCII'), '""" + args.bdate + """', decode(unhex(hex(39)), 'US-ASCII'), ' and date_type = ', decode(unhex(hex(39)), 'US-ASCII'), '""" + date_type + """', decode(unhex(hex(39)), 'US-ASCII'),'  ) t where str_score is not null') as qry
from (
  select
  ver as ver
  ,seg_num as seg_num
  ,sc_var as sc_var

  ,concat_ws(' ', collect_list(case when (bin_cond<>'else' and s_no=1) then concat(' when seg_num = ' ,seg_num, ' and sc_var = ',decode(unhex(hex(39)), 'US-ASCII'), sc_var,decode(unhex(hex(39)), 'US-ASCII'),' then concat(seg_num,',decode(unhex(hex(39)), 'US-ASCII'), '|||', decode(unhex(hex(39)), 'US-ASCII'),',',decode(unhex(hex(39)), 'US-ASCII'),sc_var,decode(unhex(hex(39)), 'US-ASCII'),',',decode(unhex(hex(39)), 'US-ASCII'),'|||',decode(unhex(hex(39)), 'US-ASCII'),', case ', hardcode1, ' (', derived_condition, ') ', hardcode2,' ', score) when (bin_cond<>'else' and s_no<>1) then concat(' ', hardcode1, ' (', derived_condition, ') ', hardcode2,' ', score) else concat('else ', score, ' end)') end
                )
        ) as sc_qry
  from
    (
      select
      insrt_process_tmstmp,
      op_field,
      ver,
      seg_num,
      sc_var,
      bin_cond,
      case when (bin_cond<>'else') then 'when' end as hardcode1,
      case when (bin_cond<>'else') then 'then' end as hardcode2,
      regexp_replace(bin_cond,'x','sc_var_val') as derived_condition,
      rank() over (partition by sc_var order by seq_num) as s_no,
      score
      from """ + cf.CBSDBName + """.cbs_model_scorecrd_lkp
      where ver in (select max(ver) from """ + cf.CBSDBName + """.cbs_model_scorecrd_lkp)
    )base1
  group by ver, seg_num,sc_var
) t
where sc_qry is not null or trim(sc_qry) <> ''
group by ver;

"""



sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

        for sql in sqls:
                print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
                cf.hive_exec(sql)
                print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")



if __name__ == '__main__':
        main()
