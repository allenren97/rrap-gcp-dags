#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/temp_cust_model_summary_1.py
#
#        USAGE: ./temp_cust_model_summary_1.py bdate datetype
#
#  DESCRIPTION: Temp Cusotmer Model Summary table 1 -- Monthly job (this will load 15 variables)
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Rahim Dobani 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 8/03/2018 16:18:33 
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



# insert into table temp_cust_model_summary_1 table with monthly data

SQL1 = """
drop table if exists """ + cf.CBSDBName + """.temp_cust_model_summary_1;

create table """ + cf.CBSDBName + """.temp_cust_model_summary_1 as
with seg_union as 
(select cust_cid, eff_dt, seg_num, sc_var, sc_var_val,
dense_rank()over(partition by cust_cid order by sc_var asc) as row_cnt
from (
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg3
where eff_dt = '""" + args.bdate + """'
UNION ALL
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg4
where eff_dt = '""" + args.bdate + """'
UNION ALL
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg5
where eff_dt = '""" + args.bdate + """'
UNION ALL
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg6
where eff_dt = '""" + args.bdate + """'
UNION ALL
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg7
where eff_dt = '""" + args.bdate + """'
UNION ALL
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg8
where eff_dt = '""" + args.bdate + """'
UNION ALL
select cust_cid, eff_dt, seg_num, sc_var, sc_var_val from """ + cf.CBSDBName + """.cbs_model_scorecrd_var_seg9
where eff_dt = '""" + args.bdate + """'
)x ),

score as (
select a.cust_cid, a.eff_dt, a.seg_num, a.sc_var, a.sc_var_val, b.score, a.row_cnt from 
seg_union a left join
""" + cf.CBSDBName + """.cbs_model_scorecrd b on a.cust_cid = b.cust_cid and a.sc_var = b.sc_var
where a.eff_dt = b.eff_dt
and a.eff_dt = '""" + args.bdate + """'
and a.seg_num = b.seg_num
),

summary_1 as (
select * from (
(select cust_cid as cust_cid_1, seg_num, sc_var as sc_var_1, sc_var_val as sc_var_val_1,  score as sc_var_score_1
from score where row_cnt = 1) a
left outer join
(select cust_cid as cust_cid_2, sc_var as sc_var_2, sc_var_val as sc_var_val_2, score as sc_var_score_2
from score where row_cnt = 2) b on a.cust_cid_1 = b.cust_cid_2
left outer join
(select cust_cid as cust_Cid_3, sc_var as sc_var_3, sc_var_val as sc_var_val_3, score as sc_var_score_3
from score where row_cnt = 3) c on a.cust_cid_1 = c.cust_cid_3
left outer join
(select cust_cid as cust_cid_4, sc_var as sc_var_4, sc_var_val as sc_var_val_4, score as sc_var_score_4
from score where row_cnt = 4) d on a.cust_cid_1 = d.cust_cid_4
left outer join
(select cust_cid as cust_cid_5, sc_var as sc_var_5, sc_var_val as sc_var_val_5, score as sc_var_score_5
from score where row_cnt = 5) e on a.cust_cid_1 = e.cust_cid_5
left outer join
(select cust_cid as cust_cid_6, sc_var as sc_var_6, sc_var_val as sc_var_val_6, score as sc_var_score_6
from score where row_cnt = 6) f on a.cust_cid_1 = f.cust_cid_6
left outer join
(select cust_cid as cust_cid_7, sc_var as sc_var_7, sc_var_val as sc_var_val_7, score as sc_var_score_7
from score where row_cnt = 7) g on a.cust_cid_1 = g.cust_cid_7
left outer join
(select cust_cid as cust_cid_8, sc_var as sc_var_8, sc_var_val as sc_var_val_8, score as sc_var_score_8
from score where row_cnt = 8) h on a.cust_cid_1 = h.cust_cid_8
left outer join
(select cust_cid as cust_Cid_9, sc_var as sc_var_9, sc_var_val as sc_var_val_9, score as sc_var_score_9
from score where row_cnt = 9) i on a.cust_cid_1 = i.cust_cid_9
left outer join
(select cust_cid as cust_cid_10, sc_var as sc_var_10, sc_var_val as sc_var_val_10, score as sc_var_score_10
from score where row_cnt = 10) j on a.cust_cid_1 = j.cust_cid_10
left outer join
(select cust_cid as cust_cid_11, sc_var as sc_var_11, sc_var_val as sc_var_val_11, score as sc_var_score_11
from score where row_cnt = 11) k on a.cust_cid_1 = k.cust_cid_11
left outer join
(select cust_cid as cust_cid_12, sc_var as sc_var_12, sc_var_val as sc_var_val_12, score as sc_var_score_12
from score where row_cnt = 12) l on a.cust_cid_1 = l.cust_cid_12
left outer join
(select cust_cid as cust_cid_13, sc_var as sc_var_13, sc_var_val as sc_var_val_13, score as sc_var_score_13
from score where row_cnt = 13) m on a.cust_cid_1 = m.cust_cid_13
left outer join
(select cust_cid as cust_cid_14, sc_var as sc_var_14, sc_var_val as sc_var_val_14, score as sc_var_score_14
from score where row_cnt = 14) n on a.cust_cid_1 = n.cust_cid_14
left outer join
(select cust_cid as cust_cid_15, sc_var as sc_var_15, sc_var_val as sc_var_val_15, score as sc_var_score_15
from score where row_cnt = 15) o on a.cust_cid_1 = o.cust_cid_15
))

select
cust_cid_1 as cust_cid
,seg_num
,sc_var_1
,sc_var_val_1
,sc_var_score_1
,sc_var_2
,sc_var_val_2
,sc_var_score_2
,sc_var_3
,sc_var_val_3
,sc_var_score_3
,sc_var_4
,sc_var_val_4
,sc_var_score_4
,sc_var_5
,sc_var_val_5
,sc_var_score_5
,sc_var_6
,sc_var_val_6
,sc_var_score_6
,sc_var_7
,sc_var_val_7
,sc_var_score_7
,sc_var_8
,sc_var_val_8
,sc_var_score_8
,sc_var_9
,sc_var_val_9
,sc_var_score_9
,sc_var_10
,sc_var_val_10
,sc_var_score_10
,sc_var_11
,sc_var_val_11
,sc_var_score_11
,sc_var_12
,sc_var_val_12
,sc_var_score_12
,sc_var_13
,sc_var_val_13
,sc_var_score_13
,sc_var_14
,sc_var_val_14
,sc_var_score_14
,sc_var_15
,sc_var_val_15
,sc_var_score_15
,'""" + args.bdate + """' as eff_dt
,'""" + date_type + """' as date_type
from 
summary_1
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
