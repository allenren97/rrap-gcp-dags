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
drop table if exists """ + cf.CBSDBName + """.temp_cust_model_summary_2;

create table """ + cf.CBSDBName + """.temp_cust_model_summary_2 as
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

summary_2 as (
select * from (
(select cust_cid as cust_cid_1, seg_num, sc_var as sc_var_1, sc_var_val as sc_var_val_1,  score as sc_var_score_1
from score where row_cnt = 1) a
left outer join
(select cust_cid as cust_Cid_16, sc_var as sc_var_16, sc_var_val as sc_var_val_16, score as sc_var_score_16
from score where row_cnt = 16) p on a.cust_cid_1 = p.cust_cid_16
left outer join
(select cust_cid as cust_cid_17, sc_var as sc_var_17, sc_var_val as sc_var_val_17, score as sc_var_score_17
from score where row_cnt = 17) q on a.cust_cid_1 = q.cust_cid_17
left outer join
(select cust_cid as cust_cid_18, sc_var as sc_var_18, sc_var_val as sc_var_val_18, score as sc_var_score_18
from score where row_cnt = 18) r on a.cust_cid_1 = r.cust_cid_18
left outer join
(select cust_cid as cust_cid_19, sc_var as sc_var_19, sc_var_val as sc_var_val_19, score as sc_var_score_19
from score where row_cnt = 19) s on a.cust_cid_1 = s.cust_cid_19
left outer join
(select cust_cid as cust_cid_20, sc_var as sc_var_20, sc_var_val as sc_var_val_20, score as sc_var_score_20
from score where row_cnt = 20) t on a.cust_cid_1 = t.cust_cid_20
left outer join
(select cust_cid as cust_cid_21, sc_var as sc_var_21, sc_var_val as sc_var_val_21, score as sc_var_score_21
from score where row_cnt = 21) u on a.cust_cid_1 = u.cust_cid_21
left outer join
(select cust_cid as cust_cid_22, sc_var as sc_var_22, sc_var_val as sc_var_val_22, score as sc_var_score_22
from score where row_cnt = 22) v on a.cust_cid_1 = v.cust_cid_22
left outer join
(select cust_cid as cust_cid_23, sc_var as sc_var_23, sc_var_val as sc_var_val_23, score as sc_var_score_23
from score where row_cnt = 23) w on a.cust_cid_1 = w.cust_cid_23
left outer join
(select cust_cid as cust_cid_24, sc_var as sc_var_24, sc_var_val as sc_var_val_24, score as sc_var_score_24
from score where row_cnt = 24) x on a.cust_cid_1 = x.cust_cid_24
left outer join
(select cust_cid as cust_Cid_25, sc_var as sc_var_25, sc_var_val as sc_var_val_25, score as sc_var_score_25
from score where row_cnt = 25) y on a.cust_cid_1 = y.cust_cid_25
left outer join
(select cust_cid as cust_cid_26, sc_var as sc_var_26, sc_var_val as sc_var_val_26, score as sc_var_score_26
from score where row_cnt = 26) z on a.cust_cid_1 = z.cust_cid_26
left outer join
(select cust_cid as cust_cid_27, sc_var as sc_var_27, sc_var_val as sc_var_val_27, score as sc_var_score_27
from score where row_cnt = 27) aa on a.cust_cid_1 = aa.cust_cid_27
left outer join
(select cust_cid as cust_cid_28, sc_var as sc_var_28, sc_var_val as sc_var_val_28, score as sc_var_score_28
from score where row_cnt = 28) ab on a.cust_cid_1 = ab.cust_cid_28
left outer join
(select cust_cid as cust_cid_29, sc_var as sc_var_29, sc_var_val as sc_var_val_29, score as sc_var_score_29
from score where row_cnt = 29) ac on a.cust_cid_1 = ac.cust_cid_29
left outer join
(select cust_cid as cust_cid_30, sc_var as sc_var_30, sc_var_val as sc_var_val_30, score as sc_var_score_30
from score where row_cnt = 30) ad on a.cust_cid_1 = ad.cust_cid_30
) )

select
cust_cid_1 as cust_cid
,seg_num
,sc_var_16
,sc_var_val_16
,sc_var_score_16
,sc_var_17
,sc_var_val_17
,sc_var_score_17
,sc_var_18
,sc_var_val_18
,sc_var_score_18
,sc_var_19
,sc_var_val_19
,sc_var_score_19
,sc_var_20
,sc_var_val_20
,sc_var_score_20
,sc_var_21
,sc_var_val_21
,sc_var_score_21
,sc_var_22
,sc_var_val_22
,sc_var_score_22
,sc_var_23
,sc_var_val_23
,sc_var_score_23
,sc_var_24
,sc_var_val_24
,sc_var_score_24
,sc_var_25
,sc_var_val_25
,sc_var_score_25
,sc_var_26
,sc_var_val_26
,sc_var_score_26
,sc_var_27
,sc_var_val_27
,sc_var_score_27
,sc_var_28
,sc_var_val_28
,sc_var_score_28
,sc_var_29
,sc_var_val_29
,sc_var_score_29
,sc_var_30
,sc_var_val_30
,sc_var_score_30
,'""" + args.bdate + """' as eff_dt
,'""" + date_type + """' as date_type
from 
summary_2
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
