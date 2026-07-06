#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_model_scorecrd_probe_output.py
#
#        USAGE: ./cbs_model_scorecrd_probe_output.py bdate datetype
#
#  DESCRIPTION: Risk SAV account snapshot table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: Template updated for cbs_model_scorecrd_probe_output table load, by Gordana
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



# insert into table cbs_model_scorecrd_probe_output table with monthly data

SQL1 = """
with default_seg as
(select cust_cid, seg_num, -999 as score
,lower(substring('""" + date_type + """',1,1))  as period_ind 
from """ + cf.CBSDBName + """.cbs_cust_segmentation
where seg_num in (1,2,10,11)
and eff_dt= '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
)
,
other_seg as
(select cust_cid, seg_num, sum(score) as score
,lower(substring('""" + date_type + """',1,1)) as period_ind 
from """ + cf.CBSDBName + """.cbs_model_scorecrd
where eff_dt= '""" + cf.bdate + """'
and date_type = '""" + date_type + """'
and seg_num in (3,4,5,6,7,8,9)
and score is not null
group by cust_cid, seg_num, date_type
)
, other_seg_final as (
select cust_cid, seg_num 
, case 
when seg_num = 3 then score + 473
when seg_num = 4 then score + 682
when seg_num = 5 then score + 589
when seg_num = 6 then score + 567
when seg_num = 7 then score + 562
when seg_num = 8 then score + 654
when seg_num = 9 then score + 609
end as score 
, period_ind 
from other_seg 
)

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_probe_output partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select 
cust_cid 
,current_timestamp() as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,seg_num 
,score
,period_ind 
from default_seg
union all
select 
cust_cid 
,current_timestamp() as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,seg_num 
,score
,period_ind 
from other_seg_final 
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
