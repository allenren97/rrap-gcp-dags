#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_cust_profile_summary.py
#
#        USAGE: ./cbs_cust_profile_summary.py bdate datetype
#
#  DESCRIPTION: CBS Customer Profile Summary table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Rahim Dobani 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 26/02/2018 16:18:33 
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



# insert into table cbs_cust_profile_summary table with monthly data

SQL1 = """
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

with score as (
select
cust_cid
,seg_num
,eff_dt
,date_type
,sum(score) as cust_score
from
""" + cf.CBSDBName + """.cbs_model_scorecrd where eff_dt = '""" + args.bdate + """' and date_type = '""" + date_type + """'
group by cust_cid, seg_num, eff_dt, date_type
)

insert overwrite table """ + cf.CBSDBName + """.cbs_model_scorecrd_summary partition (eff_dt = '""" + args.bdate + """', date_type = '""" + date_type + """', seg_num)
select 
a.cust_cid
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
,a.sc_var_1               
,a.sc_var_val_1           
,a.sc_var_score_1         
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
,case 
when c.seg_num = 3 then (cust_score + 473)
when c.seg_num = 4 then (cust_score + 682)
when c.seg_num = 5 then (cust_score + 589)
when c.seg_num = 6 then (cust_score + 567)
when c.seg_num = 7 then (cust_score + 562)
when c.seg_num = 8 then (cust_score + 654)
when c.seg_num = 9 then (cust_score + 609)
else cust_score end as cust_score
,a.seg_num
from
""" + cf.CBSDBName + """.temp_cust_model_summary_1 a,
""" + cf.CBSDBName + """.temp_cust_model_summary_2 b,
score c
where a.cust_cid = b.cust_cid
and a.cust_cid = c.cust_cid
and a.seg_num = b.seg_num
and a.seg_num = c.seg_num
and a.eff_dt = b.eff_dt
and a.eff_dt = c.eff_dt
and a.date_type = b.date_type
and a.date_type = c.date_type
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
