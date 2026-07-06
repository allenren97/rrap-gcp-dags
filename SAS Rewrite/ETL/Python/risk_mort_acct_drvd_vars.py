#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/risk_mort_acct_drvd_vars.py
#
#        USAGE: ./risk_mort_acct_drvd_vars.py bdate datetype
#
#  DESCRIPTION: Risk Mortgage Account Derived Variables table -- Monthly job
#
#      OPTIONS: ---
# REQUIREMENTS: two arguments: business date & date load type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu, Suhel D
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 08/28/2018  
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



# insert into table risk_mort_acct_drvd_vars table with monthly data

SQL1 = """
with 
step1 as (
select max(day_dt) as last_business_day 
from """ + cf.RCRRDBName + """.tm_dim 
where clndr_yr = year('""" + cf.bdate + """')
and mth_clndr_cd = date_format('""" + cf.bdate + """','MMMMM')
and tm_lvl = 'Day'
and day_of_wk_desc IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
),
step2 as (
select b.last_business_day,a.mort_num, eff_dt, date_type, PD_OFF_DT, FLOAT_CD, WEEK_FRST_UNPAID_DT,FRST_UNPD_DT, CRNT_BAL_AMT, INTR_ACCR_AMT, FRCLSR_FLAG, YTD_PREPAY_AMT
, a.prim_cust_cid
, case 
when cast(trim(fund_cd) as int) >=2000 AND cast(trim(fund_cd) as int) <=2199 then 'Y'
when cast(trim(fund_cd) as int) >=2202 AND cast(trim(fund_cd) as int) <=2249 then 'Y'
when cast(trim(fund_cd) as int) >=6490 AND cast(trim(fund_cd) as int) <=6499 then 'Y'
else 'N' end as LAND_REGS_ACT_STAT_FLAG
, case 
when substr(trim(SCRTY_TYPE_2),1,1)='6' OR cast(substr(trim(SCRTY_TYPE_2),length(trim(SCRTY_TYPE_2))-2,3) as int) >=5 then 'COMMERCIAL'
else 'RESIDENTIAL' end as COMM_TYPE_CD
, (CRNT_BAL_AMT + INTR_ACCR_AMT) as OS_BAL_AMT
, case 
when PD_OFF_DT is not null OR PD_OFF_DT <> '' OR PD_OFF_FLAG = 'Y' then 0 
when FLOAT_CD IN ('W','B','S') then 
    case 
    when datediff(b.last_business_day, WEEK_FRST_UNPAID_DT) is null then null  
    when datediff(b.last_business_day, WEEK_FRST_UNPAID_DT) < 0 then 0 
    else datediff(b.last_business_day, WEEK_FRST_UNPAID_DT)
    end 
else 
    case 
    when datediff(b.last_business_day,FRST_UNPD_DT) is null then null
    when datediff(b.last_business_day,FRST_UNPD_DT) < 0 then 0 
    else datediff(b.last_business_day,FRST_UNPD_DT)
    end 
end as DLQNT_DAY_CNT
, case 
when PD_OFF_DT is not null OR PD_OFF_DT <> '' OR PD_OFF_FLAG = 'Y' then 0 
when FLOAT_CD IN ('W','B','S') then 
     case 
     when cast(months_between(date_add(eff_dt,1), date_add(last_day(add_months(WEEK_FRST_UNPAID_DT,-1)),1)) as int) is null 
      OR cast(months_between(date_add(eff_dt,1), date_add(last_day(add_months(WEEK_FRST_UNPAID_DT,-1)),1)) as int) < 0 then 0
     else  cast(months_between(date_add(eff_dt,1), date_add(last_day(add_months(WEEK_FRST_UNPAID_DT,-1)),1)) as int) 
     end 
else 
    case 
    when cast(months_between(date_add(eff_dt,1), date_add(last_day(add_months(FRST_UNPD_DT,-1)),1)) as int) is null 
      OR cast(months_between(date_add(eff_dt,1), date_add(last_day(add_months(FRST_UNPD_DT,-1)),1)) as int) < 0 then 0
    else  cast(months_between(date_add(eff_dt,1), date_add(last_day(add_months(FRST_UNPD_DT,-1)),1)) as int)
    end 
end as DLQNT_MTH_CNT
from """ + cf.CBSDBName + """.risk_mort_acct_snapshot a
left outer join step1 b ON 1=1
where a.eff_dt = '""" + cf.bdate + """'
and a.date_type = '""" + date_type + """' 
),
step3 as 
(
select a.*
, case 
when ucase(trim(COMM_TYPE_CD)) <> 'RESIDENTIAL' OR PD_OFF_DT is not null OR OS_BAL_AMT <=0 then 'Z' 
else 'A'
end as CONSMR_PROD_TREATMNT_CD
from step2 a 
),
step4 as 
(
select a.*, 
case 
when COMM_TYPE_CD = 'RESIDENTIAL' 
AND (PD_OFF_DT is null OR PD_OFF_DT = '') 
AND (DLQNT_MTH_CNT <=3 OR DLQNT_MTH_CNT IS NULL)
AND (TRIM(FRCLSR_FLAG) <> 'Y' OR FRCLSR_FLAG IS NULL)
AND CRNT_BAL_AMT <> 0
AND (LAND_REGS_ACT_STAT_FLAG = 'N' OR LAND_REGS_ACT_STAT_FLAG IS NULL)
then 'CUR'
when COMM_TYPE_CD = 'RESIDENTIAL' 
AND (PD_OFF_DT is null OR PD_OFF_DT = '')
AND (DLQNT_MTH_CNT > 3 OR TRIM(FRCLSR_FLAG) = 'Y' OR LAND_REGS_ACT_STAT_FLAG = 'Y') 
AND CRNT_BAL_AMT <> 0 
then 'DEF'
else NULL end as PIT_STAT_VER_1_CD
from step3 a
),
step5 as 
(
select mort_num, eff_dt, date_type, YTD_PREPAY_AMT
from """ + cf.CBSDBName + """.risk_mort_acct_snapshot
where eff_dt = add_months('""" + cf.bdate + """',-1)
and date_type = '""" + date_type + """'
)
insert overwrite table """ + cf.CBSDBName + """.risk_mort_acct_drvd_vars partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
select e.mort_num
,current_timestamp as insrt_process_tmstmp
,'""" + os.path.realpath(__file__) + """' as op_field
, e.prim_cust_cid 
, e.last_business_day
, e.DLQNT_MTH_CNT
, e.LAND_REGS_ACT_STAT_FLAG
, e.DLQNT_DAY_CNT
, e.PIT_STAT_VER_1_CD
, e.CONSMR_PROD_TREATMNT_CD
, e.COMM_TYPE_CD
, e.OS_BAL_AMT
, If(ISNULL(f.mort_num), 0, greatest(0.00,cast((e.YTD_PREPAY_AMT-f.YTD_PREPAY_AMT) as double))) as PRPY_AMT
from step4 e
left outer join step5 f 
ON e.mort_num = f.mort_num
and e.date_type = f.date_type
"""

sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
