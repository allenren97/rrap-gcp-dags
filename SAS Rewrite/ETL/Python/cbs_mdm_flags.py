#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/cbs_cis_data_pop.py
#
#        USAGE: ./cbs_cis_data_pop.py bdate datetype
#
#  DESCRIPTION: Risk KQ account snapshot table -- Monthly job
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



# insert into table cbs_mdm_flags table with monthly data

SQL1 = """
	set hive.execution.engine=tez;
	set hive.vectorized.execution.enabled = true;
	set hive.vectorized.execution.reduce.enabled = true;
	set hive.exec.parallel=true;
	insert overwrite table """ + cf.CBSDBName + """.cbs_mdm_flags partition (eff_dt = '""" + args.bdate + """' , date_type = '""" + date_type + """')
	select
party_id,
'""" + os.path.realpath(__file__) + """' as op_field,
current_timestamp as insrt_process_tmstmp,
a.pref_lang_cd AS PREF_LANG,
a.GENDER_CD,
a.MARITAL_STAT_CD AS MARITAL_STATUS,
a.EMP_TYPE_CD,
a.OCCUP_CD,
a.OCCUP_TYPE_CD,
a.OCCUP_STAT_CD,
a.OCCUP_CAT_CD,
a.domicile_transit_num as TRANSIT_NUM,
a.SENSITIVITY_CD,
case when a.deceased_dt is not null or a.deceased_dt<='""" + args.bdate + """' then 'Y' else 'N' end as DECEASED_IND,
case when a.left_dt is not null or a.left_dt <= '""" + args.bdate + """' or a.end_dt is not null or a.end_dt <='""" + args.bdate + """' then 'Closed' else 'Open' end as CUST_STATUS,
case when a.bankruptcy_dt <= '""" + args.bdate + """' then 'Y' else 'N' end as BNKRPTCY_FLAG,
case when a.person_org_cd = 'P' and months_between('""" + args.bdate + """',a.birth_dt)/12 < 18 then 'Y'
else 'N' end as UNDER_18_FLAG,
case when a.person_org_cd = 'P' then 'Retail'
when substr(a.org_type_cd,1,3) = 'SMB' then 'Small Business'
when substr(a.org_type_cd,1,3) = 'COM' then 'Commercial'
when substr(a.org_type_cd,1,3) = 'COR' then 'Corporate'
else null end as CUST_TYPE,
months_between('""" + args.bdate + """',a.since_dt) as TIME_ON_BOOKS,
floor(months_between('""" + args.bdate + """',a.birth_dt)/12) as CUST_AGE
from
(select 
party_id,
birth_dt,
null as org_type_cd,
OCCUP_TYPE_CD,
OCCUP_STAT_CD,
OCCUP_CAT_CD,
person_org_cd,
cast(since_dt as date) since_dt,
cast(bankruptcy_dt as date) bankruptcy_dt,
businesseffectivedate,
domicile_transit_num,
gender_cd,
emp_type_cd,
marital_stat_cd,
pref_lang_cd,
occup_cd,
deceased_dt,
cast(left_dt as date) left_dt,
end_dt,
SENSITIVITY_CD
from """ + cf.TSZDBName + """.os_tos_person
where businesseffectivedate in (select max(b.businesseffectivedate) from """ + cf.TSZDBName + """.os_tos_person b where b.businesseffectivedate <= '""" + args.bdate + """')

UNION ALL

select 
party_id,
null as birth_dt,
org_type_cd,
null as OCCUP_TYPE_CD,
null as OCCUP_STAT_CD,
null as OCCUP_CAT_CD,
person_org_cd,
cast(since_dt as date) since_dt,
cast(bankruptcy_dt as date) bankruptcy_dt,
businesseffectivedate,
domicile_transit_num,
null as gender_cd,
null as emp_type_cd,
null as marital_stat_cd,
pref_lang_cd,
null as occup_cd,
null as deceased_dt,
cast(left_dt as date) left_dt,
null as end_dt,
SENSITIVITY_CD
from """ + cf.TSZDBName + """.os_tos_organization
where businesseffectivedate in (select max(c.businesseffectivedate) from """ + cf.TSZDBName + """.os_tos_organization c where c.businesseffectivedate <= '""" + args.bdate + """')
) a
"""
 
sqls=map(lambda r:r[1],sorted([(int(k.split('SQL')[1]),v) for k,v in locals().items() if k.startswith('SQL')],key=lambda r:r[0]))

def main():

	for sql in sqls:
		print ("[Info]: " + str(datetime.now()) + " begin to run hive sql.")
		cf.hive_exec(sql)
		print ("[Info]: " + str(datetime.now()) + " finished to run hive sql successfully.")
		


if __name__ == '__main__':
	main()
