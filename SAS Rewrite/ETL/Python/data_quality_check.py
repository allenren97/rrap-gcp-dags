#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/main.py
#
#        USAGE: ./data_quality_check.py job_name business_date date_type
#
#  DESCRIPTION: data quality check, run data quality check logic using SQL
#               1. get data quality check SQL using job_name 
#               2. replace variables in SQL using business_date and date_type
#               3. run the SQL 
#               4. Check the result and log it into log table
#
#      OPTIONS: ---
# REQUIREMENTS: three arguments: job name, business date and date type
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Justin Liu 
#      COMPANY: bns
#      VERSION: 1.0
#      CREATED: 01/20/2019 16:18:33 
#     REVIEWER: 
#     REVISION: ---
#    SRC_TABLE:  
#    TGT_TABLE: 
#===============================================================================

import os,re,sys
import subprocess
import json
import ConfigParser
import datetime,time
import logging
from hive_task import CBS_Configuration




class CBS_Data_Quality_Check():
	
	def __init__(self,job_name,pdate,bdate,date_type = None):
		self.job_name=job_name
		self.pdate=pdate
		self.bdate=bdate
		self.date_type=date_type
		self.cf = CBS_Configuration(bdate)
		self.temp_result = "temp_" + job_name + "-data-quality-" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') 
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.DEBUG)
		ch = logging.StreamHandler(sys.stdout)
		ch.setLevel(self.cf.LoggingLevel)
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		ch.setFormatter(formatter)
		self.logger.addHandler(ch)


	def runCommand(self,cmd):
		self.logger.debug (cmd)
		res=subprocess.call(cmd, shell=True)
		if res != 0:
			self.logger.fatal( "run job failure: " + cmd )
			sys.exit(res)
		return 0

	def Run_Check(self):
		hive_sql="""
			select concat(qry_id,'|',subj_area_nm,'|',src_cd,'|',vldtn_type,'|',
				src_tbl_nm,'|',target_tbl_nm,'|',step_num,'|',variance_lmt,'|',parm_nm,'|',severity_lvl,'|'), step_num
			from """ + self.cf.CBSDBName + """.""" + self.cf.TblDataQuality + """
			where job_nm = '""" + self.job_name + """' and active_f='Y' and date_type = '""" + self.date_type + """' order by step_num;
		"""

		self.logger.debug (hive_sql)
		self.cf.hive_exec2 (hive_sql, self.temp_result+".chklst")
		with open(self.temp_result+".chklst", 'r') as myfile:
			check_list = myfile.readlines()
		check_no = 0
		error_no = 0
		warning_no = 0
		for check_item in check_list:
		
			check_no = check_no + 1

			dataArray = check_item.split('|')
			qry_id=dataArray[0]
			subj_area_nm=dataArray[1]
			src_cd=dataArray[2]
			vldtn_type=dataArray[3]
			src_tbl_nm=dataArray[4]
			target_tbl_nm=dataArray[5]
			step_num=dataArray[6]
			variance_lmt=dataArray[7]
			parm_nm=dataArray[8]
			severity_lvl=dataArray[9]
			
			#prepare the sql , replace variables in the sql with executing data
			hive_sql="""
			select regexp_replace(sql_text,'\n',' ')
			from """ + self.cf.CBSDBName + """.""" + self.cf.TblDataQuality + """
			where qry_id=""" + qry_id + """;
			"""
			self.logger.debug (hive_sql)
			self.cf.hive_exec2 (hive_sql, self.temp_result+".sql")
			
			# replace the varaiables
			cmd = "sed -i.bak 's/&mth_end_dt/" + self.cf.last_month_end_dt + "/g' " + self.temp_result+".sql"
			self.runCommand(cmd)
			cmd = "sed -i.bak 's/&eff_dt/" + self.cf.bdate + "/g' " + self.temp_result+".sql"
			self.runCommand(cmd)
			
			#execute the sql
			myfile = open(self.temp_result+".sql", 'r')
			hive_sql = myfile.read()			
			self.logger.debug (hive_sql)
			self.cf.hive_exec2 (hive_sql, self.temp_result+".data")
			
			#check the result 
			#If find the error, we need send email alert and exit the program with error code.
			#If find the warning, we need to save it and send email alert later.
			with open (self.temp_result+".data", "r") as f:
				data_result=f.read()
			with open (self.temp_result+".data", "r") as f:
				dataArray=f.readline().split("\t")
			self.logger.debug("the execution result is " + data_result)
			if dataArray[4] > variance_lmt:
				if severity_lvl == 1:
					error_no = 1
					self.logger.debug("found error, will break the job running and exit with abnormal")
				else:
					warning_no = warning_no + 1
					self.logger.debug("found warnings, will continue the job running and send email with this warning information")
				msg_type = severity_lvl
			else:
				msg_type = 0
			
			file=open(self.temp_result + ".rslt","a")
			file.write (str(qry_id) + "	" + str(msg_type) + "	" + data_result)

			if msg_type == 1:
				break
		
		if check_no > 0:
			file.close()
			cmd="hdfs dfs -put " + self.temp_result + ".rslt /data/crz/bbcx/int/tmp/"
			self.runCommand(cmd)
		
			hive_sql = """
			    load data inpath '/data/crz/bbcx/int/tmp/""" + self.temp_result + """.rslt' overwrite into table """ +  self.cf.CBSDBName + """.tmp_""" + self.cf.TblDataQualityLog + """ partition  (job_nm = '""" +   self.job_name + """')"""
			self.logger.debug(hive_sql)
			self.cf.hive_exec(hive_sql)
			
			hive_sql = """
			insert into """ +  self.cf.CBSDBName + """.""" + self.cf.TblDataQualityLog + """ partition  (job_nm = '""" +   self.job_name + """', date_type='m')
			select current_timestamp(), '','""" + self.pdate + """',a.qry_id,b.subj_area_nm,b.src_cd,b.vldtn_type, b.src_tbl_nm, b.target_tbl_nm,
			step_num, variance_lmt, severity_lvl, a.crnt_dt, a.crnt_val, a.basln_dt, a.basln_val, a.variance_val, 0, 
			regexp_replace(regexp_replace(regexp_replace(sql_text,'&mth_end_dt','""" + self.cf.last_month_end_dt + """'),'&eff_dt','
			""" + self.cf.bdate + """'),'\n','XYZABC'), a.rslt_text from """ +  self.cf.CBSDBName + """.tmp_""" + self.cf.TblDataQualityLog + """
			a inner join """ +  self.cf.CBSDBName + """.""" + self.cf.TblDataQuality + """ b on a.qry_id = b.qry_id where a.job_nm = 
			'""" +   self.job_name + """';
			"""
			self.logger.debug(hive_sql)
			self.cf.hive_exec(hive_sql)
			
			if error_no > 0 or warning_no > 0:
				hive_sql="""
				select '""" + self.pdate + """', '""" + self.cf.bdate + """', b.vldtn_type, b.src_tbl_nm,a.crnt_val, b.target_tbl_nm, 
				a.basln_val, a.variance_val,
				case when msg_type=1 then 'Error' else 'Warning' end as msg_type,a.rslt_text
				from """ +  self.cf.CBSDBName + """.tmp_""" + self.cf.TblDataQualityLog + """ a inner join 
				""" +  self.cf.CBSDBName + """.""" + self.cf.TblDataQuality + """ b on a.qry_id = b.qry_id 
				where a.msg_type > 0 and a.job_nm = '""" +   self.job_name + """'
				sort by msg_type desc;
				"""
				self.logger.debug(hive_sql)
				self.cf.hive_exec2(hive_sql,self.temp_result + ".report")
				
				cmd="./mk_data_check_report.sh " + self.pdate + " " + self.temp_result + ".report " + str(error_no)
				self.runCommand(cmd)

		return 0;

		
def main():

	dq = CBS_Data_Quality_Check("risk_sav_acct_snapshot", "2019-02-18","2016-05-31","m")
	dq.Run_Check()
	
	
if __name__ == '__main__':
	main()
