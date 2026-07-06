#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/hive_task.py
#
#        USAGE: ./hive_task.py business_date
#
#  DESCRIPTION: initialize global variables, including database schema names, edge node and EDL connections, processing dates, etc.
#
#      OPTIONS: ---
# REQUIREMENTS: one argument: business date
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

import os,re,sys
import subprocess
import json
import ConfigParser
import datetime
import logging

CONFIG_FILE_NAME="app.properties"
settingsql="""
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=200;
set hive.support.quoted.identifiers=none;
set hive.cbo.enable=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.optimize.sort.dynamic.partition=true;
set hive.enforce.bucketing=true;
set hive.optimize.bucketmapjoin=true;
set hive.support.concurrency=true;

"""

class CBS_Configuration():
	
	def __init__(self,bdate = None):
		conf = getConfig()
		self.CBSDBName     = conf.get("database","CBSDBName")
		self.EZDBName      = conf.get("database","EZDBName")
		self.RCRRDBName    = conf.get("database","RCRRDBName")
		self.TSZDBName     = conf.get("database","TSZDBName")
		self.TSZRMADBName  = conf.get("database","TSZRMADBName")
		self.TSZRRAPDBName = conf.get("database","TSZRRAPDBName")
        
		if not (bdate is None):
			self.bdate=bdate
			self.yesterday_dt=(datetime.datetime.strptime(bdate,'%Y-%m-%d').date()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
			self.last_1week_dt=(datetime.datetime.strptime(bdate,'%Y-%m-%d').date()-datetime.timedelta(days=7)).strftime('%Y-%m-%d')
			self.last_2week_dt=(datetime.datetime.strptime(bdate,'%Y-%m-%d').date()-datetime.timedelta(days=14)).strftime('%Y-%m-%d')
			self.last_3week_dt=(datetime.datetime.strptime(bdate,'%Y-%m-%d').date()-datetime.timedelta(days=21)).strftime('%Y-%m-%d')
			self.last_4week_dt=(datetime.datetime.strptime(bdate,'%Y-%m-%d').date()-datetime.timedelta(days=28)).strftime('%Y-%m-%d')
			self.last_month_end_dt=(datetime.datetime.strptime(bdate[0:7]+'-01','%Y-%m-%d').date()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')

		self.Server          = conf.get("connection","Server")
		self.KeyTab          = conf.get("connection","KeyTab")
		self.KeyTabPrincipal = conf.get("connection","KeyTabPrincipal")
		self.KeyTabPrincipalDomain = conf.get("connection","KeyTabPrincipalDomain")
		self.JDBCConn        = conf.get("connection","JDBCConn")

		self.TblJobInfo	     = conf.get("table","TblJobInfo")
		self.TblAuditLog     = conf.get("table","TblAuditLog")
		self.TblDataQuality  = conf.get("table","TblDataQuality")
		self.TblDataQualityLog = conf.get("table","TblDataQualityLog")
		self.TblJobDepInfo = conf.get("table", "TblJobDepInfo")

		self.LoggingLevel    = conf.get("system","LoggingLevel")
		
	
	def hive_exec(self, sql):
		hive_cmd="beeline --showHeader=false --outputformat=tsv2 -u \"" + self.JDBCConn + "\"  -e \"" + settingsql +  sql + " \" "
		print (hive_cmd)
		res=subprocess.call(hive_cmd, shell=True)
    		if res != 0:
        		print ("[Fatal]:")
        		sys.exit(res)
	

	def hive_exec2(self, sql, output):
		hive_cmd="beeline --showHeader=false --outputformat=tsv2 -u \"" + self.JDBCConn + "\"  -e \"" + settingsql +  sql + " \" > " + output
		print (hive_cmd)
		res=subprocess.call(hive_cmd, shell=True)
    		if res != 0:
        		print ("[Fatal]:")
        		sys.exit(res)

def getConfig():
    conf = ConfigParser.ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + "/resources/" +  CONFIG_FILE_NAME
    conf.read(path)
    return conf








