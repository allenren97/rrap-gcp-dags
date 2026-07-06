#!/usr/bin/python
#-*- coding:utf-8 -*-
#===============================================================================
#
#         FILE: /app/bbcx_cbs_appid/ETL/python/main.py
#
#        USAGE: ./main.py job_name business_date date_type
#
#  DESCRIPTION: main python, as entrance to execute single python job to run
#               1. get job mata data infornmation from job_info table 
#               2. execute job according to the data load type
#               3. run data quality check using data quality check table
#               4. run data audit
#
#      OPTIONS: ---
# REQUIREMENTS: three arguments: job name, business date and date load type
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
import sys,re,os,logging
import subprocess
import datetime,time
import argparse
from hive_task import CBS_Configuration
from data_quality_check import CBS_Data_Quality_Check


parser = argparse.ArgumentParser(description='Usage for arguments')
parser.add_argument('jobname', type=str,
           help='job name')
parser.add_argument('bdate', type=str,
           help='business effective date')
parser.add_argument('datetype', type=str,
           help='date load type (m/w/d/a)')
args = parser.parse_args()

cf = CBS_Configuration(args.bdate)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(cf.LoggingLevel)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

BusinessDate=args.bdate
ProcessingDate=args.bdate

############# History record check
historyRecord = "historyRecord.txt"
historyCacheDays = 60
############# History record check

# prepare the two input arguments:
if (args.datetype.upper()  == 'M'):
    date_type = 'Monthly'
    BusinessDate=cf.last_month_end_dt
elif (args.datetype.upper()  == 'W'):
        date_type = 'Weekly'
elif (args.datetype.upper()  == 'D'):
        date_type = 'Dailyly'
elif (args.datetype.upper()  == 'A'):
        date_type = 'Adhoc'
else:
    logger.fatal("[Fatal]: date load type is incorrect, please make sure the datetype argument should be m/w/d/a!")
    sys.exit(-1)

############# History record check

# cleanUp temp file - done
def clearUpTemp(f, f_temp):
    os.remove(f.name)
    os.rename(f_temp.name, f.name)

# abstract the temp file i/o into method
def hiveExecutePure(sqlString):
    temp_result = "temp_hiveExecutePure-" + args.jobname + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + ".rslt"
    cf.hive_exec2(sqlString, temp_result)
    with open(temp_result, 'r') as myfile:
        result = myfile.readlines()
    os.remove(temp_result)
    return result


# get job's dependencies' name by jobName
def getDependencies(jobName):
    getDependenciesSql = "select d.parent_job_nm from " + cf.CBSDBName + "." + cf.TblJobDepInfo + " d where d.job_nm = '" + jobName.rstrip('\n') + "'"
    temp_result = "temp_dependencies-" + jobName.rstrip('\n') + "-" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + ".rslt"
    cf.hive_exec2 (getDependenciesSql, temp_result)
    with open(temp_result, 'r') as myfile:
        dependencies = myfile.readlines()
    os.remove(temp_result)
    return dependencies

# consume the record file to check if execute. - done
def isExecutable(jobName, businessDate, datetype):
    ifExecute = True
    currentTaskSignature = jobName + ' ' + businessDate + ' ' + datetype
    dependencies = getDependencies(jobName)
    for i in range(len(dependencies)):
        # convert the dependency jobs' name to job signature
        dependencies[i] = dependencies[i].rstrip('\n') + ' ' + businessDate + ' ' + datetype
    logger.debug ("the dependencies are:")
    logger.debug (dependencies)
    
    with open(historyRecord, 'r') as f:
        x = f.readlines()
    logger.debug ("the " + historyRecord + " files:")
    logger.debug (x)
    from datetime import datetime, timedelta
    format_str = '%Y-%m-%d'
    outDate = datetime.now() - timedelta(days=historyCacheDays)

    f_temp = open(f.name+"_temp","w+")
    for l in range(len(x)):
        currentLine = x[l].rstrip('\r\n')
        # check if this task has already executed
        logger.debug ("Comparing [" +currentLine+"] with ["+currentTaskSignature+"]...")
        if currentLine == currentTaskSignature:
            ifExecute = False
            logger.debug ("JOB START ERROR: " + jobName + " already executed!")
        # check if this record is required dependency
        if (currentLine in dependencies):
            dependencies.remove(currentLine)
        # check if necessary to delete this line if it is out-of-date
        if (len(currentLine)>3):
            recordDateObj = datetime.strptime(currentLine.split(' ')[1], format_str)
            if recordDateObj > outDate:
                f_temp.write(currentLine + '\n')
    if len(dependencies) != 0:
        ifExecute = False
        logger.debug ("JOB START ERROR: " + jobName + "'s dependencies are not satisfied! Required dependencies are:")
        logger.debug (dependencies)
    f_temp.close()
    clearUpTemp(f, f_temp)
    logger.debug (jobName + " is executable: " + str(ifExecute))
    return ifExecute



# produce one record in the record file to  - done
def injectRunRecord(jobName, businessDate, datetype):
    currentTaskSignature = jobName + ' ' + businessDate + ' ' + datetype + '\n'
    f = open(historyRecord,"a+")
    f.write(currentTaskSignature)
    f.close()

############# History record check


def HIVEInitialize():
    cmd = "kinit -kt " + cf.KeyTab + " " + cf.KeyTabPrincipalDomain 
    logger.debug (cmd)
    res=subprocess.call(cmd, shell=True)
    if res != 0:
        logger.fatal( "initializing Hive failed using kerberos: " + cmd )
        sys.exit(res)


def NormalRun():

    cmd = "./" + args.jobname + ".py " + BusinessDate + " " + args.datetype
    logger.debug (cmd)
    res=subprocess.call(cmd, shell=True)
    if res != 0:
        logger.fatal( "run job failure: " + cmd )
        sys.exit(res)

def IncrementalRun():
    logger.debug ("TBD...")

def CleanupBeforeRunning():

    # clean up the temporary files related to job
        cmd="rm -f temp_" + args.jobname + "-*.*"
        logger.debug (cmd)
        res=subprocess.call(cmd, shell=True)
        if res != 0:
            logger.fatal( "clean up failure ...")
            sys.exit(res)

def AuditJob():   

    jobname = args.jobname
    hive_sql="""
    select concat(target_schema_nm,'|',target_tbl_nm,'|',target_col_nm,'|',COALESCE(target_sql_crtria, ''),'|',
                src_schema_nm,'|',src_tbl_nm,'|',src_col_nm,'|',COALESCE(src_sql_crtria, ''))
            from """+cf.CBSDBName+"""."""+cf.TblJobInfo+"""
            where job_nm = '"""+jobname+"""'
    """
    logger.debug (hive_sql)
    temp_result = "temp_" + args.jobname + "-" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + ".rslt"
    cf.hive_exec2 (hive_sql, temp_result)
    with open(temp_result, 'r') as myfile:
        data=myfile.read().replace('\n', '')
    dataArray = data.split('|')
    target_schema_nm=dataArray[0]
    target_tbl_nm=dataArray[1]
    target_col_nm=dataArray[2]
    target_sql_crtria=dataArray[3]
    src_schema_nm=dataArray[4]
    src_tbl_nm=dataArray[5]
    src_col_nm=dataArray[6]
    srcsql_crtria=dataArray[7]
    logger.debug ("|" + src_schema_nm + "|" + src_tbl_nm + "|" + src_col_nm + "|")
    where = "where"
    if target_sql_crtria!="":
        where+=target_sql_crtria+"and"
    
    hive_sql = """
    select count("""+target_col_nm+""") as cnt
                from """+target_schema_nm+"""."""+target_tbl_nm+"""
                 """+where+""" """+target_col_nm+"""='"""+BusinessDate+"""'; 
    """
    
    logger.debug (hive_sql)
    cf.hive_exec2 (hive_sql, temp_result)
    
    with open(temp_result, 'r') as myfile:
        target_cnt=myfile.read().replace('\n', '')
    has_data_f = "Y"    
    if target_cnt == "":
        target_cnt = "0"
        has_data_f = "N"
    hive_sql="""
                insert into table """+cf.CBSDBName+""".""" + cf.TblAuditLog + """ 
                select cast ('"""+BusinessDate+"""' as date) as eff_dt,
                       '"""+date_type+"""' as date_type,   
                       current_timestamp() as insrt_process_tmstmp, 
                       '' op_field,                                                   
                       '"""+src_schema_nm+"""' as src_schema_nm, 
                       '"""+ src_tbl_nm+"""' as src_tbl_nm, 
                       0 as src_tbl_row_cnt, 
                       '"""+target_schema_nm+"""' as target_schema_nm,   
                       '"""+target_tbl_nm+"""' as target_tbl_nm,                           
                       """+target_cnt+""" as target_tbl_row_cnt,
                       '"""+has_data_f+"""' as has_data_f 
                       from """+cf.CBSDBName+"""."""+cf.TblJobInfo+""" 
                       where job_nm = '"""+jobname+"""'    
     """
    logger.debug (hive_sql)
    cf.hive_exec (hive_sql)
    os.remove(temp_result)

def main():

    CleanupBeforeRunning()
        
    HIVEInitialize()

    ############# History record check  
    # check flat-file if the current record exist: not exist, normal run, exist skip, 2019-01 JOB_NAME M/D/A
    if ( isExecutable(args.jobname, BusinessDate, args.datetype.upper() ) ):
        ############# History record check
        temp_result = "temp_" + args.jobname + "-" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + ".rslt" 
        # get meta data information for job
        hive_sql="""
        select data_load_type
            from """ + cf.CBSDBName + """.""" + cf.TblJobInfo + """
            where job_nm = '""" + args.jobname + """'

        """
        cf.hive_exec2 (hive_sql, temp_result)

        f = open (temp_result,'r');lines = f.readlines(); f.close() 
        logger.debug ("[" + lines[0] + "]")
        data_load_type = lines[0].upper().strip()
        if ( data_load_type == "NORMAL"):
            NormalRun()
        elif ( data_load_type == "INCREMENTAL"):
            IncrementalRun()

        logger.debug ("finished the job running: " + args.jobname + " with business date : " + BusinessDate)
        AuditJob()
	#dq = CBS_Data_Quality_Check(args.jobname, ProcessingDate, BusinessDate, args.datetype.upper() )
	#if dq.Run_Check() != 0:
        #	logger.fatal( "run job failure: " + cmd )
        #	sys.exit(res)
		
        ############ History record check
        # modify flat-file
        # risk_cust_acct_rltnp 2019-02-12 m
        injectRunRecord(args.jobname, BusinessDate, args.datetype.upper())
        ############# History record check


if __name__ == '__main__':
    main()


