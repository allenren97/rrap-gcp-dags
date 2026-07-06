import os
import sys
import os.path
import subprocess
import ConfigParser
import logging


logging.basicConfig(filename='cbs.log',level=logging.DEBUG)
CONFIG_FILE_NAME="app.properties"
PY_PATH="./ETL/Python/"
DDL_PATH="./ETL/DDL/"
CURRENT_PATH=sys.argv[2];
print(CURRENT_PATH)

#part of the Hive URL, it include '=' which make it can not be part fo the properties file.
#TODO UAT is different

# load app.properties
def getConfig():
    conf = ConfigParser.ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + "/resources/" +  CONFIG_FILE_NAME
    conf.read(path)
    return conf

#get the hive JDBC URL from file, different evnironment has different hive URL.
def initJDBCURL():
    jdbcMap = dict(line.strip().split('=') for line in open(PY_PATH+'/resources/'+'hive-db.properties') if not line.startswith('#') and not line.startswith('\n'))
    print(jdbcMap)
    return jdbcMap

# get the table name  from sql file name
def getTableNameByFileName(fileName):
    #kq_ddls/CBS_KQ_CUST_SUM_FACT.sql
    basefilename = os.path.basename(fileName)
    fileNameNoExt = os.path.splitext(basefilename)[0]

    return fileNameNoExt.lower()

# call DDL sql to create table.
def createTable(fileName, dburl):
    tablename = getTableNameByFileName(fileName)
    print("create table begin table name is :" + tablename)
    # exist = isTableExist(tablename,dburl);
    # if(exist == 0):
    beelinecmd = "beeline --showHeader=false --outputformat=tsv2 -u \"" + dburl + "\" -f " + DDL_PATH + fileName
    print(beelinecmd)
    os.system(beelinecmd)
    crz_url = dburl.replace("/default","/"+conf.get("database","CBSDBName"))
    print("crz_dburl:"+crz_url)
    tableCreated = isTableExist(tablename, crz_url)
    if (tableCreated == 0):
        print("create table failed:" + tablename)
    else:
        print("create table successfully " + tablename)
    # else:
    #   print("table already exits"+tablename)


def isTableExist(tableName, dburl):
    print("isTableExist tableName is:" + tableName)
    command ='beeline --showHeader=false --outputformat=tsv2 -u "'+dburl+'" -e "show tables ;"|grep '+tableName
    print("check table exist cmd:"+command)
    p = subprocess.Popen(command, universal_newlines=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    cmdoutput = p.stdout.read()
    retcode = p.wait()
    if cmdoutput.find(tableName) == -1:
        return 0
    else:
        return 1

# the shell that trigger this py is in /app/dev_bbcx_cbs_appid/deploy
# so the path should be ./ETL/Python/
filename=PY_PATH+"TablesTOCreate.txt"
environment= sys.argv[1];
print("input env is:"+environment)
# TODO production need to setup keytab?
if environment == "DEV":
   ktabcmd = "kinit -kt /app/dev_bbcx_cbs_appid/.keytab/dev_bbcx_cbs_appid.keytab dev_bbcx_cbs_appid@SCGLOBALUAT.ADUAT.SCOTIACAPITAL.COM";
   print("ktabcmd is "+ktabcmd)
   os.system(ktabcmd)

if environment == "UAT":
    ktabcmd = "kinit -kt /app/bbcx_cbs_appid/.keytab/bbcx_cbs_appid.keytab bbcx_cbs_appid@SCGLOBALUAT.ADUAT.SCOTIACAPITAL.COM";
    print("ktabcmd is "+ktabcmd)
    os.system(ktabcmd)
    filename=PY_PATH+"TablesTOCreate_UAT.txt"
    print("fileName is "+filename)

if environment == "PROD":
    ktabcmd = "kinit -kt /app/bbcx_cbs_appid/.keytab/bbcx_cbs_appid.keytab bbcx_cbs_appid@scglobal.ad.scotiacapital.com";
    print("ktabcmd is "+ktabcmd)
    os.system(ktabcmd)
    filename=PY_PATH+"TablesTOCreate_PROD.txt"
    print("fileName is "+filename)

if environment == "CAZ":
    ktabcmd = "kinit -kt /home/cliu5/userkey/cliu5.keytab cliu5@SCGLOBAL.AD.SCOTIACAPITAL.COM";
    print("ktabcmd is "+ktabcmd)
    logging.info("ktabcmd is "+ktabcmd)
    os.system(ktabcmd)

    replaceDBNamecmd = "find "+CURRENT_PATH+"/ETL/DDL -name \"*.sql\" -exec sed -i 's/crz_cust_scorecard/caz_cbs/ig' {} \;  2>&1"
    print("Command is " + replaceDBNamecmd)
    logging.info("Command is " + replaceDBNamecmd)
    os.system(replaceDBNamecmd)

    replacePathcmd =  "find "+ CURRENT_PATH + "/ETL/DDL -name '*.sql' -exec sed -i 's/\/data\/crz\/bbcx\//\/data\/caz\/cbs\//ig' {} \;  2>&1"
    print("Command is " + replacePathcmd)
    logging.info("Command is " + replacePathcmd)
    os.system(replacePathcmd)

    filename=PY_PATH+"TablesTOCreate_CAZ.txt"
    print("fileName is "+filename)
    logging.info("fileName is "+filename)

# jdbcMap = initJDBCURL()
conf=getConfig()
# beelineURL = jdbcMap["jdbc.hive."+environment.lower()]+ URL_APPEND
jdbcURL = conf.get("connection","JDBCConn")
print("jdbcURL:"+jdbcURL)

with open(filename, 'r') as myfile:
    for line in myfile:
      line = line.strip('\n')
      if not line.startswith('#'):
        createTable(line,jdbcURL)

print("create tables done")
