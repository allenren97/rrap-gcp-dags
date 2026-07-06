#main script that will trigger DDL later.
# this script should be called from path: /app/dev_bbcx_cbs_appid/deploy
import logging
import datetime
import os
import sys
logging.basicConfig(filename='cbs.log',level=logging.DEBUG)
#argv[0] is './ETL/Python/initial_ddl.py'
logging.info('main script is called at:'+ str(datetime.datetime.now())+ " with "+sys.argv[1])
print("main script is called at:"+str(datetime.datetime.now())+ " with "+sys.argv[1])

# the shell that trigger this py is in /app/dev_bbcx_cbs_appid/deploy
# so the path should be ./ETL/Python/
os.system("python ./ETL/Python/createTable.py "+ sys.argv[1]  + " " + sys.argv[2] + " > run_ddl.log")
logging.info('create table is called at:'+ str(datetime.datetime.now()))
