from main import historyRecord, clearUpTemp, CleanupBeforeRunning, HIVEInitialize, hiveExecutePure
import os, logging, sys
from hive_task import CBS_Configuration
import argparse
import datetime,time


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



# get direct child id for one job
def getDependenciesSingle(jobName):
    getDependenciesSql = "select d.job_nm from " + cf.CBSDBName + "." + cf.TblJobDepInfo + " d where d.parent_job_nm = '" + jobName.rstrip('\n') + "'"
    return hiveExecutePure(getDependenciesSql)

# get all child job ids for root job id
def getDependenciesRecursive(jobName):
    dependencies = getDependenciesSingle(jobName)
    result = dependencies
    for job in dependencies:
        childs = getDependenciesRecursive(job)
        # make sure there is no redundancy
        for childName in childs:
            if not childName in result:
                result.append(childName)
    return result
    
# get all child job names for root job id
def getDependencyNamesRecursive(rootJobName):
    dependencies = getDependenciesRecursive(rootJobName)
    logger.debug("All the dependencies is: \n")
    logger.debug(dependencies)
    return dependencies


def main():
    
    CleanupBeforeRunning()
        
    HIVEInitialize()

    with open(historyRecord, 'r') as f:
        x = f.readlines()
    dependencyNames = getDependencyNamesRecursive(args.jobname)
    allTaskSignature = []
    allTaskSignature.append(args.jobname + ' ' + args.bdate + ' ' + args.datetype.upper())
    for dependencyName in dependencyNames:
        currentTaskSignature = dependencyName.rstrip('\n') + ' ' + args.bdate + ' ' + args.datetype.upper()
        logger.debug("allTaskSignature is appending: ["+currentTaskSignature+"]")
        allTaskSignature.append(currentTaskSignature)
    f_temp = open(f.name+"_temp","w+")
    found = False
    for line in x:
        pureLine = line.replace('\n','').replace('\r','')
        if (not pureLine in allTaskSignature):
            logger.debug("Checking: ["+pureLine+"]")
            f_temp.write(line)
            if not found: 
                found = True
    f_temp.close()
    clearUpTemp(f, f_temp)
    if (not found):
        raise ValueError('Job: ' + currentTaskSignature + ' does not correspond to any record in the history. Please make sure the provided information is correct.')


# def main():
#     with open(historyRecord, 'r') as f:
#         x = f.readlines()
#     currentTaskSignature = args.jobname + ' ' + BusinessDate + ' ' + args.datetype.upper()
#     logger.debug("Job: " + args.jobname + "; Date: " + BusinessDate + "; Datetype: " + args.datetype)
#     f_temp = open(f.name+"_temp","w+")
#     found = False
#     for l in range(len(x)):
#         currentLine = x[l].rstrip('\r\n')
#         logger.debug (currentLine)
#         if (currentTaskSignature == currentLine):
#             found = True
#         if (len(currentLine)>3):
#             recordDate = currentLine.split(' ')[1]
#             if (not found) or (BusinessDate <> recordDate):
#                 f_temp.write(currentLine + '\n')
#     f_temp.close()
#     clearUpTemp(f, f_temp)
#     if (not found):
#         raise ValueError('Job: ' + currentTaskSignature + ' does not correspond to any record in the history. Please make sure the provided information is correct.')

        

if __name__ == '__main__':
    main()
