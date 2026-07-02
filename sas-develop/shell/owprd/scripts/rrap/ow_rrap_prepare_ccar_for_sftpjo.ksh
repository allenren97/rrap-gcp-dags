#!/bin/ksh
set -x
############################################################################
#
# OPERATIONAL INFORMATION
#
# Script Name  : ow_rrap_prepare_ccar_for_sftpjo.ksh
#                (base is ow_common_sftp.ksh)
#
# Function     : Prepare CCAR files for SFTPJO by renaming files as per trancodes given from SFTPJO.
#                                NOTE:- Trancodes : JOAACCA1 (for .csv ) and JOAACCA2 ( for .ctl) files
#
# Parameter    : $1 (Mandatory): Directory path of source (ccar) files
#                                Note- A "/" is not required at the end of directory path
#                example: ow_rrap_prepare_ccar_for_sftpjo.ksh $OWFTP/cmf/outgoing
#
# Author       : Nikhil Gaikwad
# Create Date  : Sept 28, 2021
############################################################################
# Modification History :
#
# Date        Description                                Modified By
# ----------  -----------------------------------------  ---------------
# 18-nov-2021 filter only DR*.csv and DR*.ctl files       Nikhil G
############################################################################

# Define the 'job_info' function for displaying job status
# information

. $SCRIPT_DIR/ow_job_info.ksh
export directory_path=$1
export jofiles_path=${directory_path}/jofiles
echo ${jofiles_path}

export SCRIPTNAME=$(basename ${0} .ksh)
export LOGFILE=$OW_HOME/logs/${SCRIPTNAME}.log
export RESULT=$OW_HOME/logs/${SCRIPTNAME}.result


# Clean up work area and log files

rm -f $LOGFILE 2>> $LOGFILE


######################################################################

f_usage() {
   msg=${1:-}
   print "Parameter missing :\n Usage:" | tee -a ${LOGFILE}
   print "$SCRIPTNAME.ksh ccar_folder_fullpath " | tee -a ${LOGFILE}
   print " " | tee -a ${LOGFILE}

   STATUS=99
   job_info $STATUS "$SCRIPTNAME.ksh Step $STEP abended ---"
   exit $STATUS
}

#
# when not enough parameters, display the usage
#
if [[ $# -lt 1 ]]
then
  f_usage
fi


######################################################################
#
# Step Name        : Step 000
# Step Description : Start the batch job
#
######################################################################

STEP="000"

job_info 0 "$SCRIPTNAME.ksh Step $STEP batch process started"


######################################################################
#
# Step 020
#
# Step Description: This step will copy ccar files to jofiles folder
#
######################################################################

STEP="020"

### Create "jofiles" directory to keep ccar files for JO. If present already, then clean it.

        #cd $OW_FTP/cmf/outgoing
        #cd ${directory_path}
        #mkdir -p jofiles1
        mkdir -p ${jofiles_path}
                   STATUS=$?
                   if [[ $STATUS -gt 0 ]]
                   then
                          job_info $STATUS "$SCRIPTNAME.ksh Step $STEP File put abended ---"
                          print "Make directory failed : ${jofiles_path} " | tee -a ${LOGFILE}
                          exit $STATUS
                   fi

        print "Contents of ${jofiles_path} Before cleanup:" | tee -a ${LOGFILE}
        ls -lrt ${jofiles_path} | tee -a ${LOGFILE}
        print " " | tee -a ${LOGFILE}
        rm -rf ${jofiles_path}/*

### Read every DR*.CSV filename and copy to "jofiles" directory by changing to correct trancode
        #ls *.csv|while read FILE
        ls ${directory_path}/DR*.csv|while read FILE
        do
                NEWFILE=$(basename "$FILE" .csv).JOAACCA1
                echo FILE": " $FILE " "NEWFILE: " " $NEWFILE
                #cp $FILE "jofiles1/"$NEWFILE
                cp $FILE ${jofiles_path}/$NEWFILE
                   STATUS=$?
                   if [[ $STATUS -gt 0 ]]
                   then
                          job_info $STATUS "$SCRIPTNAME.ksh Step $STEP File put abended ---"
                          print "Copy failed : File=$FILE : NEWFILE=${jofiles_path}/$NEWFILE " | tee -a ${LOGFILE}
                          exit $STATUS
                   fi
                sleep 1
        done

### Read every DR*.CTL filename and copy to "jofiles" directory by changing to correct trancode
        #ls *.ctl|while read FILE
        ls ${directory_path}/DR*.ctl|while read FILE
        do
                NEWFILE=$(basename "$FILE" .ctl).JOAACCA2
                echo FILE": " $FILE " "NEWFILE: " " $NEWFILE
                #cp $FILE "jofiles1/"$NEWFILE
                cp $FILE ${jofiles_path}/$NEWFILE
                   STATUS=$?
                   if [[ $STATUS -gt 0 ]]
                   then
                          job_info $STATUS "$SCRIPTNAME.ksh Step $STEP File put abended ---"
                          print "Copy failed : File=$FILE : NEWFILE=${jofiles_path}/$NEWFILE " | tee -a ${LOGFILE}
                          exit $STATUS
                   fi
                sleep 1
        done

        print "Contents of ${jofiles_path} After copying:" | tee -a ${LOGFILE}
        ls -lrt ${jofiles_path} | tee -a ${LOGFILE}
        print " " | tee -a ${LOGFILE}

        job_info 0 "${SCRIPTNAME}.ksh Step ${STEP} ended normally"

######################################################################
#
# Step Name        : Step 999
# Step Description : End of the batch job
#
######################################################################

STEP="999"

   job_info 0 "$SCRIPTNAME.ksh Step $STEP ended normally"

