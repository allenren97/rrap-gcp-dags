#!/usr/bin/bash
#---------------------------------------------------------------------------------------------
#   FILE NAME:         deploy.sh
#   OWNER:             RAMS
#   DESCRIPTION:       This script will run the main script to trigger ETL job.
#
#   DEPENDENCIES:      None
#
#   PARAMETERS:        $1 - Environment Name specified in build.yml
#                      $2 - zip package name
#                      $3 - folder where the zip package file resides
#
#   MANUAL RUN:       ./deploy.sh <Environment Name> <release package.zip> /app/dev_bbcx_cbs_appid/deploy
#
#   OUTPUT:            /app/dev_bbcx_cbs_appid/deploy/cbs.log
#
#  ----------------------------------------------------------------------------------------------------------
#  Change History:
#  Date         Author          Description of Change
#  ---------    --------------  -----------------------------------------------------------------------------
#  2018-07-04    Sen Li        Initial creation
#############################################################################################################

#----------------------------------------------------
# Constants
#----------------------------------------------------
DEPLOY_FOLDER="deploy"
script_name=`basename $0`
#mailto:ERMS_RMAS@scotiabank.com
email_to="ERMS_RMAS@scotiabank.com"

#----------------------------------------------------
# This function decrypts the password
#----------------------------------------------------
Error_Check()
{
  rc=$?
  error_msg=$1
  if [[ "${rc}" -ne 0 ]]; then
    echo "Error: ${error_msg} - ${rc}"
    exit "$rc"
  fi
}

#----------------------------------------------------
# script starts
#----------------------------------------------------
if [ $# -ne 3 ]; then 
  echo "Missing Parameters $#" 
  exit 1
fi

echo "deploy.sh parameters: $1, $2, $3"
#----------------------------------------------------
# cd to the right environment folder
#----------------------------------------------------
JENKINS_ENV=$1
JENKINS_LINK="https://jenkins.agile.bns/job/rmascbs/job/rmas_cbs/job/DEV/job/rmascbs-rmas_cbs-deploy-dev/"
if [[ $JENKINS_ENV == "DEV" ]]; then
  cd "/app/dev_bbcx_cbs_appid/deploy"
  SOURCE_PROPERTIES="dev.properties"
elif [[ $JENKINS_ENV == "UAT" ]]; then
  SOURCE_PROPERTIES="uat.properties"
  cd "/app/bbcx_cbs_appid/deploy"
elif [[ $JENKINS_ENV == "PROD" ]]; then
  SOURCE_PROPERTIES="prod.properties"
  cd "/app/bbcx_cbs_appid/deploy"
elif [[ $JENKINS_ENV == "CAZ" ]]; then
  SOURCE_PROPERTIES="caz.properties"
else
  echo "Error: wrong environment $JENKINS_ENV"
  Exit 2
fi

#---------------------------------------------------
# assign parameters
#---------------------------------------------------
RELEASE_PACKAGE_NAME=$2
RELEASE_PACKAGE="${RELEASE_PACKAGE_NAME%.*}"
TEMP_FOLDER="/tmp/bak"
CURRENT_DIR=`pwd`
RELEASE_PACKAGE_DIR=$3
LOG_FILE="/tmp/deploy_cbs.log"
echo "temp package" ${TEMP_FOLDER}
echo "release package" ${RELEASE_PACKAGE_DIR}

script_name=`basename $0`
export job_id=${RELEASE_PACKAGE}_${script_name%.*}

echo "Current folder: $CURRENT_DIR"
#----------------------------------------------------
# create temp folder if does not exist.
#----------------------------------------------------
if [ ! -d "$TEMP_FOLDER" ]; then
  #Log "Deleting $TEMP_FOLDER"
  #rm -r -f $TEMP_FOLDER
  mkdir $TEMP_FOLDER
  Error_Check "Failed to delete: $TEMP_FOLDER"
fi

#-------------------------------------------------------------
# unzip the release package into temp folder
# identify the release folder name including version & build #
#-------------------------------------------------------------
if [[ ! -f "$RELEASE_PACKAGE_DIR/$RELEASE_PACKAGE_NAME" ]]; then
  echo "Error: Missing $RELEASE_PACKAGE_DIR/$RELEASE_PACKAGE_NAME"
  exit 2
fi

#-------------------------------------------------------------
# if the script is triggering from Jenkins thru sudo account
#   $RELEASE_PACKAGE_DIR will be /app/ciad_bbc3amli/deploy
# if it's trigger manually, it will be /app/bbc3amli/common/deploy
# only copy the zip file to /app/bbc3amli/common/deploy if the zip 
# package is from different folder
#-------------------------------------------------------------
if [[ "$RELEASE_PACKAGE_DIR" != "$CURRENT_DIR" ]]; then
  if [[ ! -f $CURRENT_DIR/$RELEASE_PACKAGE_NAME ]]; then
    echo "cp -f $RELEASE_PACKAGE_DIR/$RELEASE_PACKAGE_NAME ."
    cp -f $RELEASE_PACKAGE_DIR/$RELEASE_PACKAGE_NAME .
  fi
  Error_Check "Failed to copy: $RELEASE_PACKAGE_DIR/$RELEASE_PACKAGE_NAME ."
fi

#cp current script to temp back folder
if [ -d "$RELEASE_PACKAGE_DIR/" ]; then
  echo "backup current script to tmp folder"
  cp -rf $RELEASE_PACKAGE_DIR/* $TEMP_FOLDER
fi

#unzip release zip to deployment folder.
if [[ -f $RELEASE_PACKAGE_NAME ]]; then
  echo "unzip -d $RELEASE_PACKAGE_DIR $RELEASE_PACKAGE_NAME"
  unzip -d  $RELEASE_PACKAGE_DIR -o $RELEASE_PACKAGE_NAME >> $LOG_FILE 2>&1
  Error_Check "Failed to unzip: $RELEASE_PACKAGE_DIR/$RELEASE_PACKAGE_NAME"
  chmod +x $RELEASE_PACKAGE_DIR/ETL/Python/*.py
else
  echo "Error : $RELEASE_PACKAGE_NAME does not exists."
  exit 2
fi

#cp dev.properties, or caz.properties to app.properties.
cat $RELEASE_PACKAGE_DIR/ETL/Python/resources/$SOURCE_PROPERTIES > $RELEASE_PACKAGE_DIR/ETL/Python/resources/app.properties
echo "cat $RELEASE_PACKAGE_DIR/ETL/Python/resources/$SOURCE_PROPERTIES > $RELEASE_PACKAGE_DIR/ETL/Python/resources/app.properties"
#-----------------------------------------------------------------------
# call the main  script: test, TODO, this part need to be modified later
#------------------------------------------------------------------------
if [ -f "$RELEASE_PACKAGE_DIR/Devops/scripts/test.sh" ]; then
  echo "Run $RELEASE_PACKAGE_DIR/Devops/scripts/test.sh $1 $3"
  chmod +x $RELEASE_PACKAGE_DIR/Devops/scripts/test.sh
  #TODO change to another script name
  sh $RELEASE_PACKAGE_DIR/Devops/scripts/test.sh $1 $3 > $LOG_FILE 2>&1
  Error_Check "Failed to run $RELEASE_PACKAGE_DIR/Devops/scripts/test.sh"
else
  echo "Warning: Missing deploy_always.sh"
fi


#---------------------------------
# send deployment completion email
#---------------------------------
echo "Send deployment completion email"
email_subject="CBS $JENKINS_ENV deployment $release_package_version is completed.  Please verify."
if [[ $RUN_ENVIRONMENT == "IST" ]]; then
  email_body="$RUN_ENVIRONMENT deployment is completed for $release_package_version. $JENKINS_LINK \n  Deployed files are listed in the attached repackage_gitdiff_modifyfiles.txt.\n\nPlease verify your code.\nPlease update the sequence if required."
  echo -e "$email_body" | mailx -s "$email_subject" -a "$repackage_gitdiff_filelist" $email_to
else
  email_body="$RUN_ENVIRONMENT deployment is completed for cbs job $release_package_version. $JENKINS_LINK \n\nPlease proceed to the next step according to the implementation plan."
  echo -e "$email_body" | mailx -s "$email_subject"  -a "/tmp/deploy_cbs.log"  $email_to
fi

#------------
# script ends
#------------
echo "Deployment script finishes."
