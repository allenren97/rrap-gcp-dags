#!/usr/bin/bash
#---------------------------------------------------------------------------------------------
#   FILE NAME:         sudo_deploy.sh
#   OWNER:             DMS
#   DESCRIPTION:       This script unzip the release package and copy the latest
#                      deploy.sh to the application deploy folder and sudo to the
#                      application id for running the deploy.sh script
#                      this script is triggered by Jenkins , ./sudo_deploy.sh IST1 cbs.zip
#
#   DEPENDENCIES:      None
#
#   PARAMETERS:        $1 - Environment Name specified in build.yml
#                      $2 - zip package name
#
#   MANUAL RUN:       ./deploy.sh <Environment Name> <release package.zip>
#
#   OUTPUT:            in jenkins console log.
#
#  ----------------------------------------------------------------------------------------------------------
#  Change History:
#  Date         Author          Description of Change
#  ---------    --------------  -----------------------------------------------------------------------------
# 2018-06-22    Sen Li        Initial creation
#############################################################################################################

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
if [ $# -ne 2 ]; then 
  echo "Missing Parameters $#" 
  exit 1
fi

#------------------------------------------------------------
# identify path and application id based on Jenkins env. name
#------------------------------------------------------------
JENKINS_ENV=$1
if [[ $JENKINS_ENV == "DEV" ]]; then
	sudo_id=dev_bbcx_cbs_appid
	TARGET_DIR="/app/dev_bbcx_cbs_appid/deploy/"
elif [[ $JENKINS_ENV == "UAT" ]]; then
	sudo_id=bbcx_cbs_appid
	TARGET_DIR="/app/bbcx_cbs_appid/deploy/"
elif [[ $JENKINS_ENV == "PROD" ]]; then
	sudo_id=bbcx_cbs_appid
	TARGET_DIR="/app/bbcx_cbs_appid/deploy/"
elif [[ $JENKINS_ENV == "CAZ" ]]; then
#   TODO
	sudo_id=dev_bbcx_cbs_appid
	TARGET_DIR="/app/dev_bbcx_cbs_appid/deploy/"
else
	echo "Error: Unknown environment $JENKINS_ENV"
	exit 3
fi

#---------------------------------------------------
# assign parameters
#---------------------------------------------------
#from deploy.yml, $1 is the environment name, $2 is the package name.
RELEASE_PACKAGE_NAME=$2
RELEASE_PACKAGE="${RELEASE_PACKAGE_NAME%.*}"
# folder is "temp_cbs"
TEMP_FOLDER="temp_${RELEASE_PACKAGE}"
#target deployment folder, TODO may change baesd on DEV/UAT/PROD environment.
#TARGET_DIR="/app/dev_bbcx_cbs_appid/deploy/"
echo "TARGET_DIR is $TARGET_DIR"
echo "TEMP_FOLDER is $TEMP_FOLDER"
CURR=`pwd`
echo "current dir is $CURR"
#----------------------------------------------------
# remove the previous release package unzipped folder
#----------------------------------------------------
if [[ -d "$TEMP_FOLDER" ]]; then
  echo "Deleting $TEMP_FOLDER"
  rm -rf $TEMP_FOLDER
  Error_Check "Failed to delete: $TEMP_FOLDER"
fi

echo "unzip -d $TEMP_FOLDER $RELEASE_PACKAGE_NAME"
if [[ -f  $RELEASE_PACKAGE_NAME ]]; then
  unzip -d $TEMP_FOLDER $RELEASE_PACKAGE_NAME 
  Error_Check "Failed to unzip: $RELEASE_PACKAGE_NAME"
else
  echo "Error: $RELEASE_PACKAGE_NAME does not exists."
  exit 2
fi

#----------------------------------------------------------
# sudo command needs to be hardcoded fullpath.  
#   copy deploy.sh to $TARGET_DIR/deploy.sh so that
#   security team can update /etc/sudoers to have hardcoded 
#   path for copying and running deploy.sh
#----------------------------------------------------------
release_package_version=`ls $TEMP_FOLDER | grep $RELEASE_PACKAGE`
echo "Processing release package version: $release_package_version"

if [ ! -d "$TARGET_DIR" ]; then
  #Log "create $TARGET_DIR"
  echo "sudo -u $sudo_id /bin/mkdir -p $TARGET_DIR"
  sudo -u $sudo_id /bin/mkdir -p $TARGET_DIR
  Error_Check "Failed to create folder: $TARGET_DIR"
fi


#if [[ -f "$TARGET_DIR/deploy.sh" ]]; then
#    #should not remove?
#	echo "sudo -u $sudo_id rm -f $TARGET_DIR/deploy.sh"
#	sudo -u $sudo_id rm -f "$TARGET_DIR/deploy.sh"
#	Error_Check "Error: Failed to delete $TARGET_DIR/deploy.sh"
#fi

#------------------------------------------------------------------------
# use sudo command to copy the deploy.sh to the application deploy folder
#-----------------------------------------------------------------------"
echo "sudo -u $sudo_id /bin/rm -rf $TARGET_DIR*"
sudo -u $sudo_id /bin/rm -rf "$TARGET_DIR*"
echo "sudo -u $sudo_id /bin/cp -fp $CURR/$TEMP_FOLDER/deploy.sh $TARGET_DIR"
sudo -u $sudo_id /bin/cp -fp "$CURR/$TEMP_FOLDER/deploy.sh" "$TARGET_DIR"
Error_Check "Error: Failed to sudo to $sudo_id and run cp -f $TARGET_DIR/deploy.sh /app/$sudo_id/deploy"
#copy zip package to target folder.
echo "sudo -u $sudo_id /bin/cp -fp $RELEASE_PACKAGE_NAME $TARGET_DIR"
sudo -u $sudo_id /bin/cp -fp $RELEASE_PACKAGE_NAME "$TARGET_DIR"

#---------------------------------------------------------------
# use sudo command to run the deploy.sh using the application id
#---------------------------------------------------------------
echo "sudo -u $sudo_id /bin/chmod +x /app/$sudo_id/deploy/deploy.sh"
sudo -u $sudo_id /bin/chmod +x /app/$sudo_id/deploy/deploy.sh
echo "sudo -u $sudo_id /app/$sudo_id/deploy/deploy.sh $1 $2 $TARGET_DIR"
sudo -u $sudo_id /app/$sudo_id/deploy/deploy.sh $1 $2 $TARGET_DIR
Error_Check "Error: Failed to sudo to $sudo_id and run /app/$sudo_id/deploy/deploy.sh $TARGET_DIR"



#---------------------------------------------------------------------------
# Same set of files have been copied to the application common/deploy folder
#   Clean up after the deployment 
#---------------------------------------------------------------------------
if [[ -d "$TEMP_FOLDER" ]]; then
  echo "Cleaning up $TEMP_FOLDER"
  rm -rf $TEMP_FOLDER
  Error_Check "Failed to delete: $TEMP_FOLDER"
fi

if [[ -f "$RELEASE_PACKAGE_NAME" ]]; then
  echo "Cleaning up $RELEASE_PACKAGE_NAME"
  rm -f $RELEASE_PACKAGE_NAME
  Error_Check "Failed to delete: $RELEASE_PACKAGE_NAME"
fi

if [[ -f "./deploy.sh" ]]; then
  echo "Cleaning up deploy.sh"
  rm -f ./deploy.sh
  Error_Check "Failed to delete: deploy.sh"
fi