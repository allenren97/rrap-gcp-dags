#!/usr/bin/bash
#---------------------------------------------------------------------------------------------
#   FILE NAME:         deploy.sh
#   OWNER:             DMS
#   DESCRIPTION:       This script unzip the release package and copy the latest
#                      deploy.sh to the application deploy folder and sudo to the
#                      application id for running the deploy.sh script
#
#   DEPENDENCIES:      None
#
#   PARAMETERS:        $1 - Environment Name specified in build.yml
#                      $2 - zip package name
#
#   MANUAL RUN:       ./deploy.sh <Environment Name> <release package.zip>
#
#   OUTPUT:            NONE
#
#  ----------------------------------------------------------------------------------------------------------
#  Change History:
#  Date         Author          Description of Change
#  ---------    --------------  -----------------------------------------------------------------------------
# 2018-12-27    Henry Iek       Initial creation
#############################################################################################################

# ---------------------------------------------------
# Generic functions
# ---------------------------------------------------
Log()
{
	Msg=$1
	tmsp=`date +%Y%m%d_%H%M%S`
	# printf "${job_id} ${tmsp} ${Msg}\n" >> "${LOG_FILE}"
  echo "${job_id} ${tmsp} ${Msg}" >> "${LOG_FILE}"
}

#----------------------------------------------------
# This function decrypts the password
#----------------------------------------------------
Error_Check()
{
  rc=$?
  error_msg=$1
  if [[ "${rc}" -ne 0 ]]; then
    echo "Error: ${error_msg} - ${rc}"
    exit "${rc}"
  fi
}

#----------------------------------------------------
# script starts
#----------------------------------------------------
if [ $# -ne 2 ]; then 
  echo "Missing Parameters $#" 
  exit 1
fi

#----------------------------------------------------
# Constants
#----------------------------------------------------
REPACKAGE_FOLDER="repackage"
DEPLOY_FOLDER="src"
MODIFIED_FILELIST="gitdiff_modifyfiles.txt"
BACKOUT_TRIGGER_FILE="datastage_backout.trg"
script_name=`basename $0`
#email_to="ZhiYong.Sun@scotiabank.com"

#---------------------------------------------------
# assign parameters
#---------------------------------------------------
JENKINS_ENV=$1
RELEASE_PACKAGE_NAME=$2
RELEASE_PACKAGE="${RELEASE_PACKAGE_NAME%.*}"
TEMP_FOLDER="temp_${RELEASE_PACKAGE}"
ARCHIVE_DIR="archive"
LOG_DIR="log"
CURRENT_DIR=`pwd`
cur_datetime=`date +%Y%m%d_%H%M%S`
LOG_FILE="${CURRENT_DIR}/${LOG_DIR}/${RELEASE_PACKAGE}_${cur_datetime}.log"

#----------------------------------------------------
# create log folder
#----------------------------------------------------
if [[ ! -d "$CURRENT_DIR/$LOG_DIR" ]]; then
  echo "Creating $CURRENT_DIR/$LOG_DIR"
  mkdir "$CURRENT_DIR/$LOG_DIR"
fi

#----------------------------------------------------
# create archive folder
#----------------------------------------------------
# if [[ ! -d "$CURRENT_DIR/$ARCHIVE_DIR" ]]; then
#   echo "Creating $CURRENT_DIR/$ARCHIVE_DIR"
#   mkdir "$CURRENT_DIR/$ARCHIVE_DIR"
# fi

#----------------------------------------------------
# remove the previous release package unzipped folder
#----------------------------------------------------
if [[ -d "$TEMP_FOLDER" ]]; then
  echo "Deleting $TEMP_FOLDER"
  rm -rf $TEMP_FOLDER
  Error_Check "Failed to delete: $TEMP_FOLDER"
fi

echo "tar -xvf $RELEASE_PACKAGE_NAME -C $TEMP_FOLDER"
if [[ -f  $RELEASE_PACKAGE_NAME ]]; then
  mkdir $TEMP_FOLDER
  tar -xvf $RELEASE_PACKAGE_NAME -C $TEMP_FOLDER
  Error_Check "Failed to unzip: $RELEASE_PACKAGE_NAME"
else
  echo "Error: $RELEASE_PACKAGE_NAME does not exists."
  exit 2
fi

#----------------------------------------------------------
# sudo command needs to be hardcoded fullpath.  
#   copy deploy.sh to $CURRENT_DIR/deploy.sh so that 
#   security team can update /etc/sudoers to have hardcoded 
#   path for copying and running deploy.sh
#----------------------------------------------------------
release_package_version=`ls $TEMP_FOLDER | grep $RELEASE_PACKAGE`
echo "Processing release package version: $release_package_version"

# ----------------------------------------------
# deploy etl_environment.sh
# ----------------------------------------------
if [[ -f $TEMP_FOLDER/$release_package_version/conf/etl_environment_DEV.sh ]] && [[ $JENKINS_ENV == "DEV1" ]]; then  
  cp -f "./$TEMP_FOLDER/$release_package_version/conf/etl_environment_DEV.sh" "./$TEMP_FOLDER/etl_environment.sh"
elif [[ -f $TEMP_FOLDER/$release_package_version/conf/etl_environment_UAT.sh ]] && [[ $JENKINS_ENV == "QAT1" ]]; then
  cp -f "./$TEMP_FOLDER/$release_package_version/conf/etl_environment_UAT.sh" "./$TEMP_FOLDER/etl_environment.sh"
elif [[ -f $TEMP_FOLDER/$release_package_version/conf/etl_environment_PROD.sh ]] && [[ $JENKINS_ENV == "PROD1" ]]; then
  cp -f "./$TEMP_FOLDER/$release_package_version/conf/etl_environment_PROD.sh" "./$TEMP_FOLDER/etl_environment.sh"
else
  echo "Warning: etl_environment.sh is not deployed for $JENKINS_ENV"
fi
Error_Check "Error: Failed to copy etl_environment.sh"

. "$CURRENT_DIR/$TEMP_FOLDER/etl_environment.sh"
Error_Check "Environment file not found"

# ----------------------------------------------
# This is to rollback the change
#   - The backout.zip is defined in pom.xml - GIT ARCHIVE
#   - Move the GIT tag "GoldCopy" to change the set of files for backout.zip
#   - To trigger rollback, we need IPM team to create the trigger file manually in "deployLocation" of deploy.yml. Then redeploy the last package.
# ----------------------------------------------
if [[ -f $BACKOUT_TRIGGER_FILE ]]; then
  echo "Enter Backout mode"
  mkdir ./$TEMP_FOLDER/$release_package_version/backout
  
  echo "Unzip Backout"
  tar -xvf ./$TEMP_FOLDER/$release_package_version/backout.tar -C ./$TEMP_FOLDER/$release_package_version/backout
  
  echo "Replacing delta files by Backout files so that Backout files will be deployed"
  cp -rf ./$TEMP_FOLDER/$release_package_version/backout/src/* ./$TEMP_FOLDER/$release_package_version/src/
  
  rm -f $BACKOUT_TRIGGER_FILE
fi

# #--------------------------------------------------------------------------------------
# # after etl_environment.sh run, move file components_runjobs_lookup.txt to temp folder 
# #--------------------------------------------------------------------------------------
# mv components_runjobs_lookup.txt $TEMP_FOLDER


# #----------------------------------------------
# # deploy components_lookup.txt
# #----------------------------------------------
# if [[ -f $TEMP_FOLDER/$release_package_version/conf/components_lookup.txt ]]; then  
#   cp -f "./$TEMP_FOLDER/$release_package_version/conf/components_lookup.txt" "./$TEMP_FOLDER"
# else
#   echo "Warning: components_lookup.txt is not deployed for $JENKINS_ENV"
# fi
# Error_Check "Error: Failed to copy components_lookup.txt"

# #----------------------------------------------
# # deploy request_job_run_list.txt
# #----------------------------------------------
# if [[ -f $TEMP_FOLDER/$release_package_version/conf/request_job_run_list.txt ]]; then  
#   cp -f "./$TEMP_FOLDER/$release_package_version/conf/request_job_run_list.txt" "./$TEMP_FOLDER"
# else
#   echo "Warning: request_job_run_list.txt is not deployed for $JENKINS_ENV"
# fi
# Error_Check "Error: Failed to copy request_job_run_list.txt"


Log "Current folder: $CURRENT_DIR"
#----------------------------------------------------------------------------------
# create the Release Repackage folder with only updated files based on the modified filelist 
# the modified filelist currently be generated by manual, 
# in future it will be generated thru Maven plug-in (defined in pom.xml) calling git diff command
#----------------------------------------------------------------------------------
if [[ ! -d $REPACKAGE_FOLDER ]]; then
  Log "Create folder: $REPACKAGE_FOLDER"
  mkdir $REPACKAGE_FOLDER
  Error_Check "Failed to create folder: $REPACKAGE_FOLDER"
fi

#---------------------------------------------------------------------------
# check if release_package_version folder already exists.  
#   If so, Log warning as this should not be the case
#---------------------------------------------------------------------------
if [ -d "$REPACKAGE_FOLDER/$release_package_version" ]; then
  Log "Warning : $REPACKAGE_FOLDER/$release_package_version already exists."
  Log "Deleting folder $REPACKAGE_FOLDER/$release_package_version."
  rm -r -f $REPACKAGE_FOLDER/$release_package_version
  Error_Check "Failed to delete: $REPACKAGE_FOLDER/$release_package_version"
fi

gitdiff_filelist=$TEMP_FOLDER/$release_package_version/$MODIFIED_FILELIST
repackage_gitdiff_filelist=$TEMP_FOLDER/repackage_$MODIFIED_FILELIST

if [[ -f "$repackage_gitdiff_filelist" ]]; then
  rm -f "$repackage_gitdiff_filelist"
  Error_Check "Failed to remove $repackage_gitdiff_filelist" 
fi

# if [[ -f "$gitdiff_filelist" ]]; then
#   cat "$gitdiff_filelist" | while read LINE
#   do
#     release_file="$TEMP_FOLDER/$release_package_version/$LINE"
#     Log "check $release_file."
#     if [[ -f  "$release_file" ]]; then           
# 		line_prefix=`echo $LINE | cut -d '/' -f1`
# 		[[ $DEPLOY_MAIN_FOLDER_LIST =~ (^|/)$line_prefix(/|$) ]] && echo $LINE >> $repackage_gitdiff_filelist || Log "Ignore file: $LINE"
#     else
#       if [[ "$release_file" != "$TEMP_FOLDER/$release_package_version/.gitattributes" ]]; then
#         Log "Warning : File not found: $release_file could have been deleted." 
#       fi
#     fi  
#   done < "$gitdiff_filelist"
#   #--------------------------------------------------------------------------
#   # gitdiff_modifyfiles.txt is listed in the gitdiff_modifyfiles.txt file
#   #   thus, we need to add manually so file is sync into the repackage folder 
#   #--------------------------------------------------------------------------
#   #echo "$MODIFIED_FILELIST" >> $repackage_gitdiff_filelist
# else
#   Log "Error : $gitdiff_filelist does not exists."
#   exit 3
# fi

#-----------------------------------------------------------
# sync release folder with release repackage folder
#-----------------------------------------------------------
Log "Sync to repackage directory $REPACKAGE_FOLDER/$release_package_version"
cp "$gitdiff_filelist" "$repackage_gitdiff_filelist"
mkdir "$REPACKAGE_FOLDER/$release_package_version"
# rsync -av --files-from="$repackage_gitdiff_filelist" "$TEMP_FOLDER/$release_package_version" "$REPACKAGE_FOLDER/$release_package_version" >>$LOG_FILE 2>&1 
# rsync -av "$TEMP_FOLDER/$release_package_version/conf" "$REPACKAGE_FOLDER/$release_package_version" >>$LOG_FILE 2>&1 
while read changed_file; do
    changed_dir=`dirname ${changed_file}`
    mkdir -p "$REPACKAGE_FOLDER/$release_package_version/$changed_dir"
    cp -f $TEMP_FOLDER/$release_package_version/$changed_file $REPACKAGE_FOLDER/$release_package_version/$changed_dir
    echo cp -f "$TEMP_FOLDER/$release_package_version/$changed_file" "$REPACKAGE_FOLDER/$release_package_version/$changed_dir"
done < $repackage_gitdiff_filelist
#Error_Check "Failed to rsync $repackage_gitdiff_filelist $TEMP_FOLDER/$release_package_version $REPACKAGE_FOLDER/$release_package_version"

#----------------------------------------------
# deploy SAS Jobs
#----------------------------------------------
if [ -d "$REPACKAGE_FOLDER/$release_package_version" ]; then
	Log "Start deploy SAS package in $REPACKAGE_FOLDER/$release_package_version"

  cp -rf $REPACKAGE_FOLDER/$release_package_version/src/* $TARGET_DIR
  # find "$TARGET_DIR/$release_package_version" -type f | xargs -n 1 dos2unix

  Log "Reassign applicatoin id permission"

  # Log "chmod -R 775 $TARGET_DIR/$release_package_version"
  # chmod -R 775 "$TARGET_DIR/$release_package_version"

  # Log "chown -R $APP_USER:$APP_GROUP $TARGET_DIR/$release_package_version"
  # chown -R $APP_USER:$APP_GROUP "$TARGET_DIR/$release_package_version"

  # Log "Rebuild symbolic link"
  # if [[ $JENKINS_ENV == "DEV1" ]]; then  
  #   echo rm "$TARGET_DIR/airb_recon"
  #   echo ln -s "$TARGET_DIR/$release_package_version" "$TARGET_DIR/airb_recon"
  # elif [[ $JENKINS_ENV == "QAT1" ]]; then
  #   rm "$TARGET_DIR/airb_recon"
  #   ln -s "$TARGET_DIR/$release_package_version" "$TARGET_DIR/airb_recon"
  # elif [[ $JENKINS_ENV == "PROD1" ]]; then
  #   rm "$TARGET_DIR/airb_recon"
  #   ln -s "$TARGET_DIR/$release_package_version" "$TARGET_DIR/airb_recon"
  # else
  #   echo "Error: Environment $JENKINS_ENV is not recognized. Symbolic link is not rebuilt"
  # fi

	Log "Finished SAS package deployment in $release_package_version"
else
	Log "No SAS package needs to be deployed"
fi
cd ${CURRENT_DIR}

# #---------------------------------------------------------
# # archive the deployment package
# #---------------------------------------------------------
# RELEASE_PACKAGE_EXTENSION="${RELEASE_PACKAGE_NAME##*.}"
# mv ${RELEASE_PACKAGE_NAME} ${ARCHIVE_DIR}/${release_package_version}_${cur_datetime}.${RELEASE_PACKAGE_EXTENSION}
# Log "deployment package ${RELEASE_PACKAGE_NAME} has been archived to ${ARCHIVE_DIR}/${release_package_version}_${cur_datetime}.${RELEASE_PACKAGE_EXTENSION}!!!"
rm -rf "${REPACKAGE_FOLDER}"
rm -rf "${TEMP_FOLDER}"
rm "$RELEASE_PACKAGE_NAME"

#------------
# script ends
#------------
Log "Deployment finishes."
echo "Deployment finishes."