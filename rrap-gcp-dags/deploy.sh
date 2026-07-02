#!/bin/bash
set -x
SERVER_NAME=$1
ARTIFACT=$2
DEPLOY_LOCATION=$3

USAGE="deploy.sh <SERVER_NAME[ex. DEV1]> <ARTIFACT[ex. rrap_sas_pipeline-1.0.35.tar.gz> <DEPLOY_LOCATION[ex. /u/rrapmso/.deploy]>\nConfirm scriptCommand in deploy.yml hasn't been changed and deployLocation exists."

APP_LOCATION=/bns/rrap/apps/rebuild-airflow
APP_LOG=/bns/rrap/apps/rebuild-airflow/logs

if [ "$#" -ne 3 ]; then
  echo "Invalid length of arguments.."
  exit 1
fi

if [ ! -d $APP_LOCATION ]; then
  echo "$APP_LOCATION doesn't exist.. attempting to create it.."
  mkdir -m 755 -p $APP_LOCATION
fi

# Verify artifact exist, then unpack and copy into project deployment dir
if [ -f $ARTIFACT ]; then

  echo "Cleanup existing application code.."

  DIRECTORIES=( "dags" "bin" "conf" "functions" )
  for DIR in ${DIRECTORIES[@]}; do
    if [ -d $APP_LOCATION/$DIR ]; then
      rm -rf $APP_LOCATION/$DIR/*
    else
      mkdir -m 755 $APP_LOCATION/$DIR
    fi
  done

  echo "$ARTIFACT exists.. proceeding with unpacking.."

  unzip -o $ARTIFACT

  echo "$ARTIFACT unpacked.. moving code to application location.."

  for DIR in ${DIRECTORIES[@]}; do
    cp -r $DEPLOY_LOCATION/$DIR/* $APP_LOCATION/$DIR/
    rm -rf $DEPLOY_LOCATION/$DIR
  done

  echo "Modify email list in dags folder (TSYS for non-prod, EDW for prod).."

  if [ "$SERVER_NAME" == "PROD" ] || [ "$SERVER_NAME" == "BCP" ]; then
    sed -i 's|{EMAIL_DISTRIBUTION}|TSYS-RRAP@scotiabank.com|g' $APP_LOCATION/dags/*.py
  else
    sed -i 's|{EMAIL_DISTRIBUTION}|TSYS-RRAP@scotiabank.com|g' $APP_LOCATION/dags/*.py
  fi

  echo "Application code moved.. setting correct permissions.."

  chmod -R 755 $APP_LOCATION/${ARTIFACT%.*}
  mkdir -m 775 -p $APP_LOG

  echo "Cleaning up deployment location.."

  rm -f $DEPLOY_LOCATION/$ARTIFACT
  rm -rf $DEPLOY_LOCATION/${ARTIFACT%%-*}*

  echo "Deployment complete.."

else
  echo "Artifact: $ARTIFACT - doesn't exist.. exiting.."
  exit 1
fi

exit 0

