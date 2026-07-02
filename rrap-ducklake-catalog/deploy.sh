#!/bin/bash
set -x
SERVER_NAME=$1
ARTIFACT=$2
DEPLOY_LOCATION=$3

USAGE="deploy.sh <SERVER_NAME[ex. DEV1]> <ARTIFACT[ex. rrap_sas_pipeline-1.0.35.tar.gz> <DEPLOY_LOCATION[ex. /u/rrapmso/.deploy]>\nConfirm scriptCommand in deploy.yml hasn't been changed and deployLocation exists."

APP_LOCATION=/bns/rrap/apps/rrap-ducklake-catalog
APP_LOG=/bns/rrap/apps/rrap-ducklake-catalog/logs
RUN_SQL_SCRIPT="/bns/rrap/apps/rrap-ducklake-catalog/bin/run-sql.sh"

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

  echo "$ARTIFACT exists.. proceeding with unpacking.."

  unzip -o $ARTIFACT

  echo "$ARTIFACT unpacked.. moving new application code to app location.. setting permissions.."

  echo "Update specifically bin folder before other directories.."

  if [[ ! -d $APP_LOCATION/bin ]]; then
    mkdir -m 755 -p $APP_LOCATION/bin
  fi
  cp $DEPLOY_LOCATION/bin/* $APP_LOCATION/bin
  chmod -R 755 $APP_LOCATION/bin

  DEPLOY_SCHEMAS="$DEPLOY_LOCATION/schemas"
  APP_SCHEMAS="$APP_LOCATION/schemas"

  # First handle schema-definitions at the top-level
  for FILE in $DEPLOY_SCHEMAS/*.sql; do
    RELATIVE_PATH="${FILE#$DEPLOY_SCHEMAS/}"
    DEPLOYED_FILE="$APP_SCHEMAS/$RELATIVE_PATH"

    if [ ! -f "$DEPLOYED_FILE" ]; then
      mkdir -m 755 -p "$(dirname "$DEPLOYED_FILE")"
      cp "$FILE" "$DEPLOYED_FILE"
      chmod 755 "$DEPLOYED_FILE"
      "$RUN_SQL_SCRIPT" "$DEPLOYED_FILE"
    else
      if ! diff "$FILE" "$DEPLOYED_FILE" > /dev/null; then
        cp "$FILE" "$DEPLOYED_FILE"
        chmod 755 "$DEPLOYED_FILE"
        "$RUN_SQL_SCRIPT" "$DEPLOYED_FILE"
      fi
    fi
  done

  # Then traverse all sub-directories and apply table-definitions
  find "$DEPLOY_SCHEMAS" -type f -name '*.sql' | sort -d | while read -r FILE; do
    RELATIVE_PATH="${FILE#$DEPLOY_SCHEMAS/}"
    DEPLOYED_FILE="$APP_SCHEMAS/$RELATIVE_PATH"

    if [ ! -f "$DEPLOYED_FILE" ]; then
      mkdir -m 755 -p "$(dirname "$DEPLOYED_FILE")"
      cp "$FILE" "$DEPLOYED_FILE"
      chmod 755 "$DEPLOYED_FILE"
      "$RUN_SQL_SCRIPT" "$DEPLOYED_FILE"
    else
      if ! diff "$FILE" "$DEPLOYED_FILE" > /dev/null; then
        cp "$FILE" "$DEPLOYED_FILE"
        chmod 755 "$DEPLOYED_FILE"
        "$RUN_SQL_SCRIPT" "$DEPLOYED_FILE"
      fi
    fi
  done

  echo "Application code moved.. setting correct permissions.."

  mkdir -m 775 -p $APP_LOG

  echo "Cleaning up deployment location.."

  rm -f $DEPLOY_LOCATION/$ARTIFACT
  rm -rf $DEPLOY_LOCATION/${ARTIFACT%%-*}*
  rm -rf $DEPLOY_LOCATION/*

  echo "Deployment complete.."

else
  echo "Artifact: $ARTIFACT - doesn't exist.. exiting.."
  exit 1
fi

exit 0

