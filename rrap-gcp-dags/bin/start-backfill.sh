#!/usr/bin/env bash

help()
{
   # Display Help
   echo "Wrapper script for backfill-runs.py, gets required arguments and generates API token"
   echo
   echo "Syntax: start-backfill.sh [-H|f|t|d|f|h]"
   echo "Example: start-backfill.sh -H \"http://localhost:8080\" -f \"2025-01-28\" -t \"2025-12-02\" -d \"features\" -g \"derived__amort\""
   echo "options:"
   echo "H     HOST for API calls ex. https://localhost:8080"
   echo "f     from_date for backfill ex. \"2025-01-28\""
   echo "t     to_date for backfill ex. \"2025-12-02\""
   echo "d     dag_id for backfill ex. \"features\""
   echo "g     Optional: taskgroup to clear ex. \"derived__amort\""
   echo "o     Optional: clear with only_failed enabled ex. \"true\""
   echo "h     Print help."
   echo
}

HOST=""
FROM=""
TO=""
DAG=""
GROUP=""
ONLY_FAILED=""

while getopts ":H:n:f:t:d:g:o:h" option; do
  case $option in
    H) HOST=$OPTARG;;
    f) FROM=$OPTARG;;
    t) TO=$OPTARG;;
    d) DAG=$OPTARG;;
    g) GROUP=$OPTARG;;
    o) ONLY_FAILED=$OPTARG;;
    h)
      help
      exit;;
    \?)
      echo "Error: Invalid option"
      exit;;
  esac
done

if [[ $HOST == "" ]] || [[ $FROM == "" ]] || [[ $TO == "" ]] || [[ $DAG == "" ]]; then
  echo "A required argument was empty.."
  echo "Arguments: HOST - $HOST, FROM - $FROM, TO - $TO, DAG - $DAG"
  exit 1
fi

read -p "User: " USER
read -s -p "Password: " PWD

TOKEN=`curl -X 'POST' "$HOST/auth/token"  -H 'Content-Type: application/json' -d '{"username": "'"$USER"'", "password": "'"$PWD"'"}'`

if [[ $TOKEN =~ "Invalid" ]]; then
  echo "Invalid credentials.. user/password input incorrectly"
  exit 1
fi

if [[ `ps -ef | grep backfill-runs | wc -l` -gt 1 ]]; then
  echo "backfill-runs.py is currently running.. wait until other persons run is complete.."
  echo "You can check with the command: ps -ef |grep backfill-runs"
  exit 1
fi

if [[ $GROUP == "" ]]; then
	python3 /bns/rrap/apps/rebuild-airflow/bin/backfill-runs.py --from-date "$FROM" --to-date "$TO" --dag-id "$DAG" --host "$HOST" --api-token "$TOKEN" --only-failed "$ONLY_FAILED"
else
    python3 /bns/rrap/apps/rebuild-airflow/bin/backfill-runs.py --from-date "$FROM" --to-date "$TO" --dag-id "$DAG" --host "$HOST" --api-token "$TOKEN" --taskgroup "$GROUP" --only-failed "$ONLY_FAILED"
fi

