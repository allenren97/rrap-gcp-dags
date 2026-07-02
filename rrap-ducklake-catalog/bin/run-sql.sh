#!/usr/bin/env bash

# Usage: run-sql.sh /path/to/file.sql 
USAGE="run-sql.sh /path/to/file.sql"

if [[ $# -ne 1 ]]; then
  echo "Incorrect number of arguments.."
  exit 1
fi

if [[ ! -f $1 ]]; then
  echo "First argument is not a file that exists.."
  exit 1
fi

SQLFILE=$1

# Source .duckdbrc to setup DuckLake catalog
# Runs a SQL file expecting CREATE IF NOT EXISTS and/or ALTER statements to the DuckLake catalog
ssh rraprun@localhost << EOF
set -x && duckdb < ${SQLFILE}
EOF

echo $!

