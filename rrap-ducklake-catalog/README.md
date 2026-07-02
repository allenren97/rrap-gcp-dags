# DuckLake Metastore

## Description

A repository of schema and table definitions, along with their partitioning schemes, for the RRAP rebuild.

### Use

Any newly defined tables should be included as SQL definitions that can be run in DuckDB connected to the DuckLake metastore backed by PostgreSQL.

### Connectors

A provider package in Apache Airflow or Python library to check for changes and update any metadata changes.

