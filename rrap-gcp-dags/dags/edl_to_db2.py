from airflow.sdk import dag, get_current_context, task, task_group
from bns.rrap.operators.beeline import BeelineParquetExportOperator
from bns.rrap.hooks.duckdb import DuckLakeHook

import configparser
import json
import logging
import pendulum


CONF = '/bns/rrap/apps/rebuild-airflow/conf/edl_to_db2.ini'


@dag(
    dag_id="edl_to_db2",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
)
def edl_to_db2():

    @task()
    def handle_month_context():
        context = get_current_context()
        rundate = (
            context["logical_date"]
            .subtract(months=1)
            .end_of("month")
            .strftime("%Y-%m-%d")
        )
        hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")

        mth_tm_id = hook.duckdb.sql(f"""
            SELECT TM_ID FROM ingestion.TM_DIM
            WHERE TM_LVL = 'Month' AND TM_LVL_END_DT = '{rundate}' 
        """).fetchone()[0]
        logging.warning(f"Rundate: {rundate}, MTH_TM_ID: {mth_tm_id}")

        context["ti"].xcom_push(key="mth_tm_id", value=mth_tm_id)
        context["ti"].xcom_push(key="prev_mth_tm_id", value=mth_tm_id - 40)
        context["ti"].xcom_push(key="rundate", value=rundate)
        context["ti"].xcom_push(key="popn_dt", value=context["logical_date"].strftime("%Y-%m-15"))
    
    @task
    def run(conf=CONF):
        config = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation()
        )
        config.read(CONF)

        for k in config:
            if k == 'DEFAULT':
                continue
            
            schema_name = config[k]['schema_name']
            table_name = config[k]['table_name']
            
            mth_end_dt = config[k]['mth_end_dt'] if 'mth_end_dt' in config[k] else None
            popn_dt = config[k]['popn_dt'] if 'popn_dt' in config[k] else None

            sql = rf"SELECT * FROM {schema_name}.{table_name}"
            if mth_end_dt:
                sql += rf" WHERE MTH_END_DT = '{mth_end_dt}'"
            elif popn_dt:
                sql += rf" WHERE POPN_DT = '{popn_dt}'"
            
            logging.warning(rf"SQL: {sql}")
    
    handle_month_context() >> run()
            
            
edl_to_db2()
