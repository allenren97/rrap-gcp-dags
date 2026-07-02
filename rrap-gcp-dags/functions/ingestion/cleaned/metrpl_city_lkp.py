import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ 'ingestion.EDW_CMA_CITY_LKP' ]
DOWNSTREAM_ASSET = "ingestion.METRPL_CITY_LKP"
DEPENDENCIES = {
}


# Update the EFF_TO_YR_MTH and CRNT_F of the historical records that did not flow in from CSV this month
def duckdb_metrpl_city_lkp_update(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    UPDATE ingestion.METRPL_CITY_LKP
    SET 
        EFF_TO_YR_MTH = substr(
            replace('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', '-', ''), 
        1, 6),
        CRNT_F = 'N',
        UPDT_PROCESS_TMSTMP = current_timestamp
    FROM ingestion.METRPL_CITY_LKP a
    LEFT JOIN ingestion.EDW_CMA_CITY_LKP b
    ON a.METRPL_AREA_NM = b.CMA
    AND a.CITY_NM = b.CITY
    AND a.PROV = b.PROVINCE
    WHERE b.CMA IS NULL
    """
):
    pass


def duckdb_metrpl_city_lkp_new(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO ingestion.METRPL_CITY_LKP
    FROM (
        SELECT
            b.CITY,
            b.CMA,
            substr(
                replace('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', '-', ''), 
            1, 6) AS EFF_FROM_YR_MTH,
            b.PROVINCE,
            '999912' AS EFF_TO_YR_MTH,
            'Y' AS CRNT_F,
            current_timestamp as INSRT_PROCESS_TMSTMP,
            current_timestamp as UPDT_PROCESS_TMSTMP
        FROM ingestion.METRPL_CITY_LKP a
        RIGHT JOIN ingestion.EDW_CMA_CITY_LKP b
        ON a.METRPL_AREA_NM = b.CMA
        AND a.CITY_NM = b.CITY
        AND a.PROV = b.PROVINCE
        WHERE a.METRPL_AREA_NM IS NULL
    )
    """
):
    pass





# def export_edw_cma_city_lkp(
#     duckdb_conn_id='duckdb-conn',
#     sql="""SELECT * FROM ingestion.EDW_CMA_CITY_LKP"""
# ):
#     pass


# def export_metrpl_city_lkp(
#     duckdb_conn_id='duckdb-conn',
#     sql="""SELECT * FROM ingestion.METRPL_CITY_LKP"""
# ):
#     pass


# def duckdb_load_into_ducklake(
#     duckdb_conn_id='duckdb-conn',
#     sql=r"""
#     INSERT INTO ingestion.METRPL_CITY_LKP
#     BY NAME FROM (
#         SELECT * FROM '{{{{ task_instance.xcom_pull(task_ids="duckdb_metrpl_city_lkp_update", key="parquet") }}}}'
#         UNION
#         SELECT * FROM '{{{{ task_instance.xcom_pull(task_ids="duckdb_metrpl_city_lkp_new", key="parquet") }}}}'
#     )
#     """
# ):
#     pass