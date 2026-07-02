import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ 'reference.STANDARDIZED_CMA' ]
DOWNSTREAM_ASSET = "ingestion.METRPL_CITY_LKP_CMA"
DEPENDENCIES = {
}


# Update the EFF_TO_YR_MTH and CRNT_F of the historical records that did not flow in from CSV this month
def duckdb_metrpl_city_lkp_cma_update(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    UPDATE ingestion.METRPL_CITY_LKP_CMA
    SET 
        EFF_TO_YR_MTH = substr(
            replace('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', '-', ''), 
        1, 6),
        CRNT_F = 'N',
        UPDT_PROCESS_TMSTMP = current_timestamp
    FROM { DOWNSTREAM_ASSET } a
    LEFT JOIN reference.EDW_CMA_CITY_LKP_CMA b
    ON a.METRPL_AREA_NM_ORIG = b.CMA
    AND a.CITY_NM = b.CITY
    AND a.PROV = b.PROVINCE
    WHERE b.CMA IS NULL
    """
):
    pass


def duckdb_metrpl_city_lkp_cma_new(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO ingestion.METRPL_CITY_LKP_CMA
    BY NAME FROM (
        SELECT
            b.CITY AS CITY_NM,
            b.CMA AS METRPL_AREA_NM_ORIG,
            c.STANDARDIZED_CMA AS METRPL_AREA_NM,
            substr(
                replace('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', '-', ''), 
            1, 6) AS EFF_FROM_YR_MTH,
            b.PROVINCE AS PROV,
            '999912' AS EFF_TO_YR_MTH,
            'Y' AS CRNT_F,
            current_timestamp as INSRT_PROCESS_TMSTMP,
            current_timestamp as UPDT_PROCESS_TMSTMP
        FROM { DOWNSTREAM_ASSET } a
        RIGHT JOIN reference.EDW_CMA_CITY_LKP_CMA b
        ON a.METRPL_AREA_NM = b.CMA
        AND a.CITY_NM = b.CITY
        AND a.PROV = b.PROVINCE
        INNER JOIN { UPSTREAM_ASSET[0] } c
        ON b.CMA = c.SOURCE_CMA
        WHERE a.METRPL_AREA_NM IS NULL
    )
    """
):
    pass


