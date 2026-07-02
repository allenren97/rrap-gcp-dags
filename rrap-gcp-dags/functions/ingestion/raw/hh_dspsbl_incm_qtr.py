import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


DUCKLAKE_SCHEMA = "ingestion"
UPSTREAM_ASSET = [ f"{DUCKLAKE_SCHEMA}.TM_DIM" ]
DOWNSTREAM_ASSET = f"{DUCKLAKE_SCHEMA}.HH_DSPSBL_INCM_QTR"
DEPENDENCIES = {
    'sensor_wait_for_table': ['duckdb_update'],
    'duckdb_update' : ['duckdb_insert'],
}
INPUT_PATH = 'jb0431_airb_statcan_hh_dspsbl_incm_ext.parquet'


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_update(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH
    jb0432_join_tm_id AS (
        SELECT
            t.TM_ID as MTH_TM_ID,
            j.* EXCLUDE(QTR_END_DT)
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="raw__hh_dspsbl_incm_qtr.sensor_wait_for_table", key="parquet") }}}}') j
        INNER JOIN ingestion.TM_DIM t ON
            j.QTR_END_DT = t.TM_LVL_END_DT
    )
    UPDATE {DOWNSTREAM_ASSET} SET
        CRNT_F = 'N',
        UPDT_PROCESS_TMSTMP = CURRENT_TIMESTAMP
    FROM jb0432_join_tm_id b
    WHERE
        {DOWNSTREAM_ASSET}.MTH_TM_ID = b.MTH_TM_ID
        AND {DOWNSTREAM_ASSET}.HH_DSPSBL_INCM_MILLNTH_AMT != b.HH_DSPSBL_INCM_MILLNTH_AMT
    """
):
    pass


def duckdb_insert(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH
    jb0432_join_tm_id AS (
        SELECT
            t.TM_ID as MTH_TM_ID,
            j.* EXCLUDE(QTR_END_DT)
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="raw__hh_dspsbl_incm_qtr.sensor_wait_for_table", key="parquet") }}}}') j
        INNER JOIN ingestion.TM_DIM t ON
            j.QTR_END_DT = t.TM_LVL_END_DT
    )
    , jb0432_update AS (
        SELECT
            a.MTH_TM_ID,
            a.EFF_FROM_YR_MTH,
            '999912' as EFF_TO_YR_MTH,
            a.HH_DSPSBL_INCM_MILLNTH_AMT,
            'Y' AS CRNT_F,
            CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,
            NULL as UPDT_PROCESS_TMSTMP
        FROM jb0432_join_tm_id a
        INNER JOIN {DOWNSTREAM_ASSET} b ON
            a.MTH_TM_ID = b.MTH_TM_ID
        WHERE
            a.HH_DSPSBL_INCM_MILLNTH_AMT != b.HH_DSPSBL_INCM_MILLNTH_AMT
    )
    , jb0432_new AS (
        SELECT
            a.MTH_TM_ID,
            a.EFF_FROM_YR_MTH,
            '999912' as EFF_TO_YR_MTH,
            a.HH_DSPSBL_INCM_MILLNTH_AMT,
            'Y' AS CRNT_F,
            CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,
            NULL as UPDT_PROCESS_TMSTMP
        FROM jb0432_join_tm_id a
        LEFT OUTER JOIN {DOWNSTREAM_ASSET} b ON
            a.MTH_TM_ID = b.MTH_TM_ID
        WHERE
            b.MTH_TM_ID IS NULL
    )
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT * FROM jb0432_update
        UNION
        SELECT * FROM jb0432_new
    )
    """
):
    pass


