import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.AIRB_MORT_MTH_SNAPSHOT']
DOWNSTREAM_ASSET = "features.PROVINCE_CD"
DEPENDENCIES = {
    'duckdb_clear_derive_province_cd': ['duckdb_derive_province_cd'],
}

REFERENCES = ['reference.PROVINCE_REF']


def duckdb_clear_derive_province_cd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_province_cd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    with AIRB_MORT_MTH_SNAPSHOT as (
            SELECT
            MORT_NUM,
            PROP_PROV
            from {UPSTREAM_ASSET[0]}
            where tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ) 
            SELECT 
            MORT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            PROVINCE_CD 
            FROM AIRB_MORT_MTH_SNAPSHOT
            left join 
            {REFERENCES[0]}
            on TRY_CAST(PROP_PROV AS INT32) = TRY_CAST(PROVINCE_ID AS INT32)
    """

):
    pass

