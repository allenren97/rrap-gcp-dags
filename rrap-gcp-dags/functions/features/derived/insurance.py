import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.AIRB_MORT_MTH_SNAPSHOT']
DOWNSTREAM_ASSET = "features.INSURANCE"
DEPENDENCIES = {
    'duckdb_clear_derive_insurance': ['duckdb_derive_insurance'],
}


def duckdb_clear_derive_insurance(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_insurance(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET}
    SELECT 
        MORT_NUM, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        case 
        WHEN upper(TRIM(insur_grp)) = 'CONV' THEN 'Uninsured'
        WHEN upper(TRIM(insur_grp)) in ('GEM SPEC', 'GEMICO', 'GEMICO(NO DOWN)', 'MICC', 'CMHC', 'GUARANTY') then 'Insured'
        else '' 
        END AS INSURANCE 
        FROM {UPSTREAM_ASSET[0]} WHERE tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    """

):
    pass

