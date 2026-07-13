import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    "models.heloc_ead_segment",
    "models.cc_ead_segment",
    "models.loc_ead_segment",
    "models.ssla_ead_segment",
    "models.step_heloc_ead_segment",
    "models.standalone_heloc_ead_segment",
    "features.BASEL_ACCT_ID"
    ]
DOWNSTREAM_ASSET = "instruments.EAD_BASEL_SEG_NUM"
DEPENDENCIES = {	
    "duckdb_clear": ["export_result"],
    "export_result": ["duckdb_load"],
}

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET }
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """
):
    pass

def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="ead_basel_seg_num.export_result.sql",
    config_type="instrument",
):
    pass

"""
The logic for ead seg nums is to keep any values from KS portfolios, and set anything else to be 1. 
Issue is that all the ead models for non-resl are part of KS. Unsure of RESL models.

There also exists an SSL-B EAD on prod and in the benchmark but not in our versions. Be aware that this may cause mismatches or missing accounts.
"""

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        select
            *
        from '{{{{ task_instance.xcom_pull(task_ids="fact__ead_basel_seg_num.export_result", key="parquet") }}}}'
        )
    """,
):
    pass