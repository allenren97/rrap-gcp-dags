import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ 
        "models.tng_mor_pd_segment",
        "models.mor_pd_segment",
        "models.dtl_pd_segment",
        "models.itl_pd_segment",
        "models.heloc_pd_segment",
        "models.loc_pd_segment",
        "models.cc_pd_segment",
        "models.ssla_pd_segment",
        "models.sslb_pd_segment",
        "models.step_heloc_pd_segment",
        "models.step_mix_pd_segment",
        "models.step_mor_pd_segment",
        "models.standalone_heloc_pd_segment",
        "features.BASEL_ACCT_ID"
    ]
DOWNSTREAM_ASSET = "instruments.PD_BASEL_SEG_NUM"
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
    config_file="pd_basel_seg_num.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        select
            *
        from '{{{{ task_instance.xcom_pull(task_ids="fact__pd_basel_seg_num.export_result", key="parquet") }}}}'
        )
    """,
):
    pass