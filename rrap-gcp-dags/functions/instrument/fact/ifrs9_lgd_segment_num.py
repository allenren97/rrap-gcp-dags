from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.SRC_SYS_CD",
                  "instruments.LGD_MODEL_NM",
                  "instruments.LGD_BASEL_SEG_NUM"

]
DOWNSTREAM_ASSET = "instruments.IFRS9_LGD_SEGMENT_NUM"

DEPENDENCIES = {
  "duckdb_clear": ["export_result"],
  "export_result": ["duckdb_load"]
}

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    
    """
):
    pass

def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="ifrs9_lgd_segment_num.export_result.sql",
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
        from '{{{{ task_instance.xcom_pull(task_ids="fact__ifrs9_lgd_segment_num.export_result", key="parquet") }}}}'
        )
    """,
):
    pass