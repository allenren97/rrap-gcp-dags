from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# This could be replaced with an airflow variable in the future, if it is necessary to quickly modify this variable.
UNDRAWN_EXPSR_PCT = 0.5

UPSTREAM_ASSET = [
    'features.BASEL_PRD_TP_CD'
]
DOWNSTREAM_ASSET = "features.UNDRAWN_EXPSR_PCT"
DEPENDENCIES = {
    "export_all": ["duckdb_delete_undrawn_expsr_pct"],
    "duckdb_delete_undrawn_expsr_pct": ["duckdb_load_undrawn_expsr_pct"],
}

def export_all(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT 
            BASEL_ACCT_ID,
            OBSN_DT,
            SRC_SYS_CD,
            CASE 
                WHEN TRIM(BASEL_PRD_TP_CD) IN ('CARD','CL','HELOC','SLR') THEN {UNDRAWN_EXPSR_PCT} 
                ELSE NULL
            END AS UNDRAWN_EXPSR_PCT
        FROM {UPSTREAM_ASSET[0]} --BASEL_PRD_TP_CD
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_undrawn_expsr_pct(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load_undrawn_expsr_pct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                BASEL_ACCT_ID,
                OBSN_DT,
                SRC_SYS_CD,
                UNDRAWN_EXPSR_PCT
            FROM '{{{{ task_instance.xcom_pull(task_ids="derived__undrawn_expsr_pct.export_all", key="parquet") }}}}'
        )
    """,
):
    pass
