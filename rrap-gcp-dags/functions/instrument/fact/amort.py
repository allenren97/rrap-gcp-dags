from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM"]

DOWNSTREAM_ASSET = "instruments.AMORT"

DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}

def export_ks(
    duckdb_conn_id="duckdb-conn",
    config_file="amort.export_ks.sql",
    config_type="instrument",
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    config_file="amort.export_spl.sql",
    config_type="instrument",
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="amort.export_mor.sql",
    config_type="instrument",
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    config_file="amort.export_tng.sql",
    config_type="instrument",
):
    pass

def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                *
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__amort.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__amort.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__amort.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__amort.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass