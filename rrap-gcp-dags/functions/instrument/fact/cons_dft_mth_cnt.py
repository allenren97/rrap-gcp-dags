from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM",
    "features.MONTH_DEF",
    "features.MONTH_DEF_SINCE_LAST_DEF",
    "features.STEP_MONTH_DEF_SINCE_LAST_DEF",
    "features.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF",
]

DOWNSTREAM_ASSET = "instruments.CONS_DFT_MTH_CNT"

DEPENDENCIES = {
    "duckdb_clear": ["export_result"],
    "export_result": ["duckdb_load"],
}


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="cons_dft_mth_cnt.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_load(
    trigger_rule="none_failed_min_one_success",
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            CONS_DFT_MTH_CNT,
            STREAM
        FROM read_parquet(
            '{{{{ task_instance.xcom_pull(task_ids="fact__cons_dft_mth_cnt.export_result", key="parquet") }}}}'
        )
    )
    """,
):
    pass