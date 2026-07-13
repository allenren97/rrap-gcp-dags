from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT", "ingestion.TM_DIM"]
DOWNSTREAM_ASSET = "features.PE_NUM_OF_MONTHS_SINCE_LAST_PAYM"

DEPENDENCIES = {
    "export_pe_num_last_paym_snapshot": ["duckdb_clear_pe_num_of_months_since_last_paym"],
    "duckdb_clear_pe_num_of_months_since_last_paym": ["duckdb_load_pe_num_of_months_since_last_paym"],
}


def export_pe_num_last_paym_snapshot(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        m.basel_acct_id AS BASEL_ACCT_ID,
        --FLOOR(MONTHS_BETWEEN(t.tm_lvl_end_dt, m.last_pymt_dt)) AS PE_NUM_OF_MONTHS_SINCE_LAST_PAYM
        -- Using DATE_DIFF('month', ...) instead of MONTHS_BETWEEN because DuckDB does not support MONTHS_BETWEEN.
        -- DATE_DIFF('month', start_date, end_date) returns the number of full months between two dates,
        -- which is equivalent to FLOOR(MONTHS_BETWEEN(end_date, start_date)) in DB2/SAS logic.
        DATE_DIFF('month', m.last_pymt_dt::DATE, t.tm_lvl_end_dt::DATE) AS PE_NUM_OF_MONTHS_SINCE_LAST_PAYM
    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS m
    LEFT JOIN ingestion.TM_DIM AS t
           ON m.mth_tm_id = t.tm_id
    WHERE m.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
      AND m.recd_stat_cd IN (4, 5, 6, 7, 8)
    """,
):
    pass

def duckdb_clear_pe_num_of_months_since_last_paym(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_pe_num_of_months_since_last_paym(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
      SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        e.BASEL_ACCT_ID,
        e.PE_NUM_OF_MONTHS_SINCE_LAST_PAYM
      FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__pe_num_of_months_since_last_paym.export_pe_num_last_paym_snapshot", key="parquet") }}}}') e
    )
    """,
):
    pass


