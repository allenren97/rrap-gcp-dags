from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_PRFM_FACT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.TNG_ACCT_MO",
    "features.MAX_ACCT_BAL_AMT",
    "features.INDEX_TERANETV",
    "features.ACCT_SENRTY_CD",
    "features.PRD_ID",
    "instruments.CRNT_PRPTY_VAL_AMT",
    "instruments.DLGD_F",
    "instruments.EAD_FINAL_RPTG_RTO",
    "instruments.LGD_BASEL_SEG_NUM",
    "instruments.PIT_STAT_CD",
    "reference.RPTG_PRD_LKP_MOR",
]

DOWNSTREAM_ASSET = "instruments.CRNT_LTV_RTO"

DEPENDENCIES = {
    "export_crnt_ltv_rto": ["export_ks", "export_spl", "export_mor", "export_tng"],
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}

def export_crnt_ltv_rto(
    duckdb_conn_id="duckdb-conn",
    config_file="crnt_ltv_rto.export_crnt_ltv_rto.sql",
    config_type="instrument",
):
    pass

def export_ks(
    duckdb_conn_id="duckdb-conn",
    config_file="crnt_ltv_rto.export_ks.sql",
    config_type="instrument",
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    config_file="crnt_ltv_rto.export_spl.sql",
    config_type="instrument",
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="crnt_ltv_rto.export_mor.sql",
    config_type="instrument",
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    config_file="crnt_ltv_rto.export_tng.sql",
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
                OBSN_DT,
                BASEL_ACCT_ID,
                CRNT_LTV_RTO,
                STREAM
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass