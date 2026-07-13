UPSTREAM_ASSET = [
    "reference.BASEL_SEG_RPTG_PARM",
    "reference.BASEL_SEG",
    "reference.BASEL_MODEL",
    "instruments.UNINSURED_LGD_SEG_NUM",
    "instruments.LGD_MODEL_NM",
    "instruments.LGD_FINAL_RPTG_RTO",
    "features.BASEL_ACCT_ID",
    "features.BASEL_PRD_TP_CD",
]
DOWNSTREAM_ASSET = "instruments.UNINSURED_LGD_RTO"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="uninsured_lgd_rto.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            BASEL_ACCT_ID,
            UNINSURED_LGD_RTO,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="fact__uninsured_lgd_rto.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
