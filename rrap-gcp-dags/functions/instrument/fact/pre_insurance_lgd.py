UPSTREAM_ASSET = [
    "instruments.LGD_BASEL_SEG_NUM",
    "reference.BASEL_SEG_RPTG_PARM",
    "reference.BASEL_SEG",
    "reference.BASEL_MODEL",
]
DOWNSTREAM_ASSET = "instruments.PRE_INSURANCE_LGD"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="pre_insurance_lgd.export_result.sql",
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
            PRE_INSURANCE_LGD,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="fact__pre_insurance_lgd.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
