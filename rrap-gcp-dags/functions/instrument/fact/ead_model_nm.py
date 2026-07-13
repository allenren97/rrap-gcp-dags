UPSTREAM_ASSET = [
    "reference.BASEL_MODEL",
    "models.CC_EAD_SEGMENT",
    "models.HELOC_EAD_SEGMENT",
    "models.LOC_EAD_SEGMENT",
    "models.SSLA_EAD_SEGMENT",
    "models.STANDALONE_HELOC_EAD_SEGMENT",
    "models.STEP_HELOC_EAD_SEGMENT",
    "features.BASEL_ACCT_ID",
]
DOWNSTREAM_ASSET = "instruments.EAD_MODEL_NM"
DEPENDENCIES = {
    "duckdb_clear": ["export_result"],
    "export_result": ["duckdb_load"],
}


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


# TODO remove hardcoded stream
def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="ead_model_nm.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            BASEL_ACCT_ID,
            EAD_MODEL_NM,
            EAD_MODEL_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM
        FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__ead_model_nm.export_result", key="parquet") }}}}')
    )
    """,
):
    pass
