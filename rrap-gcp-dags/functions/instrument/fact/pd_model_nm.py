UPSTREAM_ASSET = [
    "models.STANDALONE_HELOC_PD_SEGMENT",
    "models.STANDALONE_MOR_PD_SEGMENT",
    "models.STEP_HELOC_PD_SEGMENT",
    "models.STEP_MOR_PD_SEGMENT",
    "models.STEP_MIX_PD_SEGMENT",
    "models.CC_PD_SEGMENT",
    "models.ITL_PD_SEGMENT",
    "models.LOC_PD_SEGMENT",
    "models.SSLA_PD_SEGMENT",
    "models.SSLB_PD_SEGMENT",
    "models.TNG_MOR_PD_SEGMENT",
    "models.CC_PD_SEGMENT",
    "models.DTL_PD_SEGMENT",
    "models.HELOC_PD_SEGMENT",
    "models.ITL_PD_SEGMENT",
    "models.LOC_PD_SEGMENT",
    "models.MOR_PD_SEGMENT",
    "models.SSLA_PD_SEGMENT",
    "models.SSLB_PD_SEGMENT",
    "models.TNG_MOR_PD_SEGMENT",
    "features.BASEL_ACCT_ID",
    "reference.BASEL_MODEL",
]

DOWNSTREAM_ASSET = "instruments.PD_MODEL_NM"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="pd_model_nm.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_clear(
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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM,
            BASEL_ACCT_ID,
            PD_MODEL_ID,
            PD_MODEL_NM
        FROM 
            '{{{{ task_instance.xcom_pull(task_ids="fact__pd_model_nm.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
