UPSTREAM_ASSET = [
    "reference.BASEL_MODEL",
    "features.BASEL_ACCT_ID",
    "models.CC_LGDD_SEGMENT",
    "models.CC_LGDND_SEGMENT",
    "models.DTL_LGDD_SEGMENT",
    "models.DTL_LGDND_SEGMENT",
    "models.HELOC_LGDD_SEGMENT",
    "models.HELOC_LGDND_SEGMENT",
    "models.ITL_LGDD_SEGMENT",
    "models.ITL_LGDND_SEGMENT",
    "models.LOC_LGDD_SEGMENT",
    "models.LOC_LGDND_SEGMENT",
    "models.MOR_LGDD_SEGMENT",
    "models.MOR_LGDND_SEGMENT",
    "models.SSLA_LGDD_SEGMENT",
    "models.SSLA_LGDND_SEGMENT",
    "models.SSLB_LGDD_SEGMENT",
    "models.SSLB_LGDND_SEGMENT",
    "models.TNG_MOR_LGDD_SEGMENT",
    "models.TNG_MOR_LGDND_SEGMENT",
    "models.STANDALONE_HELOC_LGDD_SEGMENT",
    "models.STANDALONE_HELOC_LGDND_SEGMENT",
    "models.STEP_HELOC_LGDD_SEGMENT",
    "models.STEP_HELOC_LGDND_SEGMENT",
]
DOWNSTREAM_ASSET = "instruments.LGD_MODEL_NM"
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
    config_file="lgd_model_nm.export_result.sql",
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
            LGD_MODEL_ID,
            LGD_MODEL_NM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM
        FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__lgd_model_nm.export_result", key="parquet") }}}}')
    )
    """,
):
    pass
