UPSTREAM_ASSET = [
    "reference.PD_BAND_DIM",
    "features.PD_BAND_EXPSR_CL_KEY_VAL",
    "instruments.PD_FLRD_RPTG_RTO",
    "features.CMHC_F",
    "features.TRANSACTOR_FLAG_QRR",
]
DOWNSTREAM_ASSET = "instruments.NCR_PD_BAND_KEY_VAL"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}

def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="ncr_pd_band_key_val.export_result.sql",
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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            NCR_PD_BAND_KEY_VAL,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="fact__ncr_pd_band_key_val.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
