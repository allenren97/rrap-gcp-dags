UPSTREAM_ASSET = [
    "features.PRD_ID",
    "features.BASEL_ACCT_ID",
    "features.TRANSACTOR_FLAG_QRR",
    "instruments.PD_FLRD_RPTG_RTO",
    "features.CMHC_F",
    "reference.RPTG_PRD_LKP_KS",
    "reference.RPTG_PRD_LKP_SPL",
    "reference.RPTG_PRD_LKP_MOR",
]
DOWNSTREAM_ASSET = "instruments.PD_BAND"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="pd_band.export_result.sql",
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
            PD_BAND,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="fact__pd_band.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
