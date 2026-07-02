UPSTREAM_ASSET = [
    "features.BASEL_ACCT_ID",
    "instruments.PD_FINAL_RPTG_RTO",
    "features.HELOC_F",
    "features.BASEL_PRD_CD",
    "instruments.PIT_STAT_CD",
    "features.CONSM_PRD_TREATMNT_CD_IF",
    "features.CONSM_SCORECRD_EXCLSN_F",
]
DOWNSTREAM_ASSET = "instruments.PD_90_DAY_F"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="pd_90_day_f.export_result.sql",
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
            PD_90_DAY_F,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="fact__pd_90_day_f.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
