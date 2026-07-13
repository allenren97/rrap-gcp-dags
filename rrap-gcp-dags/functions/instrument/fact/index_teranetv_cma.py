UPSTREAM_ASSET = [
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",
    "ingestion.PROVNCL_HOUSE_INDEX_SUM_CMA",
    "ingestion.TM_DIM",
    "ingestion.AIRB_MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_MORT_MTH_SNAPSHOT",
    "instruments.METRPL_AREA_NM"
]

DOWNSTREAM_ASSET = "instruments.INDEX_TERANETV_CMA"
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


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="index_teranetv_cma.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        select
            *
        from '{{{{ task_instance.xcom_pull(task_ids="fact__index_teranetv_cma.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
