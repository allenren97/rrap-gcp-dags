UPSTREAM_ASSET = [
    "instruments.LGD_BASEL_SEG_NUM",
    "reference.BASEL_SEG_RPTG_PARM",
    "reference.BASEL_SEG",
    "reference.BASEL_MODEL",
    "features.BASEL_ACCT_ID"
]
DOWNSTREAM_ASSET = "instruments.LGD_LD_PV_AD_SV_DT_RPTG_RTO"
DEPENDENCIES = {
    "duckdb_clear": ["duckdb_load"],
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
    config_file="lgd_ld_pv_ad_sv_dt_rptg_rto.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT 
            *
        FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__lgd_ld_pv_ad_sv_dt_rptg_rto.export_result", key="parquet") }}}}')
    )
    """,
):
    pass