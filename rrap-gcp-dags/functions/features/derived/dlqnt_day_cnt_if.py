from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM",
                  "ingestion.BASELAYER_MOR"]

DOWNSTREAM_ASSET = "features.DLQNT_DAY_CNT_IF"

DEPENDENCIES = {
    "export_ks": ["duckdb_delete_dlqnt_day_cnt_if"],
    "export_spl": ["duckdb_delete_dlqnt_day_cnt_if"],
    "export_mor": ["duckdb_delete_dlqnt_day_cnt_if"],
    "export_tng": ["duckdb_delete_dlqnt_day_cnt_if"],
    "duckdb_delete_dlqnt_day_cnt_if": ["duckdb_load_dlqnt_day_cnt_if"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        BNS_DLQNT_DAY AS DLQNT_DAY_CNT_IF
    FROM {UPSTREAM_ASSET[0]} ks
    WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        DAY_ODUE AS DLQNT_DAY_CNT_IF
    FROM {UPSTREAM_ASSET[1]} spl
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        base.DAYSDELINQUENT AS DLQNT_DAY_CNT_IF
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[5]} base ON
        mor.MORT_NUM = base.MORT_NUM
        AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        DAYS_ARREARS_CNT AS DLQNT_DAY_CNT_IF
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_dlqnt_day_cnt_if(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_dlqnt_day_cnt_if(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                DLQNT_DAY_CNT_IF
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt_if.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt_if.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt_if.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt_if.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass