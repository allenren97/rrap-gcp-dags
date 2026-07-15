from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.COMM_TP_CD",
    "features.LAND_RGSTRN_ACT_STAT_F",
    "features.DLQNT_MTH_CNT",
]
DOWNSTREAM_ASSET = "features.PIT_STAT_VER_1_CD"
DEPENDENCIES = {
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            s.BASEL_ACCT_ID,
            CASE
                WHEN c.COMM_TP_CD = 'RESIDENTIAL'
                     AND s.CRNT_BAL_AMT <> 0
                     AND s.PD_OFF_DT IS NULL
                     AND (m.DLQNT_MTH_CNT <= 3 OR m.DLQNT_MTH_CNT IS NULL)
                     AND TRIM(COALESCE(s.FRCLSR_F, '')) <> 'Y'
                     AND TRIM(COALESCE(l.LAND_RGSTRN_ACT_STAT_F, '')) IN ('', 'N')
                THEN 'CUR'
                WHEN s.CRNT_BAL_AMT <> 0
                     AND c.COMM_TP_CD = 'RESIDENTIAL'
                     AND s.PD_OFF_DT IS NULL
                     AND (
                         m.DLQNT_MTH_CNT IS NULL
                         OR m.DLQNT_MTH_CNT > 3
                         OR TRIM(COALESCE(s.FRCLSR_F, '')) IN ('', 'Y')
                         OR TRIM(COALESCE(l.LAND_RGSTRN_ACT_STAT_F, '')) IN ('', 'Y')
                     )
                THEN 'DEF'
            END AS PIT_STAT_VER_1_CD,
            'MOR' AS SRC_SYS_CD
        FROM {UPSTREAM_ASSET[0]} s
        LEFT JOIN {UPSTREAM_ASSET[2]} c ON
            c.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            AND c.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[3]} l ON
            l.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            AND l.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[4]} m ON
            m.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            AND m.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND m.SRC_SYS_CD = 'MO'
        WHERE s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '4' AND DAY_ODUE <= 90 THEN 'CUR'
                WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '4' AND DAY_ODUE > 90 THEN 'DEF'
                WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '5' THEN 'DEF'
                WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '6' THEN 'CHG'
                WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '7' THEN 'CHG'
                WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '8' THEN 'CHG'
            END AS PIT_STAT_VER_1_CD,
            'SPL' AS SRC_SYS_CD
        FROM {UPSTREAM_ASSET[1]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__pit_stat_ver_1_cd.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__pit_stat_ver_1_cd.export_spl", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
