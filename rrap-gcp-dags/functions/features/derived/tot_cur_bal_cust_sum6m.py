from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.TOT_CUR_BAL_CUST_SUM6M"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            SUM(COALESCE(tot_new_bal_amt, 0)) AS OS_BAL
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS ks
            LEFT JOIN (
                SELECT
                    tm_id,
                    tm_lvl_end_dt
                FROM
                    ingestion.TM_DIM
            ) tm ON ks.mth_tm_id = tm.tm_id
            LEFT JOIN (
                SELECT
                    *
                FROM
                    features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE
                    obsn_dt > last_day (DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 6 MONTH)
                    AND obsn_dt <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ) AS pit ON ks.basel_acct_id = pit.basel_acct_id
            AND pit.obsn_dt = tm.tm_lvl_end_dt
            LEFT JOIN (
                SELECT
                    SRC_PRD_CD,
                    SRC_SUB_PRD_CD,
                    SML_BUS_F,
                    CRNT_F,
                    PRD_SYS_CD
                FROM
                    reference.SRC_PRD_LKP
                WHERE
                    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) AS sp ON TRIM(ks.PRD_CD) = TRIM(sp.SRC_PRD_CD)
            AND TRIM(ks.SUB_PRD_CD) = TRIM(sp.SRC_SUB_PRD_CD)
        WHERE
            ks.mth_tm_id <= {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND ks.mth_tm_id > {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 6 * 40
            AND PRIM_BASEL_CUST_ID IS NOT NULL
            AND PRIM_BASEL_CUST_ID > 0
            AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
            AND TRIM(sp.SML_BUS_F) = 'N'
            AND TRIM(sp.CRNT_F) = 'Y'
            AND TRIM(sp.PRD_SYS_CD) = 'KS'
        GROUP BY
            PRIM_BASEL_CUST_ID
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            SUM(
                COALESCE(crnt_bal_amt, 0) + COALESCE(intr_accr_amt, 0)
            ) AS OS_BAL
        FROM
            ingestion.MORT_MTH_SNAPSHOT AS mor
            LEFT JOIN (
                SELECT
                    tm_id,
                    tm_lvl_end_dt
                FROM
                    ingestion.TM_DIM
            ) tm ON mor.mth_tm_id = tm.tm_id
            LEFT JOIN (
                SELECT
                    *
                FROM
                    features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE
                    obsn_dt > last_day (DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 6 MONTH)
                    AND obsn_dt <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ) AS pit ON mor.basel_acct_id = pit.basel_acct_id
            AND pit.obsn_dt = tm.tm_lvl_end_dt
        WHERE
            mor.mth_tm_id <= {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND mor.mth_tm_id > {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 6 * 40
            AND PRIM_BASEL_CUST_ID IS NOT NULL
            AND PRIM_BASEL_CUST_ID > 0
            AND TRIM(PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
        GROUP BY
            PRIM_BASEL_CUST_ID
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            SUM(
                COALESCE(tot_crnt_bal_amt, 0) + COALESCE(add_on_bal_amt, 0) + COALESCE(accr_intr, 0)
            ) AS OS_BAL
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
            LEFT JOIN (
                SELECT
                    tm_id,
                    tm_lvl_end_dt
                FROM
                    ingestion.TM_DIM
            ) tm ON spl.mth_tm_id = tm.tm_id
            LEFT JOIN (
                SELECT
                    *
                FROM
                    features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE
                    obsn_dt > last_day (
                        DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 6 MONTH
                    )
                    AND obsn_dt <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ) AS pit ON spl.basel_acct_id = pit.basel_acct_id
            AND pit.obsn_dt = tm.tm_lvl_end_dt
        WHERE
            spl.mth_tm_id <= {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND spl.mth_tm_id > {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 6 * 40
            AND PRIM_BASEL_CUST_ID IS NOT NULL
            AND PRIM_BASEL_CUST_ID > 0
            AND TRIM(PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
        GROUP BY
            PRIM_BASEL_CUST_ID
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
        select
            BASEL_CUST_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as obsn_dt,
            sum(
                coalesce(OS_BAL, 0)
            )::decimal(24,6) as TOT_CUR_BAL_CUST_SUM6M
        from
            read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__tot_cur_bal_cust_sum6m.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__tot_cur_bal_cust_sum6m.export_spl", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__tot_cur_bal_cust_sum6m.export_mor", key="parquet") }}}}'], union_by_name = true)
        group by
            BASEL_CUST_ID
    )
    """,
):
    pass


