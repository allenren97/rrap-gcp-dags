from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# TODO: make dependent on OS_BAL_AMT

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.CONSM_PRD_TREATMNT_CD",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.PLANUTILIZATIONSTP"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            trim(step_pln_agrmnt_num) as STEP_PLN_AGRMNT_NUM,
            sum(
                COALESCE(tot_crnt_bal_amt, 0) + COALESCE(add_on_bal_amt, 0) + COALESCE(accr_intr, 0)
            ) AS OS_BAL_AMT
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
        WHERE
            spl.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND coalesce(trim(step_pln_agrmnt_num),'') != ''
        group by
            trim(step_pln_agrmnt_num)
    """,
):
    pass


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            TRIM(rvl.step_pln_agrmnt_num) AS STEP_PLN_AGRMNT_NUM,
            SUM(COALESCE(rvl.tot_new_bal_amt, 0)) AS OS_BAL_AMT,
            SUM(COALESCE(rvl.cr_lmt_amt, 0)) AS CR_LMT_AMT
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS rvl
            LEFT JOIN (
                SELECT
                    SRC_PRD_CD,
                    SRC_SUB_PRD_CD,
                    SML_BUS_F
                FROM
                    reference.SRC_PRD_LKP
                WHERE
                    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) AS sp ON TRIM(rvl.PRD_CD) = TRIM(sp.SRC_PRD_CD)
            AND TRIM(rvl.SUB_PRD_CD) = TRIM(sp.SRC_SUB_PRD_CD)
            LEFT JOIN features.CONSM_PRD_TREATMNT_CD AS lkp ON rvl.basel_acct_id = lkp.basel_acct_id and OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        WHERE
            rvl.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND TRIM(sp.SML_BUS_F) = 'N'
            AND coalesce(trim(step_pln_agrmnt_num),'') != ''
            AND TRIM(CONSM_PRD_TREATMNT_CD) = 'A'
        GROUP BY
            TRIM(rvl.step_pln_agrmnt_num)
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            trim(step_pln_agrmnt_num) as STEP_PLN_AGRMNT_NUM,
            sum(
                COALESCE(crnt_bal_amt, 0) + COALESCE(intr_accr_amt, 0)
            ) AS OS_BAL_AMT
        FROM
            ingestion.MORT_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND coalesce(trim(step_pln_agrmnt_num),'') != ''
        group by
            trim(step_pln_agrmnt_num)
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
        WITH
        step_id AS (
            SELECT
                step_pln_snapshot_id,
                TRIM(step_pln_agrmnt_num) AS step_pln_agrmnt_num,
            FROM
                ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT
            WHERE
                mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND coalesce(trim(step_pln_agrmnt_num),'') != ''
            GROUP BY
                TRIM(step_pln_agrmnt_num),
                step_pln_snapshot_id
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
        main.step_pln_snapshot_id,
        nullif(trim(main.step_pln_agrmnt_num), '') as STEP_PLN_AGRMNT_NUM,
        CASE
            WHEN SUM(
                COALESCE(spl.OS_BAL_AMT, 0) + COALESCE(rvl.OS_BAL_AMT, 0) + COALESCE(mor.OS_BAL_AMT, 0)
            ) = 0
            OR SUM(
                COALESCE(rvl.CR_LMT_AMT, 0) + COALESCE(spl.OS_BAL_AMT, 0) + COALESCE(mor.OS_BAL_AMT, 0)
            ) = 0 THEN 0
            ELSE (
                SUM(
                    COALESCE(spl.OS_BAL_AMT, 0) + COALESCE(rvl.OS_BAL_AMT, 0) + COALESCE(mor.OS_BAL_AMT, 0)
                ) / SUM(
                    COALESCE(rvl.CR_LMT_AMT, 0) + COALESCE(spl.OS_BAL_AMT, 0) + COALESCE(mor.OS_BAL_AMT, 0)
                )
            )::decimal(20,12)
        END AS PLANUTILIZATIONSTP
    FROM
        step_id AS main
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__planutilizationstp.export_spl", key="parquet") }}}}' AS spl ON main.step_pln_agrmnt_num = spl.step_pln_agrmnt_num
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__planutilizationstp.export_ks", key="parquet") }}}}' AS rvl ON main.step_pln_agrmnt_num = rvl.step_pln_agrmnt_num
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__planutilizationstp.export_mor", key="parquet") }}}}' AS mor ON main.step_pln_agrmnt_num = mor.step_pln_agrmnt_num
    WHERE
        spl.step_pln_agrmnt_num IS NOT NULL
        OR rvl.step_pln_agrmnt_num IS NOT NULL
        OR mor.step_pln_agrmnt_num IS NOT NULL
    GROUP BY
        main.step_pln_snapshot_id,
        main.step_pln_agrmnt_num
    )
    """,
):
    pass
