from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.COMM_TP_CD",
    "reference.SRC_PRD_LKP",
    "reference.TRNST_EXCLSN_LKP",
]
DOWNSTREAM_ASSET = "features.CONSM_PRD_TREATMNT_CD"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}
SRC_PRD_LKP = "reference.SRC_PRD_LKP"


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            BASEL_CUST_ID,
            CASE
                WHEN (
                    CR_LMT_AMT <= 0
                    AND TOT_NEW_BAL_AMT <= 0
                ) THEN 'Z'
                WHEN TOT_NEW_BAL_AMT <= 0
                AND (
                    SUBSTR (TRIM(BLOCK_RECL_CD), 1, 1) = 'V'
                    OR TRIM(BLOCK_RECL_CD) = 'FX'
                ) THEN 'Z'
                ELSE TRIM(CONSM_PRD_TREATMNT_CD)
            END AS CONSM_PRD_TREATMNT_CD,
            'KS' as SRC_SYS_CD
        FROM
            (
                SELECT
                    BASEL_ACCT_ID,
                    PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                    CR_LMT_AMT,
                    TOT_NEW_BAL_AMT,
                    BLOCK_RECL_CD,
                    CONSM_PRD_TREATMNT_CD
                FROM
                    {UPSTREAM_ASSET[0]} a
                    LEFT JOIN (
                        SELECT
                            SRC_PRD_CD,
                            SRC_SUB_PRD_CD,
                            CONSM_PRD_TREATMNT_CD
                        FROM
                            {SRC_PRD_LKP}
                        WHERE
                            strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                    ) SP ON TRIM(A.PRD_CD) = TRIM(SP.SRC_PRD_CD)
                    AND TRIM(A.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
                WHERE
                    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )   
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            a.BASEL_ACCT_ID,
            a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            CASE
                WHEN COMM_TP_CD != 'RESIDENTIAL'
                OR PD_OFF_DT IS NOT NULL
                OR (CRNT_BAL_AMT + INTR_ACCR_AMT) <= 0 THEN 'Z'
                ELSE 'A'
            END AS CONSM_PRD_TREATMNT_CD,
            'MOR' as SRC_SYS_CD
        FROM
            ingestion.MORT_MTH_SNAPSHOT a
            LEFT JOIN features.COMM_TP_CD c ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
            AND c.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        WHERE
            a.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            a.BASEL_ACCT_ID,
            a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            CASE
                WHEN (
                    TRIM(EXCLUDED_TRNST_NUM) != ''
                    AND EXCLUDED_TRNST_NUM IS NOT NULL
                )
                OR ROUND(TOT_CRNT_BAL_AMT + ADD_ON_BAL_AMT + ACCR_INTR, 3) <= 0 THEN 'Z'
                ELSE 'A'
            END AS CONSM_PRD_TREATMNT_CD,
            'SPL' as SRC_SYS_CD
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
            LEFT JOIN reference.TRNST_EXCLSN_LKP C ON a.CRNT_BR_LOCTN_TRNST = C.EXCLUDED_TRNST_NUM
        WHERE
            a.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
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
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd.export_mor", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd.export_spl", key="parquet") }}}}'], union_by_name = true)
    )
    """,
):
    pass