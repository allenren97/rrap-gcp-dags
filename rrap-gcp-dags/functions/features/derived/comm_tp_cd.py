from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "reference.TRNST_EXCLSN_LKP",
]
DOWNSTREAM_ASSET = "features.COMM_TP_CD"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


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
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN SUBSTR (TRIM(SCRTY_TP_2), 1, 1) = '6'
                OR TRY_CAST (
                    SUBSTR (
                        TRIM(SCRTY_TP_2),
                        LENGTH (TRIM(SCRTY_TP_2)) - 2,
                        3
                    ) AS INTEGER
                ) >= 5 THEN 'COMMERCIAL'
                ELSE 'RESIDENTIAL'
            END AS COMM_TP_CD
        FROM
            (
                SELECT
                    A.*,
                    TRIM(ACCT_NUM) AS ACCT_NUM,
                    EXCLUDED_TRNST_NUM
                FROM
                    ingestion.MORT_MTH_SNAPSHOT A
                    LEFT JOIN reference.TRNST_EXCLSN_LKP BR ON A.SERV_BR_TRNST_NUM = BR.EXCLUDED_TRNST_NUM
                    LEFT JOIN ingestion.BASEL_ACCT_DIM BA ON A.BASEL_ACCT_ID = BA.BASEL_ACCT_ID
                    AND SRC_APP_CD = 'MO'
                WHERE
                    A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )
    )
    """,
):
    pass


