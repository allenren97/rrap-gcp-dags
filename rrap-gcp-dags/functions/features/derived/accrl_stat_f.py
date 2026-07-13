from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "reference.CHRG_OFF_LKP",
]
DOWNSTREAM_ASSET = "features.ACCRL_STAT_F"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}
CHRG_OFF_LKP = "reference.CHRG_OFF_LKP"


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
            ACCRL_STAT_F
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
            LEFT JOIN (
                SELECT
                    TRIM(ACCRL_STAT_F) AS ACCRL_STAT_F,
                    TRIM(CHRG_OFF_CD) AS CHRG_OFF_CD
                FROM
                    {CHRG_OFF_LKP}
                WHERE
                    (
                        EFF_FROM_YR_MTH <= strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
                        AND strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') <= EFF_TO_YR_MTH
                    )
                    AND TRIM(CHRG_OFF_STAT_F) = 'Y'
                    AND TRIM(CHRG_OFF_CD) IN (
                        SELECT
                            TRIM(CHRG_OFF_CD)
                        FROM
                            {CHRG_OFF_LKP}
                        WHERE
                            TRIM(ACCRL_STAT_F) IN ('N', 'Q')
                            AND EFF_FROM_YR_MTH <= strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
                            AND strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') <= EFF_TO_YR_MTH
                    )
            ) asf ON TRIM(a.CHRG_OFF_CD) = TRIM(asf.CHRG_OFF_CD)
        WHERE
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass
