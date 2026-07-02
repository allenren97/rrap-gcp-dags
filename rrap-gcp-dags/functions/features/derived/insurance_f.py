from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "features.BULK_IND",
    "features.INSURANCE",
    "ingestion.MORT_MTH_SNAPSHOT"
]
DOWNSTREAM_ASSET = "features.INSURANCE_F"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
                ss.BASEL_ACCT_ID,
                CASE
                    WHEN BULK_IND = 'Y' THEN 'BULK'
                    WHEN BULK_IND = 'N' AND INSURANCE = 'Insured' THEN 'Insured'
                    ELSE 'uninsured'
                END AS INSURANCE_F
            FROM features.BULK_IND bi
            JOIN features.INSURANCE i ON bi.MORT_NUM = i.MORT_NUM
            LEFT JOIN ingestion.MORT_MTH_SNAPSHOT ss ON ss.MORT_NUM = i.MORT_NUM
            WHERE bi.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND i.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """,
):
    pass


