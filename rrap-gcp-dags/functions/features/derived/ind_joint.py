import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras, 
    _push_asset_event_extras)


UPSTREAM_ASSET = ['ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT', 'ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT']
DOWNSTREAM_ASSET = "features.IND_JOINT"

DEPENDENCIES = {
    'duckdb_clear_ind_joint': ['duckdb_derive_ind_joint'],
}


def duckdb_clear_ind_joint(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        DELETE FROM { DOWNSTREAM_ASSET }
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_ind_joint(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                m.basel_acct_id AS BASEL_ACCT_ID,
                CASE
                    WHEN c.rel_cd_total > 0 THEN 1
                    ELSE 0
                END AS IND_JOINT
            FROM
                ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS m
            LEFT JOIN
                (
                    SELECT
                        mth_tm_id,
                        basel_acct_id,
                        sum(CASE WHEN rel_cd IN ('COS', 'COB') THEN 1 ELSE 0 END) rel_cd_total
                    FROM
                        ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                    WHERE
                        PRIM_CUST_F = 'N'
                        AND mth_tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
                    GROUP BY mth_tm_id, basel_acct_id) AS c
            ON m.basel_acct_id = c.basel_acct_id
                AND m.mth_tm_id = c.mth_tm_id
            WHERE
                m.mth_tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
                AND m.RECD_STAT_CD IN (4, 5, 6, 7, 8)
        )
    """
):
    pass

