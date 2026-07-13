from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "features.DLQNT_DAY_CNT"
]

DOWNSTREAM_ASSET = "features.DLQNT_MTH_CNT"

DEPENDENCIES = {
    "export_dlqnt_mth_cnt":["duckdb_delete_dlqnt_mth_cnt"],
    "duckdb_delete_dlqnt_mth_cnt": ["duckdb_load_dlqnt_mth_cnt"],
}


def duckdb_delete_dlqnt_mth_cnt(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def export_dlqnt_mth_cnt(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        WITH reporting_month AS (
            SELECT
                TM_ID,
                TM_LVL_ST_DT
            FROM {UPSTREAM_ASSET[1]} -- ingestion.TM_DIM
            WHERE TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,

            s.BASEL_ACCT_ID,

            CASE

                WHEN d.DLQNT_DAY_CNT = 0
                THEN 0

                WHEN TRIM(s.FLOAT_CD) IN ('W', 'B', 'S')
                THEN
                    CASE
                        WHEN s.WK_FRST_UNPAID_DT IS NULL
                        THEN 0
                        ELSE GREATEST(
                            DATE_DIFF(
                                'month',
                                s.WK_FRST_UNPAID_DT,
                                rm.TM_LVL_ST_DT
                            ) + 1,
                            0
                        )
                    END

                ELSE
                    CASE
                        WHEN s.FRST_UNPAID_DT IS NULL
                        THEN 0
                        ELSE GREATEST(
                            DATE_DIFF(
                                'month',
                                s.FRST_UNPAID_DT,
                                rm.TM_LVL_ST_DT
                            ) + 1,
                            0
                        )
                    END

            END AS DLQNT_MTH_CNT,

            'MO' AS SRC_SYS_CD

        FROM {UPSTREAM_ASSET[0]} s -- ingestion.MORT_MTH_SNAPSHOT

        INNER JOIN {UPSTREAM_ASSET[2]} d -- features.DLQNT_DAY_CNT
            ON s.BASEL_ACCT_ID = d.BASEL_ACCT_ID
           AND d.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
           AND d.SRC_SYS_CD = 'MO'

        CROSS JOIN reporting_month rm

        WHERE s.MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass



def duckdb_load_dlqnt_mth_cnt(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT *
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_mth_cnt.export_dlqnt_mth_cnt", key="parquet") }}}}'
        ])
    )
    """,
):
    pass