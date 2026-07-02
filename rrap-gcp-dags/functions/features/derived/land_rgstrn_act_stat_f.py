from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT"
]

DOWNSTREAM_ASSET = "features.LAND_RGSTRN_ACT_STAT_F"

DEPENDENCIES = {
    "export_land_rgstrn_act_stat_f":["duckdb_delete_land_rgstrn_act_stat_f"],
    "duckdb_delete_land_rgstrn_act_stat_f": ["duckdb_load_land_rgstrn_act_stat_f"],
}



def export_land_rgstrn_act_stat_f(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,

            BASEL_ACCT_ID,

            CASE
                WHEN TRY_CAST(TRIM(FUND_CD) AS INTEGER) BETWEEN 2000 AND 2199
                  OR TRY_CAST(TRIM(FUND_CD) AS INTEGER) BETWEEN 2202 AND 2249
                  OR TRY_CAST(TRIM(FUND_CD) AS INTEGER) BETWEEN 6490 AND 6499
                THEN 'Y'
                ELSE 'N'
            END AS LAND_RGSTRN_ACT_STAT_F

        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass



def duckdb_delete_land_rgstrn_act_stat_f(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass



def duckdb_load_land_rgstrn_act_stat_f(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT *
            FROM read_parquet([
                '{{{{ task_instance.xcom_pull(task_ids="derived__land_rgstrn_act_stat_f.export_land_rgstrn_act_stat_f", key="parquet") }}}}'
            ])
        )
    """,
):
    pass