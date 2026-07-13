from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
]
DOWNSTREAM_ASSET = "features.REMAIN_AMORT2"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from { DOWNSTREAM_ASSET } where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } by name
    FROM (
        select
            a.month_end_dt as obsn_dt,
            b.basel_acct_id,
            trim(a.account_id) AS account_id,
            CASE
                WHEN a.remain_amort=0 AND DATE_DIFF('month', a.open_dt, a.month_end_dt)=0 THEN amort_period
                ELSE a.remain_amort
            END AS remain_amort2
        from { UPSTREAM_ASSET[1] } a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
        WHERE
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


