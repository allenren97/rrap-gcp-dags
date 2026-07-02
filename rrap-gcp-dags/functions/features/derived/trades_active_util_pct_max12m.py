from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "ingestion.TNG_CUST_TU"
]
DOWNSTREAM_ASSET = "features.TRADES_ACTIVE_UTIL_PCT_MAX12M"
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
        with
        base as (
            select
                trim(account_id) AS account_id,
                max(COALESCE(TRADES_ACTIVE_UTIL_PCT, 0)) as TRADES_ACTIVE_UTIL_PCT_MAX12M
            from { UPSTREAM_ASSET[2] }
            where
                month_end_dt between
                    date_add(date '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -interval 11 month)
                    and
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            group by
                trim(account_id)
        )
        select
            a.month_end_dt as obsn_dt,
            b.basel_acct_id,
            trim(a.account_id) AS account_id,
            c.TRADES_ACTIVE_UTIL_PCT_MAX12M
        from { UPSTREAM_ASSET[1] } a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
        left outer join base c on
            a.account_id = c.account_id
        where
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


