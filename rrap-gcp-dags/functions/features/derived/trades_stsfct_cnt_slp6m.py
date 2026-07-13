from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "ingestion.TNG_CUST_TU",
]
DOWNSTREAM_ASSET = "features.TRADES_STSFCT_CNT_SLP6M"
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
                ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY month_end_dt DESC) AS row_number,
                month_end_dt,
                trim(account_id) AS account_id,
                CASE
                    WHEN COUNT(distinct month_end_dt) OVER (PARTITION BY account_id) > 1 THEN (
                            (FIRST_VALUE(TRADES_STSFCT_CNT) OVER (PARTITION BY account_id ORDER BY month_end_dt DESC)-FIRST_VALUE(TRADES_STSFCT_CNT) OVER (PARTITION BY account_id ORDER BY month_end_dt)) / (COUNT(distinct month_end_dt) OVER (PARTITION BY account_id)-1)
                        )::decimal(13,2)
                ELSE null
                END as TRADES_STSFCT_CNT_SLP6M
            from { UPSTREAM_ASSET[2] }
            where
                month_end_dt between
                    date_add(date '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -interval 5 month)
                    and
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        select
            a.month_end_dt as obsn_dt,
            b.basel_acct_id,
            trim(a.account_id) AS account_id,
            c.TRADES_STSFCT_CNT_SLP6M
        from { UPSTREAM_ASSET[1] } a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
        left outer join base c on
            c.row_number = 1
            and a.month_end_dt = c.month_end_dt
            and a.account_id = c.account_id
        where
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


