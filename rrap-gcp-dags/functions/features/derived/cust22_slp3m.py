from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["features.CUST22"]
DOWNSTREAM_ASSET = "features.CUST22_SLP3M"
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
        with min_dt as (
            select min(obsn_dt) as min_dt, basel_cust_id 
            from features.CUST22 
            where obsn_dt between 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 2 MONTH)
                and '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            group by basel_cust_id
    ), prev as (
        select a.*, b.CUST22 
        From min_dt a inner join features.CUST22 b 
        on a.BASEL_CUST_ID = b.BASEL_CUST_ID and a.min_dt = b.obsn_dt
    ), curr as (
        select * from features.CUST22 where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ), base as (
        select curr.BASEL_CUST_ID, curr.CUST22, prev.CUST22 as cust22_prev, 
            date_diff('month', prev.MIN_DT, curr.OBSN_DT) as age 
        from curr inner join prev 
        on curr.BASEL_CUST_ID = prev.BASEL_CUST_ID
    ) select 
        basel_cust_id, 
        (case 
            when age = 0 then NULL 
            else (cust22 - cust22_prev) / age 
        end) as cust22_slp3m, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as obsn_dt 
    from base
        )
    """,
):
    pass


