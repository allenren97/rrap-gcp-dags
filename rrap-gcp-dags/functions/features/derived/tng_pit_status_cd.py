from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "features.TNG_FINAL_DEFAULT_IND",
]
DOWNSTREAM_ASSET = "features.TNG_PIT_STATUS_CD"
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
            trim(a.account_id) as account_id,
            case
                when a.close_dt is not null or a.end_principal_balance <= 0 then 'CLO'
                when upper(c.tng_final_default_ind)='Y' then 'DEF'
                else 'CUR'
            end as tng_pit_status_cd
        from { UPSTREAM_ASSET[1] } a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
        left outer join { UPSTREAM_ASSET[2] } c on
            a.month_end_dt = c.obsn_dt
            and a.account_id = c.account_id
        where
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


