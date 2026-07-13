import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.STEP_PRIM_CUST_ID",
]
DOWNSTREAM_ASSET = 'features.STEP_HGST_DLQNT_MTH_MTG_MAX12M'
DEPENDENCIES = {
    'duckdb_clear_step_hgst_dlqnt_mth_mtg_max12m': ['duckdb_derive_step_hgst_dlqnt_mth_mtg_max12m'],
}


def duckdb_clear_step_hgst_dlqnt_mth_mtg_max12m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_step_hgst_dlqnt_mth_mtg_max12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME FROM (
        with
        pop as (
            select
                s.prim_basel_cust_id
                , s.basel_acct_id
            from {UPSTREAM_ASSET[0]} s
            where
                s.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                and s.prim_basel_cust_id in (
                    select step_prim_cust_id
                    from {UPSTREAM_ASSET[1]}
                    where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                )
        )
        , max12m as (
            select
                p.prim_basel_cust_id
                , max(s.dlqnt_mth) as max_dlqnt_mth
            from pop p
            left outer join {UPSTREAM_ASSET[0]} s on
                p.basel_acct_id = s.basel_acct_id
            where
                s.mth_tm_id >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-440 and s.mth_tm_id <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                and upper(trim(s.comm_tp)) = 'RESIDENTIAL'
                and s.crnt_bal_amt > 0
                and upper(trim(s.pd_off_f)) = 'N'
            group by
                p.prim_basel_cust_id
        )
        select
            c.obsn_dt
            , c.step_pln_agrmnt_num
            , max(m.max_dlqnt_mth) as STEP_HGST_DLQNT_MTH_MTG_MAX12M
        from {UPSTREAM_ASSET[1]} c
        left outer join max12m m on
            c.step_prim_cust_id = m.prim_basel_cust_id
        where
            c.obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        group by
            c.obsn_dt
            , c.step_pln_agrmnt_num
    )
    """
):
    pass
