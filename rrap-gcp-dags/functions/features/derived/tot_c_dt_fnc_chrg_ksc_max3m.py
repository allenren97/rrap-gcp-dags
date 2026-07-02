import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.TOT_C_DT_FNC_CHRG_KSC_MAX3M"
DEPENDENCIES = {
    "duckdb_clear_derive_tot_c_dt_fnc_chrg_ksc_max3m": [
        "duckdb_derive_tot_c_dt_fnc_chrg_ksc_max3m"
    ],
}


def duckdb_clear_derive_tot_c_dt_fnc_chrg_ksc_max3m(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_derive_tot_c_dt_fnc_chrg_ksc_max3m(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        with accounts as (
            select basel_acct_id 
            from ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT 
            where MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
            and PRIM_BASEL_CUST_ID > 0 group by basel_acct_id
        ),
        mthsum as(
            SELECT
                PRIM_BASEL_CUST_ID as BASEL_CUST_ID,
                sum(TOT_CYCL_TO_DT_FNCL_CHRG_AMT)::DECIMAL(31,3) AS TOT_C_DT_FNC_CHRG_KSC
            FROM {UPSTREAM_ASSET[0]} as main
            inner join accounts on accounts.basel_acct_id = main.basel_acct_id
            WHERE
                MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*2 AND 
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND PRIM_BASEL_CUST_ID > 0
            GROUP BY PRIM_BASEL_CUST_ID, MTH_TM_ID
        )
        select 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' as obsn_dt,
            BASEL_CUST_ID,
            max(TOT_C_DT_FNC_CHRG_KSC) as TOT_C_DT_FNC_CHRG_KSC_MAX3M
        from mthsum
        group by BASEL_CUST_ID
    )
    """,
):
    pass
