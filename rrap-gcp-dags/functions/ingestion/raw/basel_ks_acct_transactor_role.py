import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

DUCKLAKE_SCHEMA = "ingestion"
UPSTREAM_ASSET = [
    f"{DUCKLAKE_SCHEMA}.TM_DIM",
    f"{DUCKLAKE_SCHEMA}.BASEL_ACCT_PFRM_FACT",
    f"{DUCKLAKE_SCHEMA}.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = f"{DUCKLAKE_SCHEMA}.BASEL_KS_ACCT_TRANSACTOR_ROLE"
DEPENDENCIES = {
    'duckdb_delete' : ['duckdb_load'],
}


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT DISTINCT
            SNAP.MTH_TM_ID,
            SNAP.BASEL_ACCT_ID,
            CASE WHEN GREATEST(0, SNAP.BNS_DLQNT_DAY -30) > 0 THEN 'D' WHEN FACT.TRNSCTR_IND = 'T' THEN 'T' WHEN FACT.TRNSCTR_IND = 'N' THEN 'R' ELSE '' END AS ROLE_IND,
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM
            {UPSTREAM_ASSET[2]} SNAP
        LEFT JOIN (
            SELECT DISTINCT
                tm_id AS mth_tm_id,
                LPAD(a.ACCT_NUM, 23, '0') AS acct_num,
                TRNSCTR_IND
            FROM
                {UPSTREAM_ASSET[1]} a,
                {UPSTREAM_ASSET[0]} b
            WHERE
                a.mth_end_dt = b.TM_LVL_END_DT
                AND tm_lvl = 'Month'
                AND tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND src_sys_cd = 'KQ'
        ) FACT ON
            SNAP.MTH_TM_ID = FACT.MTH_TM_ID
            AND SNAP.ACCT_NUM = FACT.ACCT_NUM
        WHERE
            SNAP.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass


