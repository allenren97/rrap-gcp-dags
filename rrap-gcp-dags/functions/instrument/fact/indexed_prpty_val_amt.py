import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "features.PROP_VAL_NEW_CMA",
    "instruments.INDEX_TERANETV_CMA",
]

DOWNSTREAM_ASSET = "instruments.INDEXED_PRPTY_VAL_AMT"

DEPENDENCIES = {
    "export_mor": ["duckdb_clear"],
    "export_tng": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


# MOR
def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH latest_mort_val AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY MORT_NUM
                       ORDER BY OBSN_DT DESC
                   ) AS rn
            FROM {UPSTREAM_ASSET[4]}
        )
        WHERE rn = 1
    ),

    mor_joined AS (
        SELECT
            mor.BASEL_ACCT_ID,
            f.INDEX_TERANETV,
            f.OBSN_DT AS feature_obsn_dt,
            ROW_NUMBER() OVER (
                PARTITION BY mor.BASEL_ACCT_ID
                ORDER BY f.INDEX_TERANETV DESC NULLS LAST,
                         f.OBSN_DT DESC
            ) AS rn
        FROM {UPSTREAM_ASSET[0]} mor
        LEFT JOIN latest_mort_val f
            ON mor.MORT_NUM = f.MORT_NUM
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        INDEX_TERANETV AS INDEXED_PRPTY_VAL_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
    FROM mor_joined
    WHERE rn = 1
    """
):
    pass


# TNG
def export_tng(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH tng_joined AS (
        SELECT
            dim.BASEL_ACCT_ID,
            prop.PROP_VAL_NEW,
            tng.MONTH_END_DT,
            ROW_NUMBER() OVER (
                PARTITION BY dim.BASEL_ACCT_ID
                ORDER BY prop.PROP_VAL_NEW DESC NULLS LAST,
                         tng.MONTH_END_DT DESC
            ) AS rn
        FROM {UPSTREAM_ASSET[1]} dim
        LEFT JOIN {UPSTREAM_ASSET[2]} tng
            ON dim.SRC_APP_CD = 'TNG-MOR'
           AND dim.SRC_SYS_DEL_F != 'Y'
           AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        LEFT JOIN {UPSTREAM_ASSET[3]} prop
            ON prop.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
           AND prop.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        PROP_VAL_NEW AS INDEXED_PRPTY_VAL_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
    FROM tng_joined
    WHERE rn = 1
    """
):
    pass


# CLEAR
def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """
):
    pass


# LOAD
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            INDEXED_PRPTY_VAL_AMT,
            STREAM
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_prpty_val_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_prpty_val_amt.export_tng", key="parquet") }}}}'
        ], union_by_name = true)
    )
    """
):
    pass