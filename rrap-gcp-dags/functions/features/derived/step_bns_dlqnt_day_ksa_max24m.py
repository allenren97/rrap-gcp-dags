from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.TREATMENT_F",
    "features.PIT_STATUS_STEP",
    "features.TOT_NEW_BAL_AMT",
    "features.CR_LMT_AMT",
]
DOWNSTREAM_ASSET = "features.STEP_BNS_DLQNT_DAY_KSA_MAX24M"
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
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME FROM (
        with curr_accts as (
            SELECT
                a.BASEL_ACCT_ID
            FROM {UPSTREAM_ASSET[0]} a  -- ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
            INNER JOIN {UPSTREAM_ASSET[1]} t  -- ingestion.TM_DIM
            ON a.MTH_TM_ID = t.TM_ID
            INNER JOIN {UPSTREAM_ASSET[2]} b  -- features.MODEL_EXCL_F
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID and b.OBSN_DT = t.TM_LVL_END_DT
            INNER JOIN {UPSTREAM_ASSET[3]} c  -- features.WRITTEN_OUT_F
            ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID  and c.OBSN_DT = t.TM_LVL_END_DT
            INNER JOIN {UPSTREAM_ASSET[4]} d  -- features.TREATMENT_F
            ON c.BASEL_ACCT_ID = d.BASEL_ACCT_ID  and d.OBSN_DT = t.TM_LVL_END_DT
            INNER JOIN {UPSTREAM_ASSET[5]} e  -- features.PIT_STATUS_STEP
            on TRIM(a.STEP_PLN_AGRMNT_NUM) = TRIM(e.STEP_PLN_AGRMNT_NUM) and e.OBSN_DT = t.TM_LVL_END_DT
            INNER JOIN {UPSTREAM_ASSET[6]} f  -- features.TOT_NEW_BAL_AMT
            ON d.BASEL_ACCT_ID = f.BASEL_ACCT_ID  and f.OBSN_DT = t.TM_LVL_END_DT
            INNER JOIN {UPSTREAM_ASSET[7]} g  -- features.CR_LMT_AMT
            ON f.BASEL_ACCT_ID = g.BASEL_ACCT_ID  and g.OBSN_DT = t.TM_LVL_END_DT
            WHERE a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND a.STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(a.STEP_PLN_AGRMNT_NUM) <> ''
            AND UPPER(TRIM(b.MODEL_EXCL_F)) = 'N'
            AND UPPER(TRIM(c.WRITTEN_OUT_F)) = 'N'
            AND UPPER(TRIM(d.TREATMENT_F)) = 'A'
            AND UPPER(TRIM(e.PIT_STATUS_STEP)) IN ('CUR', 'DEF')
            AND (f.TOT_NEW_BAL_AMT > 0 OR g.CR_LMT_AMT > 0)
  ) SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        TRIM(ss.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
        MAX(ss.BNS_DLQNT_DAY) as STEP_BNS_DLQNT_DAY_KSA_MAX24M
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
    INNER JOIN curr_accts
    ON ss.BASEL_ACCT_ID = curr_accts.BASEL_ACCT_ID
    WHERE ss.MTH_TM_ID BETWEEN
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40 * 23
        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY TRIM(ss.STEP_PLN_AGRMNT_NUM)
    )
    """,
):
    pass


