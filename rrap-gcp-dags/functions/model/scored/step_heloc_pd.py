from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import (
    _get_upstream_assets,
    _get_input_sql,
    _get_score,
)

MODEL = "step_heloc_pd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.TREATMENT_F",
    "features.STEP_SUB_PORT",
    "features.STEP_PRIM_CUST_ID",
    "features.PIT_STATUS_STEP",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.TOT_NEW_BAL_AMT",
    "features.CR_LMT_AMT",
] + (_get_upstream_assets(f"{MODEL}_scoring_config.csv"))

DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SCORE"

DEPENDENCIES = {
    "export_acct_list": ["export_acct_dv"],
    "export_acct_dv": ["get_score"],
    "get_score": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
SELECT
    s.STEP_PLN_AGRMNT_NUM,
    c.STEP_PRIM_CUST_ID AS BASEL_CUST_ID,
    s.BASEL_ACCT_ID
FROM
    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT s
INNER JOIN features.TREATMENT_F tf ON
    s.BASEL_ACCT_ID = tf.BASEL_ACCT_ID
    AND tf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND tf.TREATMENT_F = 'A'
INNER JOIN features.STEP_SUB_PORT sp ON
    s.BASEL_ACCT_ID = sp.BASEL_ACCT_ID
    AND sp.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND UPPER(TRIM(sp.STEP_SUB_PORT)) = 'STEP_HELOC'
INNER JOIN features.STEP_PRIM_CUST_ID c ON
    s.STEP_PLN_AGRMNT_NUM = c.STEP_PLN_AGRMNT_NUM
    AND c.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
INNER JOIN features.PIT_STATUS_STEP d ON
    s.BASEL_ACCT_ID = d.BASEL_ACCT_ID
    AND d.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND d.PIT_STATUS_STEP = 'CUR'
INNER JOIN features.MODEL_EXCL_F e ON
    s.BASEL_ACCT_ID = e.BASEL_ACCT_ID
    AND e.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND e.MODEL_EXCL_F = 'N'
INNER JOIN features.WRITTEN_OUT_F f ON
    s.BASEL_ACCT_ID = f.BASEL_ACCT_ID
    AND f.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND f.WRITTEN_OUT_F = 'N'
LEFT OUTER JOIN features.TOT_NEW_BAL_AMT g ON
    s.BASEL_ACCT_ID = g.BASEL_ACCT_ID
    AND g.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
LEFT OUTER JOIN features.CR_LMT_AMT h ON
    s.BASEL_ACCT_ID = h.BASEL_ACCT_ID
    AND h.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
WHERE
    s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    AND (g.TOT_NEW_BAL_AMT > 0 OR h.CR_LMT_AMT > 0)
GROUP BY
    s.STEP_PLN_AGRMNT_NUM,
    c.STEP_PRIM_CUST_ID,
    s.BASEL_ACCT_ID
""",
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        f"""{{{{ task_instance.xcom_pull(task_ids="scored__step_heloc_pd.export_acct_list", key="parquet") }}}}""",
    ),
):
    pass


def get_score():
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"scored__step_heloc_pd.export_acct_dv", key="parquet"
    )
    output_file = _get_score(
        f"{MODEL}_scoring_config.csv", input_file
    )  # TODO check if file is empty before running?
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__step_heloc_pd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass

