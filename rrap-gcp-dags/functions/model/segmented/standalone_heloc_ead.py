from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import _get_segment


DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

MODEL = "standalone_heloc_ead"

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.HELOC_F",
    "features.TREATMENT_F",
    "features.STEP_SUB_PORT",
    "features.PIT_STATUS_ACCOUNT",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT",
]
DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SEGMENT"
DEPENDENCIES = {
    "export_acct_list": ["export_segment_input"],
    "export_segment_input": ["get_segment"],
    "get_segment": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
SELECT
    s.BASEL_ACCT_ID,
    t1.PIT_STATUS_ACCOUNT,
    t2.MODEL_EXCL_F,
    t3.WRITTEN_OUT_F,
    t4.CR_LMT_AMT,
    f2.TREATMENT_F,
    t5.TOT_NEW_BAL_AMT
FROM
    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT s
INNER JOIN features.HELOC_F f1 ON
    s.BASEL_ACCT_ID = f1.BASEL_ACCT_ID
    AND f1.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND f1.HELOC_F = 'Y'
INNER JOIN features.TREATMENT_F f2 ON
    s.BASEL_ACCT_ID = f2.BASEL_ACCT_ID
    AND f2.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND UPPER(TRIM(f2.TREATMENT_F)) = 'A'
INNER JOIN features.STEP_SUB_PORT sp ON
    s.BASEL_ACCT_ID = sp.BASEL_ACCT_ID
    AND sp.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND UPPER(TRIM(sp.STEP_SUB_PORT)) = 'STANDALONE_HELOC'
INNER JOIN features.PIT_STATUS_ACCOUNT t1 ON
    s.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
    AND t1.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND UPPER(TRIM(t1.PIT_STATUS_ACCOUNT)) IN ('CUR', 'DEF')
LEFT OUTER JOIN features.MODEL_EXCL_F t2 ON
    s.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
    AND t2.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
LEFT OUTER JOIN features.WRITTEN_OUT_F t3 ON
    s.BASEL_ACCT_ID = t3.BASEL_ACCT_ID
    AND t3.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
LEFT OUTER JOIN features.CR_LMT_AMT t4 ON
    s.BASEL_ACCT_ID = t4.BASEL_ACCT_ID
    AND t4.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
LEFT OUTER JOIN features.TOT_NEW_BAL_AMT t5 ON
    s.BASEL_ACCT_ID = t5.BASEL_ACCT_ID
    AND t5.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
WHERE
    s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
""",
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, null as SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__standalone_heloc_ead.export_acct_list", key="parquet") }}}}' b
    """,
):
    pass


def get_segment():
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"segmented__standalone_heloc_ead.export_segment_input", key="parquet"
    )
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
    )
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__standalone_heloc_ead.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
