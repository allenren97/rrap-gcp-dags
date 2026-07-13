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

MODEL = "step_heloc_ead"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.HELOC_F",
    "features.PIT_STATUS_STEP",
    "features.TREATMENT_F",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT",
    "features.STEP_PRIM_CUST_ID",
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
    sql=r"""
WITH
    rvl AS (
        SELECT
            basel_acct_id,
            trim(step_pln_agrmnt_num) as step_pln_agrmnt_num
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND STEP_PLN_AGRMNT_NUM IS NOT NULL
            AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
    ),
    HELOC_F AS (
        SELECT * FROM features.HELOC_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    PIT_STATUS AS (
        SELECT * FROM features.PIT_STATUS_STEP WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    TREATMENT_F AS (
        SELECT * FROM features.TREATMENT_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    CR_LMT_AMT AS (
        SELECT * FROM features.CR_LMT_AMT WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    TOT_NEW_BAL_AMT AS (
        SELECT * FROM features.TOT_NEW_BAL_AMT WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    MODEL_EXCL_F AS (
        SELECT * FROM features.MODEL_EXCL_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    WRITTEN_OUT_F AS (
        SELECT * FROM features.WRITTEN_OUT_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    STEP_PRIM_CUST_ID AS (
        SELECT * FROM features.STEP_PRIM_CUST_ID WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
SELECT
    main.*,
    HELOC_F.HELOC_F,
    PIT_STATUS.PIT_STATUS_STEP,
    TREATMENT_F.TREATMENT_F,
    CR_LMT_AMT.CR_LMT_AMT,
    TOT_NEW_BAL_AMT.TOT_NEW_BAL_AMT,
    MODEL_EXCL_F.MODEL_EXCL_F,
    WRITTEN_OUT_F.WRITTEN_OUT_F,
    STEP_PRIM_CUST_ID.STEP_PRIM_CUST_ID AS BASEL_CUST_ID
FROM
    rvl AS main
    LEFT JOIN HELOC_F ON main.BASEL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
    LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
    LEFT JOIN TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
    LEFT JOIN CR_LMT_AMT ON main.BASEL_ACCT_ID = CR_LMT_AMT.BASEL_ACCT_ID
    LEFT JOIN TOT_NEW_BAL_AMT ON main.BASEL_ACCT_ID = TOT_NEW_BAL_AMT.BASEL_ACCT_ID
    LEFT JOIN MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
    LEFT JOIN WRITTEN_OUT_F ON main.BASEL_ACCT_ID = WRITTEN_OUT_F.BASEL_ACCT_ID
    LEFT JOIN STEP_PRIM_CUST_ID on main.STEP_PLN_AGRMNT_NUM = STEP_PRIM_CUST_ID.STEP_PLN_AGRMNT_NUM
WHERE
    UPPER(TRIM(HELOC_F.HELOC_F)) = 'Y'
    AND UPPER(TRIM(PIT_STATUS.PIT_STATUS_STEP)) = 'CUR'
    AND UPPER(TRIM(TREATMENT_F.TREATMENT_F)) = 'A'
    AND (CR_LMT_AMT.CR_LMT_AMT > 0 OR TOT_NEW_BAL_AMT.TOT_NEW_BAL_AMT > 0)
    AND UPPER(TRIM(MODEL_EXCL_F.MODEL_EXCL_F)) = 'N'
    AND UPPER(TRIM(WRITTEN_OUT_F.WRITTEN_OUT_F)) = 'N'
""",
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        r"""{{ task_instance.xcom_pull(task_ids="scored__step_heloc_ead.export_acct_list", key="parquet") }}""",
    ),
):
    pass


def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="scored__step_heloc_ead.export_acct_dv", key="parquet"
    )
    output_file = _get_score(
        f"{MODEL}_scoring_config.csv",
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__step_heloc_ead.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
