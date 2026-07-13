from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import _get_segment

MODEL = "standalone_mor_lgdnd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.PIT_STATUS_ACCOUNT",
    "features.TREATMENT_F",
    "features.STEP_SUB_PORT",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.TOTAL_BALANCE",
    "features.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF",
    "features.INSURANCE_F",
]

DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SEGMENT"

DEPENDENCIES = {
    "export_acct_list": ["get_segment"],
    "get_segment": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
    SELECT
        main.basel_acct_id,
        main.mort_num,
        main.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        MODEL_EXCL_F.MODEL_EXCL_F,
        WRITTEN_OUT_F.WRITTEN_OUT_F,
        TREATMENT_F.TREATMENT_F,
        TOTAL_BALANCE.TOTAL_BALANCE,
        STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF,
        INSURANCE_F.INSURANCE_F,
        NULL AS SCORE
    FROM
        (
            SELECT
                basel_acct_id,
                mort_num,
                PRIM_BASEL_CUST_ID,
                TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
            FROM
                ingestion.MORT_MTH_SNAPSHOT
            WHERE
                mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            GROUP BY
                basel_acct_id,
                mort_num,
                PRIM_BASEL_CUST_ID,
                TRIM(STEP_PLN_AGRMNT_NUM)
        ) AS main
        LEFT JOIN (
            SELECT
                *
            FROM
                features.PIT_STATUS_ACCOUNT
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                *
            FROM
                features.TREATMENT_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                STEP_SUB_PORT,
                BASEL_ACCT_ID
            FROM
                features.STEP_SUB_PORT
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
          ) AS STEP_SUB_PORT ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                *
            FROM
                features.MODEL_EXCL_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                *
            FROM
                features.WRITTEN_OUT_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS WRITTEN_OUT_F ON main.BASEL_ACCT_ID = WRITTEN_OUT_F.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                *
            FROM
                features.TOTAL_BALANCE
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS TOTAL_BALANCE ON main.BASEL_ACCT_ID = TOTAL_BALANCE.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                *
            FROM
                features.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF ON main.BASEL_ACCT_ID = STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT
                *
            FROM
                features.INSURANCE_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) AS INSURANCE_F ON main.BASEL_ACCT_ID = INSURANCE_F.BASEL_ACCT_ID
    WHERE TRIM(PIT_STATUS.PIT_STATUS_ACCOUNT) = 'CUR'
    AND TRIM(TREATMENT_F) = 'A'
    AND TRIM(UPPER(STEP_SUB_PORT)) = 'STANDALONE_MOR'
""",
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__standalone_mor_lgdnd.export_acct_list", key="parquet"
    )
    output_file = _get_segment(
        f"{MODEL}_segmentation_config.csv",
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__standalone_mor_lgdnd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
