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

MODEL = "standalone_mor_pd"

# TODO: Make config configurable? Read the config for the model from a csv?

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.PIT_STATUS_ACCOUNT",
    "features.STEP_SUB_PORT",
    "features.TREATMENT_F",
    "features.TOTAL_BALANCE",
    "ingestion.MORT_MTH_SNAPSHOT",
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
            MOR_PD AS (SELECT
                              BASEL_ACCT_ID,
                              CASE
                                    WHEN PRIM_BASEL_CUST_ID <= 0 THEN NULL
                                    ELSE PRIM_BASEL_CUST_ID
                              END AS BASEL_CUST_ID,
                              STEP_PLN_AGRMNT_NUM
                        FROM
                              INGESTION.MORT_MTH_SNAPSHOT
                        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                        ),
            MODEL_EXCL_F AS (
                        SELECT * FROM FEATURES.MODEL_EXCL_F WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                        ),
            WRITTEN_OUT_F AS (
                        SELECT * FROM FEATURES.WRITTEN_OUT_F WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                        ),
            PIT_STATUS AS (
                        SELECT * FROM FEATURES.PIT_STATUS_ACCOUNT WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                        ),
            STEP_SUB_PORT AS (
                        SELECT * FROM FEATURES.STEP_SUB_PORT WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                        ),
            TREATMENT_F AS (
                        SELECT * FROM FEATURES.TREATMENT_F WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            TOTAL_BALANCE AS (
                        SELECT * FROM FEATURES.TOTAL_BALANCE WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )
            SELECT
                        MAIN.*,
                        MODEL_EXCL_F.MODEL_EXCL_F,
                        WRITTEN_OUT_F.WRITTEN_OUT_F,
                        PIT_STATUS.PIT_STATUS_ACCOUNT,
                        STEP_SUB_PORT.STEP_SUB_PORT,
                        TREATMENT_F.TREATMENT_F,
                        TOTAL_BALANCE.TOTAL_BALANCE
            FROM
                        MOR_PD AS MAIN
                        LEFT JOIN MODEL_EXCL_F ON MAIN.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
                        LEFT JOIN WRITTEN_OUT_F ON MAIN.BASEL_ACCT_ID = WRITTEN_OUT_F.BASEL_ACCT_ID
                        LEFT JOIN PIT_STATUS ON MAIN.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
                        LEFT JOIN STEP_SUB_PORT ON MAIN.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
                        LEFT JOIN TREATMENT_F ON MAIN.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
                        LEFT JOIN TOTAL_BALANCE ON MAIN.BASEL_ACCT_ID = TOTAL_BALANCE.BASEL_ACCT_ID
            WHERE
                        TRIM(MODEL_EXCL_F.MODEL_EXCL_F) = 'N'
                        AND TRIM(WRITTEN_OUT_F.WRITTEN_OUT_F) = 'N'
                        AND TRIM(PIT_STATUS.PIT_STATUS_ACCOUNT) = 'CUR'
                        AND MAIN.BASEL_CUST_ID <> -1
                        AND STEP_SUB_PORT.STEP_SUB_PORT = 'Standalone_MOR'
                        AND TREATMENT_F.TREATMENT_F = 'A'
                        AND TOTAL_BALANCE.TOTAL_BALANCE > 0
      """,
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        r"""{{ task_instance.xcom_pull(task_ids="scored__standalone_mor_pd.export_acct_list", key="parquet") }}""",
    ),
):
    pass


def get_score():
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="scored__standalone_mor_pd.export_acct_dv", key="parquet"
    )
    output_file = _get_score(
        f"{MODEL}_scoring_config.csv",
        input_file,
    )  # TODO check if file is empty before running?
    context["ti"].xcom_push(key="parquet", value=output_file)


# TODO model name? if big table
def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


# TODO model name? if big table
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__standalone_mor_pd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass