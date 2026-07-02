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

MODEL = "dtl_lgdd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.MODEL_EXCL_F",
    "features.TREATMENT_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.SUB_PORT_F",
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
        main AS (
            SELECT
                BASEL_ACCT_ID,
                CASE
                    WHEN prim_basel_cust_id <= 0 THEN NULL
                    ELSE prim_basel_cust_id
                END AS BASEL_CUST_ID
            FROM
                ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
            WHERE 
                mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),
        SUB_PORT_F AS (
            SELECT
                *
            FROM
                features.SUB_PORT_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),  
        TREATMENT_F AS (
            SELECT
                *
            FROM
                features.TREATMENT_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        PIT_STATUS AS (
            SELECT
                *
            FROM
                features.PIT_STATUS_CROSS_DEFAULT_ORIG
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        )
        SELECT
            main.*,
            SUB_PORT_F.SUB_PORT_F,
            TREATMENT_F.TREATMENT_F,
            PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
        FROM
            main
        LEFT JOIN 
            SUB_PORT_F ON main.BASEL_ACCT_ID = SUB_PORT_F.BASEL_ACCT_ID
        LEFT JOIN 
            TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
        LEFT JOIN 
            PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
        WHERE
            SUB_PORT_F.SUB_PORT_F = 'DIRECT'
            AND TREATMENT_F.TREATMENT_F = 'A'
            AND PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG = 'DEF'
    """,
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        r"""{{ task_instance.xcom_pull(task_ids="scored__dtl_lgdd.export_acct_list", key="parquet") }}""",
    ),
):
    pass


def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="scored__dtl_lgdd.export_acct_dv", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__dtl_lgdd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
