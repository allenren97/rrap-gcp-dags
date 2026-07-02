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

MODEL = "loc_ead"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.HELOC_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CONSM_PRD_TREATMNT_CD",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
] 

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
            CASE
                WHEN prim_basel_cust_id <= 0 THEN NULL
                ELSE prim_basel_cust_id
            END AS BASEL_CUST_ID
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    CONSM_SCORECRD_EXCLSN_F AS (
        SELECT
            *
        FROM
            features.CONSM_SCORECRD_EXCLSN_F
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    BASEL_PRD_CD as (
            SELECT 
                * 
            FROM 
                features.BASEL_PRD_CD 
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),

    HELOC_F AS (
        SELECT
            *
        FROM
            features.HELOC_F
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
    ),
    CONSM_PRD_TREATMNT_CD AS (
        SELECT
            *
        FROM
            features.CONSM_PRD_TREATMNT_CD
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
SELECT
    main.*,
    BASEL_PRD_CD.BASEL_PRD_CD,
    CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F,
    PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
    HELOC_F.HELOC_F,
    CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD
FROM
    rvl AS main
    LEFT JOIN BASEL_PRD_CD ON main.BASEL_ACCT_ID = BASEL_PRD_CD.BASEL_ACCT_ID
    LEFT JOIN CONSM_SCORECRD_EXCLSN_F AS CONSM_SCORECRD_EXCLSN_F ON main.BASEL_ACCT_ID = CONSM_SCORECRD_EXCLSN_F.BASEL_ACCT_ID
    LEFT JOIN HELOC_F AS HELOC_F ON main.BASEL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
    LEFT JOIN PIT_STATUS AS PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
    LEFT JOIN CONSM_PRD_TREATMNT_CD AS CONSM_PRD_TREATMNT_CD ON main.BASEL_ACCT_ID = CONSM_PRD_TREATMNT_CD.BASEL_ACCT_ID
WHERE
    TRIM(BASEL_PRD_CD.BASEL_PRD_CD) = 'LOC'
    AND TRIM(CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F) = 'N'
    AND TRIM(HELOC_F.HELOC_F) = 'N'
    AND TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR')
    AND TRIM(CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD) = 'A'
""",
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    config_file=f"{MODEL}_scoring_config.csv",
    config_type="model_dv",
):
    pass

def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"scored__loc_ead.export_acct_dv", key="parquet"
    )
    stream = (
        context["ti"].xcom_pull(task_ids="handle_month_context", key="stream").lower()
    )
    output_file = _get_score(
        filename=f"{MODEL}_scoring_config.csv",
        parquet=input_file,
        stream=stream,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = upper(trim('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, upper(trim('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}')) as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__loc_ead.get_score", key="parquet") }}}}'
        )
    """,
):
    pass


