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

MODEL = "loc_lgdnd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "features.BASEL_PRD_CD",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.HELOC_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CONSM_PRD_TREATMNT_CD",
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
                    CASE
                        WHEN prim_basel_cust_id <= 0 THEN NULL
                        ELSE prim_basel_cust_id
                    END AS BASEL_CUST_ID,
                    nullif(trim(step_pln_agrmnt_num), '') as step_pln_agrmnt_num
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            ),
            BASEL_PRD_CD as (
                select * from features.BASEL_PRD_CD where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            CONSM_SCORECRD_EXCLSN_F as (
                select * from features.CONSM_SCORECRD_EXCLSN_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            HELOC_F as (
                select * from features.HELOC_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            PIT_STATUS as (
                select * from features.PIT_STATUS_CROSS_DEFAULT_ORIG where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            CONSM_PRD_TREATMNT_CD  as (
                select * from features.CONSM_PRD_TREATMNT_CD where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
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
            LEFT JOIN CONSM_SCORECRD_EXCLSN_F ON main.BASeL_ACCT_ID = CONSM_SCORECRD_EXCLSN_F.BASEL_ACCT_ID
            LEFT JOIN HELOC_F AS HELOC_F ON main.BASeL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
            LEFT JOIN PIT_STATUS ON main.BASeL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN CONSM_PRD_TREATMNT_CD AS CONSM_PRD_TREATMNT_CD ON main.BASeL_ACCT_ID = CONSM_PRD_TREATMNT_CD.BASEL_ACCT_ID
        WHERE
            TRIM(BASEL_PRD_CD.BASEL_PRD_CD) = 'LOC'
            AND TRIM(CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F) = 'N'
            AND TRIM(HELOC_F.HELOC_F) = 'N'
            AND TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
            AND TRIM(CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD) = 'A'
    """,
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        r"""{{ task_instance.xcom_pull(task_ids="scored__loc_lgdnd.export_acct_list", key="parquet") }}""",
    ),
):
    pass


def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="scored__loc_lgdnd.export_acct_dv", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__loc_lgdnd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
