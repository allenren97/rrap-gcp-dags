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

MODEL = "loc_lgdd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "features.HELOC_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.MONTH_DEF_24M",
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
    sql=r"""
        WITH main AS (
                SELECT
                    basel_acct_id,
                    CASE
                        WHEN prim_basel_cust_id <= 0 THEN NULL
                        ELSE prim_basel_cust_id
                    END AS BASEL_CUST_ID
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
            MONTH_DEF_24M AS (
                select * from features.MONTH_DEF_24M where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            CONSM_PRD_TREATMNT_CD AS (
                select * from features.CONSM_PRD_TREATMNT_CD where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )
        SELECT
            main.*,
            BASEL_PRD_CD.BASEL_PRD_CD,
            CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F,
            PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
            HELOC_F.HELOC_F,
            MONTH_DEF_24M.MONTH_DEF_24M,
            CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD
        FROM main
            LEFT JOIN BASEL_PRD_CD ON main.BASEL_ACCT_ID = BASEL_PRD_CD.BASEL_ACCT_ID
            LEFT JOIN CONSM_SCORECRD_EXCLSN_F ON main.BASEL_ACCT_ID = CONSM_SCORECRD_EXCLSN_F.BASEL_ACCT_ID
            LEFT JOIN HELOC_F AS HELOC_F ON main.BASEL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
            LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN MONTH_DEF_24M ON main.BASEL_ACCT_ID = MONTH_DEF_24M.BASEL_ACCT_ID
            LEFT JOIN CONSM_PRD_TREATMNT_CD ON main.BASEL_ACCT_ID = CONSM_PRD_TREATMNT_CD.BASEL_ACCT_ID
        WHERE
            TRIM(BASEL_PRD_CD.BASEL_PRD_CD) = 'LOC'
            AND TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF'
            AND TRIM(CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD) = 'A'
            AND TRIM(HELOC_F.HELOC_F) = 'N'
    """,
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, a.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__loc_lgdd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[0]} where trim(VAR_NAME) = 'SCORE' and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))) a on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID

    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__loc_lgdd.export_segment_input", key="parquet"
    )
    stream = (
        context["ti"].xcom_pull(task_ids="handle_month_context", key="stream").lower()
    )
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
        stream =stream,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__loc_lgdd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
