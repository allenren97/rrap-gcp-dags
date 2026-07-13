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

MODEL = "mor_lgdd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.TOTAL_BALANCE",
    "features.INSURANCE",
    "features.BULK_IND",
    "features.MONTH_DEF_SINCE_LAST_DEF",
    "ingestion.MORT_MTH_SNAPSHOT",
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
    SELECT
        main.basel_acct_id,
        main.mort_num,
        main.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
        main.CRNT_BAL_AMT AS CURRENT_BALANCE,
        main.TOT_SUSP_BAL_AMT AS TOTAL_SUSPENSE,
        main.PD_OFF_F AS FULL_PAID_OFF_F,
        TOTAL_BALANCE,
        INSURANCE,
        BULK_IND,
        MONTH_DEF_SINCE_LAST_DEF
    FROM
    (
        SELECT
            basel_acct_id,
            mort_num,
            PRIM_BASEL_CUST_ID,
            CRNT_BAL_AMT,
            TOT_SUSP_BAL_AMT,
            PD_OFF_F
        FROM
            ingestion.MORT_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ) AS main
    LEFT JOIN (
        SELECT
            *
        FROM
            features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) AS PIT_STATUS ON main.BASeL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT
            *
        FROM
            features.TOTAL_BALANCE
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) AS TOT ON TOT.BASeL_ACCT_ID = main.BASeL_ACCT_ID
    LEFT JOIN (
        SELECT
            *
        FROM
            features.INSURANCE
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) AS ind ON ind.mort_num::bigint = main.mort_num::bigint
    LEFT JOIN (
        SELECT
            *
        FROM
            features.BULK_IND
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) AS bulk ON bulk.mort_num::bigint = main.mort_num::bigint
    LEFT JOIN (
        SELECT
            *
        FROM
            features.MONTH_DEF_SINCE_LAST_DEF
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) AS MONTH_DEF_SINCE_LAST_DEF ON MONTH_DEF_SINCE_LAST_DEF.basel_acct_id = main.basel_acct_id
WHERE
    TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF'
""",
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, a.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__mor_lgdd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[0]} 
        where trim(VAR_NAME) = 'SCORE' 
        and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        and trim(upper(model)) = trim(upper('{MODEL}')) 
        and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))) a on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID

    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__mor_lgdd.export_segment_input", key="parquet"
    )
    stream = context["ti"].xcom_pull(task_ids="handle_month_context", key="stream").lower()
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
        stream = stream,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__mor_lgdd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
