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

MODEL = "itl_lgdnd"

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.SUB_PORT_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.MODEL_EXCL_F_V2",
    "features.TREATMENT_F",
    "features.WRITTEN_OUT_F",
    "features.PRD_ID",
    "features.OS_BAL_AMT_V2",
    f"models.{MODEL.upper()}_SCORE",
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
    sql=rf"""
        SELECT
            SS.BASEL_ACCT_ID,
            CASE
                WHEN SS.prim_basel_cust_id <= 0 THEN NULL
                ELSE SS.prim_basel_cust_id
            END AS BASEL_CUST_ID,
            A.SUB_PORT_F,
            B.PIT_STATUS_CROSS_DEFAULT_ORIG,
            C.MODEL_EXCL_F_V2,
            D.TREATMENT_F,
            F.PRD_ID,
            G.OS_BAL_AMT_V2
        FROM
            {UPSTREAM_ASSET[0]} SS
            LEFT JOIN {UPSTREAM_ASSET[1]} A ON A.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[2]} B ON B.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[3]} C ON C.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[4]} D ON D.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[5]} E ON E.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[6]} F ON F.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[7]} G ON G.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
        WHERE
            SS.MTH_TM_ID = {{{{task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}}}
            AND SS.RECD_STAT_CD = 4
            AND A.SUB_PORT_F = 'INDIRECT'
            AND A.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND B.PIT_STATUS_CROSS_DEFAULT_ORIG = 'CUR'
            AND B.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND C.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND D.TREATMENT_F = 'A'
            AND D.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND E.WRITTEN_OUT_F = 'N'
            AND E.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND F.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND G.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'

    """,
):
    pass

def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, A.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__itl_lgdnd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[8]} 
                   where trim(VAR_NAME) = 'SCORE' 
                   and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                   and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}')))  a 
        on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    """,                                                                                                                                                                                                                                
):
    pass

def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__itl_lgdnd.export_segment_input", key="parquet"
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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__itl_lgdnd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass