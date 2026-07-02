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

MODEL = "sslb_lgdnd"

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.HELOC_F",
    "features.BASEL_PRD_CD",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.CONSM_PRD_TREATMNT_CD",
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
    a.PRIM_BASEL_CUST_ID as BASEL_CUST_ID,
    a.BASEL_ACCT_ID,
    a.BNS_DLQNT_DAY,
    b.PIT_STATUS_CROSS_DEFAULT_ORIG as PIT_STAT_VER_2_CD,
    CASE
        WHEN (
            (a.TOT_NEW_BAL_AMT <=0 and a.CR_LMT_AMT <= 0)
            or
            (a.BLOCK_RECL_CD='B4')
            or
            (a.BLOCK_RECL_CD='V' and a.ACCT_CLS_RSN_CD in ('G1','G2','G3'))
            or
            (a.BLOCK_RECL_CD='VW' and a.ACCT_CLS_RSN_CD='I3')
        ) THEN 'Y'
        ELSE 'N'
    END as MODEL_EXCLUSION_F
FROM {UPSTREAM_ASSET[0]} a
INNER JOIN {UPSTREAM_ASSET[1]} b ON
    a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    AND b.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND b.SRC_SYS_CD ='KS'
INNER JOIN {UPSTREAM_ASSET[2]} c ON
    a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
    AND b.OBSN_DT = c.OBSN_DT
    AND c.HELOC_F = 'N'
INNER JOIN {UPSTREAM_ASSET[3]} d ON
    a.BASEL_ACCT_ID = d.BASEL_ACCT_ID
    AND b.OBSN_DT = d.OBSN_DT
    AND d.BASEL_PRD_CD = 'SL B'
INNER JOIN {UPSTREAM_ASSET[4]} e ON
    a.BASEL_ACCT_ID = e.BASEL_ACCT_ID
    AND b.OBSN_DT = e.OBSN_DT
    AND e.CONSM_SCORECRD_EXCLSN_F = 'Y'
INNER JOIN {UPSTREAM_ASSET[5]} f ON
    a.BASEL_ACCT_ID = f.BASEL_ACCT_ID
    AND b.OBSN_DT = f.OBSN_DT
    AND f.CONSM_PRD_TREATMNT_CD = 'A'
WHERE
    a.MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
AND
    b.PIT_STATUS_CROSS_DEFAULT_ORIG NOT IN ('DEF', 'CHG')
    """,
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, null as SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__sslb_lgdnd.export_acct_list", key="parquet") }}}}' b
    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"segmented__sslb_lgdnd.export_segment_input", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__sslb_lgdnd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
