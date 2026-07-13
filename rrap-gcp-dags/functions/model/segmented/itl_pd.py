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

MODEL = "itl_pd"

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.MODEL_EXCL_F_V2",
    "features.TREATMENT_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.SUB_PORT_F",
    "features.OS_BAL_AMT_V2",
    "features.PRD_ID",
    "models.ITL_PD_SCORE",
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
            a.basel_acct_id,
            CASE
                WHEN a.prim_basel_cust_id <= 0 THEN NULL
                ELSE a.prim_basel_cust_id
            END AS BASEL_CUST_ID,
            TOT_CRNT_BAL_AMT as PRINCIPAL_BAL,
            b.MODEL_EXCL_F_V2,
            c.TREATMENT_F,
            d.PIT_STATUS_CROSS_DEFAULT_ORIG,
            e.SUB_PORT_F,
            f.OS_BAL_AMT_V2,
            g.PRD_ID
        FROM
            {UPSTREAM_ASSET[0]} a -- ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[1]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') b -- features.MODEL_EXCL_F_V2
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') c -- features.TREATMENT_F
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') d -- features.PIT_STATUS_CROSS_DEFAULT_ORIG
        ON c.BASEL_ACCT_ID = d.BASEL_ACCT_ID
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') e -- features.SUB_PORT_F
        ON d.BASEL_ACCT_ID = e.BASEL_ACCT_ID
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') f -- features.OS_BAL_AMT_V2
        ON e.BASEL_ACCT_ID = f.BASEL_ACCT_ID
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') g -- features.PRD_ID
        ON f.BASEL_ACCT_ID = g.BASEL_ACCT_ID
        WHERE a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND a.RECD_STAT_CD IN (4,5,6,7,8)
        AND UPPER(TRIM(c.TREATMENT_F)) = 'A'
        AND UPPER(TRIM(d.PIT_STATUS_CROSS_DEFAULT_ORIG)) IN ('CUR', 'DEF')
        AND UPPER(TRIM(e.SUB_PORT_F)) = 'INDIRECT'
    """,
):
    pass

def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, a.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__itl_pd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[7]} 
                   where trim(VAR_NAME) = 'SCORE' 
                   and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                   and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}')))  a 
        on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    """,                                                                                                                                                                                                                                
):
    pass

def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__itl_pd.export_segment_input", key="parquet"
    )
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, 
            '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__itl_pd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
