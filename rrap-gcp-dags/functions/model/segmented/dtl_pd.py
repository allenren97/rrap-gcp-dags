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

MODEL = "dtl_pd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.MODEL_EXCL_F_V2",
    "features.TREATMENT_F",
    "features.SUB_PORT_F",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
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
    PIT_STATUS AS (
        SELECT 
            *
        FROM
            features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    MODEL_EXCL_F_V2 AS (
        SELECT
            * 
        FROM
            features.MODEL_EXCL_F_V2
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
    SUB_PORT_F AS (
    	SELECT 
    		*
    	FROM
    		features.SUB_PORT_F
    	WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
    
    SELECT
        main.*,
        PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
        MODEL_EXCL_F_V2.MODEL_EXCL_F_V2,
        TREATMENT_F.TREATMENT_F
    FROM 
        main
        LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
        LEFT JOIN MODEL_EXCL_F_V2 ON main.BASEL_ACCT_ID = MODEL_EXCL_F_V2.BASEL_ACCT_ID
        LEFT JOIN TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
        LEFT JOIN SUB_PORT_F ON main.BASEL_ACCT_ID = SUB_PORT_F.BASEL_ACCT_ID
    WHERE
        PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR', 'DEF')
        AND TREATMENT_F.TREATMENT_F = 'A'
        AND SUB_PORT_F = 'DIRECT'
""",
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, a.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__dtl_pd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[0]} where trim(VAR_NAME) = 'SCORE' and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))) a on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID

    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__dtl_pd.export_segment_input", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__dtl_pd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
