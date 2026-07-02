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

MODEL = "dtl_lgdd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.MODEL_EXCL_F_V2",
    "features.TREATMENT_F",
    "features.PRD_ID",
    "features.MONTH_DEF",
    "features.OS_BAL_AMT_V2",
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
                basel_acct_id,
                CASE
                    WHEN prim_basel_cust_id <= 0 THEN NULL
                    ELSE prim_basel_cust_id
                END AS BASEL_CUST_ID
            FROM
                ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
            WHERE
                MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
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

        PRD_ID AS (
        	SELECT
        		* 
        	FROM
        		features.PRD_ID
        	WHERE
        		obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),

        MONTH_DEF AS (
            SELECT 
                * 
            FROM 
                features.MONTH_DEF
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),

        OS_BAL_AMT_V2 AS (
            SELECT 
                * 
            FROM 
                features.OS_BAL_AMT_V2 
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),

        TOT_CRNT_BAL_AMT AS (
            SELECT 
                BASEL_ACCT_ID,
                TOT_CRNT_BAL_AMT
            FROM
                ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
            WHERE
                MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),

        SUB_PORT_F AS (
            SELECT 
                *
            FROM
                features.SUB_PORT_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        )

        SELECT
            main.*,
            PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
            MODEL_EXCL_F_V2.MODEL_EXCL_F_V2,
            TREATMENT_F.TREATMENT_F,
            MONTH_DEF.MONTH_DEF,
            OS_BAL_AMT_V2.OS_BAL_AMT_V2 AS OS_BAL_AMT_V2_LOWER,
            OS_BAL_AMT_V2.OS_BAL_AMT_V2 AS OS_BAL_AMT_V2_UPPER,
            PRD_ID.PRD_ID,
            TOT_CRNT_BAL_AMT.TOT_CRNT_BAL_AMT,
            SUB_PORT_F.SUB_PORT_F
        FROM
            main
            LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN MODEL_EXCL_F_V2 ON main.BASEL_ACCT_ID = MODEL_EXCL_F_V2.BASEL_ACCT_ID
            LEFT JOIN TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
            LEFT JOIN MONTH_DEF ON main.BASEL_ACCT_ID = MONTH_DEF.BASEL_ACCT_ID
            LEFT JOIN OS_BAL_AMT_V2 ON main.BASEL_ACCT_ID = OS_BAL_AMT_V2.BASEL_ACCT_ID
            LEFT JOIN PRD_ID ON main.BASEL_ACCT_ID = PRD_ID.BASEL_ACCT_ID
            LEFT JOIN TOT_CRNT_BAL_AMT ON main.BASEL_ACCT_ID = TOT_CRNT_BAL_AMT.BASEL_ACCT_ID
            LEFT JOIN SUB_PORT_F ON main.BASEL_ACCT_ID = SUB_PORT_F.BASEL_ACCT_ID
        WHERE
            TRIM(UPPER(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG)) = 'DEF'
            AND TREATMENT_F.TREATMENT_F = 'A'
            AND SUB_PORT_F = 'DIRECT'
    """,
):
    pass

def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            a.*,
            b.SCORE
        FROM '{{{{ task_instance.xcom_pull(task_ids="segmented__dtl_lgdd.export_acct_list", key="parquet") }}}}' a
        LEFT JOIN (
            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS SCORE
            FROM {UPSTREAM_ASSET [0] }
            WHERE trim(VAR_NAME) = 'SCORE'
                AND obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND trim(upper(model)) = trim(upper('{MODEL}'))
                AND trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
            ) b
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    """,                                                                                                                                                                                                                             
):
    pass

def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__dtl_lgdd.export_segment_input", key="parquet"
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
            '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__dtl_lgdd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass