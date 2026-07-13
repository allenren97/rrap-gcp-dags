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

MODEL = "dtl_lgdnd"

UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.MODEL_EXCL_F",
    "features.TREATMENT_F",
    "features.CR_LMT_AMT",
    "features.PRD_ID",
    "features.IND_DELQ_SUM24M",
    "features.AT29_MAX6M",
    "features.OS_BAL_AMT",
    "features.IND_JOINT",
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
        MODEL_EXCL_F AS (
            SELECT 
                * 
            FROM 
                features.MODEL_EXCL_F 
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
        CR_LMT_AMT AS (
            SELECT 
                * 
            FROM 
                features.CR_LMT_AMT 
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        OS_BAL_AMT AS (
            SELECT 
                * 
            FROM 
                features.OS_BAL_AMT 
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
        IND_DELQ_SUM24M AS (
            SELECT
                *
            FROM
                features.IND_DELQ_SUM24M
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        AT29_MAX6M AS (
            SELECT
                *
            FROM
                features.AT29_MAX6M
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        IND_JOINT AS (
            SELECT
                *
            FROM
                features.IND_JOINT
            WHERE 
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        )

        SELECT
            main.*,
            PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
            MODEL_EXCL_F.MODEL_EXCL_F,
            TREATMENT_F.TREATMENT_F,
            CR_LMT_AMT.CR_LMT_AMT,
            CAST(OS_BAL_AMT.OS_BAL_AMT AS DOUBLE) AS OS_BAL_AMT,
            IND_DELQ_SUM24M.IND_DELQ_SUM24M,
            AT29_MAX6M.AT29_MAX6M,
            IND_JOINT.IND_JOINT,
            PRD_ID.PRD_ID
        FROM
            main
            LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
            LEFT JOIN TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
            LEFT JOIN CR_LMT_AMT ON main.BASEL_ACCT_ID = CR_LMT_AMT.BASEL_ACCT_ID
            LEFT JOIN OS_BAL_AMT ON main.BASEL_ACCT_ID = OS_BAL_AMT.BASEL_ACCT_ID
            LEFT JOIN IND_DELQ_SUM24M ON main.BASEL_ACCT_ID = IND_DELQ_SUM24M.BASEL_ACCT_ID
            LEFT JOIN AT29_MAX6M ON main.BASEL_CUST_ID = AT29_MAX6M.BASEL_CUST_ID
            LEFT JOIN IND_JOINT ON main.BASEL_ACCT_ID = IND_JOINT.BASEL_ACCT_ID
            LEFT JOIN PRD_ID ON main.BASEL_ACCT_ID = PRD_ID.BASEL_ACCT_ID
        WHERE
        TREATMENT_F.TREATMENT_F = 'A'
        AND TRIM(UPPER(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG)) IN ('CUR')
        AND MODEL_EXCL_F.MODEL_EXCL_F IN ('N')
        and PRD_ID.PRD_ID IN ('S01', 'S02', 'S03', 'S04', 'S05', 'S06', 'S07', 'S08')
    """,
):
    pass

def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, NULL AS SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__dtl_lgdnd.export_acct_list", key="parquet") }}}}' b
        
    """,                                                                                                                                                                                                                                
):
    pass

def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__dtl_lgdnd.export_segment_input", key="parquet"
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
            '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__dtl_lgdnd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
