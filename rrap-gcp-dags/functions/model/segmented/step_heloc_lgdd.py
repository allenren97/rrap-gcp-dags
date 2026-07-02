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

MODEL = "step_heloc_lgdd"

UPSTREAM_ASSET = [
    "features.PIT_STATUS_STEP",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT",
    "features.STEP_MONTH_DEF_SINCE_LAST_DEF",
    "features.WRITTEN_OUT_F",
    "features.MODEL_EXCL_F",
    "features.STEP_SUB_PORT",
    "features.STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M",
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
    rvl AS (
        SELECT
            basel_acct_id,
	        TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
            CASE
                WHEN prim_basel_cust_id <= 0 THEN NULL
                ELSE prim_basel_cust_id
            END AS BASEL_CUST_ID
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND STEP_PLN_AGRMNT_NUM IS NOT NULL
            AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
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
            features.PIT_STATUS_STEP
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
    TOT_NEW_BAL_AMT AS (
        SELECT 
            *
        FROM
            features.TOT_NEW_BAL_AMT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    STEP_MONTH_DEF_SINCE_LAST_DEF AS (
        SELECT 
            *
        FROM
            features.STEP_MONTH_DEF_SINCE_LAST_DEF
        WHERE 
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    WRITTEN_OUT_F AS (
        SELECT 
            *
        FROM
            features.WRITTEN_OUT_F
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
    STEP_SUB_PORT AS ( 
	SELECT 
		*
	FROM 
		features.STEP_SUB_PORT
	WHERE 
		obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    
    STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M AS (
        SELECT
            *  
        FROM
            features.STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M
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
    )

SELECT
    main.BASEL_ACCT_ID,
    main.STEP_PLN_AGRMNT_NUM,
    PIT_STATUS.PIT_STATUS_STEP,
    CR_LMT_AMT.CR_LMT_AMT,
    TOT_NEW_BAL_AMT.TOT_NEW_BAL_AMT,
    STEP_MONTH_DEF_SINCE_LAST_DEF.STEP_MONTH_DEF_SINCE_LAST_DEF,
    WRITTEN_OUT_F.WRITTEN_OUT_F,
    MODEL_EXCL_F.MODEL_EXCL_F,
    STEP_SUB_PORT.STEP_SUB_PORT,
    STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M.STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M,
    TREATMENT_F,
     
FROM
    rvl AS main
    LEFT JOIN TREATMENT_F AS TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
    LEFT JOIN PIT_STATUS AS PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
    LEFT JOIN CR_LMT_AMT AS CR_LMT_AMT ON main.BASEL_ACCT_ID = CR_LMT_AMT.BASEL_ACCT_ID
    LEFT JOIN TOT_NEW_BAL_AMT AS TOT_NEW_BAL_AMT ON main.BASEL_ACCT_ID = TOT_NEW_BAL_AMT.BASEL_ACCT_ID    
    LEFT JOIN STEP_MONTH_DEF_SINCE_LAST_DEF AS STEP_MONTH_DEF_SINCE_LAST_DEF ON TRIM(main.STEP_PLN_AGRMNT_NUM) = STEP_MONTH_DEF_SINCE_LAST_DEF.STEP_PLN_AGRMNT_NUM
    LEFT JOIN WRITTEN_OUT_F AS WRITTEN_OUT_F ON main.BASEL_ACCT_ID = WRITTEN_OUT_F.BASEL_ACCT_ID
    LEFT JOIN MODEL_EXCL_F AS MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
    LEFT JOIN STEP_SUB_PORT AS STEP_SUB_PORT ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID 
    LEFT JOIN STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M AS STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M ON main.STEP_PLN_AGRMNT_NUM = STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M.STEP_PLN_AGRMNT_NUM
WHERE
    TRIM(PIT_STATUS.PIT_STATUS_STEP) IN ('DEF')
    AND TRIM(STEP_SUB_PORT.STEP_SUB_PORT) = 'STEP_HELOC'
    AND TRIM(TREATMENT_F) = 'A'
     
""",
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, NULL AS SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__step_heloc_lgdd.export_acct_list", key="parquet") }}}}' b
        
    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__step_heloc_lgdd.export_segment_input", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__step_heloc_lgdd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
