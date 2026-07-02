from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.model_utility import _get_segment


DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

MODEL = "step_mix_pd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.WRITTEN_OUT_F",
    "features.MODEL_EXCL_F",
    "features.TREATMENT_F",
    "features.PIT_STATUS_STEP",
    "features.STEP_SUB_PORT",
    "features.STEP_PRIM_CUST_ID",
    "FEATURES.OS_BAL_AMT_V2",
    "FEATURES.TOT_NEW_BAL_AMT",
    "FEATURES.TOTAL_BALANCE",
    "FEATURES.CR_LMT_AMT",
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
            MODEL_EXCL_F AS (
                SELECT
                    basel_acct_id,
                    MODEL_EXCL_F
                FROM
                    features.MODEL_EXCL_F
                WHERE
                    obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            WRITTEN_OUT_F AS (
                SELECT
                    basel_acct_id,
                    WRITTEN_OUT_F
                FROM
                    features.WRITTEN_OUT_F
                WHERE
                    obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            TREATMENT_F AS (
                SELECT
                    basel_acct_id,
                    TREATMENT_F
                FROM
                    features.TREATMENT_F
                WHERE
                    obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            pit_status_step AS (
                SELECT
                    basel_acct_id,
                    pit_status_step
                FROM
                    features.pit_status_step
                WHERE
                    obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            step_sub_port AS (
                SELECT
                    basel_acct_id,
                    step_sub_port
                FROM
                    features.step_sub_port
                WHERE
                    obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            os_bal_amt_v2 AS (
                SELECT
                    BASEL_ACCT_ID,
                    OS_BAL_AMT_V2
                FROM
                    FEATURES.OS_BAL_AMT_V2
                WHERE
                    OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            tot_new_bal_amt AS (
                SELECT
                    BASEL_ACCT_ID,
                    tot_new_bal_amt
                FROM
                    FEATURES.tot_new_bal_amt
                WHERE
                    OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            cr_lmt_amt AS (
                SELECT
                    BASEL_ACCT_ID,
                    CR_LMT_AMT
                FROM
                    FEATURES.CR_LMT_AMT
                WHERE
                    OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            total_balance AS (
                SELECT
                    BASEL_ACCT_ID,
                    TOTAL_BALANCE
                FROM
                    FEATURES.TOTAL_BALANCE
                WHERE
                    OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            mor AS (
                SELECT
                    TRIM(main.step_pln_agrmnt_num) AS step_pln_agrmnt_num,
                    main.basel_acct_id,
                    WRITTEN_OUT_F,
                    MODEL_EXCL_F,
                    total_balance as os_bal_amt,
                    total_balance as cr_lmt_amt,
                    pit_status_step
                FROM
                    ingestion.mort_mth_snapshot AS main
                    LEFT JOIN MODEL_EXCL_F ON MODEL_EXCL_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN WRITTEN_OUT_F ON WRITTEN_OUT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN TREATMENT_F ON TREATMENT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN pit_status_step ON pit_status_step.basel_acct_id = main.basel_acct_id
                    LEFT JOIN step_sub_port ON step_sub_port.basel_acct_id = main.basel_acct_id
                    LEFT JOIN total_balance ON total_balance.basel_acct_id = main.basel_acct_id
                WHERE
                    NULLIF(TRIM(main.step_pln_agrmnt_num), '') IS NOT NULL
                    AND TREATMENT_F = 'A'
                    AND TRIM(step_sub_port) = 'STEP_MIX'
                    AND pit_status_step in ('CUR','DEF')
                    AND mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            ),
            spl AS (
                SELECT
                    TRIM(main.step_pln_agrmnt_num) AS step_pln_agrmnt_num,
                    main.basel_acct_id,
                    WRITTEN_OUT_F,
                    MODEL_EXCL_F,
                    os_bal_amt_v2 as os_bal_amt,
                    os_bal_amt_v2 as cr_lmt_amt,
                    pit_status_step
                FROM
                    ingestion.basel_psnl_loan_mth_snapshot AS main
                    LEFT JOIN MODEL_EXCL_F ON MODEL_EXCL_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN WRITTEN_OUT_F ON WRITTEN_OUT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN TREATMENT_F ON TREATMENT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN pit_status_step ON pit_status_step.basel_acct_id = main.basel_acct_id
                    LEFT JOIN step_sub_port ON step_sub_port.basel_acct_id = main.basel_acct_id
                    LEFT JOIN os_bal_amt_v2 ON os_bal_amt_v2.basel_acct_id = main.basel_acct_id
                WHERE
                    NULLIF(TRIM(main.step_pln_agrmnt_num), '') IS NOT NULL
                    AND TREATMENT_F = 'A'
                    AND TRIM(step_sub_port) = 'STEP_MIX'
                    AND pit_status_step in ('CUR','DEF')
                    AND mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            ),
            ks AS (
                SELECT
                    TRIM(main.step_pln_agrmnt_num) AS step_pln_agrmnt_num,
                    main.basel_acct_id,
                    WRITTEN_OUT_F,
                    MODEL_EXCL_F,
                    tot_new_bal_amt.tot_new_bal_amt as os_bal_amt,
                    cr_lmt_amt.cr_lmt_amt as cr_lmt_amt,
                    pit_status_step
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS main
                    LEFT JOIN MODEL_EXCL_F ON MODEL_EXCL_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN WRITTEN_OUT_F ON WRITTEN_OUT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN TREATMENT_F ON TREATMENT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN pit_status_step ON pit_status_step.basel_acct_id = main.basel_acct_id
                    LEFT JOIN step_sub_port ON step_sub_port.basel_acct_id = main.basel_acct_id
                    LEFT JOIN tot_new_bal_amt ON tot_new_bal_amt.basel_acct_id = main.basel_acct_id
                    LEFT JOIN cr_lmt_amt ON cr_lmt_amt.basel_acct_id = main.basel_acct_id
                WHERE
                    NULLIF(TRIM(main.step_pln_agrmnt_num), '') IS NOT NULL
                    AND TREATMENT_F = 'A'
                    AND TRIM(step_sub_port) = 'STEP_MIX'
                    AND pit_status_step in ('CUR','DEF')
                    AND mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            ),
            uni AS (
                SELECT
                    *,
                    'SPL' AS SRC_SYS_CD
                FROM
                    spl
                UNION
                SELECT
                    *,
                    'MOR' AS SRC_SYS_CD
                FROM
                    mor
                UNION
                SELECT
                    *,
                    'KS' AS SRC_SYS_CD
                FROM
                    ks
            )
        SELECT
            uni.*,
            step.STEP_PRIM_CUST_ID
        FROM
            uni
            LEFT JOIN features.STEP_PRIM_CUST_ID AS step ON step.step_pln_agrmnt_num = uni.step_pln_agrmnt_num
            AND step.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    """,
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            b.*,
            a.SCORE
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="segmented__step_mix_pd.export_acct_list", key="parquet") }}}}' b
            LEFT JOIN (
                SELECT
                    BASEL_ACCT_ID,
                    VAR_SCORE AS SCORE
                FROM
                    {UPSTREAM_ASSET[0]}
                WHERE
                    TRIM(VAR_NAME) = 'SCORE'
                    AND obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND TRIM(UPPER(model)) = TRIM(UPPER('{MODEL}'))
                    AND TRIM(UPPER(stream)) = TRIM(
                        UPPER('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}')
                    )
            ) a ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__step_mix_pd.export_segment_input", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__step_mix_pd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
