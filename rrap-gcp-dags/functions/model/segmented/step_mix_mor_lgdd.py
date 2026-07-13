from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.model_utility import _get_segment


DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

MODEL = "step_mix_mor_lgdd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "INGESTION.MORT_MTH_SNAPSHOT",
    "INGESTION.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "INGESTION.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "FEATURES.WRITTEN_OUT_F",
    "FEATURES.MODEL_EXCL_F",
    "FEATURES.TREATMENT_F",
    "FEATURES.PIT_STATUS_STEP",
    "FEATURES.STEP_SUB_PORT",
    "FEATURES.STEP_PRIM_CUST_ID",
    "FEATURES.OS_BAL_AMT_V2",
    "FEATURES.TOT_NEW_BAL_AMT",
    "FEATURES.TOTAL_BALANCE",
    "FEATURES.CR_LMT_AMT",
    "FEATURES.STEP_MONTH_DEF_SINCE_LAST_DEF",
    "FEATURES.STEP_INSURANCE",
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
        WITH model_excl_f AS (
            SELECT
                BASEL_ACCT_ID,
                MODEL_EXCL_F
            FROM
                FEATURES.MODEL_EXCL_F
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        written_out_f AS (
            SELECT
                BASEL_ACCT_ID,
                WRITTEN_OUT_F
            FROM
                FEATURES.WRITTEN_OUT_F
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        treatment_f AS (
            SELECT
                BASEL_ACCT_ID,
                TREATMENT_F
            FROM
                FEATURES.TREATMENT_F
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        pit_status_step AS (
            SELECT
                BASEL_ACCT_ID,
                PIT_STATUS_STEP
            FROM
                FEATURES.PIT_STATUS_STEP
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        step_sub_port AS (
            SELECT
                BASEL_ACCT_ID,
                STEP_SUB_PORT
            FROM
                FEATURES.STEP_SUB_PORT
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
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
        cons_dft_mth_cnt AS (
            SELECT
                STEP_PLN_AGRMNT_NUM,
                STEP_MONTH_DEF_SINCE_LAST_DEF
            FROM
                FEATURES.STEP_MONTH_DEF_SINCE_LAST_DEF
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        step_insurance AS (
            SELECT
                STEP_PLN_AGRMNT_NUM,
                STEP_INSURANCE
            FROM
                FEATURES.STEP_INSURANCE
            WHERE
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        mor AS (
            SELECT
                TRIM(main.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                main.BASEL_ACCT_ID,
                WRITTEN_OUT_F,
                MODEL_EXCL_F,
                TREATMENT_F,
                TOTAL_BALANCE AS OS_BAL_AMT,
                TOTAL_BALANCE AS CR_LMT_AMT,
                PIT_STATUS_STEP,
                COALESCE(STEP_MONTH_DEF_SINCE_LAST_DEF, 0) AS STEP_MONTH_DEF_SINCE_LAST_DEF,
                STEP_INSURANCE
            FROM
                INGESTION.MORT_MTH_SNAPSHOT AS main
                LEFT JOIN model_excl_f ON model_excl_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN written_out_f ON written_out_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN treatment_f ON treatment_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN pit_status_step ON pit_status_step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN step_sub_port ON step_sub_port.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN total_balance ON main.BASEL_ACCT_ID = total_balance.BASEL_ACCT_ID
                LEFT JOIN cons_dft_mth_cnt ON TRIM(main.STEP_PLN_AGRMNT_NUM) = cons_dft_mth_cnt.STEP_PLN_AGRMNT_NUM
                LEFT JOIN step_insurance ON TRIM(main.STEP_PLN_AGRMNT_NUM) = TRIM(step_insurance.STEP_PLN_AGRMNT_NUM)
            WHERE
                NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL
                AND TREATMENT_F = 'A'
                AND TRIM(STEP_SUB_PORT) IN ('STEP_MOR', 'STEP_MIX')
                AND PIT_STATUS_STEP IN ('DEF')
                AND MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),
        spl AS (
            SELECT
                TRIM(main.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                main.BASEL_ACCT_ID,
                WRITTEN_OUT_F,
                MODEL_EXCL_F,
                TREATMENT_F,
                OS_BAL_AMT_V2 AS OS_BAL_AMT,
                OS_BAL_AMT_V2 AS CR_LMT_AMT,
                PIT_STATUS_STEP,
                COALESCE(STEP_MONTH_DEF_SINCE_LAST_DEF, 0) AS STEP_MONTH_DEF_SINCE_LAST_DEF,
                STEP_INSURANCE
            FROM
                INGESTION.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS main
                LEFT JOIN model_excl_f ON model_excl_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN written_out_f ON written_out_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN treatment_f ON treatment_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN pit_status_step ON pit_status_step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN step_sub_port ON step_sub_port.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN os_bal_amt_v2 ON main.BASEL_ACCT_ID = os_bal_amt_v2.BASEL_ACCT_ID
                LEFT JOIN cons_dft_mth_cnt ON TRIM(main.STEP_PLN_AGRMNT_NUM) = cons_dft_mth_cnt.STEP_PLN_AGRMNT_NUM
                LEFT JOIN step_insurance ON TRIM(main.STEP_PLN_AGRMNT_NUM) = TRIM(step_insurance.STEP_PLN_AGRMNT_NUM)
            WHERE
                NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL
                AND TREATMENT_F = 'A'
                AND TRIM(STEP_SUB_PORT) = 'STEP_MIX'
                AND PIT_STATUS_STEP IN ('DEF')
                AND MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),
        ks AS (
            SELECT
                TRIM(main.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                main.BASEL_ACCT_ID,
                WRITTEN_OUT_F,
                MODEL_EXCL_F,
                TREATMENT_F,
                tot_new_bal_amt.tot_new_bal_amt as os_bal_amt,
                cr_lmt_amt.cr_lmt_amt as cr_lmt_amt,
                PIT_STATUS_STEP,
                COALESCE(STEP_MONTH_DEF_SINCE_LAST_DEF, 0) AS STEP_MONTH_DEF_SINCE_LAST_DEF,
                STEP_INSURANCE
            FROM
                INGESTION.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS main
                LEFT JOIN model_excl_f ON model_excl_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN written_out_f ON written_out_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN treatment_f ON treatment_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN pit_status_step ON pit_status_step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN step_sub_port ON step_sub_port.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN tot_new_bal_amt ON main.BASEL_ACCT_ID = tot_new_bal_amt.BASEL_ACCT_ID
                LEFT JOIN cr_lmt_amt ON main.BASEL_ACCT_ID = cr_lmt_amt.BASEL_ACCT_ID
                LEFT JOIN cons_dft_mth_cnt ON TRIM(main.STEP_PLN_AGRMNT_NUM) = cons_dft_mth_cnt.STEP_PLN_AGRMNT_NUM
                LEFT JOIN step_insurance ON TRIM(main.STEP_PLN_AGRMNT_NUM) = TRIM(step_insurance.STEP_PLN_AGRMNT_NUM)
            WHERE
                NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL
                AND TREATMENT_F = 'A'
                AND TRIM(STEP_SUB_PORT) = 'STEP_MIX'
                AND PIT_STATUS_STEP IN ('DEF')
                AND MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        )
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
            '{{{{ task_instance.xcom_pull(task_ids="segmented__step_mix_mor_lgdd.export_acct_list", key="parquet") }}}}' b
            LEFT JOIN (
                SELECT
                    BASEL_ACCT_ID,
                    VAR_SCORE AS SCORE
                FROM
                    {UPSTREAM_ASSET[0]}
                WHERE
                    TRIM(VAR_NAME) = 'SCORE'
                    AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND TRIM(UPPER(model)) = TRIM(UPPER('{MODEL}'))
                    AND TRIM(UPPER(stream)) = TRIM(
                        UPPER('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}')
                    )
            ) a ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=8):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__step_mix_mor_lgdd.export_segment_input", key="parquet"
    )
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND TRIM(UPPER(MODEL)) = TRIM(UPPER('{MODEL}')) AND TRIM(UPPER(STREAM)) = TRIM(UPPER('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT *, 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                '{MODEL}' AS MODEL,
                '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' AS STREAM
            FROM '{{{{ task_instance.xcom_pull(task_ids="segmented__step_mix_mor_lgdd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
