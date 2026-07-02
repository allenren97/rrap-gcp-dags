from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import (
    _get_upstream_assets,
    _get_input_sql,
    _get_score,
)

MODEL = "step_mix_mor_lgdd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
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
] + (_get_upstream_assets(f"{MODEL}_scoring_config.csv"))

DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SCORE"

DEPENDENCIES = {
    "export_acct_list": ["export_acct_dv"],
    "export_acct_dv": ["get_score"],
    "get_score": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
       WITH
        model_excl_f AS (
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
        step_prim_cust_id AS (
            SELECT
                STEP_PLN_AGRMNT_NUM,
                STEP_PRIM_CUST_ID
            FROM 
                FEATURES.STEP_PRIM_CUST_ID
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
        mor AS (
            SELECT
                TRIM(main.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                main.BASEL_ACCT_ID
            FROM
                INGESTION.MORT_MTH_SNAPSHOT main
                LEFT JOIN model_excl_f ON model_excl_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN written_out_f ON written_out_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN treatment_f ON treatment_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN pit_status_step ON pit_status_step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN step_sub_port ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
                LEFT JOIN total_balance ON main.BASEL_ACCT_ID = total_balance.BASEL_ACCT_ID
            WHERE
                NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL
                AND MODEL_EXCL_F = 'N'
                AND WRITTEN_OUT_F = 'N'
                AND TREATMENT_F = 'A'
                AND TRIM(STEP_SUB_PORT) IN ('STEP_MOR', 'STEP_MIX')
                AND PIT_STATUS_STEP = 'DEF'
                AND total_balance > 0
                AND MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            GROUP BY
                TRIM(main.STEP_PLN_AGRMNT_NUM),
                main.BASEL_ACCT_ID,
                main.PRIM_BASEL_CUST_ID
        ),
        spl AS (
            SELECT
                TRIM(main.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                main.BASEL_ACCT_ID
            FROM
                INGESTION.basel_psnl_loan_mth_snapshot main
                LEFT JOIN model_excl_f ON model_excl_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN written_out_f ON written_out_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN treatment_f ON treatment_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN pit_status_step ON pit_status_step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN step_sub_port ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
                LEFT JOIN os_bal_amt_v2 ON main.BASEL_ACCT_ID = os_bal_amt_v2.BASEL_ACCT_ID
            WHERE
                NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL
                AND MODEL_EXCL_F = 'N'
                AND WRITTEN_OUT_F = 'N'
                AND TREATMENT_F = 'A'
                AND TRIM(STEP_SUB_PORT) = 'STEP_MIX'
                AND PIT_STATUS_STEP = 'DEF'
                AND OS_BAL_AMT_V2 > 0
                AND MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            GROUP BY
                TRIM(main.STEP_PLN_AGRMNT_NUM),
                main.BASEL_ACCT_ID,
                main.PRIM_BASEL_CUST_ID
        ),
        ks AS (
            SELECT
                TRIM(main.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                main.BASEL_ACCT_ID
            FROM
                INGESTION.BASEL_REVLVNG_CR_MTH_SNAPSHOT main
                LEFT JOIN model_excl_f ON model_excl_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN written_out_f ON written_out_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN treatment_f ON treatment_f.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN pit_status_step ON pit_status_step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
                LEFT JOIN step_sub_port ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
                LEFT JOIN tot_new_bal_amt ON tot_new_bal_amt.basel_acct_id = main.basel_acct_id
                LEFT JOIN cr_lmt_amt ON cr_lmt_amt.basel_acct_id = main.basel_acct_id
            WHERE
                NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL
                AND MODEL_EXCL_F = 'N'
                AND WRITTEN_OUT_F = 'N'
                AND TREATMENT_F = 'A'
                AND TRIM(STEP_SUB_PORT) = 'STEP_MIX'
                AND PIT_STATUS_STEP = 'DEF'
                AND (cr_lmt_amt.CR_LMT_AMT > 0 OR tot_new_bal_amt.TOT_NEW_BAL_AMT > 0)
                AND MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            GROUP BY
                TRIM(main.STEP_PLN_AGRMNT_NUM),
                main.BASEL_ACCT_ID,
                main.PRIM_BASEL_CUST_ID
        ), 
        uni AS (
            SELECT *, 'SPL' AS SRC_SYS_CD FROM spl
            UNION
            SELECT *, 'MOR' AS SRC_SYS_CD FROM mor
            UNION
            SELECT *, 'KS' AS SRC_SYS_CD FROM ks
        )
        SELECT
            uni.STEP_PLN_AGRMNT_NUM,
            uni.BASEL_ACCT_ID,
            step_prim_cust_id.STEP_PRIM_CUST_ID AS BASEL_CUST_ID
        FROM
            uni
            LEFT JOIN step_prim_cust_id ON TRIM(step_prim_cust_id.STEP_PLN_AGRMNT_NUM) = uni.STEP_PLN_AGRMNT_NUM
            LEFT JOIN cons_dft_mth_cnt ON cons_dft_mth_cnt.STEP_PLN_AGRMNT_NUM = uni.STEP_PLN_AGRMNT_NUM
        WHERE
            cons_dft_mth_cnt.STEP_MONTH_DEF_SINCE_LAST_DEF <= 36
    """,
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        r"""{{ task_instance.xcom_pull(task_ids="scored__step_mix_mor_lgdd.export_acct_list", key="parquet") }}""",
    ),
):
    pass


def get_score(pool="duckdb_pool", pool_slots=8):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="scored__step_mix_mor_lgdd.export_acct_dv", key="parquet"
    )
    output_file = _get_score(
        f"{MODEL}_scoring_config.csv",
        input_file,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__step_mix_mor_lgdd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
