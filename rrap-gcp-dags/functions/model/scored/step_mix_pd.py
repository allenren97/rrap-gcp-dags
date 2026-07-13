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

MODEL = "step_mix_pd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
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
                    main.basel_acct_id
                FROM
                    ingestion.mort_mth_snapshot AS main
                    LEFT JOIN MODEL_EXCL_F ON MODEL_EXCL_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN WRITTEN_OUT_F ON WRITTEN_OUT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN TREATMENT_F ON TREATMENT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN pit_status_step ON pit_status_step.basel_acct_id = main.basel_acct_id
                    LEFT JOIN step_sub_port ON step_sub_port.basel_acct_id = main.basel_acct_id
                    LEFT JOIN total_balance ON main.basel_acct_id = total_balance.basel_acct_id
                WHERE
                    NULLIF(TRIM(main.step_pln_agrmnt_num), '') IS NOT NULL
                    AND MODEL_EXCL_F = 'N'
                    AND WRITTEN_OUT_F = 'N'
                    AND TREATMENT_F = 'A'
                    AND TRIM(step_sub_port) = 'STEP_MIX'
                    AND pit_status_step = 'CUR'
                    AND (total_balance > 0)
                    AND mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                GROUP BY
                    TRIM(main.step_pln_agrmnt_num),
                    main.basel_acct_id
            ),
            spl AS (
                SELECT
                    TRIM(main.step_pln_agrmnt_num) AS step_pln_agrmnt_num,
                    main.basel_acct_id
                FROM
                    ingestion.basel_psnl_loan_mth_snapshot AS main
                    LEFT JOIN MODEL_EXCL_F ON MODEL_EXCL_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN WRITTEN_OUT_F ON WRITTEN_OUT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN TREATMENT_F ON TREATMENT_F.basel_acct_id = main.basel_acct_id
                    LEFT JOIN pit_status_step ON pit_status_step.basel_acct_id = main.basel_acct_id
                    LEFT JOIN step_sub_port ON step_sub_port.basel_acct_id = main.basel_acct_id
                    LEFT JOIN os_bal_amt_v2 ON main.basel_acct_id = os_bal_amt_v2.basel_acct_id
                WHERE
                    NULLIF(TRIM(main.step_pln_agrmnt_num), '') IS NOT NULL
                    AND MODEL_EXCL_F = 'N'
                    AND WRITTEN_OUT_F = 'N'
                    AND TREATMENT_F = 'A'
                    AND TRIM(step_sub_port) = 'STEP_MIX'
                    AND pit_status_step = 'CUR'
                    AND (os_bal_amt_v2 > 0)
                    AND mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                GROUP BY
                    TRIM(main.step_pln_agrmnt_num),
                    main.basel_acct_id
            ),
            ks AS (
                SELECT
                    TRIM(main.step_pln_agrmnt_num) AS step_pln_agrmnt_num,
                    main.basel_acct_id
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
                    AND MODEL_EXCL_F = 'N'
                    AND WRITTEN_OUT_F = 'N'
                    AND TREATMENT_F = 'A'
                    AND TRIM(step_sub_port) = 'STEP_MIX'
                    AND pit_status_step = 'CUR'
                    AND (
                        cr_lmt_amt.cr_lmt_amt > 0
                        OR tot_new_bal_amt.tot_new_bal_amt > 0
                    )
                    AND mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                GROUP BY
                    TRIM(main.step_pln_agrmnt_num),
                    main.basel_acct_id
            ),
            uni AS (
                SELECT
                    *
                FROM
                    spl
                UNION
                SELECT
                    *
                FROM
                    mor
                UNION
                SELECT
                    *
                FROM
                    ks
            )
        SELECT
            uni.*,
            step.STEP_PRIM_CUST_ID as BASEL_CUST_ID
        FROM
            uni
            LEFT JOIN features.STEP_PRIM_CUST_ID AS step ON step.step_pln_agrmnt_num = uni.step_pln_agrmnt_num
            AND step.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    """,
):
    pass


def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    sql=_get_input_sql(
        f"{MODEL}_scoring_config.csv",
        r"""{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}""",
        r"""{{ task_instance.xcom_pull(task_ids="scored__step_mix_pd.export_acct_list", key="parquet") }}""",
    ),
):
    pass


def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="scored__step_mix_pd.export_acct_dv", key="parquet"
    )
    output_file = _get_score(
        f"{MODEL}_scoring_config.csv",
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__step_mix_pd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
