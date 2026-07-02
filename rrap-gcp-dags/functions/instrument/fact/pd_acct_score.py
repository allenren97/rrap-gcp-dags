UPSTREAM_ASSET = [
    "models.CC_PD_DELINQUENT_SCORE",
    "models.HELOC_PD_SCORE",
    "models.CC_PD_REVOLVER_SCORE",
    "models.CC_PD_TRANSACTOR_SCORE",
    "models.LOC_PD_SCORE",
    "models.MOR_PD_SCORE",
    "models.DTL_PD_SCORE",
    "models.ITL_PD_SCORE",
    "models.TNG_MOR_PD_SCORE",
]
# SCHEMAS & portfolios where the PD scores are being pulled from

DOWNSTREAM_ASSET = "instruments.PD_ACCT_SCORE"

# looking to load into instruments.PD_ACCT_SCORE

DEPENDENCIES = {
    "duckdb_clear": ["duckdb_load"],
}


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    trigger_rule="none_failed_min_one_success",
    # note: Trigger rule isn't necessary here but we will not remove it since it doesn't affect the output
    # trigger_rule used here since we have more than one UPSTREAM ASSET which essentially runs the code if at least one upstream model table succeeds
    duckdb_conn_id="duckdb-conn",
    # SQL code below will create a temp table 'population' to derive the valid IF population
    # Added Instrument Fact population filtering (KS, MOR, SPL, TNG-MOR) using Airflow-injected parameters and joined the population to
    # consolidated PD score output to align DEV output with the benchmark population.
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        WITH population AS (

            -- KS
            SELECT
                BASEL_ACCT_ID,
                'KS' AS SRC_SYS_CD
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
            WHERE MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'

            UNION

            -- MOR
            SELECT
                BASEL_ACCT_ID,
                'MOR' AS SRC_SYS_CD
            FROM ingestion.MORT_MTH_SNAPSHOT
            WHERE MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'

            UNION

            -- SPL
            SELECT
                BASEL_ACCT_ID,
                'SPL' AS SRC_SYS_CD
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
            WHERE MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'

            UNION

            -- TNG-MOR
            SELECT
                dim.BASEL_ACCT_ID,
                'TNG-MOR' AS SRC_SYS_CD
            FROM ingestion.BASEL_ACCT_DIM dim
            INNER JOIN ingestion.TNG_ACCT_MO tng
                ON dim.SRC_APP_CD = 'TNG-MOR'
                AND dim.SRC_SYS_DEL_F != 'Y'
                AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
            WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'

        ),

        -- SQL code below will create a temp table 'pd_score', select ACCT_ID and VAR_SCORE as display columns
        -- with where conditions on current date injected from airflow
        -- AND where column, VAR_NAME = SCORE

        pd_score AS (

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.HELOC_PD_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.CC_PD_DELINQUENT_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.CC_PD_REVOLVER_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.CC_PD_TRANSACTOR_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.LOC_PD_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.MOR_PD_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.DTL_PD_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.ITL_PD_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

            UNION ALL

            SELECT
                BASEL_ACCT_ID,
                VAR_SCORE AS PD_ACCT_SCORE
            FROM models.TNG_MOR_PD_SCORE
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND VAR_NAME = 'SCORE'

        )

        SELECT
            pop.BASEL_ACCT_ID,
            pd.PD_ACCT_SCORE,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM population pop
        LEFT JOIN pd_score pd
            ON pop.BASEL_ACCT_ID = pd.BASEL_ACCT_ID
    )
    """,
):
    pass


# Load logic derives the valid Instrument Fact population (KS, MOR, SPL, TNG-MOR)
# then consolidates PD scores from all model tables that exist in DuckLake IST
# (HELOC, CC, LOC, MOR, ITL, DTL, TNG), maps VAR_SCORE to PD_ACCT_SCORE
# and loads the filtered result into the downstream table