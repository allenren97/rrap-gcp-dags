import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.MORT_MTH_SNAPSHOT']

DOWNSTREAM_ASSET = 'features.MAX_LEND_VALUE'
DEPENDENCIES = {
    'duckdb_clear_max_lend_value': ['duckdb_derive_max_lend_value'],
}


def duckdb_clear_max_lend_value(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_max_lend_value(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    WITH MORT_AUTH_DT_NULLS_IN_CUR_MONTH AS (
    SELECT
        STEP_PLN_AGRMNT_NUM,
        MORT_NUM
    FROM
        {UPSTREAM_ASSET[0]}
    WHERE
        MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND MORT_AUTH_DT IS NULL
        AND STEP_PLN_AGRMNT_NUM IS NOT NULL 
        AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
        AND UPPER(TRIM(COMM_TP)) = 'RESIDENTIAL'
        AND CRNT_BAL_AMT > 0
        AND TRIM(PD_OFF_F) = 'N'
    ),
    MAX_MORT_AUTH_DT_FOR_NULLS AS (
    SELECT
        s.MORT_NUM,
        MAX(MORT_AUTH_DT) AS MORT_AUTH_DT
    FROM {UPSTREAM_ASSET[0]} s
    INNER JOIN MORT_AUTH_DT_NULLS_IN_CUR_MONTH n
        ON n.MORT_NUM = s.MORT_NUM
    WHERE
        s.MORT_AUTH_DT IS NOT NULL
        AND s.MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        s.MORT_NUM
    ),
    NULLS_WITH_FETCHED_MORT_AUTH_DT AS (
    SELECT DISTINCT
        s.MORT_NUM,
        s.STEP_PLN_AGRMNT_NUM,
        m.MORT_AUTH_DT
    FROM MAX_MORT_AUTH_DT_FOR_NULLS m
    LEFT JOIN MORT_AUTH_DT_NULLS_IN_CUR_MONTH s
        ON m.MORT_NUM = s.MORT_NUM
    ),

    MORT_NUM_AND_AUTH_DT_AT_OBSN AS (
	SELECT 
		STEP_PLN_AGRMNT_NUM,
		MORT_NUM,
		MORT_AUTH_DT
	FROM
		(
		SELECT 
			STEP_PLN_AGRMNT_NUM,
			MORT_NUM,
			MORT_AUTH_DT,
			MAX(MORT_AUTH_DT) OVER (
                    PARTITION BY STEP_PLN_AGRMNT_NUM
                ) AS max_auth_dt
		FROM (
            SELECT
                STEP_PLN_AGRMNT_NUM,
                MORT_NUM,
                MORT_AUTH_DT
            FROM
                {UPSTREAM_ASSET[0]}
            WHERE 
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND STEP_PLN_AGRMNT_NUM IS NOT NULL 
                AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
                AND UPPER(TRIM(COMM_TP))='RESIDENTIAL'
                AND CRNT_BAL_AMT>0
                AND TRIM(PD_OFF_F)='N'
                AND MORT_AUTH_DT IS NOT NULL
            
            UNION ALL

            SELECT
                STEP_PLN_AGRMNT_NUM,
                MORT_NUM,
                MORT_AUTH_DT
            FROM NULLS_WITH_FETCHED_MORT_AUTH_DT
        ) obs_population
	)
	WHERE
		MORT_AUTH_DT = max_auth_dt
	),

    MAX_LEND_VALUE AS (
    SELECT
        mortauth.MORT_NUM,
        mortauth.mort_auth_dt,
        mortauth.step_pln_agrmnt_num,
        MAX(mortsnp.LND_VAL) AS MAX_LEND_VALUE
    FROM
        MORT_NUM_AND_AUTH_DT_AT_OBSN AS mortauth
        LEFT JOIN {UPSTREAM_ASSET[0]} AS mortsnp
        ON mortauth.mort_num = mortsnp.mort_num
        AND mortauth.mort_auth_dt = mortsnp.mort_auth_dt
        AND mortsnp.MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        mortauth.MORT_NUM, mortauth.mort_auth_dt, mortauth.step_pln_agrmnt_num
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        STEP_PLN_AGRMNT_NUM,
        MORT_AUTH_DT,
        MORT_NUM,
        MAX_LEND_VALUE
    FROM (
        SELECT
            A.STEP_PLN_AGRMNT_NUM,
            A.MORT_AUTH_DT,
            A.MORT_NUM,
            B.MAX_LEND_VALUE,
            ROW_NUMBER() OVER(
                PARTITION BY A.STEP_PLN_AGRMNT_NUM
                ORDER BY B.MAX_LEND_VALUE DESC
			) AS ROW_NUM
        FROM MORT_NUM_AND_AUTH_DT_AT_OBSN A
        INNER JOIN
            MAX_LEND_VALUE B
        ON  
            A.MORT_NUM = B.MORT_NUM
            AND A.MORT_AUTH_DT = B.MORT_AUTH_DT
            AND A.STEP_PLN_AGRMNT_NUM = B.STEP_PLN_AGRMNT_NUM
        )
    WHERE 
        ROW_NUM = 1
    )
    """
):
    pass

