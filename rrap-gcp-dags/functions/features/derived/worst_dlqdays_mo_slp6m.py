import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'ingestion.BASEL_MORT_MTH_SNAPSHOT',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'ingestion.TM_DIM',
    'features.SML_BUS_F',
    'features.CONSM_PRD_TREATMNT_CD',
    'features.COMM_TP_CD',
    'features.DLQNT_DAY_CNT',
]

DOWNSTREAM_ASSET = 'features.WORST_DLQDAYS_MO_SLP6M'
DEPENDENCIES = {
    'duckdb_clear_worst_dlqdays_mo_slp6m': ['duckdb_derive_worst_dlqdays_mo_slp6m'],
}


def duckdb_clear_worst_dlqdays_mo_slp6m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_worst_dlqdays_mo_slp6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
        BY NAME FROM (
        WITH
        accounts AS (
            SELECT
                ss.PRIM_BASEL_CUST_ID,
                ss.BASEL_ACCT_ID,
                'KS' as SRC_SYS_CD
            FROM {UPSTREAM_ASSET[0]}  ss
            INNER JOIN (
                SELECT BASEL_ACCT_ID, SML_BUS_F FROM features.SML_BUS_F WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                ) t1 ON
                ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
                AND t1.SML_BUS_F = 'N'
            INNER JOIN (
                SELECT BASEL_ACCT_ID, CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                ) t2 ON
                ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
                AND t2.CONSM_PRD_TREATMNT_CD = 'A'
            WHERE
                ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND ss.PRIM_BASEL_CUST_ID IS NOT NULL
                AND ss.PRIM_BASEL_CUST_ID != -1
            UNION ALL
            SELECT
                ss.PRIM_BASEL_CUST_ID,
                ss.BASEL_ACCT_ID,
                'MO' as SRC_SYS_CD
            FROM {UPSTREAM_ASSET[1]}  ss
            INNER JOIN (
                SELECT BASEL_ACCT_ID, CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                ) t1 ON
                ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
                AND t1.CONSM_PRD_TREATMNT_CD = 'A'
            WHERE
                ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND ss.PRIM_BASEL_CUST_ID IS NOT NULL
                AND ss.PRIM_BASEL_CUST_ID != -1
            UNION ALL
            SELECT
                ss.PRIM_BASEL_CUST_ID,
                ss.BASEL_ACCT_ID,
                'SPL' as SRC_SYS_CD
            FROM {UPSTREAM_ASSET[2]}  ss
            INNER JOIN (
                SELECT BASEL_ACCT_ID, CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                ) t1 ON
                ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
                AND t1.CONSM_PRD_TREATMNT_CD = 'A'
            WHERE
                ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND ss.PRIM_BASEL_CUST_ID IS NOT NULL
                AND ss.PRIM_BASEL_CUST_ID != -1
        )
        , population as (
            select PRIM_BASEL_CUST_ID from accounts group by PRIM_BASEL_CUST_ID
        )
        , L6 as (
            SELECT
                ss.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                MAX(t2.DLQNT_DAY_CNT) AS v_MAX_DLQNT_DAY_CNT_CurrMth
            FROM {UPSTREAM_ASSET[1]} ss
            INNER JOIN (
                SELECT BASEL_ACCT_ID, COMM_TP_CD FROM features.COMM_TP_CD WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                ) t1 ON
                ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
                AND TRIM(UPPER(t1.COMM_TP_CD)) = 'RESIDENTIAL'
            INNER JOIN (
                SELECT BASEL_ACCT_ID, DLQNT_DAY_CNT FROM features.DLQNT_DAY_CNT WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                ) t2 ON
                ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
            WHERE
                ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND ss.CRNT_BAL_AMT > 0
                AND TRIM(ss.PD_OFF_F) = 'N'
            GROUP BY ss.PRIM_BASEL_CUST_ID
        )
        , L7 as (
            SELECT
                ss.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                MAX(t2.DLQNT_DAY_CNT) AS v_MAX_DLQNT_DAY_CNT_5Mth
            FROM {UPSTREAM_ASSET[1]} ss
            INNER JOIN (
                SELECT BASEL_ACCT_ID, COMM_TP_CD FROM features.COMM_TP_CD WHERE OBSN_DT = DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 5 MONTH)
                ) t1 ON
                ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
                AND TRIM(UPPER(t1.COMM_TP_CD)) = 'RESIDENTIAL'
            INNER JOIN (
                SELECT BASEL_ACCT_ID, DLQNT_DAY_CNT FROM features.DLQNT_DAY_CNT WHERE OBSN_DT = DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 5 MONTH)
                ) t2 ON
                ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
            WHERE
                ss.MTH_TM_ID = ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-200)
                AND ss.CRNT_BAL_AMT > 0
                AND TRIM(ss.PD_OFF_F) = 'N'
            GROUP BY ss.PRIM_BASEL_CUST_ID
        )
        , L8 as (
            SELECT
                ss.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                SUM(COALESCE(t2.DLQNT_DAY_CNT,0)) AS v_MAX_DLQNT_DAY_CNT_between_mont
            FROM {UPSTREAM_ASSET[1]} ss
            INNER JOIN ingestion.TM_DIM t on
                ss.MTH_TM_ID = t.TM_ID
                AND t.TM_ID > ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-200)
                AND t.TM_ID < {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            INNER JOIN features.COMM_TP_CD t1 ON
                t.TM_LVL_END_DT = t1.OBSN_DT
                AND ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
                AND TRIM(UPPER(t1.COMM_TP_CD)) = 'RESIDENTIAL'
            LEFT OUTER JOIN features.DLQNT_DAY_CNT t2 ON
                t.TM_LVL_END_DT = t2.OBSN_DT
                AND ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
            WHERE
                ss.CRNT_BAL_AMT > 0
                AND TRIM(ss.PD_OFF_F) = 'N'
            GROUP BY ss.PRIM_BASEL_CUST_ID
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
            , p.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID
            , CASE
                WHEN v_MAX_DLQNT_DAY_CNT_CurrMth=0 AND v_MAX_DLQNT_DAY_CNT_5Mth=0 THEN 0
                WHEN v_MAX_DLQNT_DAY_CNT_5Mth IS NULL AND v_MAX_DLQNT_DAY_CNT_CurrMth=0 THEN NULL
                WHEN v_MAX_DLQNT_DAY_CNT_5Mth IS NULL AND (v_MAX_DLQNT_DAY_CNT_CurrMth IS NOT NULL AND v_MAX_DLQNT_DAY_CNT_CurrMth != 0) THEN NULL
                WHEN v_MAX_DLQNT_DAY_CNT_5Mth = 0 AND v_MAX_DLQNT_DAY_CNT_between_mont = 0 AND (v_MAX_DLQNT_DAY_CNT_CurrMth IS NOT NULL AND v_MAX_DLQNT_DAY_CNT_CurrMth != 0) THEN v_MAX_DLQNT_DAY_CNT_CurrMth
                WHEN v_MAX_DLQNT_DAY_CNT_5Mth IS NULL OR v_MAX_DLQNT_DAY_CNT_CurrMth IS NULL THEN NULL
                ELSE (v_MAX_DLQNT_DAY_CNT_CurrMth-v_MAX_DLQNT_DAY_CNT_5Mth)/5
            END	AS WORST_DLQDAYS_MO_SLP6M
        FROM POPULATION p
        LEFT JOIN L6 ON
            p.PRIM_BASEL_CUST_ID = L6.BASEL_CUST_ID
        LEFT JOIN L7 ON
            p.PRIM_BASEL_CUST_ID = L7.BASEL_CUST_ID
        LEFT JOIN L8 ON
            p.PRIM_BASEL_CUST_ID = L8.BASEL_CUST_ID
    )
    """
):
    pass
