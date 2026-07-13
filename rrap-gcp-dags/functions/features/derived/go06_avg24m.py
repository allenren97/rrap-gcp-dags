import pyarrow as pa

from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'features.GO06',
    'ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT',
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'reference.SRC_PRD_LKP',
    'ingestion.TM_DIM',
    'ingestion.MORT_MTH_SNAPSHOT',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'reference.TRNST_EXCLSN_LKP',
    'ingestion.BASEL_ACCT_DIM'
]

DOWNSTREAM_ASSET = 'features.GO06_AVG24M'

DEPENDENCIES = {
    'duckdb_clear_go06_avg24m': ['export_basel_cust_scorecrd_drvd_vars'],
    'export_basel_cust_scorecrd_drvd_vars': ['export_go06_avg24m_base'],
    'export_go06_avg24m_base': ['duckdb_load_go06_avg24m'],
}


def duckdb_clear_go06_avg24m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}';
    """
):
    pass

def export_basel_cust_scorecrd_drvd_vars(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        WITH
        PARAMS AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID
        )
        , S_SRC_PRD_LKP AS (
            SELECT DISTINCT
                TRIM(S.BASEL_PRD_CD) as BASEL_PRD_CD
                , TRIM(S.BASEL_PRD_DESC) as BASEL_PRD_DESC
                , TRIM(S.LTV_TP_CD) as LTV_TP_CD
                , TRIM(S.SML_BUS_F) as SML_BUS_F
                , TRIM(S.CONSM_SCORECRD_EXCLSN_F) as CONSM_SCORECRD_EXCLSN_F3
                , TRIM(S.CONSM_PRD_TREATMNT_CD) as CONSM_PRD_TREATMNT_CD
                , TRIM(S.SRC_PRD_CD) as SRC_PRD_CD
                , TRIM(S.SRC_SUB_PRD_CD) as SRC_SUB_PRD_CD
            FROM reference.SRC_PRD_LKP S
            INNER JOIN PARAMS P ON 1 = 1
            INNER JOIN ingestion.TM_DIM T ON
                P.MTH_TM_ID = T.TM_ID
            WHERE
                TRIM(S.PRD_SYS_CD) = 'KS'
                AND YEAR(T.TM_LVL_END_DT) || STRFTIME(T.TM_LVL_END_DT, '%m') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
        )
        , LD_BASEL_REVL_CR_BASE_DRVD_VARS AS (
            SELECT
                A.MTH_TM_ID
                , A.PRIM_BASEL_CUST_ID
                , A.BASEL_ACCT_ID
                , SP.SML_BUS_F
                , CASE
                    WHEN (A.CR_LMT_AMT <= 0 AND A.TOT_NEW_BAL_AMT <= 0) THEN 'Z'
                    WHEN A.TOT_NEW_BAL_AMT <= 0 and (SUBSTR(A.BLOCK_RECL_CD, 1, 1) ='V' OR A.BLOCK_RECL_CD = 'FX') THEN 'Z'
                    ELSE SP.CONSM_PRD_TREATMNT_CD
                END AS CONSM_PRD_TREATMNT_CD
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT A
            INNER JOIN PARAMS P ON 1=1
            LEFT JOIN S_SRC_PRD_LKP SP ON
                TRIM(A.PRD_CD) = TRIM(SP.SRC_PRD_CD)
                AND TRIM(A.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
            WHERE
                A.MTH_TM_ID = P.MTH_TM_ID
        )
        , IN_BASEL_MORT_MTH_SNAPSHOT AS (
            SELECT
                A.MTH_TM_ID
                , A.PRIM_BASEL_CUST_ID
                , A.BASEL_ACCT_ID
                , A.PD_OFF_DT
                , TRIM(BA.ACCT_NUM) as ACCT_NUM
                , BR.EXCLUDED_TRNST_NUM
                , A.CRNT_BAL_AMT + A.INTR_ACCR_AMT AS OS_BAL_AMT
                , CASE
                    WHEN SCRTY_TP_2 = '' OR SCRTY_TP_2 IS NULL THEN NULL
                    WHEN SUBSTR(TRIM(A.SCRTY_TP_2), 1, 1) = '6' OR CAST(SUBSTR(TRIM(A.SCRTY_TP_2), LENGTH(TRIM(A.SCRTY_TP_2)) - 2, 3) AS DOUBLE) >= 5 THEN 'COMMERCIAL'
                    ELSE 'RESIDENTIAL'
                END AS COMM_TP_CD
            FROM ingestion.MORT_MTH_SNAPSHOT A
            INNER JOIN PARAMS P ON 1=1
            LEFT JOIN reference.TRNST_EXCLSN_LKP BR ON
                A.SERV_BR_TRNST_NUM = BR.EXCLUDED_TRNST_NUM
            LEFT JOIN ingestion.BASEL_ACCT_DIM BA ON
                A.BASEL_ACCT_ID = BA.BASEL_ACCT_ID
                AND BA.SRC_APP_CD = 'MO'
            WHERE
                A.MTH_TM_ID = P.MTH_TM_ID
        )
        , LD_BASEL_MORT_MTH_SNAPSHOT AS (
            SELECT
                A.*
                , CASE
                    WHEN A.COMM_TP_CD != 'RESIDENTIAL' OR A.PD_OFF_DT IS NOT NULL OR A.OS_BAL_AMT <= 0 THEN 'Z'
                    ELSE 'A'
                END AS CONSM_PRD_TREATMNT_CD
            FROM IN_BASEL_MORT_MTH_SNAPSHOT A
        )
        , S_BASEL_PSNL_LOAN_MTH_SNAPSHOT AS (
            SELECT
                A.MTH_TM_ID
                , A.PRIM_BASEL_CUST_ID
                , A.BASEL_ACCT_ID
                , TRIM(BA.ACCT_NUM) AS ACCT_NUM
                , BR.EXCLUDED_TRNST_NUM
                , ROUND(A.TOT_CRNT_BAL_AMT + A.ADD_ON_BAL_AMT + A.ACCR_INTR, 3) AS OS_BAL_AMT
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT A
            INNER JOIN PARAMS P ON 1=1
            LEFT JOIN ingestion.BASEL_ACCT_DIM BA ON
                A.BASEL_ACCT_ID = BA.BASEL_ACCT_ID
            LEFT JOIN reference.TRNST_EXCLSN_LKP BR ON
                A.CRNT_BR_LOCTN_TRNST = BR.EXCLUDED_TRNST_NUM
            WHERE
                A.MTH_TM_ID = P.MTH_TM_ID
        )
        , LD_BASEL_PSNL_LOAN_DRV_VARS AS (
            SELECT
                A.*
                , CASE
                    WHEN TRIM(COALESCE(EXCLUDED_TRNST_NUM, '')) = '' THEN 'N'
                    ELSE 'Y'
                END AS TRNST_EXCLSN_F
                , CASE
                    WHEN TRIM(COALESCE(EXCLUDED_TRNST_NUM, '')) != '' OR OS_BAL_AMT <= 0 THEN 'Z'
                    ELSE 'A'
                END AS CONSM_PRD_TREATMNT_CD
            FROM S_BASEL_PSNL_LOAN_MTH_SNAPSHOT A
        )
        SELECT
            A.MTH_TM_ID
            , A.PRIM_BASEL_CUST_ID
        FROM LD_BASEL_REVL_CR_BASE_DRVD_VARS A
        WHERE
            A.PRIM_BASEL_CUST_ID IS NOT NULL
            AND A.PRIM_BASEL_CUST_ID <> '-1'
            AND A.SML_BUS_F='N'
            AND A.CONSM_PRD_TREATMNT_CD='A'
        UNION
        SELECT
            B.MTH_TM_ID
            , B.PRIM_BASEL_CUST_ID
        FROM LD_BASEL_MORT_MTH_SNAPSHOT B
        WHERE
            B.PRIM_BASEL_CUST_ID IS NOT NULL
            AND B.PRIM_BASEL_CUST_ID <> '-1'
            AND B.CONSM_PRD_TREATMNT_CD='A'
        UNION
        SELECT
            C.MTH_TM_ID
            , C.PRIM_BASEL_CUST_ID
        FROM LD_BASEL_PSNL_LOAN_DRV_VARS C
        WHERE
            C.PRIM_BASEL_CUST_ID IS NOT NULL
            AND C.PRIM_BASEL_CUST_ID <> '-1'
            AND C.CONSM_PRD_TREATMNT_CD='A'
    """,
    resource_tier='MED',
    pool_slots=96,
):
    pass

def export_go06_avg24m_base(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        WITH relationship_months AS (
            SELECT DISTINCT
                r.BASEL_CUST_ID,
                DATE(t.TM_LVL_END_DT) AS TM_LVL_END_DT
            FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT r
            JOIN ingestion.TM_DIM t 
            ON t.TM_ID = r.MTH_TM_ID
            WHERE r.MTH_TM_ID BETWEEN
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 920
            AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        go06_by_month AS (
            SELECT
                BASEL_CUST_ID,
                OBSN_DT,
                SUM(CAST(GO06 AS DOUBLE)) AS go06_sum_for_month
            FROM features.GO06
            WHERE OBSN_DT BETWEEN 
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 23 MONTH
            AND DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            GROUP BY BASEL_CUST_ID, OBSN_DT
        )
        SELECT
            rm.BASEL_CUST_ID,
            COUNT(*) AS relationship_month_count,
            CASE
                WHEN COUNT(*) = 0 THEN NULL
                WHEN SUM(g.go06_sum_for_month) IS NULL THEN NULL
                ELSE CAST(SUM(g.go06_sum_for_month) / COUNT(*) AS DECIMAL(11,4))
            END AS GO06_AVG24M
        FROM relationship_months rm
        LEFT JOIN go06_by_month g
            ON g.BASEL_CUST_ID = rm.BASEL_CUST_ID
            AND g.OBSN_DT = rm.TM_LVL_END_DT
        GROUP BY rm.BASEL_CUST_ID
    """,
    resource_tier='MED',
    pool_slots=32,
):
    pass

def duckdb_load_go06_avg24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                b.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                g.GO06_AVG24M
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__go06_avg24m.export_basel_cust_scorecrd_drvd_vars", key="parquet") }}}}') b
            LEFT JOIN read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__go06_avg24m.export_go06_avg24m_base", key="parquet") }}}}') g
                ON g.BASEL_CUST_ID = b.PRIM_BASEL_CUST_ID
            WHERE b.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """
):
    pass

