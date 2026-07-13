
UPSTREAM_ASSET = [
    'ingestion.MORT_MTH_SNAPSHOT',
    'features.PREPAY_YTD'
    ]
DOWNSTREAM_ASSET = 'features.TOT_PREPAY_YTD_MTG_MAX24M_ACCT'
DEPENDENCIES = {
    'duckdb_clear_tot_prepay_ytd_mtg_max24m_acct': ['duckdb_derive_tot_prepay_ytd_mtg_max24m_acct'],
}


def duckdb_clear_tot_prepay_ytd_mtg_max24m_acct(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_tot_prepay_ytd_mtg_max24m_acct(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        WITH BASE AS(
            SELECT
                A.BASEL_ACCT_ID,
                A.PREPAY_YTD
            FROM 
                {UPSTREAM_ASSET[1]} A
            INNER JOIN 
                {UPSTREAM_ASSET[0]} B
            ON 
                A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
            AND
                A.OBSN_DT = B.MTH_END_DT
            WHERE
                A.OBSN_DT BETWEEN
                    DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 23 MONTH) 
                    AND
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND
                B.CRNT_BAL_AMT>0
            AND
                UPPER(TRIM(B.COMM_TP))='RESIDENTIAL'
            AND
                TRIM(B.PD_OFF_F) ='N')
            
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            MAX(PREPAY_YTD) AS TOT_PREPAY_YTD_MTG_MAX24M_ACCT
        FROM BASE
        GROUP BY
            BASEL_ACCT_ID
    )
    """
):
    pass

