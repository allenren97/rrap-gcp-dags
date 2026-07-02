from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


##pre-aggregated logic from step_month_def_since_last_def JIRA RRAP-1740
UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',#0
    'reference.SRC_PRD_LKP',#1
    'ingestion.MORT_MTH_SNAPSHOT',#2
    'features.PIT_STATUS_ACCOUNT',#3
    'ingestion.BASEL_ACCT_DIM',#4
    'ingestion.TM_DIM',#5
]
DOWNSTREAM_ASSET = 'features.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF'
DEPENDENCIES = {
    'duckdb_delete': ['export_rsn'],
    'export_rsn': ['export_pit_status'],
    'export_pit_status': ['export_max_non_dft'],
    'export_max_non_dft': ['export_cons_dft_mth_cnt'],
    'export_cons_dft_mth_cnt': ['export_months_def'],
    'export_months_def': ['export_ks'],
    'export_ks': ['export_mor'],
    'export_mor': ['export_spl'],
    'export_spl': ['duckdb_load'],
}


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def export_rsn(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
        SELECT
            BASEL_ACCT_ID,
            MTH_TM_ID,
            PRD_CD,
            SUB_PRD_CD,
            cast(nullif(trim(step_pln_agrmnt_num), '') as bigint) as step_pln_agrmnt_num
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40 * 40
        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND (STEP_PLN_AGRMNT_NUM IS NULL or trim(STEP_PLN_AGRMNT_NUM) = '')
    """
):
    pass


def export_pit_status(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
        SELECT
            TM.TM_LVL_END_DT AS OBSN_DT,
            RSN.BASEL_ACCT_ID,
            TRIM(PIT.PIT_STATUS_ACCOUNT) AS PIT_STATUS,
            RSN.step_pln_agrmnt_num
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_rsn", key="parquet") }}}}' RSN
        INNER JOIN reference.SRC_PRD_LKP LKP ON
        TRIM(RSN.PRD_CD) = TRIM(LKP.SRC_PRD_CD)
            AND TRIM(RSN.SUB_PRD_CD) = TRIM(LKP.SRC_SUB_PRD_CD)
            AND TRIM(LKP.PRD_SYS_CD) = 'KS'
            AND TRIM(LKP.SML_BUS_F) = 'N'
            AND TRIM(LKP.CRNT_F) = 'Y'
        INNER JOIN ingestion.TM_DIM TM ON
        TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
            AND RSN.MTH_TM_ID = TM.TM_ID
        LEFT JOIN {UPSTREAM_ASSET[3]} PIT ON
        RSN.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
            AND TM.TM_LVL_END_DT = PIT.OBSN_DT
        WHERE TRIM(SRC_SYS_CD) = 'KS'
    """
):
    pass


def export_max_non_dft(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT 
            BASEL_ACCT_ID,
            MAX(OBSN_DT) AS MAX_OBSN_DT
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_pit_status", key="parquet") }}}}' s
        WHERE s.OBSN_DT < '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND (s.PIT_STATUS IS NULL OR s.PIT_STATUS NOT IN ('DEF','CHG') OR s.STEP_PLN_AGRMNT_NUM IS NOT NULL)
        GROUP BY BASEL_ACCT_ID
    """
):
    pass


def export_cons_dft_mth_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            pit_status.BASEL_ACCT_ID,
            COUNT(pit_status.obsn_dt) AS CONS_DFT_MTH_CNT
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_pit_status", key="parquet") }}}}' pit_status
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_max_non_dft", key="parquet") }}}}' max_non_dft
        ON pit_status.BASEL_ACCT_ID = max_non_dft.BASEL_ACCT_ID
        WHERE
            pit_status.OBSN_DT < '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND (TRIM(pit_status.PIT_STATUS) IN ('DEF', 'CHG'))
            AND(
                max_non_dft.MAX_OBSN_DT IS NULL
                OR pit_status.OBSN_DT > max_non_dft.MAX_OBSN_DT
             )
         GROUP BY pit_status.BASEL_ACCT_ID
    """
):
    pass


def export_months_def(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
          s.OBSN_DT,
          s.BASEL_ACCT_ID,
          rsn.STEP_PLN_AGRMNT_NUM,
          CASE
              WHEN s.PIT_STATUS IN ('DEF','CHG')
                  THEN COALESCE(t.CONS_DFT_MTH_CNT, 0)
              ELSE NULL
          END AS MONTH_DEF_SINCE_LAST_DEF
      FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_pit_status", key="parquet") }}}}' s 
      inner join '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_rsn", key="parquet") }}}}' rsn 
      on s.BASEL_ACCT_ID = rsn.BASEL_ACCT_ID
      LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_cons_dft_mth_cnt", key="parquet") }}}}' t
      ON s.BASEL_ACCT_ID=t.BASEL_ACCT_ID
      WHERE s.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
      AND rsn.MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    """
):
    pass


def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    select
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    MONTH_DEF_SINCE_LAST_DEF AS STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF,
    BASEL_ACCT_ID 
    from '{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_months_def", key="parquet") }}}}'
    """
):
    pass


def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH source AS (
        SELECT
            PIT.BASEL_ACCT_ID,
            PIT.SRC_SYS_CD,
            MORT.MTH_TM_ID,
            CAST(NULLIF(TRIM(MORT.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            TRIM(PIT.PIT_STATUS_ACCOUNT) AS PIT_STATUS,
            MORT.MORT_NUM::VARCHAR AS MORT_NUM
        FROM {UPSTREAM_ASSET[3]} PIT
        INNER JOIN {UPSTREAM_ASSET[5]} TM ON
            TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
            AND PIT.OBSN_DT = TM.TM_LVL_END_DT
        INNER JOIN {UPSTREAM_ASSET[2]} MORT ON
            PIT.BASEL_ACCT_ID = MORT.BASEL_ACCT_ID
            AND TM.TM_ID = MORT.MTH_TM_ID
        WHERE
            TRIM(SRC_SYS_CD) = 'MOR'
        AND
            MORT.MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - (40 * 40)
            AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND (MORT.STEP_PLN_AGRMNT_NUM IS NULL or trim(MORT.STEP_PLN_AGRMNT_NUM) = '')
    ),
    def_block AS (
        SELECT BASEL_ACCT_ID, COUNT(*) AS CONS_COUNT
        FROM (
            SELECT
                BASEL_ACCT_ID,
                PIT_STATUS,
                MTH_TM_ID,
                CASE WHEN PIT_STATUS IN ('DEF','CHG') THEN 1 ELSE 0 END AS IS_DEF,
                SUM(CASE WHEN PIT_STATUS IN ('CUR','CLO') OR PIT_STATUS IS NULL OR STEP_PLN_AGRMNT_NUM IS NOT NULL THEN 1 ELSE 0 END)
                OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID DESC ROWS UNBOUNDED PRECEDING) AS BREAK_AFTER
            FROM source
            WHERE MTH_TM_ID < {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ) sub
        WHERE IS_DEF = 1 AND BREAK_AFTER = 0
        GROUP BY BASEL_ACCT_ID
    ),
    mon_def AS (
        SELECT
            s.BASEL_ACCT_ID,
            s.MTH_TM_ID,
            s.MORT_NUM,
            s.STEP_PLN_AGRMNT_NUM,
            s.PIT_STATUS,
            s.SRC_SYS_CD,
            CASE
                WHEN s.MTH_TM_ID != {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} THEN NULL
                WHEN s.PIT_STATUS IS NULL THEN NULL
                WHEN s.PIT_STATUS IN ('DEF','CHG') THEN COALESCE(db.CONS_COUNT, 0)
                WHEN s.PIT_STATUS = 'CUR' THEN NULL
                ELSE NULL
            END AS MONTH_DEF_SINCE_LAST_DEF
        FROM source s
        LEFT JOIN def_block db ON s.BASEL_ACCT_ID = db.BASEL_ACCT_ID
    ), last_def as (
    SELECT
        BASEL_ACCT_ID,
        MORT_NUM,
        STEP_PLN_AGRMNT_NUM,
        MONTH_DEF_SINCE_LAST_DEF,
        SRC_SYS_CD
    FROM mon_def
    WHERE
    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    select
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    MONTH_DEF_SINCE_LAST_DEF as STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF,
    BASEL_ACCT_ID 
    from last_def
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH source AS (
        SELECT PIT.OBSN_DT,
            PIT.BASEL_ACCT_ID,
            PIT.SRC_SYS_CD,
            TRIM(PIT.PIT_STATUS_ACCOUNT) AS PIT_STATUS,
            CAST(
                NULLIF(TRIM(PIT.STEP_PLN_AGRMNT_NUM), '') AS BIGINT
            ) AS STEP_PLN_AGRMNT_NUM
        FROM { UPSTREAM_ASSET [3] } PIT
        WHERE PIT.SRC_SYS_CD = 'SPL'
            AND PIT.OBSN_DT >= DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 40 MONTH
            AND PIT.OBSN_DT <= '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND (PIT.STEP_PLN_AGRMNT_NUM IS NULL or trim(PIT.STEP_PLN_AGRMNT_NUM) = '')
    ),
    def_block AS (
        SELECT BASEL_ACCT_ID, COUNT(*) AS CONS_COUNT
        FROM (
            SELECT
                BASEL_ACCT_ID,
                PIT_STATUS,
                OBSN_DT,
                CASE WHEN PIT_STATUS IN ('DEF','CHG') THEN 1 ELSE 0 END AS IS_DEF,
                SUM(CASE WHEN PIT_STATUS IN ('CUR','CLO') OR PIT_STATUS IS NULL OR STEP_PLN_AGRMNT_NUM IS NOT NULL THEN 1 ELSE 0 END)
                OVER (PARTITION BY BASEL_ACCT_ID ORDER BY OBSN_DT DESC ROWS UNBOUNDED PRECEDING) AS BREAK_AFTER
            FROM source
            WHERE OBSN_DT < '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ) sub
        WHERE IS_DEF = 1 AND BREAK_AFTER = 0
        GROUP BY BASEL_ACCT_ID
    )
    SELECT 
        s.OBSN_DT AS OBSN_DT,
        s.BASEL_ACCT_ID AS BASEL_ACCT_ID,
        CASE
            WHEN s.OBSN_DT != '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' THEN NULL
            WHEN s.PIT_STATUS IS NULL THEN NULL
            WHEN s.PIT_STATUS IN ('DEF','CHG') THEN COALESCE(db.CONS_COUNT, 0)
            WHEN s.PIT_STATUS = 'CUR' THEN NULL
            ELSE NULL
        END AS STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF
    FROM 
        source s
        LEFT JOIN def_block db ON s.BASEL_ACCT_ID = db.BASEL_ACCT_ID
    WHERE  
        OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT OBSN_DT, 
        BASEL_ACCT_ID, 
        STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_ks", key="parquet") }}}}')
        UNION ALL
        SELECT OBSN_DT, 
        BASEL_ACCT_ID, 
        STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_mor", key="parquet") }}}}')
        UNION ALL
        SELECT OBSN_DT,
        BASEL_ACCT_ID,
        STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_standalone_month_def_since_last_def.export_spl", key="parquet") }}}}')
    )
    """
):
    pass


"""
SOME NOTES FOR OPTIMIZATION, MOR, KS and SPL populations may be able to be pulled entirely from PIT_STATUS_ACCOUNT.
"""

