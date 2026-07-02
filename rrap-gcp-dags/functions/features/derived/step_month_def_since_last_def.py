from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


##utilizing previously developed logic from month_def_since_last_def JIRA RRAP-1876
UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',#0
    'reference.SRC_PRD_LKP',#1
    'ingestion.MORT_MTH_SNAPSHOT',#2
    'features.PIT_STATUS_STEP',#3
    'ingestion.BASEL_ACCT_DIM',#4
    'ingestion.TM_DIM',#5
]
DOWNSTREAM_ASSET = 'features.STEP_MONTH_DEF_SINCE_LAST_DEF'
DEPENDENCIES = {

    'duckdb_delete': ['export_ks', 'export_mor', 'export_spl'],
    'export_ks': ['duckdb_load'],
    'export_mor': ['duckdb_load'],
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

def export_ks(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
    WITH
    source AS (
        SELECT
            TM.TM_LVL_END_DT AS OBSN_DT,
            RSN.MTH_TM_ID,
            RSN.BASEL_ACCT_ID,
            CAST(NULLIF(TRIM(RSN.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            TRIM(PIT.PIT_STATUS_STEP) AS PIT_STATUS
        FROM {UPSTREAM_ASSET[0]} RSN
        INNER JOIN {UPSTREAM_ASSET[1]} LKP ON
            TRIM(RSN.PRD_CD) = TRIM(LKP.SRC_PRD_CD)
            AND TRIM(RSN.SUB_PRD_CD) = TRIM(LKP.SRC_SUB_PRD_CD)
            AND TRIM(LKP.PRD_SYS_CD) = 'KS'
            AND TRIM(LKP.SML_BUS_F) = 'N'
            AND TRIM(LKP.CRNT_F) = 'Y'
        INNER JOIN {UPSTREAM_ASSET[5]} TM ON
            TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
            AND RSN.MTH_TM_ID = TM.TM_ID
        LEFT JOIN {UPSTREAM_ASSET[3]} PIT ON
            RSN.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
            AND TM.TM_LVL_END_DT = PIT.OBSN_DT
        WHERE
        TRIM(SRC_SYS_CD) = 'KS'
        AND
            RSN.MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - (40 * 40)
            AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    ),
    max_non_dft AS (
        SELECT BASEL_ACCT_ID, 
        MAX(OBSN_DT) AS MAX_OBSN_DT
        FROM source s
        WHERE
            s.OBSN_DT < '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND (s.PIT_STATUS IS NULL OR s.PIT_STATUS NOT IN ('DEF','CHG'))
        GROUP BY BASEL_ACCT_ID
    ),
    tmp AS (
        SELECT source.BASEL_ACCT_ID, 
        COUNT(source.MTH_TM_ID) AS CONS_DFT_MTH_CNT
        FROM source 
        LEFT JOIN max_non_dft p 
        ON source.BASEL_ACCT_ID = p.BASEL_ACCT_ID
        WHERE
            source.OBSN_DT < '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND (TRIM(source.PIT_STATUS) IN ('DEF', 'CHG'))
            AND(
                p.MAX_OBSN_DT IS NULL
             OR source.OBSN_DT > p.MAX_OBSN_DT
            )
        GROUP BY source.BASEL_ACCT_ID
    ), months_def as(
    SELECT
        s.OBSN_DT,
        s.BASEL_ACCT_ID,
        s.STEP_PLN_AGRMNT_NUM,
        NULL AS MORT_NUM,
        CASE
            WHEN s.PIT_STATUS IN ('DEF','CHG')
                THEN COALESCE(t.CONS_DFT_MTH_CNT, 0)
            ELSE NULL
        END AS MONTH_DEF_SINCE_LAST_DEF
    FROM source s
    LEFT JOIN tmp t
    ON s.BASEL_ACCT_ID=t.BASEL_ACCT_ID
    WHERE s.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ), final as (
    select 
    max(MONTH_DEF_SINCE_LAST_DEF) AS  step_month_def_since_last_def, 
    step_pln_agrmnt_num,
    BASEL_ACCT_ID
    from months_def 
    group by basel_acct_id, step_pln_agrmnt_num
    )
    select
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    step_month_def_since_last_def,
    step_pln_agrmnt_num,
    BASEL_ACCT_ID
    from final
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
    WITH source AS (
        SELECT
            PIT.BASEL_ACCT_ID,
            PIT.SRC_SYS_CD,
            MORT.MTH_TM_ID,
            CAST(NULLIF(TRIM(MORT.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            TRIM(PIT.PIT_STATUS_STEP) AS PIT_STATUS,
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
    ),
    def_block AS (
        SELECT BASEL_ACCT_ID, COUNT(*) AS CONS_COUNT
        FROM (
            SELECT
                BASEL_ACCT_ID,
                PIT_STATUS,
                MTH_TM_ID,
                CASE WHEN PIT_STATUS IN ('DEF','CHG') THEN 1 ELSE 0 END AS IS_DEF,
                SUM(CASE WHEN PIT_STATUS IN ('CUR','CLO') OR PIT_STATUS IS NULL THEN 1 ELSE 0 END)
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
    ), final as (
    select 
    max(MONTH_DEF_SINCE_LAST_DEF) as step_month_def_since_last_def, 
    step_pln_agrmnt_num,
    BASEL_ACCT_ID
    FROM last_def 
    GROUP BY 
    BASEL_ACCT_ID,
    STEP_PLN_AGRMNT_NUM
    )
    select
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    step_month_def_since_last_def,
    step_pln_agrmnt_num,
    BASEL_ACCT_ID
    from final
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
    WITH source AS (
        SELECT PIT.OBSN_DT,
            PIT.BASEL_ACCT_ID,
            PIT.SRC_SYS_CD,
            TRIM(PIT.PIT_STATUS_STEP) AS PIT_STATUS,
            CAST(
                NULLIF(TRIM(PIT.STEP_PLN_AGRMNT_NUM), '') AS BIGINT
            ) AS STEP_PLN_AGRMNT_NUM
        FROM { UPSTREAM_ASSET [3] } PIT
        WHERE PIT.SRC_SYS_CD = 'SPL'
            AND PIT.OBSN_DT >= DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 40 MONTH
            AND PIT.OBSN_DT <= '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND (PIT.STEP_PLN_AGRMNT_NUM IS NOT NULL or trim(PIT.STEP_PLN_AGRMNT_NUM) != '')
    ),
    def_block AS (
        SELECT BASEL_ACCT_ID, COUNT(*) AS CONS_COUNT
        FROM (
            SELECT
                BASEL_ACCT_ID,
                PIT_STATUS,
                OBSN_DT,
                CASE WHEN PIT_STATUS IN ('DEF','CHG') THEN 1 ELSE 0 END AS IS_DEF,
                SUM(CASE WHEN PIT_STATUS IN ('CUR','CLO') OR PIT_STATUS IS NULL THEN 1 ELSE 0 END)
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
        s.STEP_PLN_AGRMNT_NUM,
        CASE
            WHEN s.OBSN_DT != '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' THEN NULL
            WHEN s.PIT_STATUS IS NULL THEN NULL
            WHEN s.PIT_STATUS IN ('DEF','CHG') THEN COALESCE(db.CONS_COUNT, 0)
            WHEN s.PIT_STATUS = 'CUR' THEN NULL
            ELSE NULL
        END AS STEP_MONTH_DEF_SINCE_LAST_DEF
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
        with combo as (
            SELECT OBSN_DT,
            BASEL_ACCT_ID,
            STEP_PLN_AGRMNT_NUM, 
            STEP_MONTH_DEF_SINCE_LAST_DEF,
            'KS' AS src
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_month_def_since_last_def.export_ks", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT,
            BASEL_ACCT_ID, 
            STEP_PLN_AGRMNT_NUM, 
            STEP_MONTH_DEF_SINCE_LAST_DEF,
            'MOR' AS src
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_month_def_since_last_def.export_mor", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID,
            STEP_PLN_AGRMNT_NUM, 
            STEP_MONTH_DEF_SINCE_LAST_DEF,
            'SPL' AS src
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_month_def_since_last_def.export_spl", key="parquet") }}}}')
        )
        select 
        OBSN_DT,
        STEP_PLN_AGRMNT_NUM,
        COALESCE(
                MAX(CASE WHEN src = 'MOR' THEN STEP_MONTH_DEF_SINCE_LAST_DEF END),
                MAX(CASE WHEN src = 'KS'  THEN STEP_MONTH_DEF_SINCE_LAST_DEF END),
                MAX(CASE WHEN src = 'SPL' THEN STEP_MONTH_DEF_SINCE_LAST_DEF END)
            ) AS STEP_MONTH_DEF_SINCE_LAST_DEF
        --max(STEP_MONTH_DEF_SINCE_LAST_DEF) as STEP_MONTH_DEF_SINCE_LAST_DEF
        from combo
        group by STEP_PLN_AGRMNT_NUM,OBSN_DT
    )
    """
):
    pass

