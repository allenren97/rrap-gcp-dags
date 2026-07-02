from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT', 
    'reference.SRC_PRD_LKP',
    'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
    'ingestion.TM_DIM',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
]

DOWNSTREAM_ASSET = 'features.MONTH_DEF'

DEPENDENCIES = {
    'duckdb_delete_month_def': ['export_account_buckets'],
    'export_account_buckets': [
        'export_spl',
        'export_ks_batch_1',
        'export_ks_batch_2',
        'export_ks_batch_3',
        'export_ks_batch_4',
        'export_ks_batch_5',
        'export_ks_batch_6',
    ],
    'export_spl': ['duckdb_load_month_def'],
    'export_ks_batch_1': ['duckdb_load_month_def'],
    'export_ks_batch_2': ['duckdb_load_month_def'],
    'export_ks_batch_3': ['duckdb_load_month_def'],
    'export_ks_batch_4': ['duckdb_load_month_def'],
    'export_ks_batch_5': ['duckdb_load_month_def'],
    'export_ks_batch_6': ['duckdb_load_month_def'],
}


RENDER_SQL="""
WITH
params AS (
    SELECT
        DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS run_dt,
        REPLACE_COUNT AS batch_count,
        REPLACE_ID AS batch_id
),
batch_accounts AS MATERIALIZED (
    SELECT
        B.BASEL_ACCT_ID
    FROM '{{ task_instance.xcom_pull(task_ids="derived__month_def.export_account_buckets", key="parquet") }}' B
    CROSS JOIN params P
    WHERE
        B.RUN_DT = P.run_dt
        AND B.BATCH_COUNT = P.batch_count
        AND B.BATCH_ID = P.batch_id
),
source AS MATERIALIZED (
    SELECT
        TM.TM_LVL_END_DT AS OBSN_DT,
        RSN.BASEL_ACCT_ID,
        TRIM(PIT.PIT_STATUS_CROSS_DEFAULT_ORIG) AS PIT_STATUS
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT RSN
    INNER JOIN reference.SRC_PRD_LKP LKP ON
        TRIM(RSN.PRD_CD) = TRIM(LKP.SRC_PRD_CD)
        AND TRIM(RSN.SUB_PRD_CD) = TRIM(LKP.SRC_SUB_PRD_CD)
        AND TRIM(LKP.PRD_SYS_CD) = 'KS'
        AND TRIM(LKP.SML_BUS_F) = 'N'
        AND TRIM(LKP.CRNT_F) = 'Y'
    INNER JOIN ingestion.TM_DIM TM ON
        TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
        AND RSN.MTH_TM_ID = TM.TM_ID
    INNER JOIN batch_accounts BA ON
        RSN.BASEL_ACCT_ID = BA.BASEL_ACCT_ID
    LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG PIT ON
        RSN.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
        AND TM.TM_LVL_END_DT = PIT.OBSN_DT
),
last_non_def AS (
    SELECT
        S.BASEL_ACCT_ID,
        MAX(S.OBSN_DT) AS LAST_NON_DEF_OBSN_DT
    FROM source S
    CROSS JOIN params P
    WHERE
        S.OBSN_DT < P.run_dt
        AND (S.PIT_STATUS IS NULL OR S.PIT_STATUS NOT IN ('DEF','CHG'))
    GROUP BY S.BASEL_ACCT_ID
),
tmp AS (
    SELECT
        S.BASEL_ACCT_ID,
        COUNT(*) AS CONS_DFT_MTH_CNT
    FROM source S
    CROSS JOIN params P
    LEFT JOIN last_non_def L ON
        S.BASEL_ACCT_ID = L.BASEL_ACCT_ID
    WHERE
        S.OBSN_DT < P.run_dt
        AND S.PIT_STATUS IN ('DEF','CHG')
        AND (
            L.LAST_NON_DEF_OBSN_DT IS NULL
            OR S.OBSN_DT > L.LAST_NON_DEF_OBSN_DT
        )
    GROUP BY S.BASEL_ACCT_ID
)
SELECT
    P.run_dt AS OBSN_DT,
    S.BASEL_ACCT_ID,
    CASE
        WHEN S.PIT_STATUS IN ('DEF', 'CHG') THEN COALESCE(TMP.CONS_DFT_MTH_CNT, 0)
        ELSE NULL
    END AS MONTH_DEF,
    'KS' AS SRC_SYS_CD
FROM source S
CROSS JOIN params P
LEFT JOIN tmp TMP ON
    S.BASEL_ACCT_ID = TMP.BASEL_ACCT_ID
WHERE S.OBSN_DT = P.run_dt
"""


def duckdb_delete_month_def(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def export_account_buckets(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH _bucket_params AS (
        SELECT 
            DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS run_dt,
            6 AS batch_count,
            0 AS batch_id
    )
    SELECT DISTINCT
        (SELECT run_dt FROM _bucket_params) AS RUN_DT,
        (SELECT batch_count FROM _bucket_params) AS BATCH_COUNT,
        MOD(HASH(RSN.BASEL_ACCT_ID), (SELECT batch_count FROM _bucket_params)) AS BATCH_ID,
        RSN.BASEL_ACCT_ID
    FROM {UPSTREAM_ASSET[0]} RSN
    INNER JOIN {UPSTREAM_ASSET[1]} LKP ON
        TRIM(RSN.PRD_CD) = TRIM(LKP.SRC_PRD_CD)
        AND TRIM(RSN.SUB_PRD_CD) = TRIM(LKP.SRC_SUB_PRD_CD)
        AND TRIM(LKP.PRD_SYS_CD) = 'KS'
        AND TRIM(LKP.SML_BUS_F) = 'N'
        AND TRIM(LKP.CRNT_F) = 'Y'
    INNER JOIN {UPSTREAM_ASSET[3]} TM ON
        TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
        AND RSN.MTH_TM_ID = TM.TM_ID
    LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG PIT ON
        RSN.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
        AND TM.TM_LVL_END_DT = PIT.OBSN_DT
    WHERE 
        TM.TM_LVL_END_DT = (SELECT run_dt FROM _bucket_params)
    """
):
    pass


def export_ks_batch_1(
    duckdb_conn_id='duckdb-conn',
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "0"),
):
    pass


def export_ks_batch_2(
    duckdb_conn_id='duckdb-conn',
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "1"),
):
    pass


def export_ks_batch_3(
    duckdb_conn_id='duckdb-conn',
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "2"),
):
    pass


def export_ks_batch_4(
    duckdb_conn_id='duckdb-conn',
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "3"),
):
    pass


def export_ks_batch_5(
    duckdb_conn_id='duckdb-conn',
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "4"),
):
    pass


def export_ks_batch_6(
    duckdb_conn_id='duckdb-conn',
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "5"),
):
    pass


def export_spl(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=f"""
    WITH source AS (
        SELECT
            PIT.BASEL_ACCT_ID,
            PSNL.MTH_TM_ID,
            TRIM(PIT.PIT_STATUS_CROSS_DEFAULT_ORIG) AS PIT_STATUS
        FROM {UPSTREAM_ASSET[2]} PIT
        INNER JOIN {UPSTREAM_ASSET[3]} TM ON
            TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
            AND PIT.OBSN_DT = TM.TM_LVL_END_DT
        INNER JOIN {UPSTREAM_ASSET[4]} PSNL ON
            PIT.BASEL_ACCT_ID = PSNL.BASEL_ACCT_ID
            AND TM.TM_ID = PSNL.MTH_TM_ID
    ),

    ordered AS (
        SELECT
            BASEL_ACCT_ID,
            MTH_TM_ID,
            PIT_STATUS,

            LEAD(MTH_TM_ID) OVER (
                PARTITION BY BASEL_ACCT_ID
                ORDER BY MTH_TM_ID DESC
            ) AS PREV_MTH_TM_ID

        FROM source
        WHERE
            MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}

            -- Note the "<=" inclusive operator here - current month's DEF/CHG
            -- is included in the consecutive default count. This aligns SPL
            -- logic with baseline behavior where a new default in the current
            -- month correctly results in MONTH_DEF = 1.
    ),

    last_break AS (
        SELECT
            BASEL_ACCT_ID,
            MAX(MTH_TM_ID) AS LAST_BREAK_MTH_TM_ID
        FROM ordered
        WHERE
            PIT_STATUS IN ('CUR', 'CLO')
            OR PIT_STATUS IS NULL

            -- Break the consecutive default streak when monthly
            -- snapshots are missing. This mirrors the PROD SAS behavior
            -- where cons_mths_default is derived from the immediately
            -- preceding month only. If one or more months are absent,
            -- the default sequence restarts at the current DEF/CHG month.
            OR (
                PREV_MTH_TM_ID IS NOT NULL
                AND MTH_TM_ID - PREV_MTH_TM_ID <> 40
            )
        GROUP BY BASEL_ACCT_ID
    ),

    def_block AS (
        SELECT
            o.BASEL_ACCT_ID,
            COUNT(*) AS CONS_COUNT
        FROM ordered o
        LEFT JOIN last_break b ON
            o.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        WHERE
            o.PIT_STATUS IN ('DEF', 'CHG')
            AND (
                b.LAST_BREAK_MTH_TM_ID IS NULL
                OR o.MTH_TM_ID >= b.LAST_BREAK_MTH_TM_ID
            )
        GROUP BY o.BASEL_ACCT_ID
    ),

    final AS (
        SELECT
            s.BASEL_ACCT_ID,
            s.MTH_TM_ID,
            s.PIT_STATUS,
            CASE
                WHEN s.PIT_STATUS IN ('DEF', 'CHG')
                    THEN COALESCE(db.CONS_COUNT, 0)
                ELSE 0
            END AS MONTH_DEF
        FROM source s
        LEFT JOIN def_block db ON
            s.BASEL_ACCT_ID = db.BASEL_ACCT_ID
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        MONTH_DEF,
        'SPL' AS SRC_SYS_CD
    FROM final
    WHERE
        MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def duckdb_load_month_def(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME FROM (
        SELECT OBSN_DT, BASEL_ACCT_ID, MONTH_DEF, SRC_SYS_CD
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_ks_batch_1", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_ks_batch_2", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_ks_batch_3", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_ks_batch_4", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_ks_batch_5", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_ks_batch_6", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__month_def.export_spl", key="parquet") }}}}'
        ], union_by_name = true)
    )
    """
):
    pass