from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT', 
    'reference.SRC_PRD_LKP',
    'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
    'ingestion.TM_DIM',
    'ingestion.MORT_MTH_SNAPSHOT',
    'features.TNG_FINAL_DEFAULT_IND',
    'ingestion.BASEL_ACCT_DIM',
    'ingestion.TNG_ACCT_MO'
]

DOWNSTREAM_ASSET = 'features.MONTH_DEF_24M'


DEPENDENCIES = {
    'duckdb_delete_month_def_24m': [
        'export_ks',
        'export_mor',
        'export_tng',
    ],
    'export_ks': ['duckdb_load_month_def_24m'],
    'export_mor': ['duckdb_load_month_def_24m'],
    'export_tng': ['duckdb_load_month_def_24m'],
}


def duckdb_delete_month_def_24m(
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
    sql=rf"""
    WITH source AS (
        SELECT
            TM.TM_LVL_END_DT AS OBSN_DT,
            RSN.MTH_TM_ID,
            RSN.BASEL_ACCT_ID,
            TRIM(PIT.PIT_STATUS_CROSS_DEFAULT_ORIG) AS PIT_STATUS
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT RSN
        INNER JOIN reference.SRC_PRD_LKP LKP
            ON TRIM(RSN.PRD_CD) = TRIM(LKP.SRC_PRD_CD)
            AND TRIM(RSN.SUB_PRD_CD) = TRIM(LKP.SRC_SUB_PRD_CD)
            AND TRIM(LKP.PRD_SYS_CD) = 'KS'
            AND TRIM(LKP.SML_BUS_F) = 'N'
            AND TRIM(LKP.CRNT_F) = 'Y'
        INNER JOIN ingestion.TM_DIM TM
            ON TRIM(UPPER(TM.TM_LVL)) = 'MONTH'
            AND RSN.MTH_TM_ID = TM.TM_ID
        LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG PIT
            ON RSN.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
            AND TM.TM_LVL_END_DT = PIT.OBSN_DT
        WHERE
            RSN.MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - (40 * 24)
            AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    ),

    max_non_dft AS (
        SELECT BASEL_ACCT_ID, MAX(OBSN_DT) AS MAX_OBSN_DT
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG PIT
        WHERE
            BASEL_ACCT_ID IN (
                SELECT BASEL_ACCT_ID FROM source WHERE TRIM(PIT_STATUS) IN ('DEF', 'CHG')
            )
            AND TRIM(PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
            AND OBSN_DT BETWEEN 
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 24 MONTH
            AND DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY BASEL_ACCT_ID
    ),

    tmp AS (
        SELECT source.BASEL_ACCT_ID, COUNT(source.MTH_TM_ID) AS CONS_DFT_MTH_CNT
        FROM source
        LEFT JOIN max_non_dft p
            ON source.BASEL_ACCT_ID = p.BASEL_ACCT_ID
        WHERE
            source.OBSN_DT > p.MAX_OBSN_DT
            AND source.OBSN_DT < '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND TRIM(source.PIT_STATUS) IN ('DEF', 'CHG')
        GROUP BY source.BASEL_ACCT_ID
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        s.BASEL_ACCT_ID,
        CASE
            WHEN s.PIT_STATUS NOT IN ('DEF', 'CHG') OR s.PIT_STATUS IS NULL THEN NULL
            WHEN CONS_DFT_MTH_CNT IS NULL AND MAX_OBSN_DT IS NOT NULL THEN 0
            WHEN (CONS_DFT_MTH_CNT IS NULL OR CONS_DFT_MTH_CNT = 0) AND MAX_OBSN_DT IS NULL THEN 24
            ELSE CONS_DFT_MTH_CNT
        END AS MONTH_DEF_24M,
        'KS' AS SRC_SYS_CD
    FROM source s
    LEFT JOIN max_non_dft p
        ON s.BASEL_ACCT_ID = p.BASEL_ACCT_ID
    LEFT JOIN tmp
        ON s.BASEL_ACCT_ID = tmp.BASEL_ACCT_ID
    WHERE s.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def export_mor(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=rf"""
    WITH source AS (
        SELECT
            PIT.BASEL_ACCT_ID,
            MORT.MTH_TM_ID,
            TRIM(PIT.PIT_STATUS_CROSS_DEFAULT_ORIG) AS PIT_STATUS
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG PIT
        INNER JOIN ingestion.TM_DIM TM
            ON PIT.OBSN_DT = TM.TM_LVL_END_DT
        INNER JOIN ingestion.MORT_MTH_SNAPSHOT MORT
            ON PIT.BASEL_ACCT_ID = MORT.BASEL_ACCT_ID
            AND TM.TM_ID = MORT.MTH_TM_ID
        WHERE
            MORT.MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - (40 * 24)
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
                SUM(
                    CASE WHEN PIT_STATUS IN ('CUR','CLO') OR PIT_STATUS IS NULL THEN 1 ELSE 0 END
                ) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID DESC) AS BREAK_AFTER
            FROM source
            WHERE MTH_TM_ID < {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ) sub
        WHERE IS_DEF = 1 AND BREAK_AFTER = 0
        GROUP BY BASEL_ACCT_ID
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        s.BASEL_ACCT_ID,
        CASE
            WHEN s.PIT_STATUS IN ('DEF','CHG') THEN COALESCE(d.CONS_COUNT, 0)

        /*
        * SAS lineage alignment:
        *
        * Legacy Mortgage implementation ultimately populates
        * BASEL_ANALYTCL_BL_INSTRMNT_FACT.CONS_DFT_MTH_CNT via:
        *
        * [SAS job: RRAP_MOR_MODEL_23_BNS_MOR_LGD_D_G]
        *     -> ALL_LGD_D.MONTH_DEF     (%DEFMONTHS Macro)
        *     -> BNS_LGD_D_SCORED_SEG_ACCTS.MONTH_DEF
        *
        * [SAS job: RRAP_MOR_MODEL_55_LOAD_PRE_06_LARGE_JOIN]
        *     -> BASE_BNS_MOR_RPT_TBL.CONS_DFT_MTH_CNT
        *        = CASE WHEN STATUS='DEF'
        *               THEN MONTH_DEF
        *               ELSE 0
        *          END
        *
        * [SAS job: RRAP_MOR_MODEL_60_BNS_INSTRUMENT_FACT]
        *     -> BASEL_ANALYTCL_BL_INSTRMNT_FACT.CONS_DFT_MTH_CNT
        *
        * Verification of historical SAS output showed that
        * CONS_DFT_MTH_CNT contains values only in the range
        * 0-24 and does not contain NULL values.
        *
        * Therefore non-default accounts are assigned 0 instead of NULL
        */

            --ELSE NULL
            ELSE 0
        END AS MONTH_DEF_24M,
        'MOR' AS SRC_SYS_CD
    FROM source s
    LEFT JOIN def_block d USING (BASEL_ACCT_ID)
    WHERE s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def export_tng(
    duckdb_conn_id='duckdb-conn',
    resource_tier='HIGH',
    pool_slots=96,
    sql=rf"""
    WITH def AS (
        SELECT
            obsn_dt,
            TRIM(account_id) account_id
        FROM features.TNG_FINAL_DEFAULT_IND
        WHERE
            tng_final_default_ind = 'Y'
            AND obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ),

    lag AS (
        SELECT
            a.obsn_dt,
            TRIM(a.account_id) account_id,
            a.tng_final_default_ind,
            LAG(a.tng_final_default_ind) OVER (PARTITION BY a.account_id ORDER BY a.obsn_dt) AS prev
        FROM features.TNG_FINAL_DEFAULT_IND a
        INNER JOIN def b
            ON TRIM(a.account_id) = TRIM(b.account_id)
            AND a.obsn_dt <= b.obsn_dt
    ),

    t AS (
        SELECT
            a.account_id,
            DATE_DIFF('month', MAX(b.obsn_dt), a.obsn_dt) AS months_in_default
        FROM def a
        INNER JOIN lag b
            ON a.account_id = b.account_id
        WHERE
            b.tng_final_default_ind = 'Y'
            AND (b.prev = 'N' OR b.prev IS NULL)
        GROUP BY a.account_id, a.obsn_dt
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        b.basel_acct_id,
        CASE
            WHEN t.months_in_default IS NULL THEN NULL
            WHEN t.months_in_default > 24 THEN 24             -- Capped at 24 months as per BASEL_ANALYTCL_BL_INSTRMNT_FACT constraint
            ELSE t.months_in_default
        END AS MONTH_DEF_24M,
        b.src_app_cd AS SRC_SYS_CD
    FROM ingestion.TNG_ACCT_MO a
    INNER JOIN ingestion.BASEL_ACCT_DIM b
        ON TRIM(a.account_id) = TRIM(b.src_app_id)
        AND b.src_app_cd = 'TNG-MOR'
        AND b.src_sys_del_f != 'Y'
    LEFT JOIN t
        ON a.account_id = t.account_id
    WHERE
        a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load_month_def_24m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT OBSN_DT, BASEL_ACCT_ID, MONTH_DEF_24M, SRC_SYS_CD
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__month_def_24m.export_ks", key="parquet") }}}}')

        UNION ALL
        SELECT OBSN_DT, BASEL_ACCT_ID, MONTH_DEF_24M, SRC_SYS_CD
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__month_def_24m.export_mor", key="parquet") }}}}')

        UNION ALL
        SELECT OBSN_DT, BASEL_ACCT_ID, MONTH_DEF_24M, SRC_SYS_CD
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__month_def_24m.export_tng", key="parquet") }}}}')
    )
    """
):
    pass