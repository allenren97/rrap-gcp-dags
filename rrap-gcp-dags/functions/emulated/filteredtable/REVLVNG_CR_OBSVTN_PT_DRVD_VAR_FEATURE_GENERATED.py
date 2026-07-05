"""
Rewrite of J_RRII_KS10_2510 via pre-materialized features (skips 2103).

Builds emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR from:
  snapshot balances + features.* (PIT_STATUS_CD = PIT_STATUS_CROSS_DEFAULT_ORIG)
  export_dataprep -> export_pdead / export_lgd -> duckdb_load

Requires features DAG backfill for every OBSN_DT in the PD/LGD history windows
(~38 month-ends for a single process month).
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "features.BASEL_PRD_CD",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.HELOC_F",
    "features.ACCRL_STAT_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.SML_BUS_F",
    "features.TRNST_EXCLSN_F",
]

DOWNSTREAM_ASSET = "emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR"

_TASK_GROUP = "filteredtable__REVLVNG_CR_OBSVTN_PT_DRVD_VAR_FEATURE_GENERATED"

DEPENDENCIES = {
    "duckdb_delete": ["export_dataprep"],
    "export_dataprep": ["export_pdead", "export_lgd"],
    "export_pdead": ["duckdb_load"],
    "export_lgd": ["duckdb_load"],
}

# Shared CASE from rrap_defaulter_model.sas (used by export_pdead and export_lgd).
_NEW_DEFAULT_FLG = """
    CASE
        WHEN SRC_SYS_CD IN ('SPL')
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND OS_BAL_AMT >= 1
         AND TOT_CRNT_BAL_AMT > 0
         AND COALESCE(LAG_PIT_STATUS_CD, 'CUR') NOT IN ('DEF', 'CHG')
        THEN 1
        WHEN SRC_SYS_CD IN ('TNG-MOR', 'MOR')
         AND PIT_STATUS_CD IN ('DEF')
         AND LAG_PIT_STATUS_CD = 'CUR'
        THEN 1
        WHEN SRC_SYS_CD IN ('KS')
         AND BASEL_PRD_CD = 'CC'
         AND HELOC_F = 'N'
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND LAG_PIT_STATUS_CD = 'CUR'
         AND OS_BAL_AMT > 0
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
        THEN 1
        WHEN SRC_SYS_CD IN ('KS')
         AND BASEL_PRD_CD = 'CC'
         AND HELOC_F = 'N'
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND LAG_PIT_STATUS_CD = 'CUR'
         AND OS_BAL_AMT > 0
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <= 0
        THEN 1
        WHEN SRC_SYS_CD IN ('KS')
         AND BASEL_PRD_CD = 'CC'
         AND HELOC_F = 'N'
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND LAG_PIT_STATUS_CD = 'CUR'
         AND OS_BAL_AMT > 0
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT > 0
         AND TOT_UNPAID_FNCL_CHRG_AMT <> OS_BAL_AMT
         AND PIT_STATUS_CD <> 'CHG'
         AND OS_BAL_AMT >= 5
        THEN 1
        WHEN SRC_SYS_CD IN ('KS')
         AND BASEL_PRD_CD = 'CC'
         AND HELOC_F = 'N'
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND LAG_PIT_STATUS_CD = 'CUR'
        THEN 0
        WHEN SRC_SYS_CD IN ('KS')
         AND BASEL_PRD_CD = 'CC'
         AND HELOC_F = 'Y'
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND LAG_PIT_STATUS_CD = 'CUR'
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
         AND OS_BAL_AMT > 0
        THEN 1
        WHEN SRC_SYS_CD IN ('KS')
         AND BASEL_PRD_CD <> 'CC'
         AND PIT_STATUS_CD IN ('DEF', 'CHG')
         AND LAG_PIT_STATUS_CD = 'CUR'
         AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
         AND OS_BAL_AMT > 0
        THEN 1
        ELSE 0
    END
"""


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_dataprep(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        )
    SELECT
        snp.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        snp.MTH_TM_ID,
        snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT,
        snp.TOT_UNPAID_FNCL_CHRG_AMT,
        accrl.ACCRL_STAT_F,
        pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS_CD,
        prd.BASEL_PRD_CD,
        CAST(NULL AS DOUBLE) AS TOT_CRNT_BAL_AMT,
        trt.CONSM_PRD_TREATMNT_CD,
        sml.SML_BUS_F,
        trnst.TRNST_EXCLSN_F,
        heloc.HELOC_F
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
    INNER JOIN ingestion.TM_DIM tm
        ON snp.MTH_TM_ID = tm.TM_ID
       AND TRIM(tm.TM_LVL) = 'Month'
    CROSS JOIN mth_tm_id
    LEFT JOIN features.BASEL_PRD_CD prd
        ON snp.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
       AND prd.OBSN_DT = tm.TM_LVL_END_DT
    LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
       AND pit.OBSN_DT = tm.TM_LVL_END_DT
       AND pit.SRC_SYS_CD = 'KS'
    LEFT JOIN features.HELOC_F heloc
        ON snp.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
       AND heloc.OBSN_DT = tm.TM_LVL_END_DT
    LEFT JOIN features.ACCRL_STAT_F accrl
        ON snp.BASEL_ACCT_ID = accrl.BASEL_ACCT_ID
       AND accrl.OBSN_DT = tm.TM_LVL_END_DT
    LEFT JOIN features.CONSM_PRD_TREATMNT_CD trt
        ON snp.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
       AND trt.OBSN_DT = tm.TM_LVL_END_DT
    LEFT JOIN features.SML_BUS_F sml
        ON snp.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
       AND sml.OBSN_DT = tm.TM_LVL_END_DT
    LEFT JOIN features.TRNST_EXCLSN_F trnst
        ON snp.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
       AND trnst.OBSN_DT = tm.TM_LVL_END_DT
    WHERE (
        snp.MTH_TM_ID >= mth_tm_id.val - 12 * 40
        AND snp.MTH_TM_ID <= mth_tm_id.val
    ) OR (
        snp.MTH_TM_ID >= mth_tm_id.val - 48 * 40
        AND snp.MTH_TM_ID <= mth_tm_id.val - 24 * 40
    )
    ORDER BY snp.BASEL_ACCT_ID, snp.MTH_TM_ID
    """,
):
    pass


def export_pdead(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        dataprep AS (
            SELECT *
            FROM read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_dataprep", key="parquet") }}}}'
            )
        ),
        lagged AS (
            SELECT
                *,
                LAG(OS_BAL_AMT) OVER (
                    PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
                ) AS LAG_OS_BAL_AMT,
                LAG(TOT_UNPAID_FNCL_CHRG_AMT) OVER (
                    PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
                ) AS LAG_TOT_UNPAID_FNCL_CHRG_AMT,
                LAG(PIT_STATUS_CD) OVER (
                    PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
                ) AS LAG_PIT_STATUS_CD
            FROM dataprep
            CROSS JOIN mth_tm_id
            WHERE MTH_TM_ID >= mth_tm_id.val - 12 * 40
              AND MTH_TM_ID <= mth_tm_id.val
        ),
        new_defaults AS (
            SELECT BASEL_ACCT_ID, MTH_TM_ID
            FROM lagged
            WHERE {_NEW_DEFAULT_FLG} = 1
        ),
        defaults AS (
            SELECT
                BASEL_ACCT_ID,
                MAX(MTH_TM_ID) AS LAST_NEW_DEFAULT_DATE
            FROM new_defaults
            GROUP BY BASEL_ACCT_ID
        )
    SELECT
        d.BASEL_ACCT_ID,
        1 AS MODEL_DFT_F,
        d.LAST_NEW_DEFAULT_DATE,
        CASE
            WHEN snp_def.SRC_SYS_CD IN ('KS')
             AND (
                 snp_def.PIT_STATUS_CD = 'CHG'
                 OR snp_def.ACCRL_STAT_F = 'N'
             )
             AND snp_def.OS_BAL_AMT = 0
            THEN GREATEST(COALESCE(snp_def_ks_lag.OS_BAL_AMT, 0), 0)
            ELSE GREATEST(COALESCE(snp_def.OS_BAL_AMT, 0), 0)
        END AS LAST_NEW_DEFAULT_OS_BAL_AMT
    FROM defaults d
    LEFT JOIN dataprep snp_def
        ON snp_def.BASEL_ACCT_ID = d.BASEL_ACCT_ID
       AND snp_def.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE
    LEFT JOIN dataprep snp_def_ks_lag
        ON snp_def_ks_lag.BASEL_ACCT_ID = d.BASEL_ACCT_ID
       AND snp_def_ks_lag.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE - 40
    """,
):
    pass


def export_lgd(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        dataprep AS (
            SELECT *
            FROM read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_dataprep", key="parquet") }}}}'
            )
        ),
        lagged AS (
            SELECT
                *,
                LAG(OS_BAL_AMT) OVER (
                    PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
                ) AS LAG_OS_BAL_AMT,
                LAG(TOT_UNPAID_FNCL_CHRG_AMT) OVER (
                    PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
                ) AS LAG_TOT_UNPAID_FNCL_CHRG_AMT,
                LAG(PIT_STATUS_CD) OVER (
                    PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
                ) AS LAG_PIT_STATUS_CD
            FROM dataprep
            CROSS JOIN mth_tm_id
            WHERE MTH_TM_ID >= mth_tm_id.val - 48 * 40
              AND MTH_TM_ID <= mth_tm_id.val - 24 * 40
        ),
        new_defaults AS (
            SELECT BASEL_ACCT_ID, MTH_TM_ID
            FROM lagged
            WHERE {_NEW_DEFAULT_FLG} = 1
        ),
        defaults AS (
            SELECT
                BASEL_ACCT_ID,
                MAX(MTH_TM_ID) AS LAST_NEW_DEFAULT_DATE
            FROM new_defaults
            GROUP BY BASEL_ACCT_ID
        )
    SELECT
        d.BASEL_ACCT_ID,
        1 AS MODEL_DFT_F,
        d.LAST_NEW_DEFAULT_DATE,
        CASE
            WHEN snp_def.SRC_SYS_CD IN ('KS')
             AND (
                 snp_def.PIT_STATUS_CD = 'CHG'
                 OR snp_def.ACCRL_STAT_F = 'N'
             )
             AND snp_def.OS_BAL_AMT = 0
            THEN GREATEST(COALESCE(snp_def_ks_lag.OS_BAL_AMT, 0), 0)
            ELSE GREATEST(COALESCE(snp_def.OS_BAL_AMT, 0), 0)
        END AS LAST_NEW_DEFAULT_OS_BAL_AMT
    FROM defaults d
    LEFT JOIN dataprep snp_def
        ON snp_def.BASEL_ACCT_ID = d.BASEL_ACCT_ID
       AND snp_def.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE
    LEFT JOIN dataprep snp_def_ks_lag
        ON snp_def_ks_lag.BASEL_ACCT_ID = d.BASEL_ACCT_ID
       AND snp_def_ks_lag.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE - 40
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        dataprep AS (
            SELECT *
            FROM read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_dataprep", key="parquet") }}}}'
            )
        ),
        pdead AS (
            SELECT *
            FROM read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_pdead", key="parquet") }}}}'
            )
        ),
        lgd AS (
            SELECT *
            FROM read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_lgd", key="parquet") }}}}'
            )
        ),
        pdead_rows AS (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
                a.MTH_TM_ID AS OBSVTN_MTH_TM_ID,
                b.BASEL_ACCT_ID,
                b.LAST_NEW_DEFAULT_OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                b.LAST_NEW_DEFAULT_DATE AS LAST_NEW_DFT_TM_ID,
                t.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
                CASE WHEN b.MODEL_DFT_F = 1 THEN 'Y' ELSE 'N' END AS MODEL_DFT_F,
                b.LAST_NEW_DEFAULT_DATE + 24 * 40 AS RCVRY_WINDOW_CUTOFF_TM_ID,
                (t.TM_LVL_END_DT + INTERVAL 24 MONTH)::DATE AS RCVRY_WINDOW_CUTOFF_DT,
                CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
                CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP,
                mth_tm_id.val AS PROCESS_MTH_TM_ID
            FROM dataprep a
            CROSS JOIN mth_tm_id
            INNER JOIN pdead b
                ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            INNER JOIN ingestion.TM_DIM t
                ON b.LAST_NEW_DEFAULT_DATE = t.TM_ID
               AND TRIM(t.TM_LVL) = 'Month'
            WHERE a.PIT_STATUS_CD = 'CUR'
              AND a.SML_BUS_F = 'N'
              AND a.CONSM_PRD_TREATMNT_CD = 'A'
              AND a.MTH_TM_ID = mth_tm_id.val - 12 * 40
        ),
        lgd_rows AS (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
                a.MTH_TM_ID AS OBSVTN_MTH_TM_ID,
                b.BASEL_ACCT_ID,
                b.LAST_NEW_DEFAULT_OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                b.LAST_NEW_DEFAULT_DATE AS LAST_NEW_DFT_TM_ID,
                t.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
                CASE WHEN b.MODEL_DFT_F = 1 THEN 'Y' ELSE 'N' END AS MODEL_DFT_F,
                b.LAST_NEW_DEFAULT_DATE + 24 * 40 AS RCVRY_WINDOW_CUTOFF_TM_ID,
                (t.TM_LVL_END_DT + INTERVAL 24 MONTH)::DATE AS RCVRY_WINDOW_CUTOFF_DT,
                CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
                CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP,
                mth_tm_id.val AS PROCESS_MTH_TM_ID
            FROM dataprep a
            CROSS JOIN mth_tm_id
            INNER JOIN lgd b
                ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            INNER JOIN ingestion.TM_DIM t
                ON b.LAST_NEW_DEFAULT_DATE = t.TM_ID
               AND TRIM(t.TM_LVL) = 'Month'
            WHERE a.PIT_STATUS_CD <> 'CUR'
              AND a.SML_BUS_F = 'N'
              AND a.CONSM_PRD_TREATMNT_CD = 'A'
              AND a.MTH_TM_ID = mth_tm_id.val - 24 * 40
        )
    SELECT * FROM pdead_rows
    UNION ALL
    SELECT * FROM lgd_rows
    """,
):
    pass
