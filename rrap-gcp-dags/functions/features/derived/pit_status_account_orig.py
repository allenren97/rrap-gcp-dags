from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "reference.CHRG_OFF_LKP",
    "reference.BLOCK_RECL_LKP",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.PIT_STATUS_ACCOUNT_ORIG"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}
SRC_PRD_LKP = "reference.SRC_PRD_LKP"
BLOCK_RECL_LKP = "reference.BLOCK_RECL_LKP"
CHRG_OFF_LKP = "reference.CHRG_OFF_LKP"


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        WITH
            SNAP AS (
                SELECT
                    MTH_TM_ID,
                    BASEL_ACCT_ID,
                    STEP_PLN_AGRMNT_NUM,
                    PRD_CD,
                    SUB_PRD_CD,
                    rvl.BLOCK_RECL_CD,
                    TOT_NEW_BAL_AMT,
                    CHRG_OFF_CD,
                    BNS_DLQNT_DAY,
                    TOT_UNPAID_FNCL_CHRG_AMT,
                    PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                    (
                        CASE
                            WHEN TRIM(SUB_PRD_CD) = 'RS'
                            OR coalesce(trim(STEP_PLN_AGRMNT_NUM),'') != '' THEN 'Y'
                            ELSE 'N'
                        END
                    ) AS HELOC_F,
                    (
                        CASE
                            WHEN LK_RECL.BLOCK_RECL_CD IS NULL THEN '1'
                            ELSE '0'
                        END
                    ) AS v_PT_STAT_BLCK_RECL_CD_LKP_CUR
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS rvl
                    LEFT JOIN (
                        SELECT
                            BLOCK_RECL_CD AS BLOCK_RECL_CD
                        FROM
                            {BLOCK_RECL_LKP}
                        WHERE
                            TRIM(BNKRPY_F) = 'Y'
                            AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                    ) AS LK_RECL ON TRIM(rvl.BLOCK_RECL_CD) = TRIM(LK_RECL.BLOCK_RECL_CD)
                WHERE
                    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ),
            PREV_SNAP AS (
                SELECT
                    BASEL_ACCT_ID,
                    PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                    TOT_NEW_BAL_AMT AS PREV_TOT_NEW_BAL_AMT,
                    rvl.CHRG_OFF_CD AS PREV_CHRG_OFF_CD,
                    (
                        CASE
                            WHEN TOT_NEW_BAL_AMT > 0
                            AND LK_RECL.BLOCK_RECL_CD IS NULL THEN '1'
                            ELSE '0'
                        END
                    ) AS v_PT_STAT_BLCK_RECL_CD_LKP_PRV,
                    (
                        CASE
                            WHEN LK_CHRG.CHRG_OFF_CD IS NULL THEN '1'
                            ELSE '0'
                        END
                    ) AS v_PT_STAT_CHRG_OFF_LKP_PREV2,
                    TOT_UNPAID_FNCL_CHRG_AMT AS PREV_TOT_UNPAID_FNCL_CHRG_AMT
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS rvl
                    LEFT JOIN (
                        SELECT
                            BLOCK_RECL_CD AS BLOCK_RECL_CD
                        FROM
                            {BLOCK_RECL_LKP}
                        WHERE
                            TRIM(BNKRPY_F) = 'Y'
                            AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                    ) AS LK_RECL ON TRIM(rvl.BLOCK_RECL_CD) = TRIM(LK_RECL.BLOCK_RECL_CD)
                    LEFT JOIN (
                        SELECT
                            CHRG_OFF_CD AS CHRG_OFF_CD
                        FROM
                            {CHRG_OFF_LKP}
                        WHERE
                            TRIM(CHRG_OFF_STAT_F) = 'Y'
                            AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                        ORDER BY
                            CHRG_OFF_CD
                    ) LK_CHRG ON TRIM(rvl.CHRG_OFF_CD) = TRIM(LK_CHRG.CHRG_OFF_CD)
                WHERE
                    MTH_TM_ID = ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40)
            )
        SELECT
            'KS' AS SRC_SYS_CD,
            s.BASEL_ACCT_ID as BASEL_ACCT_ID,
            case when TRIM(s.STEP_PLN_AGRMNT_NUM) = '' then null else TRIM(s.STEP_PLN_AGRMNT_NUM) end as STEP_PLN_AGRMNT_NUM,
            /* this mimics logic in J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS for Pit status derivation */
            (
                CASE
                    WHEN TRIM(SP.BASEL_PRD_CD) = 'CC'
                    AND TRIM(s.HELOC_F) = 'N' THEN (
                        CASE
                            WHEN TRIM(CHRG_OFF_CD) = '1' THEN 'CHG'
                            WHEN (
                                BNS_DLQNT_DAY < 210
                                AND NOT (
                                    TOT_NEW_BAL_AMT > 0
                                    AND TRIM(CHRG_OFF_CD) IN ('N', 'Q')
                                )
                                AND TRIM(v_PT_STAT_BLCK_RECL_CD_LKP_CUR) = '1'
                            ) THEN 'CUR'
                            WHEN (
                                TOT_NEW_BAL_AMT > 0
                                AND TOT_NEW_BAL_AMT = TOT_UNPAID_FNCL_CHRG_AMT
                                AND TRIM(v_PT_STAT_CHRG_OFF_LKP_PREV2) = '1'
                                AND NOT (
                                    PREV_TOT_NEW_BAL_AMT > 0
                                    AND TRIM(PREV_CHRG_OFF_CD) IN ('N', 'Q')
                                )
                                AND (
                                    PREV_TOT_NEW_BAL_AMT > 0
                                    AND TRIM(v_PT_STAT_BLCK_RECL_CD_LKP_PRV) = '1'
                                )
                            ) THEN 'CUR'
                            WHEN (
                                TOT_NEW_BAL_AMT = 0
                                AND PREV_TOT_NEW_BAL_AMT = PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                AND TRIM(v_PT_STAT_CHRG_OFF_LKP_PREV2) = '1'
                                AND NOT (
                                    PREV_TOT_NEW_BAL_AMT > 0
                                    AND TRIM(PREV_CHRG_OFF_CD) IN ('N', 'Q')
                                )
                                AND (
                                    PREV_TOT_NEW_BAL_AMT > 0
                                    AND TRIM(v_PT_STAT_BLCK_RECL_CD_LKP_PRV) = '1'
                                )
                            ) THEN 'CUR'
                            ELSE 'DEF'
                        END
                    )
                    ELSE (
                        CASE
                            WHEN TRIM(s.HELOC_F) = 'N' THEN CASE
                                WHEN TRIM(CHRG_OFF_CD) = '1' THEN 'CHG'
                                WHEN (
                                    BNS_DLQNT_DAY < 120
                                    AND NOT (
                                        TOT_NEW_BAL_AMT > 0
                                        AND TRIM(CHRG_OFF_CD) IN ('N', 'Q')
                                    )
                                    AND (v_PT_STAT_BLCK_RECL_CD_LKP_CUR) = '1'
                                ) THEN 'CUR'
                                WHEN (
                                    TOT_NEW_BAL_AMT > 0
                                    AND TOT_NEW_BAL_AMT = TOT_UNPAID_FNCL_CHRG_AMT
                                    AND (v_PT_STAT_CHRG_OFF_LKP_PREV2) = '1'
                                    AND NOT (
                                        PREV_TOT_NEW_BAL_AMT > 0
                                        AND (PREV_CHRG_OFF_CD) IN ('N', 'Q')
                                    )
                                    AND (
                                        PREV_TOT_NEW_BAL_AMT > 0
                                        AND TRIM(v_PT_STAT_BLCK_RECL_CD_LKP_PRV) = '1'
                                    )
                                ) THEN 'CUR'
                                WHEN (
                                    TOT_NEW_BAL_AMT = 0
                                    AND PREV_TOT_NEW_BAL_AMT = PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                    AND TRIM(v_PT_STAT_CHRG_OFF_LKP_PREV2) = '1'
                                    AND NOT (
                                        PREV_TOT_NEW_BAL_AMT > 0
                                        AND TRIM(PREV_CHRG_OFF_CD) IN ('N', 'Q')
                                    )
                                    AND (
                                        PREV_TOT_NEW_BAL_AMT > 0
                                        AND TRIM(v_PT_STAT_BLCK_RECL_CD_LKP_PRV) = '1'
                                    )
                                ) THEN 'CUR'
                                ELSE 'DEF'
                            END
                            ELSE CASE
                                WHEN TRIM(CHRG_OFF_CD) = '1' THEN 'CHG'
                                WHEN (
                                    BNS_DLQNT_DAY < 120
                                    AND NOT (
                                        TOT_NEW_BAL_AMT > 0
                                        AND TRIM(CHRG_OFF_CD) IN ('N', 'Q')
                                    )
                                ) THEN 'CUR'
                                WHEN (
                                    TOT_NEW_BAL_AMT > 0
                                    AND TOT_NEW_BAL_AMT = TOT_UNPAID_FNCL_CHRG_AMT
                                    AND CHRG_OFF_CD <> '1'
                                    AND NOT (
                                        PREV_TOT_NEW_BAL_AMT > 0
                                        AND TRIM(PREV_CHRG_OFF_CD) IN ('N', 'Q')
                                    )
                                    AND PREV_TOT_NEW_BAL_AMT > 0
                                ) THEN 'CUR'
                                WHEN (
                                    TOT_NEW_BAL_AMT = 0
                                    AND PREV_TOT_NEW_BAL_AMT = PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                    AND PREV_CHRG_OFF_CD <> '1'
                                    AND NOT (
                                        PREV_TOT_NEW_BAL_AMT > 0
                                        AND TRIM(PREV_CHRG_OFF_CD) IN ('N', 'Q')
                                    )
                                    AND PREV_TOT_NEW_BAL_AMT > 0
                                ) THEN 'CUR'
                                ELSE 'DEF'
                            END
                        END
                    )
                END
            ) AS PIT_STATUS_ACCOUNT_ORIG,
            /* mark written-out accounts as W in STEP_DFLT_F */
            CASE
                WHEN TRIM(s.CHRG_OFF_CD) = '1'
                AND TRIM(s.BLOCK_RECL_CD) LIKE 'D%' THEN 'W'
                WHEN coalesce(trim(s.STEP_PLN_AGRMNT_NUM),'') != '' THEN 'N'
            END AS STEP_DFLT_F
        FROM
            SNAP s
            LEFT JOIN PREV_SNAP ps ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID
            AND s.BASEL_CUST_ID = ps.BASEL_CUST_ID
            /* this join is used to check for CC product in order to determine 90d or 180d Pit above */
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(BASEL_PRD_CD) AS BASEL_PRD_CD,
                    TRIM(SRC_PRD_CD) AS SRC_PRD_CD,
                    TRIM(SRC_SUB_PRD_CD) AS SRC_SUB_PRD_CD
                FROM
                    {SRC_PRD_LKP}
                WHERE
                    TRIM(PRD_SYS_CD) = 'KS'
                    AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) SP ON TRIM(s.PRD_CD) = TRIM(SP.SRC_PRD_CD)
            AND TRIM(s.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            'MOR' AS SRC_SYS_CD,
            BASEL_ACCT_ID,
            case when TRIM(STEP_PLN_AGRMNT_NUM) = '' then null else TRIM(STEP_PLN_AGRMNT_NUM) end as STEP_PLN_AGRMNT_NUM,
            CASE
                WHEN TRIM(UPPER(COMM_TP)) = 'RESIDENTIAL'
                AND PD_OFF_DT IS NULL
                AND (
                    (
                        DLQNT_DAY < 90
                        AND DLQNT_MTH < 4
                    )
                    AND UPPER(COALESCE(FRCLSR_F, '')) <> 'Y'
                    AND CRNT_BAL_AMT <> 0
                    AND UPPER(COALESCE(LRA_STAT, '')) <> 'Y'
                )
                OR CRNT_BAL_AMT < 0 THEN 'CUR'
                WHEN (
                    TRIM(UPPER(COMM_TP)) = 'RESIDENTIAL'
                    AND PD_OFF_DT IS NULL
                    AND (
                        (
                            DLQNT_DAY >= 90
                            OR DLQNT_MTH >= 4
                        )
                        OR TRIM(UPPER(FRCLSR_F)) = 'Y'
                        OR TRIM(UPPER(LRA_STAT)) = 'Y'
                    )
                    AND CRNT_BAL_AMT > 0
                )
                OR (
                    TRIM(UPPER(COMM_TP)) = 'RESIDENTIAL'
                    AND TRIM(UPPER(FRCLSR_F)) = 'Y'
                    AND TRIM(UPPER(PD_OFF_F)) = 'Y'
                    AND (
                        greatest (CRNT_BAL_AMT, COALESCE(- TOT_SUSP_BAL_AMT, 0)) > 0
                    )
                ) THEN 'DEF'
            END AS PIT_STATUS_ACCOUNT_ORIG,
            CASE
                WHEN coalesce(trim(STEP_PLN_AGRMNT_NUM),'') != '' THEN 'N'
            END AS STEP_DFLT_F
        FROM
            ingestion.MORT_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            'SPL' AS SRC_SYS_CD,
            BASEL_ACCT_ID,
            case when TRIM(STEP_PLN_AGRMNT_NUM) = '' then null else TRIM(STEP_PLN_AGRMNT_NUM) end as STEP_PLN_AGRMNT_NUM,
            CASE
                WHEN (RECD_STAT_CD IN (0, 9, NULL)) THEN 'CLO'
                WHEN (RECD_STAT_CD IN (6, 7, 8)) THEN 'CHG'
                WHEN CHRG_OFF_DT IS NOT NULL THEN 'CHG'
                WHEN DAY_ODUE >= 90
                OR RECD_STAT_CD = 5 THEN 'DEF'
                WHEN DAY_ODUE < 90
                AND RECD_STAT_CD = 4 THEN 'CUR'
                ELSE NULL
            END AS PIT_STATUS_ACCOUNT_ORIG,
            CASE
                WHEN RECD_STAT_CD = 8 THEN 'W'
                WHEN COALESCE(TRIM(STEP_PLN_AGRMNT_NUM), '') != '' THEN 'N'
            END AS STEP_DFLT_F
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND
            /* between &start_period_tm_key and &end_period_tm_key and */
            trim(RECD_STAT_CD) IN (4, 5, 6, 7, 8)
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
    max_active_tis_per_dag=1,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__pit_status_account_orig.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__pit_status_account_orig.export_spl", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__pit_status_account_orig.export_mor", key="parquet") }}}}'], union_by_name = true)
    )
    """,
    max_active_tis_per_dag=1,
):
    pass


