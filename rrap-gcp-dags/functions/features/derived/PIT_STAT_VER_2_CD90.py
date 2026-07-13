
UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "reference.BLOCK_RECL_LKP",
    "reference.CHRG_OFF_LKP",
]

DOWNSTREAM_ASSET = "features.PIT_STAT_VER_2_CD90"

DEPENDENCIES = {
    "export_snap": ["duckdb_delete_pit_stat_ver_2_cd90"],
    "export_prev_snap": ["duckdb_delete_pit_stat_ver_2_cd90"],
    "duckdb_delete_pit_stat_ver_2_cd90": ["duckdb_load_pit_stat_ver_2_cd90"],
}

_MTH_TM_ID = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'
_RUNDATE = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'

_PIT_CASE_90 = """
    CASE
        WHEN s.HELOC_F = 'N' THEN
            CASE
                WHEN s.CHRG_OFF_CD = '1' THEN 'CHG'
                WHEN s.BNS_DLQNT_DAY < 120
                     AND NOT (s.TOT_NEW_BAL_AMT > 0 AND s.CHRG_OFF_CD IN ('N', 'Q'))
                     AND s.v_PT_STAT_BLCK_RECL_CD_LKP_CUR = '1'
                THEN 'CUR'
                WHEN s.TOT_NEW_BAL_AMT > 0
                     AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                     AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                     AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q'))
                     AND (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1')
                THEN 'CUR'
                WHEN s.TOT_NEW_BAL_AMT = 0
                     AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                     AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                     AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q'))
                     AND (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1')
                THEN 'CUR'
                ELSE 'DEF'
            END
        ELSE
            CASE
                WHEN s.CHRG_OFF_CD = '1' THEN 'CHG'
                WHEN s.BNS_DLQNT_DAY < 120
                     AND NOT (s.TOT_NEW_BAL_AMT > 0 AND s.CHRG_OFF_CD IN ('N', 'Q'))
                THEN 'CUR'
                WHEN s.TOT_NEW_BAL_AMT > 0
                     AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                     AND s.CHRG_OFF_CD <> '1'
                     AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q'))
                     AND ps.PREV_TOT_NEW_BAL_AMT > 0
                THEN 'CUR'
                WHEN s.TOT_NEW_BAL_AMT = 0
                     AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                     AND ps.PREV_CHRG_OFF_CD <> '1'
                     AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q'))
                     AND ps.PREV_TOT_NEW_BAL_AMT > 0
                THEN 'CUR'
                ELSE 'DEF'
            END
    END
"""


def export_snap(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH ymt AS (
        SELECT STRFTIME(TM_LVL_END_DT, '%Y%m') AS yrmth
        FROM {UPSTREAM_ASSET[1]}
        WHERE TM_ID = {_MTH_TM_ID}
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        a.BASEL_ACCT_ID,
        a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        CASE
            WHEN TRIM(a.SUB_PRD_CD) = 'RS'
              OR a.STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)
            THEN 'Y'
            ELSE 'N'
        END AS HELOC_F,
        TRIM(a.CHRG_OFF_CD) AS CHRG_OFF_CD,
        a.BNS_DLQNT_DAY,
        a.TOT_NEW_BAL_AMT,
        a.TOT_UNPAID_FNCL_CHRG_AMT,
        CASE
            WHEN lk_recl.BLOCK_RECL_CD IS NULL THEN '1'
            ELSE '0'
        END AS v_PT_STAT_BLCK_RECL_CD_LKP_CUR
    FROM {UPSTREAM_ASSET[0]} a
    CROSS JOIN ymt
    LEFT JOIN (
        SELECT TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
        FROM {UPSTREAM_ASSET[2]}, ymt
        WHERE TRIM(BNKRPY_F) = 'Y'
          AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
    ) lk_recl
        ON TRIM(a.BLOCK_RECL_CD) = lk_recl.BLOCK_RECL_CD
    WHERE a.MTH_TM_ID = {_MTH_TM_ID}
    """,
) -> None:
    pass


def export_prev_snap(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH ymt AS (
        SELECT STRFTIME(TM_LVL_END_DT, '%Y%m') AS yrmth
        FROM {UPSTREAM_ASSET[1]}
        WHERE TM_ID = {_MTH_TM_ID}
    )
    SELECT
        a.BASEL_ACCT_ID,
        a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        a.TOT_NEW_BAL_AMT AS PREV_TOT_NEW_BAL_AMT,
        TRIM(a.CHRG_OFF_CD) AS PREV_CHRG_OFF_CD,
        CASE
            WHEN a.TOT_NEW_BAL_AMT > 0 AND lk_recl.BLOCK_RECL_CD IS NULL THEN '1'
            ELSE '0'
        END AS v_PT_STAT_BLCK_RECL_CD_LKP_PRV,
        CASE
            WHEN lk_chrg.CHRG_OFF_CD IS NULL THEN '1'
            ELSE '0'
        END AS v_PT_STAT_CHRG_OFF_LKP_PREV2,
        a.TOT_UNPAID_FNCL_CHRG_AMT AS PREV_TOT_UNPAID_FNCL_CHRG_AMT
    FROM {UPSTREAM_ASSET[0]} a
    CROSS JOIN ymt
    LEFT JOIN (
        SELECT TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
        FROM {UPSTREAM_ASSET[2]}, ymt
        WHERE TRIM(BNKRPY_F) = 'Y'
          AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
    ) lk_recl
        ON TRIM(a.BLOCK_RECL_CD) = lk_recl.BLOCK_RECL_CD
    LEFT JOIN (
        SELECT TRIM(CHRG_OFF_CD) AS CHRG_OFF_CD
        FROM {UPSTREAM_ASSET[3]}, ymt
        WHERE TRIM(CHRG_OFF_STAT_F) = 'Y'
          AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
    ) lk_chrg
        ON TRIM(a.CHRG_OFF_CD) = lk_chrg.CHRG_OFF_CD
    WHERE a.MTH_TM_ID = {_MTH_TM_ID} - 40
    """,
) -> None:
    pass


def duckdb_delete_pit_stat_ver_2_cd90(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{_RUNDATE}'
    """,
):
    pass


def duckdb_load_pit_stat_ver_2_cd90(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT
        s.OBSN_DT,
        s.BASEL_ACCT_ID,
        s.BASEL_CUST_ID,
        {_PIT_CASE_90} AS PIT_STAT_VER_2_CD90
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="derived__PIT_STAT_VER_2_CD90.export_snap", key="parquet") }}}}'
    ) s
    LEFT JOIN read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="derived__PIT_STAT_VER_2_CD90.export_prev_snap", key="parquet") }}}}'
    ) ps
        ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID
       AND s.BASEL_CUST_ID = ps.BASEL_CUST_ID
    """,
) -> None:
    pass