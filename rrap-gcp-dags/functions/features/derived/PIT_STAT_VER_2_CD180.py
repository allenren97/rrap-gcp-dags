from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.TM_DIM",
                  "reference.BLOCK_RECL_LKP",
                  "reference.CHRG_OFF_LKP",
                  "reference.TRNST_EXCLSN_LKP",
                  ]

DOWNSTREAM_ASSET = "features.PIT_STAT_VER_2_CD180"
DEPENDENCIES = {
    "export_snap": ["duckdb_delete_pit_stat_ver_2_cd180"],
    "export_prev_snap": ["duckdb_delete_pit_stat_ver_2_cd180"],
    "duckdb_delete_pit_stat_ver_2_cd180": ["duckdb_load_pit_stat_ver_2_cd180"],
}
def export_snap(
    duckdb_conn_id = 'duckdb-conn',
    sql = f"""
        WITH ymt AS (
        SELECT STRFTIME(TM_LVL_END_DT, '%Y%m') AS yrmth
        FROM {UPSTREAM_ASSET[1]}
        WHERE TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        a.MTH_TM_ID,
        a.BASEL_ACCT_ID,
        a.PRIM_BASEL_CUST_ID,
        a.STEP_PLN_SNAPSHOT_ID,
        a.ACCT_NUM,
        a.PRD_CD,
        a.SUB_PRD_CD,
        a.BLOCK_RECL_CD,
        a.TOT_NEW_BAL_AMT,
        a.CR_LMT_AMT,
        a.ACCT_CLS_RSN_CD,
        a.CHRG_OFF_CD,
        a.BNS_DLQNT_DAY,
        a.TOT_UNPAID_FNCL_CHRG_AMT,
        a.CRNT_BILL_CD,
        a.SCRD_TP_CD,
        a.SWITCH_XREF,
        a.SCRTY_TP_CD,
        a.TRNST_NUM,
        c.EXCLUDED_TRNST_NUM,
        a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,

        CASE
            WHEN a.SUB_PRD_CD = 'RS'
            OR a.STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)
            THEN 'Y'
            ELSE 'N'
        END AS HELOC_F,

        CASE
            WHEN lk_recl.BLOCK_RECL_CD IS NULL THEN '1'
            ELSE '0'
        END AS v_PT_STAT_BLCK_RECL_CD_LKP_CUR,

        CASE
            WHEN a.CR_LMT_AMT > a.TOT_NEW_BAL_AMT THEN a.CR_LMT_AMT
            ELSE a.TOT_NEW_BAL_AMT
        END AS REVISED_EXPSR_AMT

    FROM {UPSTREAM_ASSET[0]} a

    CROSS JOIN ymt

    LEFT JOIN {UPSTREAM_ASSET[4]} c
        ON a.TRNST_NUM = c.EXCLUDED_TRNST_NUM

    LEFT JOIN {UPSTREAM_ASSET[2]} lk_recl
        ON a.BLOCK_RECL_CD = lk_recl.BLOCK_RECL_CD
    AND lk_recl.BNKRPY_F = 'Y'
    AND ymt.yrmth BETWEEN lk_recl.EFF_FROM_YR_MTH
                        AND lk_recl.EFF_TO_YR_MTH

    WHERE a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
"""
) -> None:
    pass


def export_prev_snap(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH ymt AS (
        SELECT STRFTIME(TM_LVL_END_DT, '%Y%m') AS yrmth
        FROM {UPSTREAM_ASSET[1]}
        WHERE TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        a.BASEL_ACCT_ID,
        a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,

        a.TOT_NEW_BAL_AMT AS PREV_TOT_NEW_BAL_AMT,
        a.CHRG_OFF_CD AS PREV_CHRG_OFF_CD,
        a.BLOCK_RECL_CD AS PREV_BLOCK_RECL_CD,

        CASE
            WHEN a.TOT_NEW_BAL_AMT > 0
             AND lk_recl.BLOCK_RECL_CD IS NULL
            THEN '1'
            ELSE '0'
        END AS v_PT_STAT_BLCK_RECL_CD_LKP_PRV,

        CASE
            WHEN lk_chrg.CHRG_OFF_CD IS NULL
            THEN '1'
            ELSE '0'
        END AS v_PT_STAT_CHRG_OFF_LKP_PREV2,

        a.TOT_UNPAID_FNCL_CHRG_AMT AS PREV_TOT_UNPAID_FNCL_CHRG_AMT

    FROM {UPSTREAM_ASSET[0]} a

    CROSS JOIN ymt

    LEFT JOIN {UPSTREAM_ASSET[2]} lk_recl
        ON a.BLOCK_RECL_CD = lk_recl.BLOCK_RECL_CD
       AND lk_recl.BNKRPY_F = 'Y'
       AND ymt.yrmth BETWEEN lk_recl.EFF_FROM_YR_MTH
                         AND lk_recl.EFF_TO_YR_MTH

    LEFT JOIN {UPSTREAM_ASSET[3]} lk_chrg 
        ON a.CHRG_OFF_CD = lk_chrg.CHRG_OFF_CD
       AND lk_chrg.CHRG_OFF_STAT_F = 'Y'
       AND ymt.yrmth BETWEEN lk_chrg.EFF_FROM_YR_MTH
                         AND lk_chrg.EFF_TO_YR_MTH

    WHERE a.MTH_TM_ID =
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40
    """
) -> None:
    pass


def duckdb_delete_pit_stat_ver_2_cd180(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_pit_stat_ver_2_cd180(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        SELECT
            s.OBSN_DT,
            s.BASEL_ACCT_ID,
            s.BASEL_CUST_ID,

            CASE
                WHEN s.HELOC_F = 'N' THEN
                    CASE
                        WHEN s.CHRG_OFF_CD = '1'
                            THEN 'CHG'

                        WHEN (
                            s.BNS_DLQNT_DAY < 210
                            AND NOT (
                                s.TOT_NEW_BAL_AMT > 0
                                AND s.CHRG_OFF_CD IN ('N', 'Q')
                            )
                            AND s.v_PT_STAT_BLCK_RECL_CD_LKP_CUR = '1'
                        )
                            THEN 'CUR'

                        WHEN (
                            s.TOT_NEW_BAL_AMT > 0
                            AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                            AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                            AND NOT (
                                ps.PREV_TOT_NEW_BAL_AMT > 0
                                AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q')
                            )
                            AND (
                                ps.PREV_TOT_NEW_BAL_AMT > 0
                                AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1'
                            )
                        )
                            THEN 'CUR'

                        WHEN (
                            s.TOT_NEW_BAL_AMT = 0
                            AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                            AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                            AND NOT (
                                ps.PREV_TOT_NEW_BAL_AMT > 0
                                AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q')
                            )
                            AND (
                                ps.PREV_TOT_NEW_BAL_AMT > 0
                                AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1'
                            )
                        )
                            THEN 'CUR'

                        ELSE 'DEF'
                    END

                ELSE
                    CASE
                        WHEN s.CHRG_OFF_CD = '1'
                            THEN 'CHG'

                        WHEN (
                            s.BNS_DLQNT_DAY < 210
                            AND NOT (
                                s.TOT_NEW_BAL_AMT > 0
                                AND s.CHRG_OFF_CD IN ('N', 'Q')
                            )
                        )
                            THEN 'CUR'

                        WHEN (
                            s.TOT_NEW_BAL_AMT > 0
                            AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                            AND s.CHRG_OFF_CD <> '1'
                            AND NOT (
                                ps.PREV_TOT_NEW_BAL_AMT > 0
                                AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q')
                            )
                            AND ps.PREV_TOT_NEW_BAL_AMT > 0
                        )
                            THEN 'CUR'

                        WHEN (
                            s.TOT_NEW_BAL_AMT = 0
                            AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                            AND ps.PREV_CHRG_OFF_CD <> '1'
                            AND NOT (
                                ps.PREV_TOT_NEW_BAL_AMT > 0
                                AND ps.PREV_CHRG_OFF_CD IN ('N', 'Q')
                            )
                            AND ps.PREV_TOT_NEW_BAL_AMT > 0
                        )
                            THEN 'CUR'

                        ELSE 'DEF'
                    END
            END AS PIT_STAT_VER_2_CD180

        FROM read_parquet(
            '{{{{ task_instance.xcom_pull(task_ids="derived__PIT_STAT_VER_2_CD180.export_snap", key="parquet") }}}}'
        ) s

        LEFT JOIN read_parquet(
            '{{{{ task_instance.xcom_pull(task_ids="derived__PIT_STAT_VER_2_CD180.export_prev_snap", key="parquet") }}}}'
        ) ps
            ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID
           AND s.BASEL_CUST_ID = ps.BASEL_CUST_ID
    """
) -> None:
    pass 