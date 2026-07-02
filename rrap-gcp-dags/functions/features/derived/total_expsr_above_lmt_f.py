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
    "features.BASEL_PRD_CD",
    "features.SML_BUS_F",
    "features.HELOC_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.TRNST_EXCLSN_F",
    "features.REVISED_EXPSR_AMT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "reference.RPTG_PRD_LKP_THRSHLD",
]
DOWNSTREAM_ASSET = "features.TOTAL_EXPSR_ABOVE_LMT_F"
DEPENDENCIES = {
    "export": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH
            inputs AS (
                WITH
                    PRD AS (
                        SELECT
                            BASEL_ACCT_ID,
                            BASEL_PRD_CD
                        FROM
                            features.BASEL_PRD_CD
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    SML AS (
                        SELECT
                            BASEL_ACCT_ID,
                            SML_BUS_F
                        FROM
                            features.SML_BUS_F
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    HELOC AS (
                        SELECT
                            BASEL_ACCT_ID,
                            HELOC_F
                        FROM
                            features.HELOC_F
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    TRT AS (
                        SELECT
                            BASEL_ACCT_ID,
                            CONSM_PRD_TREATMNT_CD
                        FROM
                            features.CONSM_PRD_TREATMNT_CD
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    TRNST AS (
                        SELECT
                            BASEL_ACCT_ID,
                            TRNST_EXCLSN_F
                        FROM
                            features.TRNST_EXCLSN_F
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    EXPSR AS (
                        SELECT
                            BASEL_ACCT_ID,
                            REVISED_EXPSR_AMT
                        FROM
                            features.REVISED_EXPSR_AMT
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    THRSHLD AS (
                        SELECT
                            THRSHLD
                        FROM
                            reference.RPTG_PRD_LKP_THRSHLD
                        WHERE
                            TRIM(CRNT_F) = 'Y'
                            AND '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                    ),
                    PIT AS (
                        SELECT
                            BASEL_ACCT_ID,
                            TRIM(PIT_STATUS_CROSS_DEFAULT_ORIG) AS PIT_STAT_CD
                        FROM
                            features.PIT_STATUS_CROSS_DEFAULT_ORIG
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    ),
                    TOT_EXPSR_prep AS (
                        SELECT
                            CRM2.BASEL_ACCT_ID AS BASEL_ACCT_ID,
                            CRM2.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                            CRM2.CR_LMT_AMT AS CR_LMT_AMT,
                            CRM2.TOT_NEW_BAL_AMT AS TOT_NEW_BAL_AMT,
                            REVISED_EXPSR_AMT,
                            BASEL_PRD_CD,
                            CONSM_PRD_TREATMNT_CD,
                            HELOC_F,
                            PIT_STAT_CD,
                            SML_BUS_F,
                            TRNST_EXCLSN_F
                        FROM
                            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT CRM2
                            LEFT JOIN EXPSR ON CRM2.BASEL_ACCT_ID = EXPSR.BASEL_ACCT_ID
                            LEFT JOIN TRNST ON CRM2.BASEL_ACCT_ID = TRNST.BASEL_ACCT_ID
                            LEFT JOIN PRD ON CRM2.BASEL_ACCT_ID = PRD.BASEL_ACCT_ID
                            LEFT JOIN SML ON CRM2.BASEL_ACCT_ID = SML.BASEL_ACCT_ID
                            LEFT JOIN HELOC ON CRM2.BASEL_ACCT_ID = HELOC.BASEL_ACCT_ID
                            LEFT JOIN TRT ON CRM2.BASEL_ACCT_ID = TRT.BASEL_ACCT_ID
                            LEFT JOIN PIT ON CRM2.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
                        WHERE
                            CRM2.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                    ),
                    TOT_EXPSR AS (
                        SELECT
                            BASEL_CUST_ID,
                            BASEL_PRD_CD,
                            SUM(REVISED_EXPSR_AMT) AS TOT_EXPSR
                        FROM
                            TOT_EXPSR_prep
                        WHERE
                            BASEL_CUST_ID > 0
                            AND TRIM(CONSM_PRD_TREATMNT_CD) = 'A'
                            AND TRIM(HELOC_F) = 'N'
                            AND TRIM(PIT_STAT_CD) IN ('CUR', 'DEF')
                            AND TRIM(SML_BUS_F) = 'N'
                            AND TRIM(TRNST_EXCLSN_F) = 'N'
                            AND TRIM(BASEL_PRD_CD) IN ('CC', 'LOC', 'SL A')
                        GROUP BY
                            BASEL_CUST_ID,
                            BASEL_PRD_CD
                    ),
                    CRM AS (
                        SELECT
                            CRM.*,
                            PRD.BASEL_PRD_CD
                        FROM
                            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT CRM
                            LEFT JOIN PRD ON CRM.BASEL_ACCT_ID = PRD.BASEL_ACCT_ID
                        WHERE
                            CRM.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                    )
                SELECT
                    CRM.MTH_TM_ID,
                    CRM.BASEL_ACCT_ID,
                    CRM.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                    CRM.CR_LMT_AMT,
                    CRM.TOT_NEW_BAL_AMT,
                    EXPSR.REVISED_EXPSR_AMT,
                    CRM.BASEL_PRD_CD,
                    TRT.CONSM_PRD_TREATMNT_CD,
                    HELOC_F,
                    PIT_STAT_CD,
                    SML_BUS_F,
                    TRNST_EXCLSN_F,
                    CASE
                        WHEN CONSM_PRD_TREATMNT_CD != 'A'
                        OR HELOC_F != 'N'
                        OR PIT_STAT_CD NOT IN ('CUR', 'DEF')
                        OR SML_BUS_F != 'N'
                        OR TRNST_EXCLSN_F != 'N' THEN NULL
                        ELSE TOT_EXPSR
                    END AS TOT_EXPSR,
                    (
                        SELECT
                            THRSHLD
                        FROM
                            THRSHLD
                    ) AS THRSHLD,
                    CASE
                        WHEN CONSM_PRD_TREATMNT_CD != 'A'
                        OR HELOC_F != 'N'
                        OR PIT_STAT_CD NOT IN ('CUR', 'DEF')
                        OR SML_BUS_F != 'N'
                        OR TRNST_EXCLSN_F != 'N' THEN NULL
                        WHEN TOT_EXPSR > (
                            SELECT
                                *
                            FROM
                                THRSHLD
                        ) THEN 'Y'
                        ELSE 'N'
                    END AS TOTAL_EXPSR_ABOVE_LMT_F
                FROM
                    CRM
                    LEFT JOIN EXPSR ON CRM.BASEL_ACCT_ID = EXPSR.BASEL_ACCT_ID
                    LEFT JOIN TRNST ON CRM.BASEL_ACCT_ID = TRNST.BASEL_ACCT_ID
                    LEFT JOIN SML ON CRM.BASEL_ACCT_ID = SML.BASEL_ACCT_ID
                    LEFT JOIN HELOC ON CRM.BASEL_ACCT_ID = HELOC.BASEL_ACCT_ID
                    LEFT JOIN TRT ON CRM.BASEL_ACCT_ID = TRT.BASEL_ACCT_ID
                    LEFT JOIN PIT ON CRM.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID
                    LEFT JOIN TOT_EXPSR ON CRM.PRIM_BASEL_CUST_ID = TOT_EXPSR.BASEL_CUST_ID
                    AND CRM.BASEL_PRD_CD = TOT_EXPSR.BASEL_PRD_CD
                WHERE
                    CRM.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            )
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            BASEL_ACCT_ID,
            COALESCE(
                UPDATE_TOTAL_EXPSR_ABOVE_LMT_F,
                TOTAL_EXPSR_ABOVE_LMT_F
            ) AS TOTAL_EXPSR_ABOVE_LMT_F
        FROM
            (
                SELECT
                    inputs.*,
                    CASE
                        WHEN REVISED_EXPSR_AMT > THRSHLD
                        AND BASEL_CUST_ID = -1
                        AND TRIM(CONSM_PRD_TREATMNT_CD) = 'A'
                        AND TRIM(HELOC_F) = 'N'
                        AND TRIM(PIT_STAT_CD) IN ('CUR', 'DEF')
                        AND TRIM(SML_BUS_F) = 'N'
                        AND TRIM(TRNST_EXCLSN_F) = 'N'
                        AND TRIM(BASEL_PRD_CD) IN ('CC', 'LOC', 'SL A') THEN 'Y'
                        WHEN REVISED_EXPSR_AMT <= THRSHLD
                        AND BASEL_CUST_ID = -1
                        AND TRIM(CONSM_PRD_TREATMNT_CD) = 'A'
                        AND TRIM(HELOC_F) = 'N'
                        AND TRIM(PIT_STAT_CD) IN ('CUR', 'DEF')
                        AND TRIM(SML_BUS_F) = 'N'
                        AND TRIM(TRNST_EXCLSN_F) = 'N'
                        AND TRIM(BASEL_PRD_CD) IN ('CC', 'LOC', 'SL A') THEN 'N'
                        WHEN TRIM(CONSM_PRD_TREATMNT_CD) = 'A'
                        AND (
                            TRIM(HELOC_F) = 'Y'
                            OR TRIM(BASEL_PRD_CD) IN ('SL', 'SL B')
                        )
                        AND TRIM(PIT_STAT_CD) IN ('CUR', 'DEF')
                        AND TRIM(SML_BUS_F) = 'N'
                        AND TRIM(TRNST_EXCLSN_F) = 'N' THEN 'N'
                    END AS UPDATE_TOTAL_EXPSR_ABOVE_LMT_F
                FROM
                    inputs
            ) AS tmp
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__total_expsr_above_lmt_f.export", key="parquet") }}}}'])
    )
    """,
):
    pass
