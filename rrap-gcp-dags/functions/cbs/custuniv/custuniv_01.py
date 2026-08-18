
UPSTREAM_ASSET = [
    "cbs.MDMFLAGS_OK",
    "emulated.CIS_DATA_NEW2",
    "ingestion.TM_DIM",
    "ingestion.BASEL_ACCT_DIM_MAY_2026",
    "reference.KQ_TKQ_KS_TSYS_XREF",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_MORT_MTH_SNAPSHOT",
    "emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS",
    "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2",
    "emulated.BASEL_MORT_ACCT_DRVD_VARS",
    "emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR",
    "emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR",
    "emulated.STATUS_FINAL",
    "emulated.TWELVE_MON_DEF_WINDOW",
]

DOWNSTREAM_ASSET = "cbs.CIS_DATA_POP_02"

_TASK_GROUP = "custuniv__custuniv_01"

_N_BATCHES = 8

DEPENDENCIES = {
    **{f"export_gather_{_i:02d}": ["export_result"] for _i in range(_N_BATCHES)},
    "export_result": ["duckdb_load"],
    "duckdb_delete": ["duckdb_load"],
}

_DROP_COLS = """
    PIT_STAT_REV, PIT_STAT_SPL, PIT_STAT_MOR,
    PRD_TREATMNT_CD_REV, PRD_TREATMNT_CD_SPL, PRD_TREATMNT_CD_MOR,
    SCORECRD_EXCLSN_F_REV, SCORECRD_EXCLSN_F_SPL, SCORECRD_EXCLSN_F_MOR,
    DFT_DT_REV, DFT_DT_SPL, DEFAULT_DATE_MOR,
    DFT_BAL_REV, DFT_BAL_SPL, DEFAULT_BAL_MOR,
    MODEL_DFT_F_REV, MODEL_DFT_F_SPL, DEFAULT_IND_MOR,
    BNS_DLQNT_DAY_REV, DAY_ODUE_SPL, DLQNT_DAY_CNT_MOR
"""

_REV_PRODUCTS = "('LOC','SCL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ')"


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def _gather_sql(k, n):
    return f"""
    WITH xref_eoc AS MATERIALIZED (
        SELECT DISTINCT
            LPAD(TSYS_ACCT_ID, 23, '0') AS tsys_key,
            LPAD(BCM_ACCT_NUM, 23, '0') AS bcm_key
        FROM reference.KQ_TKQ_KS_TSYS_XREF
        WHERE END_OF_CHAIN_INDICATOR = 'Y' AND TSYS_CUST_TYPE_CD = '0'
    )
    SELECT
        a.*,
        a1.TM_ID AS mth_tm_id,
        a1.TM_LVL_END_DT AS process_date,
        b.BASEL_ACCT_ID,
        c.PIT_STAT_VER_2_CD      AS PIT_STAT_REV,
        c.CONSM_PRD_TREATMNT_CD  AS PRD_TREATMNT_CD_REV,
        c.CONSM_SCORECRD_EXCLSN_F AS SCORECRD_EXCLSN_F_REV,
        d.PIT_STATUS_V2          AS PIT_STAT_SPL,
        d.PRD_ID                 AS PRD_ID_SPL,
        d.MODEL_EXCL_F           AS SCORECRD_EXCLSN_F_SPL,
        d.COMM_F_V2              AS COMM_FLG_SPL,
        TRY_CAST(d.OS_BAL_AMT_V2 AS DECIMAL(17, 3)) AS OS_BAL_AMT_SPL,
        d.TREATMNT_F             AS PRD_TREATMNT_CD_SPL,
        g.RECD_STAT_CD           AS RECD_STAT_CD_SPL,
        TRY_CAST(f.BNS_DLQNT_DAY AS INTEGER)        AS BNS_DLQNT_DAY_REV,
        TRY_CAST(g.DAY_ODUE AS INTEGER)             AS DAY_ODUE_SPL,
        TRY_CAST(e.DLQNT_DAY_CNT AS INTEGER)        AS DLQNT_DAY_CNT_MOR,
        k.STATUS                 AS PIT_STAT_MOR,
        k.MODEL_EXCL             AS SCORECRD_EXCLSN_F_MOR,
        k.LRA_STATUS             AS LRA_STATUS_MOR,
        k.PAID_OFF_DATE          AS PAID_OFF_DATE_MOR,
        TRY_CAST(k.CURRENT_BAL AS DECIMAL(17, 3))    AS CURRENT_BAL_MOR,
        TRY_CAST(k.TOTAL_SUSPENSE AS DECIMAL(17, 3)) AS TOTAL_SUSPENSE_MOR,
        e.CONSM_PRD_TREATMNT_CD  AS PRD_TREATMNT_CD_MOR,
        e.COMM_TP_CD             AS COMM_TP_CD_MOR,
        TRY_CAST(e.OS_BAL_AMT AS DECIMAL(17, 3))    AS OS_BAL_AMT_MOR,
        h.FRCLSR_F               AS FRCLSR_F_MOR,
        h.PD_OFF_F               AS PD_OFF_F_MOR,
        h.FUND_CD                AS FUND_CD_MOR,
        TRY_CAST(h.MTH_IN_ARRS_CNT AS INTEGER)      AS MTH_IN_ARRS_CNT_MOR,
        h.LIFE_INSUR_CD          AS LIFE_INSUR_CD_MOR,
        f.TRNST_NUM              AS TRNST_NUM_REV,
        f.SRC_CD                 AS SOURCE_CD,
        f.BLOCK_RECL_CD,
        f.ACCT_STAT_CD,
        TRY_CAST(f.CR_LMT_AMT AS DECIMAL(17, 3))     AS CR_LMT_AMT,
        TRY_CAST(f.TOT_NEW_BAL_AMT AS DECIMAL(17, 3)) AS TOT_NEW_BAL_AMT,
        f.NON_ACCRL_DT,
        f.WRITE_OFF_DT,
        f.ACCT_CLS_RSN_CD,
        f.PRD_CD                 AS PRD_CD_REV,
        i.LAST_NEW_DFT_DT        AS DFT_DT_REV,
        i.LAST_NEW_DFT_BAL_AMT   AS DFT_BAL_REV,
        i.MODEL_DFT_F            AS MODEL_DFT_F_REV,
        j.LAST_NEW_DFT_DT        AS DFT_DT_SPL,
        j.LAST_NEW_DFT_BAL_AMT   AS DFT_BAL_SPL,
        j.MODEL_DFT_F            AS MODEL_DFT_F_SPL,
        l.DEFAULT_DATE           AS DEFAULT_DATE_MOR,
        l.DEFAULT_BAL            AS DEFAULT_BAL_MOR,
        l.DEFAULT_IND            AS DEFAULT_IND_MOR,
        CASE WHEN SUBSTR(f.BLOCK_RECL_CD, 1, 1) = 'V' THEN 1 ELSE 0 END AS blocked,
        CASE WHEN f.BLOCK_RECL_CD = 'B4' THEN 1 ELSE 0 END AS deceased,
        CASE WHEN SUBSTR(f.BLOCK_RECL_CD, 1, 1) IN ('S', ' S') THEN 1 ELSE 0 END AS stolen,
        CASE WHEN a.product IN ('LOC','MOR','SCL','SPL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ')
             THEN 1 ELSE 0 END AS lend_prods
    FROM emulated.CIS_DATA_NEW2 a
    LEFT JOIN ingestion.TM_DIM a1
        ON a.file_date = a1.TM_LVL_ST_DT
       AND TRIM(a1.TM_LVL) = 'Month'
       AND a.FILE_YR_MTH = RIGHT('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}', 4)
    LEFT JOIN (
        SELECT ACCT_NUM, BASEL_ACCT_ID
        FROM ingestion.BASEL_ACCT_DIM_MAY_2026
        WHERE hash(ACCT_NUM) % {n} = {k}
    ) b
        ON LPAD(a.account, 23, '0') = b.ACCT_NUM
    LEFT JOIN emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS c
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND a1.TM_ID = c.MTH_TM_ID
       AND c.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND c.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        ON b.BASEL_ACCT_ID = d.BASEL_ACCT_ID
       AND a1.TM_ID = d.MTH_TM_ID
       AND d.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND d.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN emulated.BASEL_MORT_ACCT_DRVD_VARS e
        ON b.BASEL_ACCT_ID = e.BASEL_ACCT_ID
       AND a1.TM_ID = e.MTH_TM_ID
       AND e.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND e.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT f
        ON b.BASEL_ACCT_ID = f.BASEL_ACCT_ID
       AND a1.TM_ID = f.MTH_TM_ID
       AND f.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    LEFT JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT g
        ON b.BASEL_ACCT_ID = g.BASEL_ACCT_ID
       AND a1.TM_ID = g.MTH_TM_ID
       AND g.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    LEFT JOIN ingestion.BASEL_MORT_MTH_SNAPSHOT h
        ON b.BASEL_ACCT_ID = h.BASEL_ACCT_ID
       AND a1.TM_ID = h.MTH_TM_ID
       AND h.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    LEFT JOIN emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR i
        ON b.BASEL_ACCT_ID = i.BASEL_ACCT_ID
       AND a1.TM_ID = i.OBSVTN_MTH_TM_ID
       AND i.OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND i.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR j
        ON b.BASEL_ACCT_ID = j.BASEL_ACCT_ID
       AND a1.TM_ID = j.OBSVTN_MTH_TM_ID
       AND j.OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND j.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN emulated.STATUS_FINAL k
        ON TRY_CAST(h.MORT_NUM AS BIGINT) = k.MORTGAGE_NO
       AND a1.TM_LVL_END_DT = CAST(k.PROCESS_DATE AS DATE)
       AND CAST(k.PROCESS_DATE AS DATE) = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND k.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN emulated.TWELVE_MON_DEF_WINDOW l
        ON TRY_CAST(h.MORT_NUM AS BIGINT) = l.MORTGAGE_NO
       AND a1.TM_LVL_END_DT = l.PROCESS_DATE
       AND l.PROCESS_DATE = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND l.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    WHERE a.FILE_YR_MTH = RIGHT('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}', 4)
      AND COALESCE(a.RELATION_CODE, '') <> 'POA'
      AND COALESCE(a.PRODUCT, '') <> 'SEA'
      AND COALESCE(f.PRD_CD, '') NOT IN ('VFB', 'BLV')
      AND LPAD(a.account, 23, '0') NOT IN (SELECT tsys_key FROM xref_eoc WHERE tsys_key IS NOT NULL)
      AND LPAD(a.account, 23, '0') NOT IN (SELECT bcm_key FROM xref_eoc WHERE bcm_key IS NOT NULL)
      AND hash(LPAD(a.account, 23, '0')) % {n} = {k}

    UNION

    SELECT
        a.*,
        a1.TM_ID AS mth_tm_id,
        a1.TM_LVL_END_DT AS process_date,
        b.BASEL_ACCT_ID,
        c.PIT_STAT_VER_2_CD      AS PIT_STAT_REV,
        c.CONSM_PRD_TREATMNT_CD  AS PRD_TREATMNT_CD_REV,
        c.CONSM_SCORECRD_EXCLSN_F AS SCORECRD_EXCLSN_F_REV,
        NULL AS PIT_STAT_SPL,
        NULL AS PRD_ID_SPL,
        NULL AS SCORECRD_EXCLSN_F_SPL,
        NULL AS COMM_FLG_SPL,
        NULL AS OS_BAL_AMT_SPL,
        NULL AS PRD_TREATMNT_CD_SPL,
        NULL AS RECD_STAT_CD_SPL,
        TRY_CAST(f.BNS_DLQNT_DAY AS INTEGER) AS BNS_DLQNT_DAY_REV,
        NULL AS DAY_ODUE_SPL,
        NULL AS DLQNT_DAY_CNT_MOR,
        NULL AS PIT_STAT_MOR,
        NULL AS SCORECRD_EXCLSN_F_MOR,
        NULL AS LRA_STATUS_MOR,
        NULL AS PAID_OFF_DATE_MOR,
        NULL AS CURRENT_BAL_MOR,
        NULL AS TOTAL_SUSPENSE_MOR,
        NULL AS PRD_TREATMNT_CD_MOR,
        NULL AS COMM_TP_CD_MOR,
        NULL AS OS_BAL_AMT_MOR,
        NULL AS FRCLSR_F_MOR,
        NULL AS PD_OFF_F_MOR,
        NULL AS FUND_CD_MOR,
        NULL AS MTH_IN_ARRS_CNT_MOR,
        NULL AS LIFE_INSUR_CD_MOR,
        f.TRNST_NUM              AS TRNST_NUM_REV,
        f.SRC_CD                 AS SOURCE_CD,
        f.BLOCK_RECL_CD,
        f.ACCT_STAT_CD,
        TRY_CAST(f.CR_LMT_AMT AS DECIMAL(17, 3))     AS CR_LMT_AMT,
        TRY_CAST(f.TOT_NEW_BAL_AMT AS DECIMAL(17, 3)) AS TOT_NEW_BAL_AMT,
        f.NON_ACCRL_DT,
        f.WRITE_OFF_DT,
        f.ACCT_CLS_RSN_CD,
        f.PRD_CD                 AS PRD_CD_REV,
        i.LAST_NEW_DFT_DT        AS DFT_DT_REV,
        i.LAST_NEW_DFT_BAL_AMT   AS DFT_BAL_REV,
        i.MODEL_DFT_F            AS MODEL_DFT_F_REV,
        NULL AS DFT_DT_SPL,
        NULL AS DFT_BAL_SPL,
        NULL AS MODEL_DFT_F_SPL,
        NULL AS DEFAULT_DATE_MOR,
        NULL AS DEFAULT_BAL_MOR,
        NULL AS DEFAULT_IND_MOR,
        CASE WHEN SUBSTR(f.BLOCK_RECL_CD, 1, 1) = 'V' THEN 1 ELSE 0 END AS blocked,
        CASE WHEN f.BLOCK_RECL_CD = 'B4' THEN 1 ELSE 0 END AS deceased,
        CASE WHEN SUBSTR(f.BLOCK_RECL_CD, 1, 1) IN ('S', ' S') THEN 1 ELSE 0 END AS stolen,
        CASE WHEN a.product IN ('LOC','MOR','SCL','SPL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ')
             THEN 1 ELSE 0 END AS lend_prods
    FROM emulated.CIS_DATA_NEW2 a
    LEFT JOIN ingestion.TM_DIM a1
        ON a.file_date = a1.TM_LVL_ST_DT
       AND TRIM(a1.TM_LVL) = 'Month'
       AND a.FILE_YR_MTH = RIGHT('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}', 4)
    INNER JOIN xref_eoc b2 ON LPAD(a.account, 23, '0') = b2.tsys_key
    LEFT JOIN (
        SELECT ACCT_NUM, BASEL_ACCT_ID
        FROM ingestion.BASEL_ACCT_DIM_MAY_2026
        WHERE ACCT_NUM IN (SELECT bcm_key FROM xref_eoc)
    ) b ON b2.bcm_key = b.ACCT_NUM
    LEFT JOIN emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS c
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND a1.TM_ID = c.MTH_TM_ID
       AND c.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND c.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT f
        ON b.BASEL_ACCT_ID = f.BASEL_ACCT_ID
       AND a1.TM_ID = f.MTH_TM_ID
       AND f.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    LEFT JOIN emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR i
        ON b.BASEL_ACCT_ID = i.BASEL_ACCT_ID
       AND a1.TM_ID = i.OBSVTN_MTH_TM_ID
       AND i.OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND i.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    WHERE a.FILE_YR_MTH = RIGHT('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}', 4)
      AND COALESCE(a.RELATION_CODE, '') <> 'POA'
      AND COALESCE(a.PRODUCT, '') <> 'SEA'
      AND COALESCE(f.PRD_CD, '') NOT IN ('VFB', 'BLV')
      AND hash(LPAD(a.account, 23, '0')) % {n} = {k}
    """


def _make_gather(sql_text):
    def export_gather(
        duckdb_conn_id="duckdb-conn",
        resource_tier="HIGH",
        pool_slots=96,
        sql=sql_text,
    ):
        pass
    return export_gather


for _i in range(_N_BATCHES):
    globals()[f"export_gather_{_i:02d}"] = _make_gather(_gather_sql(_i, _N_BATCHES))

del _make_gather, _gather_sql, _i


_GATHER_PARQUETS = ",\n                ".join(
    '\'{{ task_instance.xcom_pull(task_ids="'
    + _TASK_GROUP + '.export_gather_' + f"{k:02d}"
    + '", key="parquet") }}\''
    for k in range(_N_BATCHES)
)


def export_result(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH
        g AS (
            SELECT *
            FROM read_parquet([
                {_GATHER_PARQUETS}
            ])
        ),
        d1 AS (
            SELECT
                g.*,
                TRY_CAST(g.cid AS BIGINT) AS cid_num,
                TRY_CAST(g.account AS BIGINT) AS acct_numeric,
                CASE
                    WHEN g.product = 'MOR' AND g.COMM_TP_CD_MOR = 'COMMERCIAL' THEN
                        CASE
                            WHEN (UPPER(g.COMM_TP_CD_MOR) IN ('RESIDENTIAL','COMMERCIAL')
                                  AND g.PD_OFF_F_MOR <> 'Y'
                                  AND g.DLQNT_DAY_CNT_MOR < 90
                                  AND UPPER(g.FRCLSR_F_MOR) <> 'Y'
                                  AND g.CURRENT_BAL_MOR <> 0
                                  AND UPPER(g.LRA_STATUS_MOR) <> 'Y')
                                 OR g.CURRENT_BAL_MOR < 0
                            THEN 'CUR'
                            WHEN (UPPER(g.COMM_TP_CD_MOR) IN ('RESIDENTIAL','COMMERCIAL')
                                  AND g.PD_OFF_F_MOR = 'Y'
                                  AND (g.DLQNT_DAY_CNT_MOR >= 90 OR UPPER(g.FRCLSR_F_MOR) = 'Y' OR UPPER(g.LRA_STATUS_MOR) = 'Y')
                                  AND g.CURRENT_BAL_MOR > 0)
                                 OR (UPPER(g.COMM_TP_CD_MOR) IN ('RESIDENTIAL','COMMERCIAL')
                                     AND UPPER(g.FRCLSR_F_MOR) = 'Y'
                                     AND UPPER(g.PD_OFF_F_MOR) = 'Y'
                                     AND GREATEST(g.CURRENT_BAL_MOR, -g.TOTAL_SUSPENSE_MOR) > 0)
                            THEN 'DEF'
                            ELSE g.PIT_STAT_MOR
                        END
                    ELSE g.PIT_STAT_MOR
                END AS pit_stat_mor_adj
            FROM g
        ),
        d2 AS (
            SELECT
                d1.*,
                CASE
                    WHEN product IN {_REV_PRODUCTS} AND COALESCE(PRD_CD_REV, '') <> 'BLV' AND lend_prods = 1 THEN
                        CASE
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 0 AND CR_LMT_AMT > 0 AND COALESCE(BLOCK_RECL_CD, '') <> 'B5' THEN 'CUR'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND CR_LMT_AMT > 0 AND TOT_NEW_BAL_AMT > 0 AND COALESCE(BLOCK_RECL_CD, '') <> 'B5' AND stolen = 0 AND deceased = 0 THEN 'CUR'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND CR_LMT_AMT = 0 AND TOT_NEW_BAL_AMT <= 0 AND COALESCE(BLOCK_RECL_CD, '') <> 'B5' AND deceased = 0 THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND TOT_NEW_BAL_AMT <= 0 AND deceased = 0 AND COALESCE(BLOCK_RECL_CD, '') <> 'B5' THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 1 AND COALESCE(BLOCK_RECL_CD, '') <> 'B5' AND deceased = 0 THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 0 AND deceased = 1 AND COALESCE(BLOCK_RECL_CD, '') <> 'B5' THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND stolen = 0 AND deceased = 0 AND BLOCK_RECL_CD = 'B5' THEN 'BNK'
                            WHEN PIT_STAT_REV = 'DEF' THEN 'DEF'
                            WHEN PIT_STAT_REV = 'CHG' THEN 'CHG'
                            WHEN COALESCE(PIT_STAT_REV, '') = '' THEN 'WO'
                        END
                    WHEN product IN {_REV_PRODUCTS} AND PRD_CD_REV = 'BLV' AND lend_prods = 1 THEN
                        CASE
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 0 AND CR_LMT_AMT > 0 THEN 'COMM_CUR'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND CR_LMT_AMT > 0 AND TOT_NEW_BAL_AMT > 0 AND stolen = 0 AND deceased = 0 THEN 'COMM_CUR'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND CR_LMT_AMT = 0 AND TOT_NEW_BAL_AMT <= 0 AND deceased = 0 THEN 'COMM_CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND TOT_NEW_BAL_AMT <= 0 AND deceased = 0 THEN 'COMM_CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 1 AND deceased = 0 THEN 'COMM_CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 0 AND deceased = 1 THEN 'COMM_CLO'
                            WHEN PIT_STAT_REV = 'DEF' THEN 'COMM_DEF'
                            WHEN PIT_STAT_REV = 'CHG' THEN 'COMM_CHG'
                            WHEN COALESCE(PIT_STAT_REV, '') = '' THEN 'COMM_WO'
                        END
                    WHEN product = 'SPL' AND COALESCE(CAST(COMM_FLG_SPL AS VARCHAR), '') <> '1' AND lend_prods = 1 THEN
                        CASE
                            WHEN PIT_STAT_SPL = 'CUR' AND OS_BAL_AMT_SPL > 0 THEN 'CUR'
                            WHEN PIT_STAT_SPL = 'CUR' AND OS_BAL_AMT_SPL = 0 THEN 'CLO'
                            WHEN PIT_STAT_SPL = 'DEF' THEN 'DEF'
                            WHEN PIT_STAT_SPL = 'CHG' THEN 'CHG'
                            WHEN COALESCE(PIT_STAT_SPL, '') = '' THEN 'WO'
                        END
                    WHEN product = 'SPL' AND CAST(COMM_FLG_SPL AS VARCHAR) = '1' AND lend_prods = 1 THEN
                        CASE
                            WHEN PIT_STAT_SPL = 'CUR' AND OS_BAL_AMT_SPL > 0 THEN 'COMM_CUR'
                            WHEN PIT_STAT_SPL = 'CUR' AND OS_BAL_AMT_SPL = 0 THEN 'COMM_CLO'
                            WHEN PIT_STAT_SPL = 'DEF' THEN 'COMM_DEF'
                            WHEN PIT_STAT_SPL = 'CHG' THEN 'COMM_CHG'
                            WHEN COALESCE(PIT_STAT_SPL, '') = '' THEN 'COMM_WO'
                        END
                    WHEN product = 'MOR' AND COMM_TP_CD_MOR = 'RESIDENTIAL' AND lend_prods = 1 THEN
                        CASE
                            WHEN pit_stat_mor_adj = 'CUR' AND OS_BAL_AMT_MOR > 0 THEN 'CUR'
                            WHEN pit_stat_mor_adj = 'CUR' AND OS_BAL_AMT_MOR = 0 THEN 'CLO'
                            WHEN pit_stat_mor_adj = 'DEF' THEN 'DEF'
                            WHEN pit_stat_mor_adj = 'CHG' THEN 'CHG'
                            WHEN COALESCE(pit_stat_mor_adj, '') = '' THEN 'WO'
                        END
                    WHEN product = 'MOR' AND COALESCE(COMM_TP_CD_MOR, '') <> 'RESIDENTIAL' AND lend_prods = 1 THEN
                        CASE
                            WHEN pit_stat_mor_adj = 'CUR' AND OS_BAL_AMT_MOR > 0 THEN 'COMM_CUR'
                            WHEN pit_stat_mor_adj = 'CUR' AND OS_BAL_AMT_MOR = 0 THEN 'COMM_CLO'
                            WHEN pit_stat_mor_adj = 'DEF' THEN 'COMM_DEF'
                            WHEN COALESCE(pit_stat_mor_adj, '') = '' THEN 'COMM_WO'
                        END
                END AS lend_bucket
            FROM d1
        ),
        d3 AS (
            SELECT
                d2.*,
                CASE WHEN lend_bucket = 'CUR' THEN 1 ELSE 0 END AS lend_prods_CUR,
                CASE WHEN lend_bucket = 'CLO' THEN 1 ELSE 0 END AS lend_prods_CLO,
                CASE WHEN lend_bucket = 'BNK' THEN 1 ELSE 0 END AS lend_prods_BNK,
                CASE WHEN lend_bucket = 'DEF' THEN 1 ELSE 0 END AS lend_prods_DEF,
                CASE WHEN lend_bucket = 'CHG' THEN 1 ELSE 0 END AS lend_prods_CHG,
                CASE WHEN lend_bucket = 'WO'  THEN 1 ELSE 0 END AS lend_prods_WO,
                0 AS lend_prods_COMM,
                CASE WHEN lend_bucket = 'COMM_CUR' THEN 1 ELSE 0 END AS lend_prods_COMM_CUR,
                CASE WHEN lend_bucket = 'COMM_CLO' THEN 1 ELSE 0 END AS lend_prods_COMM_CLO,
                CASE WHEN lend_bucket = 'COMM_DEF' THEN 1 ELSE 0 END AS lend_prods_COMM_DEF,
                CASE WHEN lend_bucket = 'COMM_CHG' THEN 1 ELSE 0 END AS lend_prods_COMM_CHG,
                CASE WHEN lend_bucket = 'COMM_WO'  THEN 1 ELSE 0 END AS lend_prods_COMM_WO
            FROM d2
        ),
        d4 AS (
            SELECT
                d3.*,
                GREATEST(lend_prods_CUR, lend_prods_CLO, lend_prods_DEF, lend_prods_CHG, lend_prods_WO, 0) AS check_ind,
                CASE WHEN lend_prods_CUR = 1 AND product = 'MOR' THEN 1 ELSE 0 END AS mor_ind,
                CASE WHEN lend_prods_CUR = 1 AND product = 'SPL' THEN 1 ELSE 0 END AS spl_ind,
                CASE WHEN lend_prods_CUR = 1 AND product IN ('LOC','SCL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ') THEN 1 ELSE 0 END AS rev_ind,
                CASE WHEN lend_prods_CUR = 1 AND product = 'SSL' THEN 1 ELSE 0 END AS ssl_ind,
                CASE WHEN TRNST_NUM_REV IN ('7096','8896','21436','30957','32839','38786','38836','41731',
                                            '47308','52100','62299','65912','67116','72793','73270','75523','83170','87106')
                     THEN 1 ELSE 0 END AS PRIVATE_BANK_IND,
                COALESCE(DFT_DT_REV, DFT_DT_SPL, DEFAULT_DATE_MOR) AS default_date,
                COALESCE(DFT_BAL_REV, DFT_BAL_SPL, DEFAULT_BAL_MOR) AS default_bal,
                CASE WHEN MODEL_DFT_F_REV = 'Y' OR MODEL_DFT_F_SPL = 'Y' OR DEFAULT_IND_MOR = 1 THEN 1 ELSE 0 END AS default_ind,
                COALESCE(NULLIF(PRD_TREATMNT_CD_REV, ''), NULLIF(PRD_TREATMNT_CD_SPL, ''), NULLIF(PRD_TREATMNT_CD_MOR, '')) AS PROD_TREAT,
                COALESCE(NULLIF(PIT_STAT_REV, ''), NULLIF(pit_stat_mor_adj, ''), NULLIF(PIT_STAT_SPL, '')) AS PIT_STAT,
                GREATEST(BNS_DLQNT_DAY_REV - 30, DAY_ODUE_SPL, DLQNT_DAY_CNT_MOR) AS days_dlq
            FROM d3
        ),
        d5 AS (
            SELECT
                d4.*,
                CASE
                    WHEN SOURCE_CD = '980' AND rev_ind = 1 AND (lend_prods_CUR = 1 OR lend_prods_CLO = 1) THEN 'Y'
                    WHEN (mor_ind = 1 OR spl_ind = 1) AND (lend_prods_COMM_CUR = 1 OR lend_prods_COMM_CLO = 1) THEN 'Y'
                    ELSE 'N'
                END AS CORP_COMM_EXCL,
                CASE
                    WHEN SOURCE_CD = '911' AND rev_ind = 1 AND (lend_prods_CUR = 1 OR lend_prods_CLO = 1) THEN 'Y'
                    ELSE 'N'
                END AS STAFF_EXCL,
                CASE
                    WHEN product IN ('SSL','VUS') AND COALESCE(BLOCK_RECL_CD, '') = '' THEN 'N'
                    ELSE COALESCE(NULLIF(SCORECRD_EXCLSN_F_REV, ''), NULLIF(SCORECRD_EXCLSN_F_SPL, ''), NULLIF(SCORECRD_EXCLSN_F_MOR, ''))
                END AS MODEL_EXCL
            FROM d4
        )
    SELECT
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        * EXCLUDE ({_DROP_COLS.strip()}, lend_bucket, pit_stat_mor_adj)
    FROM d5
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass
