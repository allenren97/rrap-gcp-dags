#!/usr/bin/env python3
"""
run_custuniv01.py — standalone runner for the CBS custuniv_01 ETL (CIS_DATA_POP_02).

Reproduces rrap-gcp-dags/functions/cbs/custuniv/custuniv_01.py without Airflow:
  1. export_gather  -> temp table cis_gather  (base CIS pop LEFT JOIN 12 account tables)
  2. export_result  -> the layered DATA-step derivations
  3. COPY result     -> parquet

Assumes all upstream data is already present in the DuckDB database, reachable as
ingestion.*, emulated.*  (attach your DuckLake / .duckdb first if needed).

Usage:
  python run_custuniv01.py --db /path/to/catalog.duckdb --stream <STREAM> \
      --rundate 2026-05-31 --out cis_data_pop_02_20260531.parquet
"""
import argparse
import sys

import duckdb

# --- constants lifted verbatim from custuniv_01.py --------------------------
REV_PRODUCTS = "('LOC','SCL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ')"

DROP_COLS = """
    PIT_STAT_REV, PIT_STAT_SPL, PIT_STAT_MOR,
    PRD_TREATMNT_CD_REV, PRD_TREATMNT_CD_SPL, PRD_TREATMNT_CD_MOR,
    SCORECRD_EXCLSN_F_REV, SCORECRD_EXCLSN_F_SPL, SCORECRD_EXCLSN_F_MOR,
    DFT_DT_REV, DFT_DT_SPL, DEFAULT_DATE_MOR,
    DFT_BAL_REV, DFT_BAL_SPL, DEFAULT_BAL_MOR,
    MODEL_DFT_F_REV, MODEL_DFT_F_SPL, DEFAULT_IND_MOR,
    BNS_DLQNT_DAY_REV, DAY_ODUE_SPL, DLQNT_DAY_CNT_MOR
""".strip()

# --- Stage 1: export_gather (sentinels __RUNDATE__/__STREAM__/__MTH_TM_ID__/__YYYYMM__) ---
GATHER_SQL = """
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
        d.OS_BAL_AMT_V2          AS OS_BAL_AMT_SPL,
        d.TREATMNT_F             AS PRD_TREATMNT_CD_SPL,
        g.RECD_STAT_CD           AS RECD_STAT_CD_SPL,
        f.BNS_DLQNT_DAY          AS BNS_DLQNT_DAY_REV,
        g.DAY_ODUE               AS DAY_ODUE_SPL,
        e.DLQNT_DAY_CNT          AS DLQNT_DAY_CNT_MOR,
        k.STATUS                 AS PIT_STAT_MOR,
        k.MODEL_EXCL             AS SCORECRD_EXCLSN_F_MOR,
        k.LRA_STATUS             AS LRA_STATUS_MOR,
        k.PAID_OFF_DATE          AS PAID_OFF_DATE_MOR,
        k.CURRENT_BAL            AS CURRENT_BAL_MOR,
        k.TOTAL_SUSPENSE         AS TOTAL_SUSPENSE_MOR,
        e.CONSM_PRD_TREATMNT_CD  AS PRD_TREATMNT_CD_MOR,
        e.COMM_TP_CD             AS COMM_TP_CD_MOR,
        e.OS_BAL_AMT             AS OS_BAL_AMT_MOR,
        h.FRCLSR_F               AS FRCLSR_F_MOR,
        h.PD_OFF_F               AS PD_OFF_F_MOR,
        h.FUND_CD                AS FUND_CD_MOR,
        h.MTH_IN_ARRS_CNT        AS MTH_IN_ARRS_CNT_MOR,
        h.LIFE_INSUR_CD          AS LIFE_INSUR_CD_MOR,
        f.TRNST_NUM              AS TRNST_NUM_REV,
        f.SRC_CD                 AS SOURCE_CD,
        f.BLOCK_RECL_CD,
        f.ACCT_STAT_CD,
        f.CR_LMT_AMT,
        f.TOT_NEW_BAL_AMT,
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
    FROM ingestion.CIS_DATA_NEW2 a
    LEFT JOIN ingestion.TM_DIM a1
        ON a.file_date = a1.TM_LVL_ST_DT
       AND TRIM(a1.TM_LVL) = 'Month'
       AND a.file_yr_mth = '__YYYYMM__'
    LEFT JOIN ingestion.BASEL_ACCT_DIM b
        ON LPAD(a.account, 23, '0') = b.ACCT_NUM
    LEFT JOIN emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS c
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND c.MTH_TM_ID = __MTH_TM_ID__
       AND c.STREAM = '__STREAM__'
    LEFT JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        ON b.BASEL_ACCT_ID = d.BASEL_ACCT_ID
       AND d.MTH_TM_ID = __MTH_TM_ID__
       AND d.STREAM = '__STREAM__'
    LEFT JOIN emulated.BASEL_MORT_ACCT_DRVD_VARS e
        ON b.BASEL_ACCT_ID = e.BASEL_ACCT_ID
       AND e.MTH_TM_ID = __MTH_TM_ID__
       AND e.STREAM = '__STREAM__'
    LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT f
        ON b.BASEL_ACCT_ID = f.BASEL_ACCT_ID
       AND f.MTH_TM_ID = __MTH_TM_ID__
    LEFT JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT g
        ON b.BASEL_ACCT_ID = g.BASEL_ACCT_ID
       AND g.MTH_TM_ID = __MTH_TM_ID__
    LEFT JOIN ingestion.BASEL_MORT_MTH_SNAPSHOT h
        ON b.BASEL_ACCT_ID = h.BASEL_ACCT_ID
       AND h.MTH_TM_ID = __MTH_TM_ID__
    LEFT JOIN emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR i
        ON b.BASEL_ACCT_ID = i.BASEL_ACCT_ID
       AND i.OBSVTN_MTH_TM_ID = __MTH_TM_ID__
       AND i.STREAM = '__STREAM__'
    LEFT JOIN emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR j
        ON b.BASEL_ACCT_ID = j.BASEL_ACCT_ID
       AND j.OBSVTN_MTH_TM_ID = __MTH_TM_ID__
       AND j.STREAM = '__STREAM__'
    LEFT JOIN emulated.STATUS_FINAL k
        ON TRY_CAST(h.MORT_NUM AS BIGINT) = k.MORTGAGE_NO
       AND a1.TM_LVL_END_DT = CAST(k.PROCESS_DATE AS DATE)
       AND CAST(k.PROCESS_DATE AS DATE) = DATE '__RUNDATE__'
       AND k.STREAM = '__STREAM__'
    LEFT JOIN emulated.TWELVE_MON_DEF_WINDOW l
        ON TRY_CAST(h.MORT_NUM AS BIGINT) = l.MORTGAGE_NO
       AND a1.TM_LVL_END_DT = l.PROCESS_DATE
       AND l.PROCESS_DATE = DATE '__RUNDATE__'
       AND l.STREAM = '__STREAM__'
    WHERE a.file_yr_mth = '__YYYYMM__'
      AND a.RELATION_CODE <> 'POA'
      AND COALESCE(a.PRODUCT, '') <> 'SEA'
      AND COALESCE(f.PRD_CD, '') NOT IN ('VFB', 'BLV')
"""

# --- Stage 2: export_result (reads the cis_gather temp table) ---------------
RESULT_SQL = """
    WITH
        g AS (SELECT * FROM cis_gather),
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
                    WHEN product IN __REV_PRODUCTS__ AND PRD_CD_REV <> 'BLV' AND lend_prods = 1 THEN
                        CASE
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 0 AND CR_LMT_AMT > 0 AND BLOCK_RECL_CD <> 'B5' THEN 'CUR'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND CR_LMT_AMT > 0 AND TOT_NEW_BAL_AMT > 0 AND BLOCK_RECL_CD <> 'B5' AND stolen = 0 AND deceased = 0 THEN 'CUR'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND CR_LMT_AMT = 0 AND TOT_NEW_BAL_AMT <= 0 AND BLOCK_RECL_CD <> 'B5' AND deceased = 0 THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND TOT_NEW_BAL_AMT <= 0 AND deceased = 0 AND BLOCK_RECL_CD <> 'B5' THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 1 AND BLOCK_RECL_CD <> 'B5' AND deceased = 0 THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 0 AND stolen = 0 AND deceased = 1 AND BLOCK_RECL_CD <> 'B5' THEN 'CLO'
                            WHEN PIT_STAT_REV = 'CUR' AND blocked = 1 AND stolen = 0 AND deceased = 0 AND BLOCK_RECL_CD = 'B5' THEN 'BNK'
                            WHEN PIT_STAT_REV = 'DEF' THEN 'DEF'
                            WHEN PIT_STAT_REV = 'CHG' THEN 'CHG'
                            WHEN COALESCE(PIT_STAT_REV, '') = '' THEN 'WO'
                        END
                    WHEN product IN __REV_PRODUCTS__ AND PRD_CD_REV = 'BLV' AND lend_prods = 1 THEN
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
                    WHEN product = 'SPL' AND COALESCE(COMM_FLG_SPL, '') <> '1' AND lend_prods = 1 THEN
                        CASE
                            WHEN PIT_STAT_SPL = 'CUR' AND OS_BAL_AMT_SPL > 0 THEN 'CUR'
                            WHEN PIT_STAT_SPL = 'CUR' AND OS_BAL_AMT_SPL = 0 THEN 'CLO'
                            WHEN PIT_STAT_SPL = 'DEF' THEN 'DEF'
                            WHEN PIT_STAT_SPL = 'CHG' THEN 'CHG'
                            WHEN COALESCE(PIT_STAT_SPL, '') = '' THEN 'WO'
                        END
                    WHEN product = 'SPL' AND COMM_FLG_SPL = '1' AND lend_prods = 1 THEN
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
                    WHEN product = 'MOR' AND COMM_TP_CD_MOR <> 'RESIDENTIAL' AND lend_prods = 1 THEN
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
                COALESCE(PRD_TREATMNT_CD_REV, PRD_TREATMNT_CD_SPL, PRD_TREATMNT_CD_MOR) AS PROD_TREAT,
                COALESCE(PIT_STAT_REV, pit_stat_mor_adj, PIT_STAT_SPL) AS PIT_STAT,
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
                    ELSE COALESCE(SCORECRD_EXCLSN_F_REV, SCORECRD_EXCLSN_F_SPL, SCORECRD_EXCLSN_F_MOR)
                END AS MODEL_EXCL
            FROM d4
        )
    SELECT
        DATE '__RUNDATE__' AS OBSN_DT,
        '__STREAM__' AS STREAM,
        * EXCLUDE (__DROP_COLS__, lend_bucket, pit_stat_mor_adj)
    FROM d5
"""


def sub(sql, rundate, stream, mth_tm_id, yyyymm):
    return (sql
            .replace("__RUNDATE__", rundate)
            .replace("__STREAM__", stream)
            .replace("__MTH_TM_ID__", str(mth_tm_id))
            .replace("__YYYYMM__", yyyymm)
            .replace("__REV_PRODUCTS__", REV_PRODUCTS)
            .replace("__DROP_COLS__", DROP_COLS))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", required=True, help="DuckDB database file with ingestion.* / emulated.* schemas")
    p.add_argument("--rundate", default="2026-05-31", help="month-end run date (default 2026-05-31)")
    p.add_argument("--stream", required=True, help="STREAM value the emulated tables are partitioned by")
    p.add_argument("--out", default="cis_data_pop_02_20260531.parquet", help="output parquet path")
    p.add_argument("--mth-tm-id", type=int, default=None,
                   help="override TM_ID for the run month (else derived from ingestion.TM_DIM)")
    args = p.parse_args()

    rundate = args.rundate
    yyyymm = rundate[:7].replace("-", "")        # 2026-05-31 -> 202605
    con = duckdb.connect(args.db)

    # Resolve the run month's TM_ID from TM_DIM unless overridden.
    if args.mth_tm_id is not None:
        mth_tm_id = args.mth_tm_id
    else:
        row = con.execute(
            "SELECT TM_ID FROM ingestion.TM_DIM WHERE TM_LVL_END_DT = DATE ? AND TRIM(TM_LVL) = 'Month'",
            [rundate],
        ).fetchone()
        if not row:
            sys.exit(f"ERROR: no monthly TM_DIM row for TM_LVL_END_DT = {rundate}. "
                     f"Pass --mth-tm-id explicitly.")
        mth_tm_id = row[0]

    print(f"rundate={rundate}  yyyymm={yyyymm}  mth_tm_id={mth_tm_id}  stream={args.stream}")

    # Stage 1: gather -> temp table
    con.execute("CREATE OR REPLACE TEMP TABLE cis_gather AS " + sub(GATHER_SQL, rundate, args.stream, mth_tm_id, yyyymm))
    n_gather = con.execute("SELECT count(*) FROM cis_gather").fetchone()[0]
    print(f"stage 1 (gather): {n_gather:,} rows")

    # Stage 2: derivations -> parquet
    result = sub(RESULT_SQL, rundate, args.stream, mth_tm_id, yyyymm)
    con.execute(f"COPY ({result}) TO '{args.out}' (FORMAT PARQUET)")
    n_out = con.execute(f"SELECT count(*) FROM read_parquet('{args.out}')").fetchone()[0]
    print(f"stage 2 (result): {n_out:,} rows  ->  {args.out}")


if __name__ == "__main__":
    main()
