from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.TM_DIM",
    "ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.CUST19"
DEPENDENCIES = {
    "export_sp": ["duckdb_delete"],
    "export_mo": ["duckdb_delete"],
    "export_rc": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


SQL_PARAMS = f"""
    PARAMS AS (
        SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID
    )
"""

SQL_TM = f"""
    TM AS (
        SELECT
            T2.TM_ID
            , MAX(t1.DAY_DT) as LAST_BUSINESS_DAY
        FROM { UPSTREAM_ASSET[0] } T1
        INNER JOIN (
            SELECT TM.TM_ID, TM.CLNDR_YR, TM.MTH_CLNDR_CD
            FROM { UPSTREAM_ASSET[0] } TM
            INNER JOIN PARAMS P ON TM.TM_ID = P.MTH_TM_ID
            WHERE UPPER(TRIM(TM.TM_LVL)) = 'MONTH'
        ) T2 ON
            T1.CLNDR_YR = T2.CLNDR_YR
            AND T1.MTH_CLNDR_CD = T2.MTH_CLNDR_CD
        WHERE
            UPPER(TRIM(T1.TM_LVL)) = 'DAY'
            AND UPPER(TRIM(T1.DAY_OF_WK_DESC)) IN ('MONDAY','TUESDAY','WEDNESDAY','THURSDAY', 'FRIDAY')
        GROUP BY
            T2.TM_ID
    )
"""

SQL_BASEL_MORT_ACCT_DRVD_VARS = f"""
    BASEL_MORT_ACCT_DRVD_VARS AS (
        SELECT
            S.MTH_TM_ID
            , S.PRIM_BASEL_CUST_ID
            , S.BASEL_ACCT_ID
            , S.CRNT_BAL_AMT
            , S.PD_OFF_F
            , S.CRNT_BAL_AMT + S.INTR_ACCR_AMT AS OS_BAL_AMT
            , CASE
                WHEN (S.PD_OFF_DT IS NOT NULL OR TRIM(S.PD_OFF_F) = 'Y') THEN 0
                ELSE
                    CASE
                        WHEN TRIM(S.FLOAT_CD) IN ('W', 'B', 'S') THEN GREATEST(0, DATEDIFF('day', S.WK_FRST_UNPAID_DT, TM.LAST_BUSINESS_DAY))
                        ELSE GREATEST(0, DATEDIFF('day', S.FRST_UNPAID_DT, TM.LAST_BUSINESS_DAY))
                    END
            END AS DLQNT_DAY_CNT
            , CASE
                WHEN (TRIM(S.FUND_CD)::int >= 2000 AND TRIM(S.FUND_CD)::int <= 2199) OR
                    (TRIM(S.FUND_CD)::int >= 2202 AND TRIM(S.FUND_CD)::int <= 2249) OR
                    (TRIM(S.FUND_CD)::int >= 6490 AND TRIM(S.FUND_CD)::int <= 6499) THEN
                    'Y'
                ELSE 'N'
            END AS LAND_RGSTRN_ACT_STAT_F
            , CASE WHEN
                TRY_CAST(SUBSTR(SCRTY_TP_2,1,1) AS INTEGER) = 6 OR TRY_CAST(SUBSTR(TRIM(SCRTY_TP_2),LENGTH(TRIM(SCRTY_TP_2))-2, 3) AS DOUBLE) >= 5 THEN 'COMMERCIAL'
                ELSE 'RESIDENTIAL'
            END AS COMM_TP_CD
        FROM { UPSTREAM_ASSET[3] } S
        INNER JOIN TM ON S.MTH_TM_ID = TM.TM_ID
    )
"""

def export_sp(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        WITH
        { SQL_PARAMS }
        select
            mth_tm_id
            , prim_basel_cust_id
            , max(CASE WHEN NB_CHG = 1 THEN 4 WHEN NB_DEF = 1 THEN 3 WHEN NB_DELQ = 1 THEN 2 ELSE 1 END) CUST19
        from (
            SELECT
                a.basel_cust_id prim_basel_cust_id
                , a.mth_tm_id
                , CASE WHEN RECD_STAT_CD in (6,7,8) THEN 1 ELSE 0 END NB_CHG
                , CASE WHEN RECD_STAT_CD = 5 THEN 1 WHEN (RECD_STAT_CD = 4 and DAY_ODUE >= 90) THEN 1 ELSE 0 END NB_DEF
                , CASE WHEN RECD_STAT_CD not in (6,7,8)  and DAY_ODUE > 0 THEN 1 ELSE 0 END NB_DELQ
                , ROUND(TOT_CRNT_BAL_AMT + ADD_ON_BAL_AMT + ACCR_INTR,3) BALANCE
                , DAY_ODUE
                , RECD_STAT_CD
            from { UPSTREAM_ASSET[1] } a
            inner join { UPSTREAM_ASSET[4] } S on
                a.basel_acct_id=s.basel_acct_id
                and a.mth_tm_id=s.mth_tm_id
            inner join params p on
                s.mth_tm_id = p.mth_tm_id
            where
                s.prim_basel_cust_id is not null
                and s.prim_basel_cust_id <> -1
                and s.RECD_STAT_CD in (4,5,6,7,8)
        ) as sv
        group by
            mth_tm_id
            , prim_basel_cust_id
    """,
):
    pass


def export_rc(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        WITH
        { SQL_PARAMS }
        select
            mth_tm_id
            , prim_basel_cust_id
            , max(CASE WHEN NB_CHG = 1 THEN 4 WHEN NB_DEF = 1 THEN 3 WHEN NB_DELQ = 1 THEN 2 ELSE 1 END) CUST19
        from (
            select
                a.mth_tm_id
                , a.basel_cust_id prim_basel_cust_id
                , CASE WHEN CHRG_OFF_CD ='1' THEN 1 ELSE 0 END NB_CHG
                , CASE WHEN CHRG_OFF_CD !='1' and (BLOCK_RECL_CD = 'B5' or CHRG_OFF_CD in ('N','Q') or BNS_DLQNT_DAY >= 120) THEN 1 ELSE 0 END  NB_DEF
                , CASE WHEN CHRG_OFF_CD !='1' and (BNS_DLQNT_DAY >= 30 or (CHRG_OFF_CD != '1' and ( BLOCK_RECL_CD = 'B5' or CHRG_OFF_CD in ('N','Q') or BNS_DLQNT_DAY >= 120))) THEN 1 ELSE 0 END NB_DELQ
                , BNS_DLQNT_DAY
                , TOT_NEW_BAL_AMT BALANCE
            from { UPSTREAM_ASSET[1] } a
            inner join { UPSTREAM_ASSET[2] } m on
                a.basel_acct_id=m.basel_acct_id
                and a.mth_tm_id=m.mth_tm_id
            inner join params p on
                m.mth_tm_id = p.mth_tm_id
            where
                m.prim_basel_cust_id is not null
                and m.prim_basel_cust_id <> -1
                and (m.TOT_NEW_BAL_AMT > 0 or m.CR_LMT_AMT > 0)
                and UPPER(TRIM(m.PRD_CD)) not in ('BLV')
                and UPPER(TRIM(m.SUB_PRD_CD)) not in ('CC')
                and UPPER(TRIM(m.PRD_CD)) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
        ) as sv
        group by
            mth_tm_id
            , prim_basel_cust_id
    """,
):
    pass


def export_mo(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        WITH
        { SQL_PARAMS }
        , { SQL_TM }
        , { SQL_BASEL_MORT_ACCT_DRVD_VARS }
        select
            mth_tm_id
            , prim_basel_cust_id
            , max(CASE WHEN NB_CHG = 1 THEN 4 WHEN NB_DEF = 1 THEN 3 WHEN NB_DELQ = 1 THEN 2 ELSE 1 END) CUST19
        from (
            select
                a.mth_tm_id
                , a.basel_cust_id prim_basel_cust_id
                , CASE WHEN ms.FRCLSR_F = 'Y' or dv.LAND_RGSTRN_ACT_STAT_F = 'Y' THEN 1 ELSE 0 END NB_CHG
                , CASE WHEN dv.DLQNT_DAY_CNT >= 90 and ((ms.FRCLSR_F <> 'Y' OR ms.FRCLSR_F IS NULL) AND (dv.LAND_RGSTRN_ACT_STAT_F <> 'Y' OR dv.LAND_RGSTRN_ACT_STAT_F IS NULL)) THEN 1 ELSE 0 END NB_DEF
                , CASE WHEN (ms.FRCLSR_F != 'Y' or dv.LAND_RGSTRN_ACT_STAT_F != 'Y') and (dv.DLQNT_DAY_CNT > 0 and dv.DLQNT_DAY_CNT < 90) THEN 1 ELSE 0 END NB_DELQ
                , dv.OS_BAL_AMT BALANCE
                , dv.DLQNT_DAY_CNT
            from { UPSTREAM_ASSET[1] } a
            inner join { UPSTREAM_ASSET[3] } ms on
                a.basel_acct_id=ms.basel_acct_id
                and a.mth_tm_id=ms.mth_tm_id
            inner join BASEL_MORT_ACCT_DRVD_VARS dv on
                a.basel_acct_id=dv.basel_acct_id
                and a.mth_tm_id=dv.mth_tm_id
            inner join params p on
                ms.mth_tm_id = p.mth_tm_id
            where
                ms.prim_basel_cust_id is not null
                and ms.prim_basel_cust_id <> -1
                and ms.CRNT_BAL_AMT <> 0
                and ms.PD_OFF_F ='N'
                and upper(dv.COMM_TP_CD)='RESIDENTIAL'
        ) as sv
        group by
            mth_tm_id
            , prim_basel_cust_id
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
        WITH
        { SQL_PARAMS }
        , { SQL_TM }
        , { SQL_BASEL_MORT_ACCT_DRVD_VARS }
        , SCRD_CUSTOMER_LIST_SPL AS (
            select distinct
                m.prim_basel_cust_id as basel_cust_id
            from { UPSTREAM_ASSET[3] } m
            inner join BASEL_MORT_ACCT_DRVD_VARS d ON
                m.mth_tm_id=d.mth_tm_id
                and m.basel_acct_id=d.basel_acct_id
            inner join params p on 1=1
            where
                (m.mth_tm_id between p.mth_tm_id and p.mth_tm_id)
                and m.prim_basel_cust_id is not null
                and m.prim_basel_cust_id <> -1
                and m.CRNT_BAL_AMT <> 0
                and m.PD_OFF_F ='N'
                and upper(d.COMM_TP_CD)='RESIDENTIAL'
            union
            select distinct
                m.prim_basel_cust_id as basel_cust_id
            from { UPSTREAM_ASSET[4] } m
            inner join params p on 1=1
            where
                (m.mth_tm_id between p.mth_tm_id AND p.mth_tm_id)
                and m.prim_basel_cust_id is not null
                and m.prim_basel_cust_id <> -1
                and m.RECD_STAT_CD in (4,5,6,7,8)
            union
            select distinct
                m.prim_basel_cust_id as basel_cust_id
            from { UPSTREAM_ASSET[2] } m
            inner join params p on 1=1
            where
                (m.mth_tm_id between p.mth_tm_id AND p.mth_tm_id)
                and m.prim_basel_cust_id is not null
                and m.prim_basel_cust_id <> -1
                and (m.TOT_NEW_BAL_AMT > 0 or m.CR_LMT_AMT > 0)
                and UPPER(TRIM(m.PRD_CD)) not in ('BLV')
                and UPPER(TRIM(m.SUB_PRD_CD)) not in ('CC')
                and UPPER(TRIM(m.PRD_CD)) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
        )
        , SCRD_DERIVED_INPUTS_SPL_CUST AS (
            select
                mth_tm_id
                , prim_basel_cust_id
                , max(CUST19) CUST19
            from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__cust19.export_sp", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust19.export_rc", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust19.export_mo", key="parquet") }}}}'], union_by_name = true)
            group by
                mth_tm_id
                , prim_basel_cust_id
        )
        select
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
            , l.BASEL_CUST_ID
            , b.CUST19
        from SCRD_CUSTOMER_LIST_SPL as l
        inner join SCRD_DERIVED_INPUTS_SPL_CUST as b on
            l.basel_cust_id = b.prim_basel_cust_id
        inner join params p on
            b.mth_tm_id = p.mth_tm_id
    )
    """,
):
    pass


