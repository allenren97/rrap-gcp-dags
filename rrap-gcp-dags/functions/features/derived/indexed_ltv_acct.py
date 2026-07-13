from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",
    "ingestion.TM_DIM",
]

DOWNSTREAM_ASSET = "features.INDEXED_LTV_ACCT"

DEPENDENCIES = {
    "duckdb_delete_indexed_ltv_acct": ["duckdb_load_indexed_ltv_acct"],
}


def duckdb_delete_indexed_ltv_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load_indexed_ltv_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        WITH
        PARAM AS (
            SELECT
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}::BIGINT AS OBS_MTH_TM_ID,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
        ),

        BASE AS (
            SELECT
                (SELECT OBSN_DT FROM PARAM) AS OBSN_DT,
                t1.MTH_TM_ID,
                t1.MTH_END_DT,
                t1.BASEL_ACCT_ID,
                t1.PRIM_BASEL_CUST_ID,
                t1.PRIM_CUST_CID,
                t1.MORT_NUM,
                t1.MORT_AUTH_DT,
                t1.LND_VAL,
                t1.CRNT_BAL_AMT,
                t1.INTR_ACCR_AMT,
                t1.PROPERTY_ADDR_3
            FROM {UPSTREAM_ASSET[0]} AS t1
            WHERE t1.MTH_TM_ID = (SELECT OBS_MTH_TM_ID FROM PARAM)
              AND UPPER(TRIM(t1.COMM_TP)) = 'RESIDENTIAL'
              AND t1.CRNT_BAL_AMT > 0
              AND TRIM(t1.PD_OFF_F) = 'N'
              AND CAST(NULLIF(TRIM(t1.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) IS NULL
              AND t1.PRIM_BASEL_CUST_ID <> -1
        ),

        ADDR AS (
            SELECT
                b.*,
                CASE
                    WHEN b.PROPERTY_ADDR_3 IS NULL THEN NULL
                    ELSE TRIM(regexp_replace(b.PROPERTY_ADDR_3, '\\\\s+[^ ]+\\\\s*$', ''))
                END AS CITY,
                CASE
                    WHEN b.PROPERTY_ADDR_3 IS NULL THEN NULL
                    ELSE NULLIF(TRIM(regexp_extract(b.PROPERTY_ADDR_3, '([^ ]+)\\\\s*$', 1)), '')
                END AS PROVINCE
            FROM BASE b
        ),

        ORIG_TM AS (
            SELECT
                a.*,
                LAST_DAY(CAST(a.MORT_AUTH_DT AS DATE)) AS ORIG_ME_DATE
            FROM ADDR a
        ),

        ORIG_TM_ID AS (
            SELECT
                o.*,
                td.TM_ID AS TM_ID_ORIG
            FROM ORIG_TM o
            LEFT JOIN {UPSTREAM_ASSET[2]} td
              ON td.TM_LVL = 'Month'
             AND td.TM_LVL_END_DT = o.ORIG_ME_DATE
        ),

        STD_CITY AS (
            SELECT
                a.*,
                CASE
                    WHEN UPPER(CITY) LIKE '%OTTAWA%' OR UPPER(CITY) LIKE '%GATINEAU%' THEN 'OTTAWA_GATINEAU'
                    WHEN UPPER(CITY) LIKE '%SUDBURY%' THEN 'Sudbury'
                    WHEN UPPER(CITY) LIKE '%KITCHENER%' OR UPPER(CITY) LIKE '%CAMBRIDGE%' OR UPPER(CITY) LIKE '%WATERLOO%' THEN 'Kitchener - Cambridge - Waterloo'
                    WHEN UPPER(CITY) LIKE '%VANCOUVER%' THEN 'Vancouver'
                    WHEN UPPER(CITY) LIKE '%VICTORIA%' THEN 'Victoria'
                    WHEN UPPER(CITY) LIKE '%HALIFAX%' THEN 'Halifax'
                    WHEN UPPER(CITY) LIKE '%KINGSTON%' THEN 'Kingston'
                    WHEN UPPER(CITY) LIKE '%HAMILTON%' THEN 'Hamilton'
                    WHEN UPPER(CITY) LIKE '%OSHAWA%' THEN 'Oshawa'
                    WHEN UPPER(CITY) LIKE '%BRANTFORD%' THEN 'Brantford'
                    WHEN UPPER(CITY) LIKE '%GUELPH%' THEN 'Guelph'
                    WHEN UPPER(CITY) LIKE '%BELLEVILLE%' THEN 'Belleville'
                    WHEN UPPER(CITY) LIKE '%WINNIPEG%' THEN 'Winnipeg'
                    WHEN UPPER(CITY) LIKE '%MONCTON%' THEN 'Moncton'
                    WHEN UPPER(CITY) LIKE '%SAINT JOHN%' THEN 'Saint John'
                    WHEN UPPER(CITY) LIKE '%BARRIE%' THEN 'Barrie'
                    WHEN UPPER(CITY) LIKE '%LONDON%' THEN 'London'
                    WHEN UPPER(CITY) LIKE '%ST JOHNS%' THEN 'St. Johns'
                    WHEN UPPER(CITY) LIKE '%TROIS-RIVIERES%' OR UPPER(CITY) LIKE '%RIVIERES%' OR UPPER(CITY) LIKE '%TROIS%' THEN 'Trois-Rivieres'
                    WHEN UPPER(CITY) LIKE '%SHERBROOKE%' THEN 'Sherbrooke'
                    WHEN UPPER(CITY) LIKE '%CALGARY%' THEN 'Calgary'
                    WHEN UPPER(CITY) LIKE '%QUEBEC%' THEN 'Quebec'
                    WHEN UPPER(CITY) LIKE '%EDMONTON%' THEN 'Edmonton'
                    WHEN UPPER(CITY) LIKE '%KELOWNA%' THEN 'Kelowna'
                    WHEN UPPER(CITY) LIKE '%WINDSOR%' THEN 'Windsor'
                    WHEN UPPER(CITY) LIKE '%LETHBRIDGE%' THEN 'Lethbridge'
                    WHEN UPPER(CITY) LIKE '%MONTREAL%' THEN 'Montreal'
                    WHEN UPPER(CITY) LIKE '%PETERBOROUGH%' THEN 'Peterborough'
                    WHEN UPPER(CITY) LIKE '%ABBOTSFORD%' OR UPPER(CITY) LIKE '%ABBOTSFORD-MISSION%' OR UPPER(CITY) LIKE '%MISSION%' THEN 'Abbotsford - Mission'
                    WHEN UPPER(CITY) LIKE '%THUNDER BAY%' OR UPPER(CITY) LIKE '%THUNDER-BAY%' OR UPPER(CITY) LIKE '%THUNDER%' THEN 'Thunder Bay'
                    WHEN UPPER(CITY) LIKE '%ST CATHARINES%' OR UPPER(CITY) LIKE '%NIAGARA%' OR UPPER(CITY) LIKE '%ST CATHARINES-NIAGARA%' THEN 'St. Catharines - Niagara'
                    WHEN UPPER(CITY) LIKE '%TORONTO%' THEN 'Toronto'
                    ELSE '11'
                END AS STANDARDIZED_CITY
            FROM ORIG_TM_ID a
        ),

        TERANET AS (
            SELECT
                LABEL_2,
                MTH_TM_ID,
                MAX(INDEX) AS HPI_INDEX
            FROM {UPSTREAM_ASSET[1]}
            GROUP BY LABEL_2, MTH_TM_ID
        ),

        ROW_LVL AS (
            SELECT
                s.OBSN_DT,
                s.MTH_TM_ID,
                s.BASEL_ACCT_ID,
                s.PRIM_BASEL_CUST_ID,
                s.MORT_NUM,
                s.LND_VAL,
                s.CRNT_BAL_AMT,
                s.INTR_ACCR_AMT,
                s.STANDARDIZED_CITY,
                s.TM_ID_ORIG,
                obs.HPI_INDEX AS OBS_INDEX,
                orig.HPI_INDEX AS ORIG_INDEX,
                (s.LND_VAL * obs.HPI_INDEX / NULLIF(orig.HPI_INDEX, 0)) AS INDEXED_LEND_VALUE,
                (
                    (s.CRNT_BAL_AMT + s.INTR_ACCR_AMT)
                    / NULLIF(
                        (s.LND_VAL * obs.HPI_INDEX / NULLIF(orig.HPI_INDEX, 0)),
                        0
                    )
                ) AS INDEXED_LTV_ROW
            FROM STD_CITY s
            LEFT JOIN TERANET obs
              ON s.STANDARDIZED_CITY = obs.LABEL_2
             AND s.MTH_TM_ID = obs.MTH_TM_ID
            LEFT JOIN TERANET orig
              ON s.STANDARDIZED_CITY = orig.LABEL_2
             AND s.TM_ID_ORIG = orig.MTH_TM_ID
        ),

        FINAL AS (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                AVG(INDEXED_LTV_ROW) AS INDEXED_LTV_ACCT
            FROM ROW_LVL
            GROUP BY
                OBSN_DT,
                BASEL_ACCT_ID
        )

        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            INDEXED_LTV_ACCT
        FROM FINAL
    """,
):
    pass