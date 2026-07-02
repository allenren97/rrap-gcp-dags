from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",
    "ingestion.TM_DIM",
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.MAX_LEND_VALUE",
]
DOWNSTREAM_ASSET = "features.STEP_INDEXED_APRSD_VAL_ACCT"
DEPENDENCIES = {
    "duckdb_delete_step_indexed_aprsd_val_acct": [
        "duckdb_load_step_indexed_aprsd_val_acct"
    ]
}


def duckdb_delete_step_indexed_aprsd_val_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load_step_indexed_aprsd_val_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME (
        WITH
            Mapped AS (
                SELECT
                    MTH_TM_ID,
                    MORT_AUTH_DT,
                    PRIM_BASEL_CUST_ID,
                    LND_VAL,
                    PROPERTY_ADDR_3,
                    CAST(NULLIF(TRIM(STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM
                FROM
                    ingestion.MORT_MTH_SNAPSHOT
                WHERE
                    UPPER(TRIM(COMM_TP)) = 'RESIDENTIAL'
                    AND CRNT_BAL_AMT > 0
                    AND TRIM(PD_OFF_F) = 'N'
                    AND MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL
                    AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
            ),
            City_Std AS (
                SELECT
                    m.STEP_PLN_AGRMNT_NUM,
                    m.PRIM_BASEL_CUST_ID,
                    m.MTH_TM_ID,
                    m.MORT_AUTH_DT,
                    m.LND_VAL,
                    (
                        TRIM(array_slice (TRIM(m.PROPERTY_ADDR_3), 0, -3))
                    ) AS CITY_RAW,
                    CASE
                        WHEN UPPER(CITY_RAW) LIKE '%OTTAWA%'
                        OR UPPER(CITY_RAW) LIKE '%GATINEAU%' THEN 'OTTAWA_GATINEAU'
                        WHEN UPPER(CITY_RAW) LIKE '%SUDBURY%' THEN 'Sudbury'
                        WHEN UPPER(CITY_RAW) LIKE '%KITCHENER%'
                        OR UPPER(CITY_RAW) LIKE '%CAMBRIDGE%'
                        OR UPPER(CITY_RAW) LIKE '%WATERLOO%' THEN 'Kitchener - Cambridge - Waterloo'
                        WHEN UPPER(CITY_RAW) LIKE '%VANCOUVER%' THEN 'Vancouver'
                        WHEN UPPER(CITY_RAW) LIKE '%VICTORIA%' THEN 'Victoria'
                        WHEN UPPER(CITY_RAW) LIKE '%HALIFAX%' THEN 'Halifax'
                        WHEN UPPER(CITY_RAW) LIKE '%KINGSTON%' THEN 'Kingston'
                        WHEN UPPER(CITY_RAW) LIKE '%HAMILTON%' THEN 'Hamilton'
                        WHEN UPPER(CITY_RAW) LIKE '%OSHAWA%' THEN 'Oshawa'
                        WHEN UPPER(CITY_RAW) LIKE '%BRANTFORD%' THEN 'Brantford'
                        WHEN UPPER(CITY_RAW) LIKE '%GUELPH%' THEN 'Guelph'
                        WHEN UPPER(CITY_RAW) LIKE '%BELLEVILLE%' THEN 'Belleville'
                        WHEN UPPER(CITY_RAW) LIKE '%WINNIPEG%' THEN 'Winnipeg'
                        WHEN UPPER(CITY_RAW) LIKE '%MONCTON%' THEN 'Moncton'
                        WHEN UPPER(CITY_RAW) LIKE '%SAINT JOHN%' THEN 'Saint John'
                        WHEN UPPER(CITY_RAW) LIKE '%BARRIE%' THEN 'Barrie'
                        WHEN UPPER(CITY_RAW) LIKE '%LONDON%' THEN 'London'
                        WHEN UPPER(CITY_RAW) LIKE '%ST JOHNS%' THEN 'St. Johns'
                        WHEN UPPER(CITY_RAW) LIKE '%TROIS-RIVIERES%'
                        OR UPPER(CITY_RAW) LIKE '%RIVIERES%'
                        OR UPPER(CITY_RAW) LIKE '%TROIS%' THEN 'Trois-Rivieres'
                        WHEN UPPER(CITY_RAW) LIKE '%SHERBROOKE%' THEN 'Sherbrooke'
                        WHEN UPPER(CITY_RAW) LIKE '%CALGARY%' THEN 'Calgary'
                        WHEN UPPER(CITY_RAW) LIKE '%QUEBEC%' THEN 'Quebec'
                        WHEN UPPER(CITY_RAW) LIKE '%EDMONTON%' THEN 'Edmonton'
                        WHEN UPPER(CITY_RAW) LIKE '%KELOWNA%' THEN 'Kelowna'
                        WHEN UPPER(CITY_RAW) LIKE '%WINDSOR%' THEN 'Windsor'
                        WHEN UPPER(CITY_RAW) LIKE '%LETHBRIDGE%' THEN 'Lethbridge'
                        WHEN UPPER(CITY_RAW) LIKE '%MONTREAL%' THEN 'Montreal'
                        WHEN UPPER(CITY_RAW) LIKE '%PETERBOROUGH%' THEN 'Peterborough'
                        WHEN UPPER(CITY_RAW) LIKE '%ABBOTSFORD%'
                        OR UPPER(CITY_RAW) LIKE '%ABBOTSFORD-MISSION%'
                        OR UPPER(CITY_RAW) LIKE '%MISSION%' THEN 'Abbotsford - Mission'
                        WHEN UPPER(CITY_RAW) LIKE '%THUNDER BAY%'
                        OR UPPER(CITY_RAW) LIKE '%THUNDER-BAY%'
                        OR UPPER(CITY_RAW) LIKE '%THUNDER%' THEN 'Thunder Bay'
                        WHEN UPPER(CITY_RAW) LIKE '%ST CATHARINES%'
                        OR UPPER(CITY_RAW) LIKE '%NIAGARA%'
                        OR UPPER(CITY_RAW) LIKE '%ST CATHARINES-NIAGARA%' THEN 'St. Catharines - Niagara'
                        WHEN UPPER(CITY_RAW) LIKE '%TORONTO%' THEN 'Toronto'
                        ELSE '11'
                    END AS CMA_LABEL_2
                FROM
                    Mapped m
            ),
            Made_dt_mapping AS (
                SELECT
                    MAX(teranet.INDEX) AS ORIG_INDEX,
                    teranet.MTH_TM_ID,
                    TRIM(teranet.LABEL_2) AS LABEL_2,
                    dim.TM_LVL_END_DT
                FROM
                    ingestion.TERANET_HOUSE_PRC_INDEX_CMA teranet
                    LEFT JOIN ingestion.TM_DIM dim ON dim.TM_ID = teranet.MTH_TM_ID
                GROUP BY
                    TRIM(teranet.LABEL_2),
                    MTH_TM_ID,
                    dim.TM_LVL_END_DT
            ),
            Teranet_Joined AS (
                SELECT
                    c.STEP_PLN_AGRMNT_NUM,
                    lend.MAX_LEND_VALUE,
                    obs.INDEX AS OBS_INDEX,
                    orig.ORIG_INDEX,
                    CMA_LABEL_2
                FROM
                    City_Std c
                    INNER JOIN features.MAX_LEND_VALUE lend 
                    ON 
                    c.STEP_PLN_AGRMNT_NUM = CAST(
                        NULLIF(TRIM(lend.STEP_PLN_AGRMNT_NUM), '') AS BIGINT
                    )
                    --AND lend.MORT_AUTH_DT = c.MORT_AUTH_DT
                    AND lend.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX_CMA obs ON obs.MTH_TM_ID = c.MTH_TM_ID
                    AND TRIM(obs.LABEL_2) = TRIM(c.CMA_LABEL_2)
                    LEFT JOIN Made_dt_mapping orig ON orig.TM_LVL_END_DT = LAST_DAY (lend.MORT_AUTH_DT)
                    AND TRIM(orig.LABEL_2) = TRIM(c.CMA_LABEL_2)
            ),
            index_aprsd AS (
                SELECT
                    STEP_PLN_AGRMNT_NUM,
                    CASE
                        WHEN ORIG_INDEX IS NULL
                        OR ORIG_INDEX = 0 THEN NULL
                        ELSE MAX_LEND_VALUE * (OBS_INDEX / ORIG_INDEX)
                    END AS STEP_INDEXED_APRSD_VAL_ACCT
                FROM
                    Teranet_Joined
                WHERE
                    STEP_PLN_AGRMNT_NUM IS NOT NULL
            )
        SELECT
            STEP_PLN_AGRMNT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            MAX(STEP_INDEXED_APRSD_VAL_ACCT) AS STEP_INDEXED_APRSD_VAL_ACCT
        FROM
            index_aprsd
        GROUP BY
            STEP_PLN_AGRMNT_NUM

        )
    """,
):
    pass
