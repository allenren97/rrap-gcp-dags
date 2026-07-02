import os
from datetime import timedelta
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ 
    'ingestion.AIRB_MORT_MTH_SNAPSHOT', #0
    'ingestion.TERANET_HOUSE_PRC_INDEX', #1
    'ingestion.PROVNCL_HOUSE_INDEX_SUM',#2
    'ingestion.TM_DIM', #3
    ]
DOWNSTREAM_ASSET = "features.INDEX_TERANETV"
DEPENDENCIES = {    
    'export_airb_current_mth_tm_id': ['transpose_queries'],
    'export_provncl_house_index_sum':['transpose_queries'],
    'export_house_price_index':['transpose_queries'],
    'transpose_queries': ['export_transpose_city_data','export_transpose_prov_data'],
    'export_transpose_city_data':['export_transpose_data'],
    'export_transpose_prov_data':['export_transpose_data'],
    'export_transpose_data':['export_house_index_jan_2013_calc'],
    'export_house_index_jan_2013_calc': ['export_cma_prop_prov'], 
    'export_cma_prop_prov': ['export_prop_city'],
    'export_prop_city':['export_made_date'],
    'export_made_date':['export_made_date_index','export_obs_date_index'],
    'export_made_date_index':['duckdb_clear_derive_index_teranetv'],
    'export_obs_date_index':['duckdb_clear_derive_index_teranetv'],
    'duckdb_clear_derive_index_teranetv':['duckdb_derive_index_teranetv'],
}

REFERENCES = [
    'reference.PROVINCE_REF',
    'reference.PROP_CITY_MAPPING', #this will be manually loaded with the parquet I created
    'features.HOUSE_INDEX_JAN_2013_CALC' #this will require a one time load from PROD for historical data
    ]


# #this is city data
def export_house_price_index(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        WITH CITY_BASE AS (
            SELECT 
            INDEX,
            TRIM(label_1) AS LABEL_1,
            TRIM(label_2) AS LABEL_2,
            CASE
            WHEN TRIM(label_1) = 'COMPOSITE' THEN CONCAT(TRIM(label_1), '_', TRIM(label_2))
            WHEN TRIM(label_2) = 'OTTAWA_GATINEAU' THEN 'Ottawa_t'
            ELSE CONCAT(TRIM(label_2), '_t')
            END AS CITY_CD,
            mth_tm_id
            FROM 
            {UPSTREAM_ASSET[1]}
        ),CITY_INDEX AS (
            SELECT * 
            FROM CITY_BASE 
            WHERE NOT (LABEL_1 = 'ON' AND LABEL_2 = 'OTTAWA_GATINEAU')
        ),CITY_DATA AS (
            SELECT 
            a.*, 
            b.tm_lvl_end_dt AS month_end_dt
            FROM CITY_INDEX a 
            left JOIN {UPSTREAM_ASSET[3]} b 
            ON a.mth_tm_id = b.tm_id 
            WHERE b.tm_lvl_end_dt >= CAST('2013-11-03' AS DATE)
            order by a.mth_tm_id
        ) select * from CITY_DATA
    """
):
    pass
# #this is provincial data
def export_provncl_house_index_sum(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT 
        a.mth_tm_id, 
        case 
        when a.prov_cd = 'ON' then 'ONT'
        ELSE a.prov_cd end as PROV_CD, 
        a.house_index_rto, 
        b.tm_lvl_end_dt AS month_end_dt
        FROM 
       {UPSTREAM_ASSET[2]} a LEFT JOIN {UPSTREAM_ASSET[3]} b 
        ON a.mth_tm_id = b.tm_id 
        WHERE b.tm_lvl_end_dt >= CAST('2013-11-03' AS DATE)
        order by a.mth_tm_id
    """
):
    pass


# #in order to update the Jan 2013 table, we need to transpose the current data from the teranet and the provincial sum tables
def transpose_queries():
    context = get_current_context()
    teranet = context['ti'].xcom_pull(task_ids=f'derived__index_teranetv.export_house_price_index', key='parquet')
    provncl = context['ti'].xcom_pull(task_ids=f'derived__index_teranetv.export_provncl_house_index_sum', key='parquet')
    con = duckdb.connect()
    teranet_rel = con.read_parquet(teranet)
    provncl_rel = con.read_parquet(provncl)
    tables = [(teranet_rel,'CITY_CD'),(provncl_rel,'PROV_CD')]
    for table,col in tables:
        arrow_table = table.fetch_arrow_table()
        col_list = sorted(set(str(val) for val in arrow_table.column(col).to_pylist())) 
        trans_col_sql = ", ".join(f"'{val}'" for val in col_list)
        context['ti'].xcom_push(key=f"TRANS_{col}", value=trans_col_sql)

def export_transpose_city_data(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT *
            FROM (
                SELECT 
                    mth_tm_id,
                    CITY_CD,
                    index,
                    month_end_dt
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_house_price_index", key="parquet")}}}}'
            )
            PIVOT (
                MAX(cast(index as decimal(18,4))) FOR CITY_CD IN ({{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.transpose_queries", key="TRANS_CITY_CD")}}}})
            )
    """
):
    pass

def export_transpose_prov_data(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT *
        FROM (
            SELECT 
                mth_tm_id,
                PROV_CD,
                house_index_rto,
                month_end_dt
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_provncl_house_index_sum", key="parquet")}}}}'
        )
        PIVOT (
            MAX(cast(house_index_rto as decimal(18,4))) FOR PROV_CD IN ({{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.transpose_queries", key="TRANS_PROV_CD")}}}})
        )
    """
):
    pass

def export_transpose_data(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        with curr_index as (
            SELECT *
            FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_transpose_prov_data", key="parquet")}}}}' a
            JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_transpose_city_data", key="parquet")}}}}' b 
            ON a.month_end_dt = b.month_end_dt
            ) 
            select 
            ONT
            ,strftime('%Y/%m', month_end_dt) AS DATE
            ,UPPER(strftime('%b', month_end_dt)) || substr(strftime('%Y', month_end_dt), 3, 2) as STATCAN_DATE
            ,NULL as COMPOSITE_6
            ,COMPOSITE_11
            ,VICTORIA_T
            ,VANCOUVER_T
            ,CALGARY_T
            ,EDMONTON_T
            ,WINNIPEG_T
            ,HAMILTON_T
            ,TORONTO_T
            ,OTTAWA_T
            ,MONTREAL_T
            ,QUEBEC_T
            ,HALIFAX_T
            ,AB,BC,NS,QC,MB
            ,UPPER(strftime('%b', month_end_dt)) || substr(strftime('%Y', month_end_dt), 3, 2) as TERA_DATE
            ,CAST(strftime('%Y%m', month_end_dt) AS DOUBLE) AS TERA_DATE1
            ,NULL as COMPOSITE_11_CALC
            from curr_index
    """
):
    pass

def export_house_index_jan_2013_calc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        with COMBO as (
            select * from  {REFERENCES[2]} where tera_date1 < 202412 -- this date will have to line up with the last day this table is updated
                union 
            select * from '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_transpose_data", key="parquet")}}}}' where tera_date1 >= 202412 -- this date will have to line up with the last day this table is updated
        ),curr_date AS (
            SELECT MAX(date) AS current_date
            FROM COMBO
        ),current_values AS (
            SELECT 
                date AS current_date, 
                statcan_date AS current_statcan_date, 
                tera_date AS current_tera_date, 
                tera_date1 AS current_tera_date1
            FROM COMBO
            WHERE date = (SELECT current_date FROM curr_date)
        ),ranked AS (
            SELECT 
                date, 
                ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
            FROM COMBO
        ),prev_date AS (
            SELECT 
                a.ONT,
                a.COMPOSITE_6,
                a.COMPOSITE_11,
                a.VICTORIA_T,
                a.VANCOUVER_T,
                a.CALGARY_T,
                a.EDMONTON_T,
                a.WINNIPEG_T,
                a.HAMILTON_T,
                a.TORONTO_T,
                a.OTTAWA_T,
                a.MONTREAL_T,
                a.QUEBEC_T,
                a.HALIFAX_T,
                a.AB,
                a.BC,
                a.NS,
                a.QC,
                a.MB,
                a.COMPOSITE_11_CALC
            FROM COMBO a
            JOIN ranked b ON a.date = b.date
            WHERE b.rn = 2
        ),updated_curr_row AS (
            SELECT 
                p.ONT,
                c.current_date AS DATE,
                c.current_statcan_date AS STATCAN_DATE,
                p.COMPOSITE_6,
                p.COMPOSITE_11,
                p.VICTORIA_T,
                p.VANCOUVER_T,
                p.CALGARY_T,
                p.EDMONTON_T,
                p.WINNIPEG_T,
                p.HAMILTON_T,
                p.TORONTO_T,
                p.OTTAWA_T,
                p.MONTREAL_T,
                p.QUEBEC_T,
                p.HALIFAX_T,
                p.AB,
                p.BC,
                p.NS,
                p.QC,
                p.MB,
                c.current_tera_date AS TERA_DATE,
                c.current_tera_date1 AS TERA_DATE1,
                p.COMPOSITE_11_CALC
            FROM prev_date p
            CROSS JOIN current_values c
        )
            SELECT * 
            FROM updated_curr_row
            UNION ALL
            SELECT * 
            FROM COMBO b
            WHERE b.TERA_DATE1 < (SELECT current_tera_date1 FROM current_values)
            ORDER BY DATE DESC
    """
):
    pass  


def export_airb_current_mth_tm_id(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
            LEND_VALUE,
            MADE_DT,
            INTR_ADJ_DT,
            PROP_PROV,
            PROPERTY_ADDR_1,
            PROPERTY_ADDR_2,
            PROPERTY_ADDR_3,
            MORT_NUM,
            MTH_END_DT
            from {UPSTREAM_ASSET[0]}
            where tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    """
):
    pass


def export_cma_prop_prov(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
     SELECT 
        MORT_NUM,
        LEND_VALUE, 
        PROPERTY_ADDR_3 AS ADDRESS, 
        REGEXP_EXTRACT(PROPERTY_ADDR_3, '^(.*\s+)(\S+)$', 1) AS CITY, 
        UPPER(REGEXP_EXTRACT(PROPERTY_ADDR_3, '^(.*\s+)(\S+)$', 2)) AS PROVINCE,
        PROP_PROV , 
        PROVINCE_ID ,
        CASE
        WHEN PROP_PROV IS NULL AND PROVINCE_ID IS NULL AND PROVINCE != '' THEN PROVINCE
        ELSE 
        PROVINCE_CD END AS PROP_PROV_CMA
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_airb_current_mth_tm_id", key="parquet")}}}}'
        left join 
        {REFERENCES[0]}
        on TRY_CAST(PROP_PROV AS INT32) = TRY_CAST(PROVINCE_ID AS INT32)

    """
):
    pass


def export_prop_city(
    duckdb_conn_id='duckdb-conn',
    sql=f"""      
    WITH src AS (
    SELECT
        MORT_NUM,
        LEND_VALUE,
        city,
        address,
        province,
        PROP_PROV_CMA,
        lower(city) AS norm_city,
        lower(PROP_PROV_CMA) AS norm_prov
    FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_cma_prop_prov", key="parquet")}}}}'
    ),
    -- Exact mapping by (prov, address/city)
    exact AS (
        SELECT
            lower(prov)    AS prov,
            lower(address) AS address,
            MIN(prop_city) AS prop_city
        FROM {REFERENCES[1]}
        GROUP BY 1, 2
    ),

    -- Province-level default fallback
    prov_map AS (
        SELECT
            lower(prov)    AS prov,
            MIN(prop_city) AS prop_city
        FROM {REFERENCES[1]}
        WHERE address = '__default__'
        GROUP BY 1
    )

    SELECT
        s.MORT_NUM,
        s.LEND_VALUE,
        s.city,
        s.address,
        s.province,
        s.PROP_PROV_CMA,
        -- choose best available mapping 
        COALESCE(e.prop_city, f.prop_city, p.prop_city) AS Prop_City
    FROM src s

    -- exact match first
    LEFT JOIN exact e
    ON s.norm_city = e.address
    AND s.norm_prov = e.prov

    -- fuzzy: pick ONE "best" match per row via lateral subquery
    LEFT JOIN LATERAL (
        SELECT
            trim(m.prop_city) AS prop_city
        FROM {REFERENCES[1]} m
        WHERE lower(m.prov) = s.norm_prov
        AND m.address IS NOT NULL
        AND m.address <> '__default__'
        AND s.norm_city LIKE '%' || lower(m.address) || '%'
        -- Prefer the most specific (longest) fuzzy address, then prop_city as tie-breaker
        ORDER BY length(m.address) DESC, m.prop_city
        LIMIT 1
    ) AS f ON TRUE

    -- default fallback by province
    LEFT JOIN prov_map p
    ON s.norm_prov = p.prov

    -- keep ONE row per MORT_NUM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MORT_NUM
        ORDER BY
            CASE
                WHEN e.prop_city IS NOT NULL THEN 1
                WHEN f.prop_city IS NOT NULL THEN 2
                WHEN p.prop_city IS NOT NULL THEN 3
                ELSE 4
            END,
            COALESCE(e.prop_city, f.prop_city, p.prop_city)
    ) = 1
    """
):
    pass

def export_made_date(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        WITH base AS (
            SELECT
            CAST (MADE_DT AS DATE) AS MADE_DT,
            COALESCE(MTH_END_DT,(LAST_DAY(MTH_END_DT))) AS PROCESS_DT,
            MTH_END_DT,
            INTR_ADJ_DT,
            MORT_NUM,
            FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_airb_current_mth_tm_id", key="parquet")}}}}'
        ), MADE_DT_base AS (
            SELECT
            MORT_NUM,
            MTH_END_DT,
            MADE_DT AS org_MADE_DT,
            INTR_ADJ_DT,
            PROCESS_DT
            FROM base
        ),MADE_DATE as (
            SELECT
            MORT_NUM,
            MTH_END_DT, 
            CASE
            WHEN org_MADE_DT IS NULL THEN INTR_ADJ_DT
            WHEN org_MADE_DT > PROCESS_DT THEN INTR_ADJ_DT
            ELSE org_MADE_DT
            END AS MADE_DT,
            strftime('%Y/%m', org_MADE_DT) AS DATE, 
            PROCESS_DT,
            INTR_ADJ_DT
            from MADE_DT_base
        )
            select MORT_NUM,  
            MADE_DT,
            CASE
            WHEN MADE_DT < '1990-07-01' THEN 'JUL90'
            ELSE UPPER(strftime('%b', MADE_DT)) || substr(strftime('%Y', MADE_DT), 3, 2) END 
            AS MADE_DATE,
            PROCESS_DT,
            UPPER(strftime('%b', PROCESS_DT)) || substr(strftime('%Y', PROCESS_DT), 3, 2) as PROCESS_DATE,
            PROCESS_DT - INTERVAL 1 MONTH AS PREV_MTH_PROCESS_DT,
            UPPER(strftime('%b', PREV_MTH_PROCESS_DT)) || substr(strftime('%Y', PREV_MTH_PROCESS_DT), 3, 2) as PREV_PROCESS_DATE,
            strftime('%Y/%m', MADE_DT) AS DATE
            from MADE_DATE
        """
):
    pass

def export_made_date_index(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        WITH base as (
            select MORT_NUM, 
            MADE_DT, 
            MADE_DATE,
            DATE 
            from '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_made_date", key="parquet")}}}}'
            ) 
            SELECT *
            FROM base a
            LEFT JOIN 
            '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_house_index_jan_2013_calc", key="parquet")}}}}' b
            ON b.TERA_DATE = a.MADE_DATE

    """
):
    pass

def export_obs_date_index(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        WITH base as (
                select mort_num,
                COALESCE(MTH_END_DT,(LAST_DAY(MTH_END_DT))) AS PROCESS_DT
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_airb_current_mth_tm_id", key="parquet")}}}}'
        ), obs_date as (
                select mort_num,
                strftime('%Y/%m', PROCESS_DT) AS DATE,
                UPPER(strftime('%b', PROCESS_DT - INTERVAL 1 MONTH)) || substr(strftime('%Y', PROCESS_DT - INTERVAL 1 MONTH), 3, 2) as PREV_TERA_DATE,
                UPPER(strftime('%b', PROCESS_DT)) || substr(strftime('%Y', PROCESS_DT), 3, 2) as TERA_DATE
                from base
        ) 
        SELECT *
                FROM obs_date a
            LEFT JOIN 
                '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_house_index_jan_2013_calc", key="parquet")}}}}' b
            ON  
            a.TERA_DATE = b.TERA_DATE

    """
):
    pass

def duckdb_clear_derive_index_teranetv(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_index_teranetv(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    WITH OBS AS(
        select a.MORT_NUM,
        LEND_VALUE,
        Prop_City,
        CASE 
            WHEN Prop_City = 'Calgary' THEN Calgary_t 
            WHEN Prop_City = 'Edmonton' THEN Edmonton_t 
            WHEN Prop_City = 'Halifax' THEN Halifax_t 
            WHEN Prop_City = 'Hamilton' THEN Hamilton_t 
            WHEN Prop_City = 'Montreal' THEN Montreal_t 
            WHEN Prop_City = 'Ottawa - Gatineau' THEN Ottawa_t 
            WHEN Prop_City = 'Quebec' THEN Quebec_t 
            WHEN Prop_City = 'Toronto' THEN Toronto_t 
            WHEN Prop_City = 'Vancouver' THEN Vancouver_t 
            WHEN Prop_City = 'Victoria' THEN Victoria_t 
            WHEN Prop_City = 'Winnipeg' THEN Winnipeg_t 
            WHEN Prop_City = 'AB - other' THEN AB 
            WHEN Prop_City = 'BC - other' THEN BC 
            WHEN Prop_City = 'ON - other' THEN ONT 
            WHEN Prop_City = 'NS - other' THEN NS 
            WHEN Prop_City = 'QC - other' THEN QC 
            WHEN Prop_City = 'MB - other' THEN MB
            WHEN Prop_City is NULL THEN composite_11
        END AS OBS_HPI,
        composite_11 as COMPOSITE_11_OBS
        from '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_prop_city", key="parquet")}}}}' a
        left join 
        '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_obs_date_index", key="parquet")}}}}' b
        on a.mort_num = b.mort_num
    ), MADE_DT AS (
        select A.MORT_NUM,
        LEND_VALUE,
        CASE 
            WHEN Prop_City = 'Calgary' THEN Calgary_t 
            WHEN Prop_City = 'Edmonton' THEN Edmonton_t
            WHEN Prop_City = 'Halifax' THEN Halifax_t
            WHEN Prop_City = 'Hamilton' THEN Hamilton_t 
            WHEN Prop_City = 'Montreal' THEN Montreal_t 
            WHEN Prop_City = 'Ottawa - Gatineau' THEN Ottawa_t
            WHEN Prop_City = 'Quebec' THEN Quebec_t
            WHEN Prop_City = 'Toronto' THEN Toronto_t
            WHEN Prop_City = 'Vancouver' THEN Vancouver_t 
            WHEN Prop_City = 'Victoria' THEN Victoria_t
            WHEN Prop_City = 'Winnipeg' THEN Winnipeg_t
            WHEN Prop_City = 'AB - other' THEN AB
            WHEN Prop_City = 'BC - other' THEN BC
            WHEN Prop_City = 'ON - other' THEN ONT
            WHEN Prop_City = 'NS - other' THEN NS
            WHEN Prop_City = 'QC - other' THEN QC
            WHEN Prop_City = 'MB - other' THEN MB
            WHEN Prop_City is NULL THEN COMPOSITE_11_CALC
        END AS MADE_DT_HPI,
        composite_11,
        COMPOSITE_11_CALC
        from '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_prop_city", key="parquet")}}}}' a
        LEFT JOIN
        '{{{{ task_instance.xcom_pull(task_ids="derived__index_teranetv.export_made_date_index", key="parquet")}}}}' b
        ON 
        a.MORT_NUM = b.MORT_NUM
    ), ITV AS ( 
        select A.MORT_NUM, 
        A.LEND_VALUE, 
        OBS_HPI, 
        MADE_DT_HPI,
        CASE
        WHEN A.Prop_City IS NULL THEN A.LEND_VALUE * (COMPOSITE_11_OBS/COMPOSITE_11)
        WHEN MADE_DT_HPI = 0 THEN A.LEND_VALUE * (COMPOSITE_11_OBS/COMPOSITE_11_CALC)
        ELSE A.LEND_VALUE * (OBS_HPI/MADE_DT_HPI) 
        END AS INDEX_TERANETV,
        A.Prop_City
        FROM OBS A JOIN MADE_DT B ON A.MORT_NUM = B.MORT_NUM
    ) 
        select 
         MORT_NUM, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        CASE 
        WHEN INDEX_TERANETV ='inf' THEN LEND_VALUE * (OBS_HPI/MADE_DT_HPI)
        ELSE INDEX_TERANETV
        END AS INDEX_TERANETV
        from ITV
    """
):
    pass

