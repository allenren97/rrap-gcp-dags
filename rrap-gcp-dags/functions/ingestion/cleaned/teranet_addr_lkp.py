import os

from datetime import timedelta, datetime
import pendulum
import unicodedata
import re
import logging

import pyarrow as pa
import pyarrow.parquet as pq


from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras
from bns.rrap.hooks.duckdb import DuckLakeHook


UPSTREAM_ASSET = [ 'ingestion.METRPL_CITY_LKP' ]
DOWNSTREAM_ASSET = "ingestion.TERANET_ADDR_LKP"
DEPENDENCIES = {
    'duckdb_update_records_in_ducklake':['duckdb_load_new_records_into_ducklake'],
}

SESSIONTIME = pendulum.now().format("YYYY-MM-DD HH:mm:ss")


def duckdb_load_new_records_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET}
    with new_metrpl_city_lkp as (
        SELECT 
            lower(trim(translate(prov,'àâäçéèêëîïôöûü', 'aaaceeeiiouu'))) as prov
            ,prov as LOCTN_LABEL_1
            ,lower(trim(translate(metrpl_area_nm,'àâäçéèêëîïôöûü', 'aaaceeeiiouu'))) as cma_new
            ,metrpl_area_nm as LOCTN_LABEL_2
            ,lower(trim(translate(city_nm,'àâäçéèêëîïôöûü', 'aaaceeeiiouu'))) as city
            ,city_nm
            ,LOCTN_LABEL_2 as LOCTN_LABEL_2_new
        FROM {UPSTREAM_ASSET[0]}
        where {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} >= CAST(EFF_FROM_YR_MTH AS INT) 
        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} < CAST(EFF_TO_YR_MTH AS INT)
        ORDER BY prov, metrpl_area_nm, city_nm
    ),
        existing_teranet_addr_lkp as (
        SELECT
            lower(trim(translate(LOCTN_LABEL_1,'àâäçéèêëîïôöûü', 'aaaceeeiiouu'))) as prov
            ,lower(trim(translate(LOCTN_LABEL_2,'àâäçéèêëîïôöûü', 'aaaceeeiiouu')))as cma_existing
            ,lower(trim(translate(PRPTY_LOCTN_NM, 'àâäçéèêëîïôöûü', 'aaaceeeiiouu')))as city
        FROM {DOWNSTREAM_ASSET}
        WHERE {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} >= CAST(EFF_FROM_YR_MTH AS INT) 
        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} < CAST(EFF_TO_YR_MTH AS INT)
        ORDER BY prov, city
    ) 
        SELECT 
        b.city_nm as PRPTY_LOCTN_NM
        ,'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' AS EFF_FROM_YR_MTH  
        , '999912' AS EFF_TO_YR_MTH 
        ,b.LOCTN_LABEL_1 
        ,LOCTN_LABEL_2_new AS LOCTN_LABEL_2 
        , 'Y' AS CRNT_F 
        ,'{SESSIONTIME}' AS INSRT_PROCESS_TMSTMP  
        ,'{SESSIONTIME}' AS UPDT_PROCESS_TMSTMP
        from existing_teranet_addr_lkp a right join new_metrpl_city_lkp b 
        on a.prov = b.prov 
        and a.city=b.city 
        WHERE a.cma_existing IS NULL
    """

):pass

def duckdb_update_records_in_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH translate_metrpl_city_lkp AS (
        SELECT 
            LOWER(TRIM(TRANSLATE(prov,'àâäçéèêëîïôöûü','aaaceeeiiouu'))) AS prov
            ,LOWER(TRIM(TRANSLATE(metrpl_area_nm,'àâäçéèêëîïôöûü','aaaceeeiiouu'))) AS cma_new
            ,metrpl_area_nm AS LOCTN_LABEL_2
            ,LOWER(TRIM(TRANSLATE(city_nm,'àâäçéèêëîïôöûü','aaaceeeiiouu'))) AS city
            ,LOCTN_LABEL_2 AS LOCTN_LABEL_2_new
        FROM {UPSTREAM_ASSET[0]}
        WHERE {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} >= CAST(EFF_FROM_YR_MTH AS INT) AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} < CAST(EFF_TO_YR_MTH AS INT)
    ),
    existing_teranet_addr_lkp AS (
        SELECT
            LOWER(TRIM(TRANSLATE(LOCTN_LABEL_1,'àâäçéèêëîïôöûü','aaaceeeiiouu'))) AS prov
            ,LOWER(TRIM(TRANSLATE(LOCTN_LABEL_2,'àâäçéèêëîïôöûü','aaaceeeiiouu'))) AS cma_existing
            ,LOWER(TRIM(TRANSLATE(PRPTY_LOCTN_NM,'àâäçéèêëîïôöûü','aaaceeeiiouu'))) AS city
            ,PRPTY_LOCTN_NM
            ,LOCTN_LABEL_1
            ,LOCTN_LABEL_2
        FROM {DOWNSTREAM_ASSET}
        WHERE {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} >= CAST(EFF_FROM_YR_MTH AS INT) 
        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}} < CAST(EFF_TO_YR_MTH AS INT)
    ), close as (
    SELECT 
        a.PRPTY_LOCTN_NM
        ,a.LOCTN_LABEL_1
        ,a.LOCTN_LABEL_2
    FROM existing_teranet_addr_lkp a
    LEFT JOIN translate_metrpl_city_lkp b
    ON a.prov = b.prov AND a.city = b.city
    WHERE a.cma_existing != b.cma_new)

    UPDATE {DOWNSTREAM_ASSET} AS AA
    SET EFF_TO_YR_MTH = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}
    ,CRNT_F = 'N'  
    ,UPDT_PROCESS_TMSTMP = '{SESSIONTIME}'
    WHERE EXISTS (
        SELECT 1
        FROM close
        WHERE PRPTY_LOCTN_NM = AA.PRPTY_LOCTN_NM
        AND LOCTN_LABEL_1 = AA.LOCTN_LABEL_1
        AND LOCTN_LABEL_2 = AA.LOCTN_LABEL_2
    )
        """
):pass

