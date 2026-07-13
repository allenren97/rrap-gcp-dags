import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.AIRB_MORT_MTH_SNAPSHOT", "ingestion.UNEMP_RATE"]
DOWNSTREAM_ASSET = "features.URATE"
DEPENDENCIES = {
    "duckdb_clear_derive_urate": ["duckdb_derive_urate"],
}


def duckdb_clear_derive_urate(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_derive_urate(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    WITH AIRB_MORT_MTH_SNAPSHOT AS (
        SELECT
            MORT_NUM,
            PROP_PROV,
            UPPER(REGEXP_EXTRACT(PROPERTY_ADDR_3, '^(.*\s+)(\S+)$', 2)) AS PROVINCE, 
            CASE 
                WHEN PROP_PROV  = '01' THEN 'NS'
                WHEN PROP_PROV  = '02' THEN 'NB'
                WHEN PROP_PROV  = '03' THEN 'PE'
                WHEN PROP_PROV  = '04' THEN 'QC'
                WHEN PROP_PROV  = '05' THEN 'ON'
                WHEN PROP_PROV  = '06' THEN 'MB'
                WHEN PROP_PROV  = '07' THEN 'SK'
                WHEN PROP_PROV  = '08' THEN 'AB'
                WHEN PROP_PROV  = '09' THEN 'BC'
                WHEN PROP_PROV  = '10' THEN 'NT'
                WHEN PROP_PROV  = '11' THEN 'YT'
                WHEN PROP_PROV  = '12' THEN 'NU'
                WHEN PROP_PROV  = '00' THEN 'NL'
                WHEN PROP_PROV = '' THEN PROVINCE
            END
            AS PROP_PROV_CMA
            from {UPSTREAM_ASSET[0]}
            where tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}'
    )
        select 
            MORT_NUM, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            URATE
            FROM
            AIRB_MORT_MTH_SNAPSHOT a
            LEFT JOIN
            {UPSTREAM_ASSET[1]} b
            ON 
            a.PROP_PROV_CMA = b.PROVINCE and b.time_key='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
    """,
):
    pass
