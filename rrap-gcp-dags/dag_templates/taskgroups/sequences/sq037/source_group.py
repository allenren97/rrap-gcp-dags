import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq037_rundir():
    """Create sq037 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq037_rundir = f"{rundir}/sq037"
    os.makedirs(sq037_rundir, exist_ok=True)


@task.beeline(
    task_id="get_airb_statcan_unemplymnt_rt_ext",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            BUSINESSEFFECTIVEDATE AS TIME_KEY,
            (CASE
                WHEN TRIM(GEO) = 'Newfoundland and Labrador' THEN 'NF'
                WHEN TRIM(GEO) = 'Prince Edward Island' THEN 'PE'
                WHEN TRIM(GEO) = 'Nova Scotia' THEN 'NS'
                WHEN TRIM(GEO) = 'New Brunswick' THEN 'NB'
                WHEN TRIM(GEO) = 'Quebec' THEN 'QC'
                WHEN TRIM(GEO) = 'Ontario' THEN 'ON'
                WHEN TRIM(GEO) = 'Manitoba' THEN 'MB'
                WHEN TRIM(GEO) = 'Saskatchewan' THEN 'SK'
                WHEN TRIM(GEO) = 'Alberta' THEN 'AB'
                WHEN TRIM(GEO) = 'British Columbia' THEN 'BC'
                ELSE '99'
            END) AS PROVINCE,
            (CAST(VAL AS DECIMAL(10,3)) / 100) AS URATE,
            (
                select val / 100
                FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_unemplymnt_rt_ext
                WHERE businesseffectivedate ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                AND trim(geo) = 'Canada'
            ) AS CAN_URATE
        FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_unemplymnt_rt_ext
        WHERE businesseffectivedate ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
        AND trim(geo) <> 'Canada';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq037",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="get_airb_statcan_unemplymnt_rt_ext.parquet",
    schema=pa.schema([
        ('TIME_KEY', pa.date64()),
        ('PROVINCE', pa.string()),
        ('URATE', pa.float64()),
        ('CAN_URATE', pa.float64())
    ]),
)
def get_airb_statcan_unemplymnt_rt_ext():
    """
    Extract Statistics Canada unemployment rate data.
    
    Extracts unemployment rate data with province mappings and Canada-wide
    benchmark rate. Transforms geography to 2-letter province codes, converts
    rates from basis points (divided by 100), and filters out Canada-level records.
    """
    pass


@task.parquet(
    task_id="get_unemp_ratio",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select *, 
        (CAST((URATE - CAN_URATE) / CAN_URATE AS DECIMAL(12,9))) AS RATIO
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq037/get_airb_statcan_unemplymnt_rt_ext.parquet'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq037/get_unemp_ratio.parquet",
)
def get_unemp_ratio():
    """
    Compute unemployment rate ratio.
    
    Calculates provincial vs. Canada unemployment rate differential as a ratio:
    (URATE - CAN_URATE) / CAN_URATE.
    """
    pass


rundir_task = create_sq037_rundir()
extract_task = get_airb_statcan_unemplymnt_rt_ext()
transform_task = get_unemp_ratio()

rundir_task >> extract_task >> transform_task
