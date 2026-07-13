import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq015_rundir():
    """Create sq015 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq015_rundir = f"{rundir}/sq015"
    os.makedirs(sq015_rundir, exist_ok=True)


@task.beeline(
    task_id="get_rcrr_tnif_hierarchy",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT
            LPAD(TRANSIT, 5, '0') as TRNST_NUM,
            TRANSIT_NM as ORG_UNIT_NM,
            CITY_NM as CITY,
            TRANSIT_OPEN_DT as OPN_DT,
            TRANSIT_CLS_DT as CLS_DT,
            TEL_NUM as PH_NUM,
            POSTAL_ZIP_CD,
            PROVINCE_COUNTRY_NM as PROV_STATE,
            STRT_NM as STRT_ADDR
        FROM {{ var.value.RCRR_SCHEMA }}.tnif_hierarchy
        WHERE crnt_f = 'Y';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="rcrr_tnif_hierarchy.parquet",
    schema=pa.schema([
        ('TRNST_NUM', pa.string()),
        ('ORG_UNIT_NM', pa.string()),
        ('CITY', pa.string()),
        ('OPN_DT', pa.date64()),
        ('CLS_DT', pa.date64()),
        ('PH_NUM', pa.string()),
        ('POSTAL_ZIP_CD', pa.string()),
        ('PROV_STATE', pa.string()),
        ('STRT_ADDR', pa.string())
    ]),
)
def get_rcrr_tnif_hierarchy():
    """
    Extract organizational unit dimension data.
    
    Extracts transit/organizational unit hierarchy information including
    transit number, name, city, open/close dates, phone number, postal code,
    province/state, and street address from RCRR_TNIF_HIERARCHY for current
    records (crnt_f = 'Y').
    """
    pass


"""Source layer for sq015."""
rundir_task = create_sq015_rundir()
extract_task = get_rcrr_tnif_hierarchy()

rundir_task >> extract_task
