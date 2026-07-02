import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq083_rundir():
    """Create sq083 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq083_rundir = f"{rundir}/sq083"
    os.makedirs(sq083_rundir, exist_ok=True)


@task.beeline(
    task_id="get_airb_mbr_src",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            transit,
            pplan,
            alphcurr,
            decum,
            ytd,
            date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as insrt_process_tmstmp,
            date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as updt_process_tmstmp
        from {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_mbr_src
        where businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq083",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="airb_mbr_src.parquet",
    schema=pa.schema([
        ("TRANSIT", pa.string()),
        ("PPLAN", pa.string()),
        ("ALPHCURR", pa.string()),
        ("DECUM", pa.float64()),
        ("YTD", pa.float64()),
        ("INSRT_PROCESS_TMSTMP", pa.string()),
        ("UPDT_PROCESS_TMSTMP", pa.string()),
    ]),
)
def get_airb_mbr_src():
    """Extract AIRB MBR source records for month-end."""
    pass


rundir_task = create_sq083_rundir()
extract_task = get_airb_mbr_src()

rundir_task >> extract_task
