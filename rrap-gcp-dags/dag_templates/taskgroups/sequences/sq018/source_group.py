import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq018_rundir():
    """Create sq018 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq018_rundir = f"{rundir}/sq018"
    os.makedirs(sq018_rundir, exist_ok=True)


@task.beeline(
    task_id="get_tng_acct_writeoff_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            mth_end_dt as month_end_dt,
            mort_num as mtg_num,
            trim(mort_provider_desc) as provider,
            insur_type_desc as insurance_type,
            wof_dt as writeoff_date,
            abs(wof_amt) as writeoff_amt,
            insurer_desc,
            first_deflt_dt as last_default_dt,
            fraud_ind
        FROM {{ var.value.RCRR_SCHEMA }}.TNG_ACCT_WOF_SNAPSHOT
        WHERE src_sys_cd = 'TNG_MTG'
        AND mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq018",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="tng_acct_writeoff_snapshot.parquet",
    schema=pa.schema([
        ('month_end_dt', pa.date64()),
        ('mtg_num', pa.string()),
        ('provider', pa.string()),
        ('insurance_type', pa.string()),
        ('writeoff_date', pa.date64()),
        ('writeoff_amt', pa.float64()),
        ('insurer_desc', pa.string()),
        ('last_default_dt', pa.date64()),
        ('fraud_ind', pa.string()),
    ]),
)
def get_tng_acct_writeoff_snapshot():
    """
    Extract Tangerine account writeoff snapshot.
    
    Extracts writeoff information including month-end date, mortgage number,
    provider, insurance type, writeoff date/amount, insurer description,
    last default date, and fraud indicator from TNG_ACCT_WOF_SNAPSHOT for
    Tangerine mortgage accounts (src_sys_cd = 'TNG_MTG') on the current month-end.
    """
    pass


"""Source layer for sq018."""
rundir_task = create_sq018_rundir()
extract_task = get_tng_acct_writeoff_snapshot()

rundir_task >> extract_task
