import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq016_rundir():
    """Create sq016 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq016_rundir = f"{rundir}/sq016"
    os.makedirs(sq016_rundir, exist_ok=True)


@task.beeline(
    task_id="get_rcrr_psnl_loan_subv_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            acct_num,
            orig_cab_transit,
            loan_num,
            application_num,
            subvention_ind,
            region,
            mth_end_dt
        FROM {{ var.value.RCRR_SCHEMA }}.ALSCOM_LOAN_SUBV_MTH_SNAPSHOT
        WHERE mth_end_dt ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
        AND subvention_ind = 'Y';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="rcrr_psnl_loan_subv_mth_snapshot.parquet",
    schema=pa.schema([
        ('acct_num', pa.string()),
        ('orig_cab_transit', pa.string()),
        ('loan_num', pa.string()),
        ('application_num', pa.string()),
        ('subvention_ind', pa.string()),
        ('region', pa.string()),
        ('mth_end_dt', pa.date64()),
    ]),
)
def get_rcrr_psnl_loan_subv_mth_snapshot():
    """
    Extract personal loan subvention month-end snapshot.
    
    Extracts account-level information from RCRR's ALSCOM_LOAN_SUBV_MTH_SNAPSHOT
    including account number, transit, loan number, application number, subvention
    indicator, region, and month-end date for subvented loans (subvention_ind = 'Y')
    on the current month-end date.
    """
    pass


"""Source layer for sq016."""
rundir_task = create_sq016_rundir()
extract_task = get_rcrr_psnl_loan_subv_mth_snapshot()

rundir_task >> extract_task
