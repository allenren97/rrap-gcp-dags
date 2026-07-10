import os
from airflow.sdk import get_current_context, task


@task
def create_sq084_rundir():
    """Create sq084 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq084_rundir = f"{rundir}/sq084"
    os.makedirs(sq084_rundir, exist_ok=True)


@task.beeline(
    task_id="get_cbs_mdm_flags",
    beeline_conn_id="edlr-conn",
    sql="""
        select *
        from crz_cust_scorecard.cbs_mdm_flags
        where eff_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq084",
    to_parquet=True,
    strings_can_be_null=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="cbs_mdm_flags.parquet",
)
def get_cbs_mdm_flags():
    """Extract crz_cust_scorecard.cbs_mdm_flags for month-end (copy half of J_CBS_0000)."""
    pass


rundir_task = create_sq084_rundir()
extract_task = get_cbs_mdm_flags()

rundir_task >> extract_task
