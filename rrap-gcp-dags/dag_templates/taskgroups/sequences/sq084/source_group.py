import os
from airflow.sdk import task, get_current_context


@task
def create_sq084_rundir():
    """
    Task to create RUNDIR for sequence sq084 (CBS_MDM_FLAGS).
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq084_rundir = f"{rundir}/sq084"
    os.makedirs(sq084_rundir, exist_ok=True)


@task.beeline(
    task_id="extract_cbs_mdm_flags",
    beeline_conn_id="edlr-conn",
    sql="""
    use crz_cust_scorecard;
    SELECT *
    FROM cbs_mdm_flags
    WHERE eff_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDATE") }}'
    """,
    target="cbs_mdm_flags.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq084",
    to_parquet=True,
    strings_can_be_null=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_cbs_mdm_flags():
    """
    Extract crz_cust_scorecard.cbs_mdm_flags for the process month (eff_dt) -> parquet.
    Replaces the copy portion of J_CBS_0000_MDMFLAGS_CHECK.sas.
    """
    pass


""" TaskFlow function definitions """
create_sq084_rundir = create_sq084_rundir()
extract_cbs_mdm_flags = extract_cbs_mdm_flags()

""" Dependency chaining """
create_sq084_rundir >> extract_cbs_mdm_flags
