import os
import pyarrow as pa
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
        select
            party_id,
            date_format(insrt_process_tmstmp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as insrt_process_tmstmp,
            pref_lang,
            gender_cd,
            marital_status,
            emp_type_cd,
            occup_cd,
            occup_type_cd,
            occup_stat_cd,
            occup_cat_cd,
            transit_num,
            sensitivity_cd,
            deceased_ind,
            cust_status,
            bnkrptcy_flag,
            under_18_flag,
            cust_type,
            time_on_books,
            cust_age,
            eff_dt,
            date_type
        from crz_cust_scorecard.cbs_mdm_flags
        where eff_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq084",
    to_parquet=True,
    strings_can_be_null=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="cbs_mdm_flags.parquet",
    schema=pa.schema([
        ("PARTY_ID", pa.string()),
        ("INSRT_PROCESS_TMSTMP", pa.string()),
        ("PREF_LANG", pa.string()),
        ("GENDER_CD", pa.string()),
        ("MARITAL_STATUS", pa.string()),
        ("EMP_TYPE_CD", pa.string()),
        ("OCCUP_CD", pa.string()),
        ("OCCUP_TYPE_CD", pa.string()),
        ("OCCUP_STAT_CD", pa.string()),
        ("OCCUP_CAT_CD", pa.string()),
        ("TRANSIT_NUM", pa.string()),
        ("SENSITIVITY_CD", pa.string()),
        ("DECEASED_IND", pa.string()),
        ("CUST_STATUS", pa.string()),
        ("BNKRPTCY_FLAG", pa.string()),
        ("UNDER_18_FLAG", pa.string()),
        ("CUST_TYPE", pa.string()),
        ("TIME_ON_BOOKS", pa.float64()),
        ("CUST_AGE", pa.int64()),
        ("EFF_DT", pa.date64()),
        ("DATE_TYPE", pa.string()),
    ]),
)
def get_cbs_mdm_flags():
    """Extract crz_cust_scorecard.cbs_mdm_flags for month-end."""
    pass


rundir_task = create_sq084_rundir()
extract_task = get_cbs_mdm_flags()

rundir_task >> extract_task
