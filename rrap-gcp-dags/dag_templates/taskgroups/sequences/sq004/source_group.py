import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq004_rundir():
    """
    Task to create RUNDIR for sequence sq004.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq004_rundir = f"{rundir}/sq004"
    os.makedirs(sq004_rundir, exist_ok=True)


@task.beeline(
    task_id="airb_cust_acct_rltnp",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.EZ1_SCHEMA }};
    SELECT
        DISTINCT
        TRIM(CUST_ID) AS cust_id,
        CAST(CAST(ACCT_NUM AS BIGINT) AS string) AS acct_num,
        CUST_ACCT_RLTNP_TYPE_CD AS cust_acct_rltnp_type_cd,
        PRIMARY_ACCT_HOLDER_F AS primary_acct_holder_f,
        SRC_SYS_CD AS src_sys_cd,
        MTH_END_DT AS mth_end_dt
    FROM
        cust_acct_rltnp
    WHERE
        src_sys_cd IN ('KQ', 'GZ', 'SL', 'KQ_TSYS')
        AND mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ("cust_id", pa.string()),
        ("acct_num", pa.string()),
        ("cust_acct_rltnp_type_cd", pa.string()),
        ("primary_acct_holder_f", pa.string()),
        ("src_sys_cd", pa.string()),
        ("mth_end_dt", pa.date64()),
    ]),
    target="airb_cust_acct_rltnp.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def airb_cust_acct_rltnp():
    """
    Task to extract customer/account relationship data for RRAP portfolios from EZ1.cust_acct_rltnp.
    """
    pass


create_sq004_rundir = create_sq004_rundir()
airb_cust_acct_rltnp = airb_cust_acct_rltnp()

create_sq004_rundir >> airb_cust_acct_rltnp
