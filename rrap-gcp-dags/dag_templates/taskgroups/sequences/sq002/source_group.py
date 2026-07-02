import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq002_rundir():
    """
    Task to create RUNDIR for sequence sq002.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq002_rundir = f"{rundir}/sq002"
    os.makedirs(sq002_rundir, exist_ok=True)


@task.beeline(
    task_id="make_airb_cust_dim",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.EZ1_SCHEMA }};
    SELECT
        trim(cust_id) as cust_id,
        trim(cust_type_cd) as cust_tp_cd,
        mth_end_dt
    FROM CUST_INV_PRTY_NON_PII
    WHERE
        mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
        AND CUST_SRC_SYS_CD = 'CI';
    """,
    schema=pa.schema([
        ("cust_id", pa.string()),
        ("cust_tp_cd", pa.string()),
        ("mth_end_dt", pa.date64()),
    ]),
    target="airb_cust_dim_src.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def make_airb_cust_dim():
    """
    Extract customer id and type code from EZ1.CUST_INV_PRTY_NON_PII for current month-end.
    """
    pass


@task.parquet(
    task_id="xfm",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/airb_cust_dim.parquet",
    sql="""
    SELECT
        now() as insrt_process_tmstmp,
        'version_code: abc, batch_id: def' as op_field,
        cust_id,
        CASE
            WHEN cust_tp_cd IN ('COMAB','COMBS','COMFN','COMFR','COMRE', 'CORNR','CORRE','NPERS','SMBAB','SMBAS','SMBFN', 'SMBUS','XXXXX') THEN 'NON_PSNL'
            WHEN cust_tp_cd IN ('PB','RN','RO','RM','','RX') THEN 'PSNL'
            ELSE 'UNKNOWN'
        END AS cust_tp_cd,
        mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/airb_cust_dim_src.parquet'
    """,
    export_params={},
    clear_before_write=True,
)
def xfm():
    """
    Map customer type code to PSNL or NON_PSNL categories.
    """
    pass


create_sq002_rundir = create_sq002_rundir()
make_airb_cust_dim = make_airb_cust_dim()
xfm = xfm()

create_sq002_rundir >> make_airb_cust_dim >> xfm
