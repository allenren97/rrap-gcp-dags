import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq044_rundir():
    """Create sq044 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq044_rundir = f"{rundir}/sq044"
    os.makedirs(sq044_rundir, exist_ok=True)


@task.beeline(
    task_id="extract_airb_statcan_popn_ext",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            ref_dt,
            val
        from {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_popn_ext
        where businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044",
    target="extract_airb_statcan_popn_ext.parquet",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    schema=pa.schema([
        ('ref_dt', pa.string()),
        ('val', pa.string())
    ]),
)
def extract_airb_statcan_popn_ext():
    """Extract AIRB_STATCAN_POPN_EXT records for the month-end context."""
    pass


@task.beeline(
    task_id="extract_tm_dim",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            tm_id,
            tm_lvl_end_dt
        from {{ var.value.RCRR_SCHEMA }}.tm_dim
        where tm_lvl = 'Month'
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_tm_dim.parquet",
    schema=pa.schema([
        ('tm_id', pa.int64()),
        ('tm_lvl_end_dt', pa.date64())
    ]),
)
def extract_tm_dim():
    """Extract monthly time dimension records."""
    pass


@task.parquet(
    task_id="format_tm_lvl_end_dt",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select
            tm_id,
            tm_lvl_end_dt,
            strftime(tm_lvl_end_dt, '%Y/%m') as tm_lvl_end_dt_formatted
        from '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/extract_tm_dim.parquet'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044/format_tm_lvl_end_dt.parquet",
)
def format_tm_lvl_end_dt():
    """Format TM_DIM month-end date into yyyy/MM."""
    pass


@task.parquet(
    task_id="generate_mth_end_dt",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select
            a.tm_id as MTH_TM_ID,
            a.tm_lvl_end_dt as mth_end_dt,
            cast(b.val as double) as candn_popn_thsndth_val
        from '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/format_tm_lvl_end_dt.parquet' as a
        inner join '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/extract_airb_statcan_popn_ext.parquet' as b
            on a.tm_lvl_end_dt_formatted = b.ref_dt
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044/generate_mth_end_dt.parquet",
)
def generate_mth_end_dt():
    """Join TM_DIM and StatCan population extract to derive month-end context."""
    pass


@task.parquet(
    task_id="make_airb_candn_popn",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select
            now() as insrt_process_tmstmp,
            '"version_code":"0.0.1","batch_id":"24"' as op_field,
            a.mth_end_dt,
            a.candn_popn_thsndth_val,
            a.MTH_TM_ID,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as bus_eff_dt
        from '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/generate_mth_end_dt.parquet' as a
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044/make_airb_candn_popn.parquet",
)
def make_airb_candn_popn():
    """Build final AIRB_CANDN_POPN output dataset."""
    pass


rundir_task = create_sq044_rundir()
popn_task = extract_airb_statcan_popn_ext()
tm_dim_task = extract_tm_dim()
format_task = format_tm_lvl_end_dt()
mth_end_task = generate_mth_end_dt()
final_task = make_airb_candn_popn()

rundir_task >> [popn_task, tm_dim_task]
tm_dim_task >> format_task
[format_task, popn_task] >> mth_end_task >> final_task
