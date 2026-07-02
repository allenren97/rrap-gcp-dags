import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq036_rundir():
    """Create sq036 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq036_rundir = f"{rundir}/sq036"
    os.makedirs(sq036_rundir, exist_ok=True)


@task.beeline(
    task_id="get_region_list",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            region_id,
            prpty_type,
            `desc`,
            prov_cd
        from {{ var.value.RCRR_SCHEMA }}.region_snapshot
        where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}'
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="region_list.parquet",
    schema=pa.schema([
        ("region_id", pa.string()),
        ("prpty_type", pa.string()),
        ("desc", pa.string()),
        ("prov_cd", pa.string()),
    ]),
)
def get_region_list():
    """Extract region snapshot records for previous month-end."""
    pass


@task.beeline(
    task_id="get_hpi_monthly_data",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            year,
            month,
            region_type,
            region_id,
            prpty_type,
            paircount,
            valueraw,
            valuesmoothed,
            mth_end_dt
        from {{ var.value.RCRR_SCHEMA }}.hpi_mnthly_snapshot
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="hpi_monthly_data.parquet",
    schema=pa.schema([
        ("year", pa.int16()),
        ("month", pa.int8()),
        ("region_type", pa.string()),
        ("region_id", pa.string()),
        ("prpty_type", pa.string()),
        ("paircount", pa.string()),
        ("valueraw", pa.string()),
        ("valuesmoothed", pa.string()),
        ("mth_end_dt", pa.string()),
    ]),
)
def get_hpi_monthly_data():
    """Extract HPI monthly snapshot source records."""
    pass


@task.parquet(
    task_id="load_region_list",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select
            region_id as regionid,
            prpty_type as propertytype,
            `desc` as description,
            prov_cd as province
        from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.get_region_list", key="parquet") }}'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/load_region_list.parquet",
)
def load_region_list():
    """Normalize region list field names."""
    pass


@task.parquet(
    task_id="load_hpi_monthly_data",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select
            year,
            month,
            region_type as regiontype,
            region_id as regionid,
            prpty_type as propertytype,
            paircount,
            valueraw,
            valuesmoothed,
            mth_end_dt
        from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.get_hpi_monthly_data", key="parquet") }}'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/load_hpi_monthly_data.parquet",
)
def load_hpi_monthly_data():
    """Normalize monthly HPI field names."""
    pass


@task.parquet(
    task_id="consolidate_hpi_data",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        with prev_dt as (
            select '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}' as dt
        ),
        current_rows as (
            select
                m.year,
                m.month,
                m.regiontype,
                m.regionid,
                m.propertytype,
                m.paircount,
                m.valueraw,
                m.valuesmoothed,
                r.province,
                r.description,
                case when m.regionid in (
                    'CMA_935','CMA_933','CMA_825','CMA_835','CMA_602','CMA_537',
                    'CMA_535','CMA_505','CMA_462','CMA_421','CMA_205','CA_1'
                ) then 'Y' else 'N' end as cma11flag
            from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_hpi_monthly_data", key="parquet") }}' m
            join '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_region_list", key="parquet") }}' r
                on r.regionid = m.regionid
            join prev_dt p on 1=1
            where r.regionid like '%C%'
              and r.propertytype like 'ALL%'
              and m.propertytype = 'ALL'
              and m.mth_end_dt = p.dt
        ),
        latest_fallback as (
            select
                m.year,
                m.month,
                m.regiontype,
                m.regionid,
                m.propertytype,
                m.paircount,
                m.valueraw,
                m.valuesmoothed,
                r.province,
                r.description,
                'N' as cma11flag,
                row_number() over (partition by m.regionid order by m.year desc, m.month desc) as rn
            from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_hpi_monthly_data", key="parquet") }}' m
            join '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_region_list", key="parquet") }}' r
                on r.regionid = m.regionid
            where m.regionid in ('CMA_001','CMA_305','CMA_310')
        ),
        missing_regions as (
            select regionid from (values ('CMA_001'),('CMA_305'),('CMA_310')) t(regionid)
            where regionid not in (select distinct regionid from current_rows)
        )
        select * from current_rows
        union all
        select
            f.year,
            f.month,
            f.regiontype,
            f.regionid,
            f.propertytype,
            f.paircount,
            f.valueraw,
            f.valuesmoothed,
            f.province,
            f.description,
            f.cma11flag
        from latest_fallback f
        join missing_regions mr on mr.regionid = f.regionid
        where f.rn = 1
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/consolidate_hpi_data.parquet",
)
def consolidate_hpi_data():
    """Build consolidated HPI data with fallback rows for quarterly-only CMA regions."""
    pass


""" TaskFlow function definitions """
rundir_task = create_sq036_rundir()
region_task = get_region_list()
monthly_task = get_hpi_monthly_data()
region_norm_task = load_region_list()
monthly_norm_task = load_hpi_monthly_data()
consolidate_task = consolidate_hpi_data()

""" Dependency chaining """
rundir_task >> [region_task, monthly_task]
region_task >> region_norm_task
monthly_task >> monthly_norm_task
[region_norm_task, monthly_norm_task] >> consolidate_task

consolidate_task
