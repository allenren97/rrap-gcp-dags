import os
import pyarrow as pa
from datetime import datetime
from airflow.sdk import get_current_context, task


@task
def create_sq020_rundir():
    """Create sq020 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq020_rundir = f"{rundir}/sq020"
    os.makedirs(sq020_rundir, exist_ok=True)


@task
def compute_prior_year():
    """Compute prior year from month-end date."""
    context = get_current_context()
    mth_end_dt = context["ti"].xcom_pull(task_ids="handle_month_context", key="MTH_END_DT")
    current_year = datetime.strptime(mth_end_dt, '%Y-%m-%d').year - 1
    return current_year


@task.beeline(
    task_id="get_tng_cpd10_indirect_cost",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            year,
            cost_type,
            cost_amount as amount
        FROM {{ var.value.TSZ_SCHEMA }}.TNG_CPD10_INDIRECT_COST
        WHERE year = CAST('{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' AS INTEGER) 
        AND cost_type NOT IN ('Cost per account', 'Defaulted accounts');
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="get_tng_cpd10_indirect_cost.parquet",
    schema=pa.schema([
        ('year', pa.int64()),
        ('cost_type', pa.string()),
        ('amount', pa.float64())
    ]),
)
def get_tng_cpd10_indirect_cost():
    """
    Extract Tangerine indirect cost data.
    
    Extracts year, cost type, and cost amount from TNG_CPD10_INDIRECT_COST
    for the year prior to current month-end, excluding 'Cost per account' and
    'Defaulted accounts' cost types which are recalculated separately.
    """
    pass


@task.beeline(
    task_id="get_rcrr_tng_mort_acct_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            mth_end_dt,
            day_arrs_cnt,
            acct_id
        FROM {{ var.value.RCRR_SCHEMA }}.tng_mort_acct_mth_snapshot
        WHERE day_arrs_cnt > 0 
        AND mth_end_dt = CONCAT(CAST('{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' AS STRING), '-01-31');
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="get_rcrr_tng_mort_acct_mth_snapshot.parquet",
    schema=pa.schema([
        ('mth_end_dt', pa.date64()),
        ('day_arrs_cnt', pa.int64()),
        ('acct_id', pa.string())
    ]),
)
def get_rcrr_tng_mort_acct_mth_snapshot():
    """
    Extract Tangerine mortgage account delinquency snapshot.
    
    Extracts account IDs and days in arrears from TNG_MORT_ACCT_MTH_SNAPSHOT
    for January 31st of the year prior to current month-end, where days in
    arrears count is greater than 0.
    """
    pass


@task.parquet(
    task_id="get_new_estimated_costs",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT '{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' as year, 
        'Estimated costs' as cost_type, 
        SUM(amount) as amount 
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_tng_cpd10_indirect_cost.parquet'
        WHERE cost_type IN ('Salaries', 'Benefits at 20%', 'Operating costs')
        AND NOT EXISTS (
            SELECT 1 
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_tng_cpd10_indirect_cost.parquet' 
            WHERE cost_type = 'Estimated costs'
        )
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/get_new_estimated_costs.parquet",
)
def get_new_estimated_costs():
    """
    Compute new estimated costs if not already recorded.
    
    Sums costs where cost_type IN ('Salaries', 'Benefits at 20%', 'Operating costs')
    if 'Estimated costs' record does not already exist.
    """
    pass


@task.parquet(
    task_id="combine_costs_with_estimated",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        (
            SELECT year, cost_type, amount
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_tng_cpd10_indirect_cost.parquet'
        )
        UNION
        (
            SELECT year, cost_type, amount
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_new_estimated_costs.parquet'
        )
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/combine_costs_with_estimated.parquet",
)
def combine_costs_with_estimated():
    """
    Combine cost data with newly computed estimated costs.
    
    Unions cost-related data from TNG_CPD10_INDIRECT_COST with newly
    computed estimated costs.
    """
    pass


@task.parquet(
    task_id="get_cost_per_dlqnt_acct",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        (SELECT
            CAST( 
            (SELECT amount FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/combine_costs_with_estimated.parquet' WHERE cost_type = 'Estimated costs') 
            / 
            (SELECT CAST(count(distinct acct_id) as double) FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_rcrr_tng_mort_acct_mth_snapshot.parquet') 
            AS DECIMAL(15,7)) as amount,
            'Cost per account' as cost_type,
            '{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' as year)
        UNION
        (SELECT 
            CAST(count(distinct acct_id) as double) amount, 
            'Delinquent Accounts' as cost_type,
            '{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' as year
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_rcrr_tng_mort_acct_mth_snapshot.parquet')
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/get_cost_per_dlqnt_acct.parquet",
)
def get_cost_per_dlqnt_acct():
    """
    Compute cost per delinquent account.
    
    Calculates average cost per delinquent account (estimated costs / distinct
    delinquent accounts) and counts distinct delinquent accounts with nonzero
    days in arrears.
    """
    pass


@task.parquet(
    task_id="combine_costs_with_dlqnt",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        (
            SELECT year, cost_type, amount
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/combine_costs_with_estimated.parquet'
            WHERE cost_type <> 'Benefits at 30%'
        )
        UNION
        (
            SELECT year, cost_type, amount
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_cost_per_dlqnt_acct.parquet'
        )
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/combine_costs_with_dlqnt.parquet",
)
def combine_costs_with_dlqnt():
    """
    Combine cost data with delinquent account costs.
    
    Final union of cost data, excluding 'Benefits at 30%' records, with
    cost per delinquent account and delinquent accounts count.
    """
    pass


"""Source layer for sq020."""
rundir_task = create_sq020_rundir()
prior_year_task = compute_prior_year()

extract_1 = get_tng_cpd10_indirect_cost()
extract_2 = get_rcrr_tng_mort_acct_mth_snapshot()

transform_1 = get_new_estimated_costs()
combine_1 = combine_costs_with_estimated()
transform_2 = get_cost_per_dlqnt_acct()
combine_2 = combine_costs_with_dlqnt()

# Dependency chain
rundir_task >> prior_year_task
prior_year_task >> [extract_1, extract_2]
extract_1 >> transform_1
[extract_1, transform_1] >> combine_1
[extract_2, combine_1] >> transform_2
[combine_1, transform_2] >> combine_2
