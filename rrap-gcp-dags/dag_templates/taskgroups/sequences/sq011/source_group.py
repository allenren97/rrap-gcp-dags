import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq011_rundir():
    """Create sq011 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq011_rundir = f"{rundir}/sq011"
    os.makedirs(sq011_rundir, exist_ok=True)


@task.beeline(
    task_id="get_tng_cpd2_customer_portfolio_summary_1",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            customer_id,
            customer_key,
            loan_decline_ind,
            mtg_decline_ind,
            deposit_accts_cnt,
            deposit_accts_amt,
            loan_accts_cnt,
            loan_accts_amt,
            max_dep_balance,
            concat(substr(max_dep_balance_dt, 1, 4), '-',
                substr(max_dep_balance_dt, 5, 2), '-',
                substr(max_dep_balance_dt, 7, 2)) AS max_dep_balance_dt,
            credit_score,
            concat(substr(credit_score_dt, 1, 4), '-',
                substr(credit_score_dt, 5, 2), '-',
                substr(credit_score_dt, 7, 2)) AS credit_score_dt,
            access_to_funds_amt,
            high_freq_caller_ind,
            concat(substr(high_freq_caller_dt, 1, 4), '-',
                substr(high_freq_caller_dt, 5, 2), '-',
                substr(high_freq_caller_dt, 7, 2)) AS high_freq_caller_dt,
            concat(substr(month_end_dt, 1, 4), '-',
                substr(month_end_dt, 5, 2), '-',
                substr(month_end_dt, 7, 2)) AS month_end_dt
        FROM {{ var.value.TSZ_SCHEMA }}.TNG_CPD2_CUSTOMER_PORTFOLIO_SUMMARY_1
        WHERE customer_key is not NULL
        AND businesseffectivedate='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq011",
    tmpfileloc="/bns/rrap/data/tmp",
    target="tng_cpd2_customer_portfolio_summary_1.parquet",
    to_parquet=True,
    schema=pa.schema([
        ('customer_id', pa.string()),
        ('customer_key', pa.int64()),
        ('loan_decline_ind', pa.string()),
        ('mtg_decline_ind', pa.string()),
        ('deposit_accts_cnt', pa.int64()),
        ('deposit_accts_amt', pa.float64()),
        ('loan_accts_cnt', pa.int64()),
        ('loan_accts_amt', pa.float64()),
        ('max_dep_balance', pa.float64()),
        ('max_dep_balance_dt', pa.date64()),
        ('credit_score', pa.int64()),
        ('credit_score_dt', pa.date64()),
        ('access_to_funds_amt', pa.float64()),
        ('high_freq_caller_ind', pa.string()),
        ('high_freq_caller_dt', pa.date64()),
        ('month_end_dt', pa.date64()),
    ]),
)
def get_tng_cpd2_customer_portfolio_summary_1():
    """
    Extract Tangerine customer portfolio summary data.
    
    Extracts customer portfolio summary including loan/mortgage decline indicators,
    account counts and amounts, max deposit balance, credit score, caller frequency,
    and month-end date from TNG_CPD2_CUSTOMER_PORTFOLIO_SUMMARY_1 for the specified
    business effective date where customer key is not NULL.
    """
    pass


rundir_task = create_sq011_rundir()
extract_task = get_tng_cpd2_customer_portfolio_summary_1()

rundir_task >> extract_task
