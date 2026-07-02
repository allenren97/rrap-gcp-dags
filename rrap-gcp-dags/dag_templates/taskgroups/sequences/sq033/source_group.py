import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq033_rundir():
    """Create sq033 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq033_rundir = f"{rundir}/sq033"
    os.makedirs(sq033_rundir, exist_ok=True)


@task.beeline(
    task_id="make_airb_tangrn_acct_colctn_txn",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            cast('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as DATE) as MTH_END_DT,
            case
                when
                    MORTGAGE_NUMBER not like '%[^0-9]%' or MORTGAGE_NUMBER = ''
                then
                    cast(MORTGAGE_NUMBER as BIGINT)
                else
                    0
            end as MORT_NUM,
            case 
                when
                    trim(TXN_DATE) = '' or TXN_DATE is NULL
                then
                    ''
                else
                    concat(
                        substring(TXN_DATE,1,4), '-',
                        substring(TXN_DATE,5,2), '-',
                        substring(TXN_DATE,7,2)
                    )
            end as TXN_DT,
            TXN_AMOUNT as TXN_AMT,
            TXN_COMMENT as TXN_CMNT,
            TXN_TYPE_CATEGORY as TXN_TP_CAT
        from
            {{ var.value.TSZ_SCHEMA }}.TNG_CPD8_MORTGAGE_COLLECTION_TRANSACTION
        where
            businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq033",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="make_airb_tangrn_acct_colctn_txn.parquet",
    schema=pa.schema([
        ("MTH_END_DT", pa.date64()),
        ("MORT_NUM", pa.int64()),
        ("TXN_DT", pa.string()),
        ("TXN_AMT", pa.float64()),
        ("TXN_CMNT", pa.string()),
        ("TXN_TP_CAT", pa.string()),
    ]),
)
def make_airb_tangrn_acct_colctn_txn():
    """
    Extract Tangerine mortgage collection transaction data.
    
    Extracts mortgage collection transaction information including month-end date,
    mortgage number (with numeric validation), transaction date (formatted from yyyymmdd),
    transaction amount, comment, and transaction type category from
    TNG_CPD8_MORTGAGE_COLLECTION_TRANSACTION for the current month-end date.
    """
    pass


"""Source layer for sq033."""
rundir_task = create_sq033_rundir()
extract_task = make_airb_tangrn_acct_colctn_txn()

rundir_task >> extract_task
