import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq043_rundir():
    """Create sq043 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq043_rundir = f"{rundir}/sq043"
    os.makedirs(sq043_rundir, exist_ok=True)


@task.beeline(
    task_id="get_airb_statcan_hh_dspsbl_incm_ext",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            cast(
                (CASE 
                    WHEN SUBSTR(ref_dt,6,2) = '01' then concat(substr(ref_dt,1,4),'-03-31')
                    WHEN SUBSTR(ref_dt,6,2) = '04' then concat(substr(ref_dt,1,4),'-06-30')
                    WHEN SUBSTR(ref_dt,6,2) = '07' then concat(substr(ref_dt,1,4),'-09-30')
                    WHEN SUBSTR(ref_dt,6,2) = '10' then concat(substr(ref_dt,1,4),'-12-31')
                    ELSE '0001-01-01' 
                END)
            as varchar(10)) as qtr_end_dt,
            CONCAT(SUBSTR(businesseffectivedate,1,4), SUBSTR(businesseffectivedate,6,2)) AS EFF_FROM_YR_MTH,
            val as HH_DSPSBL_INCM_MILLNTH_AMT
        FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_hh_dspsbl_incm_ext
        WHERE businesseffectivedate ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="get_airb_statcan_hh_dspsbl_incm_ext.parquet",
    schema=pa.schema([
        ('qtr_end_dt', pa.string()),
        ('EFF_FROM_YR_MTH', pa.string()),
        ('HH_DSPSBL_INCM_MILLNTH_AMT', pa.int64())
    ]),
)
def get_airb_statcan_hh_dspsbl_incm_ext():
    """
    Extract Statistics Canada household disposable income data.
    
    Extracts household disposable income from StatCan data, derives quarter-end
    date from reference date (Q1→03-31, Q2→06-30, Q3→09-30, Q4→12-31), and
    formats effective from year-month from business effective date.
    """
    pass


@task.parquet(
    task_id="hh_dspsbl_incm_qtr_src_extract",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/iias_hh_dspsbl_incm_qtr_src.parquet",
    sql="""
        SELECT
            MTH_TM_ID,
            EFF_FROM_YR_MTH,
            EFF_TO_YR_MTH,
            HH_DSPSBL_INCM_MILLNTH_AMT,
            CRNT_F,
            INSRT_PROCESS_TMSTMP,
            UPDT_PROCESS_TMSTMP
        FROM {{ params.EDW_schema_EDRTLRP1D }}.HH_DSPSBL_INCM_QTR
        WHERE CRNT_F = 'Y';
    """,
    export_params={},
    clear_before_write=True,
)
def hh_dspsbl_incm_qtr_src_extract():
    """ DuckLake extraction of household disposable income data. """
    pass


""" TaskFlow function definitions """
create_sq043_rundir = create_sq043_rundir()
get_airb_statcan_hh_dspsbl_incm_ext = get_airb_statcan_hh_dspsbl_incm_ext()
hh_dspsbl_incm_qtr_src_extract = hh_dspsbl_incm_qtr_src_extract()

""" Dependency chaining """
create_sq043_rundir >> [
    get_airb_statcan_hh_dspsbl_incm_ext,
    hh_dspsbl_incm_qtr_src_extract
]
