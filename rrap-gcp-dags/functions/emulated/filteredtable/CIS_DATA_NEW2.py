import datetime as dt
import logging
import os
import sys

from airflow.sdk import get_current_context

logger = logging.getLogger(__name__)

UPSTREAM_ASSET = []

DOWNSTREAM_ASSET = "emulated.CIS_DATA_NEW2"

DEPENDENCIES = {
    "cis_parse": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


CIS_DATA_ROOT = "/bns/rrap/data"
CIS_DATA_LANDING = "landing"
CIS_DATA_OUTPUT = "output"

_RUNDATE_TMPL = (
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
)
_OUT_ROOT_TMPL = (
    '{{ var.value.get("cis_load_out_root", "") or "'
    + CIS_DATA_ROOT
    + '/" ~ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") ~ "/'
    + CIS_DATA_OUTPUT
    + '" }}'
)
_YYMM_TMPL = (
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm")[-4:] }}'
)


def cis_parse(pool="duckdb_pool", pool_slots=8):
    from airflow.sdk import Variable

    context = get_current_context()
    rundate = context["ti"].xcom_pull(task_ids="handle_month_context", key="rundate")

    default_incoming = os.path.join(CIS_DATA_ROOT, rundate, CIS_DATA_LANDING)
    default_out_root = os.path.join(CIS_DATA_ROOT, rundate, CIS_DATA_OUTPUT)

    incoming = Variable.get("cis_load_incoming", default=default_incoming)
    out_root = Variable.get("cis_load_out_root", default=default_out_root)
    workers = int(Variable.get("cis_load_workers", default="8"))

    dags_dir = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "dags")
    if dags_dir not in sys.path:
        sys.path.insert(0, dags_dir)
    from util.cis_load import run_cis_load

    start_period_dt = dt.date.fromisoformat(rundate).replace(day=1)

    result = run_cis_load(
        incoming_dir=incoming,
        out_root=out_root,
        start_period_dt=start_period_dt,
        workers=workers,
    )
    logger.info("cis_parse: %s", result)
    return result


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE FILE_YR_MTH = '{_YYMM_TMPL}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=32,
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT
        '{_YYMM_TMPL}' AS FILE_YR_MTH,
        CIFKEY,
        PRODUCT,
        SUBSTR("ACCOUNT", 11, 13) AS "ACCOUNT",
        CID,
        PRIMARY_FLAG,
        RELATION_CODE,
        CUST_TYPE,
        EMPLOYEE_IND,
        MORT_NO,
        CAB,
        LOAN_NO,
        FILE_DATE,
        LOAD_DATE_TM,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM read_parquet('{_OUT_ROOT_TMPL}/FILE_YR_MTH={_YYMM_TMPL}/*.parquet')
    """,
):
    pass
