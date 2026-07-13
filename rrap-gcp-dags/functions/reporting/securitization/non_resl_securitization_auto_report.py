import os

from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras
from bns.reports.securitization_auto import generate_securitization_auto_report
from bns.reports import _get_reports_dir

REPORT_DIR = _get_reports_dir(report_type='securitization')
SECURITIZATION_TP_CD = 'AUTO'
ACCOUNTS_PARQUET = 'accounts_matched.parquet'
REPORT_PARQUET = 'auto_securitization_report.parquet'

UPSTREAM_ASSET = ['features.UNQ_ACCT_ID',
                  'features.TOT_CRNT_BAL_AMT',
                  'features.SML_BUS_F',
                  'features.TRNST_EXCLSN_F',
                  'features.CONSM_PRD_TREATMNT_CD',
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
                  'features.PRD_ID',
                  'ingestion.AUTO_SOURCE_FILE_ACCOUNTS',
                  'ingestion.AUTO_SOURCE_FILE_SUMMARY',
                  'features.AF_SECRTZTN_BAL_AMT',
                  'features.BEFORE_ZERO_NET_DRAWN_AMT',
                  'features.ADJUSTED_OS_BAL_AMT'
                  ]
DOWNSTREAM_ASSET = [os.path.join(REPORT_DIR, '{rundate}', SECURITIZATION_TP_CD, ACCOUNTS_PARQUET), os.path.join(REPORT_DIR, '{rundate}', SECURITIZATION_TP_CD, REPORT_PARQUET)]
MAX_PARQUETS_TO_DELETE = 2
DEPENDENCIES = {
    'delete_parquets': ['generate_report']
}


def delete_parquets(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    obsn_dt = context['ti'].xcom_pull(task_ids="handle_month_context", key="rundate")
    downstream_assets = [asset.format(rundate=obsn_dt) for asset in DOWNSTREAM_ASSET]

    if len(downstream_assets) > MAX_PARQUETS_TO_DELETE:
        raise ValueError(
            f"Refusing to delete {len(downstream_assets)} files; max allowed is {MAX_PARQUETS_TO_DELETE}"
        )

    for downstream_asset in downstream_assets:
        if os.path.exists(downstream_asset):
            os.remove(downstream_asset)


def generate_report(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    obsn_dt = context['ti'].xcom_pull(task_ids="handle_month_context", key="rundate")
    generate_securitization_auto_report(obsn_dt, REPORT_DIR)