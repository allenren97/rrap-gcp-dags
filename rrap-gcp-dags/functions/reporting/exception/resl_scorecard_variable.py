import os

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

from bns.reports import (
    generate_scorecard_variable_report,
    _get_reports_dir
)
REPORT_DIR = _get_reports_dir()
REPORT_NAME = 'resl_scorecard_variable_report.xlsx'
UPSTREAM_ASSET = [ "models.tng_mor_pd_score",
                    "models.heloc_ead_score",
                    "models.heloc_lgdd_score",
                    "models.heloc_lgdnd_score",
                    "models.heloc_pd_score",
                    "models.mor_lgdd_score",
                    "models.mor_lgdnd_score",
                    "models.mor_pd_score",
]
PITSTATUS_TABLE = "features.PIT_STATUS_CROSS_DEFAULT_GCP"
DOWNSTREAM_ASSET = os.path.join(REPORT_DIR, REPORT_NAME)
DEPENDENCIES = {
    'delete_excel_file': ['generate_report']
}

def delete_excel_file(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    obsn_dt = context['ti'].xcom_pull(task_ids="handle_month_context", key="rundate")
    file = os.path.join(REPORT_DIR, obsn_dt, REPORT_NAME)
    if os.path.exists(file):
        os.remove(file)

def generate_report(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    obsn_dt = context['ti'].xcom_pull(task_ids="handle_month_context", key="rundate")
    generate_scorecard_variable_report(
        UPSTREAM_ASSET, DOWNSTREAM_ASSET, obsn_dt,
        PITSTATUS_TABLE,      
        pitstatus_filters="", # Leave blank if no filters applied when reading in pitstatus table from ducklake
    )