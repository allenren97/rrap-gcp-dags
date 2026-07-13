import os

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

from bns.reports import (
    generate_transition_matrix_report,
    _get_reports_dir,
    _get_models
)
REPORT_DIR = _get_reports_dir()
REPORT_NAME = 'resl_transition_matrix_report.xlsx'
STREAM = 'resl'
UPSTREAM_ASSET = _get_models(stream = STREAM, type = 'segmented')
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
    generate_transition_matrix_report(UPSTREAM_ASSET, DOWNSTREAM_ASSET, obsn_dt, STREAM)
    