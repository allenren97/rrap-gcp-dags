import os

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

from bns.reports import (
    generate_dt_report,
    _get_reports_dir,
    _get_models
)
REPORT_DIR = _get_reports_dir()
REPORT_NAME = 'non_resl_decision_tree_report.xlsx'
STREAM = 'non_resl'
ALLOWED_MODELS = {
    'step_heloc_lgdd',
    'standalone_mor_lgdd',
    'dtl_lgdnd',
    'ssla_pd',
    'sslb_pd',
}
ALL_MODELS = _get_models(stream = STREAM, type = 'decision_tree')
UPSTREAM_ASSET = [m for m in ALL_MODELS if any(name in m for name in ALLOWED_MODELS)]

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
    generate_dt_report(UPSTREAM_ASSET, DOWNSTREAM_ASSET, obsn_dt, STREAM)