"""
MDMFLAGS_CHECK — gate the CBS custuniv chain on CBS_MDM_FLAGS being loaded.

Port of the *check* portion of J_CBS_0000_MDMFLAGS_CHECK.sas: verify
emulated.CBS_MDM_FLAGS has rows for the process month/stream, send the alert
email and abort the run if it is empty (mirrors %mdmflagcheck: FILENAME OUTMAIL
EMAIL + %abort). The *copy* portion now lives in sequence sq084
(source_ingestion), which produces emulated.CBS_MDM_FLAGS.

Wiring:
  consumes emulated.CBS_MDM_FLAGS  -> waits on sq084 (cross-dag sensor)
  produces cbs.MDMFLAGS_OK         -> ordering marker; custuniv_02 lists it
                                      upstream, so the MDM join never runs on an
                                      empty/absent table.
"""

import logging

from airflow.sdk import get_current_context
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email
from bns.rrap.hooks.duckdb import DuckLakeHook

logger = logging.getLogger(__name__)

UPSTREAM_ASSET = ["emulated.CBS_MDM_FLAGS"]
DOWNSTREAM_ASSET = "cbs.MDMFLAGS_OK"
DEPENDENCIES = {}

# Alert recipients / subject — carried over verbatim from J_CBS_0000_MDMFLAGS_CHECK.sas.
# (SAS FROM="CBSProdRun"; the sender here comes from the Airflow [smtp] mail-from config.)
_ALERT_TO = ["edwsupport@scotiabank.com"]
_ALERT_BCC = [
    "cheng.liu@scotiabank.com",
    "suhel.deshmukh@scotiabank.com",
    "jason.hou@scotiabank.com",
]
_ALERT_SUBJECT = "CBS_MDM_FLAGS not loaded Prior to Monthly Run"


def mdmflags_check(pool="duckdb_pool", pool_slots=1):
    """
    Fail the run if emulated.CBS_MDM_FLAGS has no rows for the process
    (EFF_DT, STREAM) — the DuckDB equivalent of the SAS abort-if-empty gate,
    including the alert email.
    """
    context = get_current_context()
    ti = context["ti"]
    rundate = ti.xcom_pull(task_ids="handle_month_context", key="rundate")
    stream = ti.xcom_pull(task_ids="handle_month_context", key="stream")

    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
    n = hook.duckdb.sql(f"""
        SELECT COUNT(*) FROM emulated.CBS_MDM_FLAGS
        WHERE EFF_DT = DATE '{rundate}'
          AND STREAM = '{stream}'
    """).fetchone()[0]

    if n == 0:
        logger.error(
            "MDMFLAGS_CHECK: no data in CBS_MDM_FLAGS for month ending %s "
            "(stream=%s). Sending alert and aborting.",
            rundate, stream,
        )
        body = (
            f"No data is available in CBS_MDM_FLAGS for the processing month ending {rundate}.<br>"
            "Please check that this table is loaded prior to restarting this job and "
            "proceeding with the schedule.<br>"
            "The production job has now aborted."
        )
        try:
            send_email(
                to=_ALERT_TO,
                subject=_ALERT_SUBJECT,
                html_content=body,
                bcc=_ALERT_BCC,
            )
        except Exception as exc:  # pragma: no cover - depends on SMTP availability
            logger.error("MDMFLAGS_CHECK: alert email failed to send: %s", exc)
        raise AirflowException(
            f"MDMFLAGS_CHECK: emulated.CBS_MDM_FLAGS has 0 rows for "
            f"EFF_DT={rundate}, STREAM={stream} — aborting "
            f"(port of J_CBS_0000_MDMFLAGS_CHECK)."
        )

    logger.info(
        "MDMFLAGS_CHECK: CBS_MDM_FLAGS loaded with %s records for month ending %s "
        "(stream=%s).", n, rundate, stream,
    )
    return n