from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="RSK_REP_M")
def RSK_REP_M():

    OW_DM_RRII_RSK_DRVR_RPTS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RSK_DRVR_RPTS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_000_RISK_DRIVER_REPORTS",
        sas_file="J_RRII_000_RISK_DRIVER_REPORTS.sas",
    )
    OW_DM_RRII_RSK_DRVR_RPTS.doc = "Risk Driver Reports , Original taskID: IW503#OW_DM_RRII_RSK_DRVR_RPTS"
