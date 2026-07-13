from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id='CMON_TSKS')
def CMON_TSKS():

    OW_DM_PRRII_SAS_SERVR_CLNUP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_PRRII_SAS_SERVR_CLNUP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_SAS_SERVER_CLEANUP",
        sas_file="RRAP_SAS_SERVER_CLEANUP.sas",
    )
    OW_DM_PRRII_SAS_SERVR_CLNUP.doc = "AUTO DELETION OF SAS TABLES TO CLEAR SPACE ON PROD SERVER , Original taskID: IW503#OW_DM_PRRII_SAS_SERVR_CLNUP"
