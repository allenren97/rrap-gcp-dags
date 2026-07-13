from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="REXCP_RW_M")
def REXCP_RW_M():
    OW_DM_RRII_RWA_EXCP_RPT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RWA_EXCP_RPT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_rrap_exception_report_4_RWA_by_Schedule",
        sas_file="J_rrap_exception_report_4_RWA_by_Schedule.sas",
    )
    OW_DM_RRII_RWA_EXCP_RPT.doc = ("Exception report rwa 4 , Original taskID: IW503#OW_DM_RRII_RWA_EXCP_RPT")
