from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="T23_DM_M")
def T23_DM_M():
    OW_DM_RRII_TL_23_SAS_DTL_SNAPSHOT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_23_SAS_DTL_SNAPSHOT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT",
        sas_file="J_RRAP_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT.sas",
    )
    OW_DM_RRII_TL_23_SAS_DTL_SNAPSHOT.doc = "J_RRAP_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT , Original taskID: IW503#OW_DM_RRII_TL_23_SAS_DTL_SNAPSHOT"

    OW_DM_RRII_TL_23_SAS_SNAPSHOT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_23_SAS_SNAPSHOT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT",
        sas_file="J_RRAP_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT.sas",
    )
    OW_DM_RRII_TL_23_SAS_SNAPSHOT.doc = "J_RRAP_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT , Original taskID: IW503#OW_DM_RRII_TL_23_SAS_SNAPSHOT"

    OW_DM_RRII_TL_23_SAS_DTL_SNAPSHOT >> OW_DM_RRII_TL_23_SAS_SNAPSHOT
