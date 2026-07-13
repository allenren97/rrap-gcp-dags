from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id='FC_DT4_M')
def FC_DT4_M():

    OW_DM_RRIIDT4_BSL_SEG_INCL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_BSL_SEG_INCL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0130_DT4_BASEL_SEG_INCL",
        sas_file="J_RRAP_DT4_0130_DT4_BASEL_SEG_INCL.sas",
    )
    OW_DM_RRIIDT4_BSL_SEG_INCL.doc = "Loads J_RRAP_DT4_0130_DT4_BASEL_SEG_INCL , Original taskID: IW503#OW_DM_RRIIDT4_BSL_SEG_INCL"
    
