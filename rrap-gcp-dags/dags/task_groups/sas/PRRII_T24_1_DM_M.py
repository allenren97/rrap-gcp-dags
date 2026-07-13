from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="T24_1_DM_M")
def T24_1_DM_M():
    OW_DM_RRIIT24_SAS_PD_ACCT_XREF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_PD_ACCT_XREF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2401_BASEL_PNL_LN_PD_SEG_ACCT_XREF",
        sas_file="J_RRAP_TL10_2401_BASEL_PNL_LN_PD_SEG_ACCT_XREF.sas",
    )
    OW_DM_RRIIT24_SAS_PD_ACCT_XREF.doc = "J_RRAP_TL10_2401_BASEL_PNL_LN_PD_SEG_ACCT_XREF , Original taskID: IW503#OW_DM_RRIIT24_SAS_PD_ACCT_XREF"
