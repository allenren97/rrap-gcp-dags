from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="TR4_T_DM_M")
def TR4_T_DM_M():
    OW_DM_RRIITR4_SAS_TL_RECON_INST_ADJ = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIITR4_SAS_TL_RECON_INST_ADJ",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2502_BASEL_PSNL_LOAN_INSTR_ADJ",
        sas_file="J_RRAP_TL10_2502_BASEL_PSNL_LOAN_INSTR_ADJ.sas",
    )
    OW_DM_RRIITR4_SAS_TL_RECON_INST_ADJ.doc = "J_RRAP_TL10_2502_BASEL_PSNL_LOAN_INSTR_ADJ , Original taskID: IW503#OW_DM_RRIITR4_SAS_TL_RECON_INST_ADJ"
