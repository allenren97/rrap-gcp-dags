from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import InformaticaOperator

# from wf_DM_RRAP_Load_GL_BASEL_RECON_CR_INSTRMNT_ADJ import wf_DM_RRAP_Load_GL_BASEL_RECON_CR_INSTRMNT_ADJ

@task_group(group_id="RR4_K_DM_M")
def RR4_K_DM_M():

    OW_DM_RRIIRR4_GL_RECON_CR_INS_AJ_INF_LD = InformaticaOperator(
        task_id="OW_DM_RRIIRR4_GL_RECON_CR_INS_AJ_INF_LD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_GL_BASEL_RECON_CR_INSTRMNT_ADJ",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_GL_BASEL_RECON_CR_INSTRMNT_ADJ",
    )
    OW_DM_RRIIRR4_GL_RECON_CR_INS_AJ_INF_LD.doc = "Load data mart table BASEL_REVLVNG_CR_INSTRMNT_ADJ , Original taskID: IW502#OW_DM_RRIIRR4_GL_RECON_CR_INS_AJ_INF_LD"
