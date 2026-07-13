from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id='RE3_DM_M')
def RE3_DM_M():

    OW_DM_RRIIRE3_INFA_BSL_EXCP_DTL_FCT = InformaticaOperator(
        task_id="OW_DM_RRIIRE3_INFA_BSL_EXCP_DTL_FCT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_RPTG_EXCPTN_DTL_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_RPTG_EXCPTN_DTL_FACT",
    )
    OW_DM_RRIIRE3_INFA_BSL_EXCP_DTL_FCT.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_RPTG_EXCPTN_DTL_FACT , Original taskID: IW502#OW_DM_RRIIRE3_INFA_BSL_EXCP_DTL_FCT"

    OW_DM_RRIIRE3_INFA_BSL_EXCP_SUM_FCT = InformaticaOperator(
        task_id="OW_DM_RRIIRE3_INFA_BSL_EXCP_SUM_FCT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_RPTG_EXCPTN_SUM_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_RPTG_EXCPTN_SUM_FACT",
    )
    OW_DM_RRIIRE3_INFA_BSL_EXCP_SUM_FCT.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_RPTG_EXCPTN_SUM_FACT , Original taskID: IW502#OW_DM_RRIIRE3_INFA_BSL_EXCP_SUM_FCT"
    
    OW_DM_RRIIRE3_INFA_BSL_EXCP_DTL_FCT >> OW_DM_RRIIRE3_INFA_BSL_EXCP_SUM_FCT
