from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id='TE3_DM_M')
def TE3_DM_M():

    OW_DM_RRIITE3_INFA_PNL_EXCP_DTL_FCT = InformaticaOperator(
        task_id="OW_DM_RRIITE3_INFA_PNL_EXCP_DTL_FCT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_PNL_LN_RPT_EXCPTN_DTL_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_PNL_LN_RPT_EXCPTN_DTL_FACT",
    )
    OW_DM_RRIITE3_INFA_PNL_EXCP_DTL_FCT.doc = "this job load Termloan exception table , Original taskID: IW502#OW_DM_RRIITE3_INFA_PNL_EXCP_DTL_FCT"

    OW_DM_RRIITE3_INFA_PNL_EXCP_SUM_FCT = InformaticaOperator(
        task_id="OW_DM_RRIITE3_INFA_PNL_EXCP_SUM_FCT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_PNL_LN_RPT_EXCPTN_SUM_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_PNL_LN_RPT_EXCPTN_SUM_FACT",
    )
    OW_DM_RRIITE3_INFA_PNL_EXCP_SUM_FCT.doc = "load Term load exception summary fact table , Original taskID: IW502#OW_DM_RRIITE3_INFA_PNL_EXCP_SUM_FCT"

    OW_DM_RRIITE3_INFA_PNL_EXCP_DTL_FCT >> OW_DM_RRIITE3_INFA_PNL_EXCP_SUM_FCT
