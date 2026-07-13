from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id='SNP_DQ_M')
def SNP_DQ_M():

    OW_DM_RRII_SNPSHOT_INFA_PRE_RPT_DTL = InformaticaOperator(
        task_id="OW_DM_RRII_SNPSHOT_INFA_PRE_RPT_DTL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_PRE_RPT_DTL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_PRE_RPT_DTL",
    )
    OW_DM_RRII_SNPSHOT_INFA_PRE_RPT_DTL.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_PRE_RPT_DTL , Original taskID: IW502#OW_DM_RRII_SNPSHOT_INFA_PRE_RPT_DTL"

    OW_DM_RRII_SNPSHOT_INFA_POST_RPT_DTL = InformaticaOperator(
        task_id="OW_DM_RRII_SNPSHOT_INFA_POST_RPT_DTL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_POST_RPT_DTL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_POST_RPT_DTL",
    )
    OW_DM_RRII_SNPSHOT_INFA_POST_RPT_DTL.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_POST_RPT_DTL , Original taskID: IW502#OW_DM_RRII_SNPSHOT_INFA_POST_RPT_DTL"

    OW_DM_RRII_SNPSHOT_INFA_POST_VAR_RPT_DTL = InformaticaOperator(
        task_id="OW_DM_RRII_SNPSHOT_INFA_POST_VAR_RPT_DTL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_POST_VAR_RPT_DTL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_POST_VAR_RPT_DTL",
    )
    OW_DM_RRII_SNPSHOT_INFA_POST_VAR_RPT_DTL.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_MODEL_SNAPSHOT_POST_VAR_RPT_DTL , Original taskID: IW502#OW_DM_RRII_SNPSHOT_INFA_POST_VAR_RPT_DTL"

    OW_DM_RRII_SNPSHOT_SAS_REPORT_ALL_PORT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_SNPSHOT_SAS_REPORT_ALL_PORT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/j_rrap_dq_report_all_portfolios",
        sas_file="j_rrap_dq_report_all_portfolios.sas",
    )
    OW_DM_RRII_SNPSHOT_SAS_REPORT_ALL_PORT.doc = "This job will gather data for all portolios and generate DQ report , Original taskID: IW503#OW_DM_RRII_SNPSHOT_SAS_REPORT_ALL_PORT"

    OW_DM_RRII_SNPSHOT_INFA_PRE_RPT_DTL >> OW_DM_RRII_SNPSHOT_INFA_POST_RPT_DTL
    OW_DM_RRII_SNPSHOT_INFA_POST_RPT_DTL >> OW_DM_RRII_SNPSHOT_INFA_POST_VAR_RPT_DTL
    OW_DM_RRII_SNPSHOT_INFA_POST_VAR_RPT_DTL >> OW_DM_RRII_SNPSHOT_SAS_REPORT_ALL_PORT
