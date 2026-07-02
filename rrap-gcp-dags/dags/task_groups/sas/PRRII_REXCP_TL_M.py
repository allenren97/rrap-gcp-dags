from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id='REXCP_TL_M')
def REXCP_TL_M():

    OW_DM_RRII_TL_EXCP_RPT_1_SCRDMODEL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_EXCP_RPT_1_SCRDMODEL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_rrap_exception_report_1_TL_Scorecard_Model_Variables",
        sas_file="J_rrap_exception_report_1_TL_Scorecard_Model_Variables.sas",
    )
    OW_DM_RRII_TL_EXCP_RPT_1_SCRDMODEL.doc = "Exception report scorecard model 1 TL , Original taskID: IW503#OW_DM_RRII_TL_EXCP_RPT_1_SCRDMODEL"

    OW_DM_RRII_TL_EXCP_RPT_2_SCRDDIST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_EXCP_RPT_2_SCRDDIST",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_rrap_exception_report_2_TL_Scorecard_Distribution",
        sas_file="J_rrap_exception_report_2_TL_Scorecard_Distribution.sas",
    )
    OW_DM_RRII_TL_EXCP_RPT_2_SCRDDIST.doc = "Exception report scorecard dist 2 TL , Original taskID: IW503#OW_DM_RRII_TL_EXCP_RPT_2_SCRDDIST"

    OW_DM_RRII_TL_EXCP_RPT_3_SEGDIST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_EXCP_RPT_3_SEGDIST",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_rrap_exception_report_3_TL_Segmentation_Distribution",
        sas_file="J_rrap_exception_report_3_TL_Segmentation_Distribution.sas",
    )
    OW_DM_RRII_TL_EXCP_RPT_3_SEGDIST.doc = "Exception report segment dist 3 TL , Original taskID: IW503#OW_DM_RRII_TL_EXCP_RPT_3_SEGDIST"
    
    OW_DM_RRII_TL_EXCP_RPT_1_SCRDMODEL >> OW_DM_RRII_TL_EXCP_RPT_2_SCRDDIST
    OW_DM_RRII_TL_EXCP_RPT_2_SCRDDIST >> OW_DM_RRII_TL_EXCP_RPT_3_SEGDIST
