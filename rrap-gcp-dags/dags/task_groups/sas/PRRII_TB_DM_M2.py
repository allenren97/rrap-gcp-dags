from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="TB_DM_M2")
def TB_DM_M2():

    OW_DM_RRIITB_SAS_SPL_MISCLASS_RPT = SasOperator(
        task_id="OW_DM_RRIITB_SAS_SPL_MISCLASS_RPT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_TL10_2602_SPL_MISCLASS_REPORT",
        ssh_conn_id="sas-conn",
        sas_file="J_RRII_TL10_2602_SPL_MISCLASS_REPORT.sas",
    )
    OW_DM_RRIITB_SAS_SPL_MISCLASS_RPT.doc = "SPL Misclassification Report , Original taskID: IW503#OW_DM_RRIITB_SAS_SPL_MISCLASS_RPT"

    OW_DM_RRIITB_SAS_AUTO_SEC_POSTLOAD = SasOperator(
        task_id="OW_DM_RRIITB_SAS_AUTO_SEC_POSTLOAD",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2602_SECURITIZATION_POST_LOAD",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_TL10_2602_SECURITIZATION_POST_LOAD.sas",
    )
    OW_DM_RRIITB_SAS_AUTO_SEC_POSTLOAD.doc = "Auto Load Process After SPL Instrument fact , Original taskID: IW503#OW_DM_RRIITB_SAS_AUTO_SEC_POSTLOAD"

    OW_DM_RRIITB_SAS_TL_RB_NCR_BD_FACT = SasOperator(
        task_id="OW_DM_RRIITB_SAS_TL_RB_NCR_BD_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2603_BASEL_P_L_RPT_BL_AGG_NCR_BD_FACT",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_TL10_2603_BASEL_P_L_RPT_BL_AGG_NCR_BD_FACT.sas",
    )
    OW_DM_RRIITB_SAS_TL_RB_NCR_BD_FACT.doc = "J_RRAP_TL10_2603_BASEL_P_L_RPT_BL_AGG_NCR_BD_FACT , Original taskID: IW503#OW_DM_RRIITB_SAS_TL_RB_NCR_BD_FACT"

    OW_DM_RRIITB_INFA_INS_TRG_TB_DM_M = InformaticaOperator(
        task_id="OW_DM_RRIITB_INFA_INS_TRG_TB_DM_M",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_RRAP_JobTgr_AnalyticalBL_TL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_RRAP_JobTgr_AnalyticalBL_TL",
    )
    OW_DM_RRIITB_INFA_INS_TRG_TB_DM_M.doc = "INSERT Trigger DETAILS to INDICATE COMPLETION OF SCHEDULE TB_DM_M , Original taskID: IW502#OW_DM_RRIITB_INFA_INS_TRG_TB_DM_M"

    OW_DM_RRIITB_SAS_TL_RB_NCR_BD_FACT >> OW_DM_RRIITB_INFA_INS_TRG_TB_DM_M
