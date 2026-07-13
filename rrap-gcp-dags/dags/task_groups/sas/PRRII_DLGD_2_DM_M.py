from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="DLGD_2_DM_M")
def DLGD_2_DM_M():
    OW_SAS_II_DLGD_BSL_STP_LTV_DRVD_VARS_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_BSL_STP_LTV_DRVD_VARS_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0080_BASEL_STEP_LTV_DRVD_VARS_CMA"
        sas_file="J_RRAP_DLGD_0080_BASEL_STEP_LTV_DRVD_VARS_CMA.sas",
    )
    OW_SAS_II_DLGD_BSL_STP_LTV_DRVD_VARS_CMA.doc = ("J_RRAP_DLGD_0080_BASEL_STEP_LTV_DRVD_VARS_CMA")


    OW_SAS_II_DLGD_BASEL_STEP_LTV_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_BASEL_STEP_LTV_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0080_BASEL_STEP_LTV_DRVD_VARS",
        sas_file="J_RRAP_DLGD_0080_BASEL_STEP_LTV_DRVD_VARS.sas",
    )
    OW_SAS_II_DLGD_BASEL_STEP_LTV_DRVD_VARS.doc = "J_RRAP_DLGD_0080_BASEL_STEP_LTV_DRVD_VARS , Original taskID: IW503#OW_SAS_II_DLGD_BASEL_STEP_LTV_DRVD_VARS"

    OW_SAS_II_DLGD_BSL_ACT_LTV_DRVD_VARS_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_BSL_ACT_LTV_DRVD_VARS_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0090_BASEL_ACCT_LTV_DRVD_VARS_CMA"
        sas_file="J_RRAP_DLGD_0090_BASEL_ACCT_LTV_DRVD_VARS_CMA.sas",
    )
    OW_SAS_II_DLGD_BSL_ACT_LTV_DRVD_VARS_CMA.doc = ("J_RRAP_DLGD_0090_BASEL_ACCT_LTV_DRVD_VARS_CMA")

    OW_SAS_II_DLGD_BASEL_ACCT_LTV_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_BASEL_ACCT_LTV_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0090_BASEL_ACCT_LTV_DRVD_VARS",
        sas_file="J_RRAP_DLGD_0090_BASEL_ACCT_LTV_DRVD_VARS.sas",
    )
    OW_SAS_II_DLGD_BASEL_ACCT_LTV_DRVD_VARS.doc = "J_RRAP_DLGD_0090_BASEL_ACCT_LTV_DRVD_VARS , Original taskID: IW503#OW_SAS_II_DLGD_BASEL_ACCT_LTV_DRVD_VARS"

    
    OW_SAS_II_DLGD_BSL_STP_LTV_DRVD_VARS_CMA >> OW_SAS_II_DLGD_BASEL_STEP_LTV_DRVD_VARS
    OW_SAS_II_DLGD_BASEL_STEP_LTV_DRVD_VARS >> OW_SAS_II_DLGD_BSL_ACT_LTV_DRVD_VARS_CMA
    OW_SAS_II_DLGD_BSL_ACT_LTV_DRVD_VARS_CMA >> OW_SAS_II_DLGD_BASEL_ACCT_LTV_DRVD_VARS
