from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="T22_DM_M")
def T22_DM_M():
    OW_DM_RRII_TL_22_SAS_PLN_OBPT_DRVD_VAR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_22_SAS_PLN_OBPT_DRVD_VAR",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR",
        sas_file="J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas",
    )
    OW_DM_RRII_TL_22_SAS_PLN_OBPT_DRVD_VAR.doc = "J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR , Original taskID: IW503#OW_DM_RRII_TL_22_SAS_PLN_OBPT_DRVD_VAR"

    OW_DM_RRII_TL_22_SAS_GATHR_REALZ_INPT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_22_SAS_GATHR_REALZ_INPT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2202_SPL_MODEL_020_GATHER_REALIZED_INPUTS",
        sas_file="J_RRAP_TL10_2202_SPL_MODEL_020_GATHER_REALIZED_INPUTS.sas",
    )
    OW_DM_RRII_TL_22_SAS_GATHR_REALZ_INPT.doc = "J_RRAP_TL10_2202_SPL_MODEL_020_GATHER_REALIZED_INPUTS , Original taskID: IW503#OW_DM_RRII_TL_22_SAS_GATHR_REALZ_INPT"

    OW_DM_RRII_TL_22_SAS_REALIZED_LGDD = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_22_SAS_REALIZED_LGDD",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2203_SPL_MODEL_025_REALIZED_LGDD",
        sas_file="J_RRAP_TL10_2203_SPL_MODEL_025_REALIZED_LGDD.sas",
    )
    OW_DM_RRII_TL_22_SAS_REALIZED_LGDD.doc = "J_RRAP_TL10_2203_SPL_MODEL_025_REALIZED_LGDD , Original taskID: IW503#OW_DM_RRII_TL_22_SAS_REALIZED_LGDD"

    OW_DM_RRII_TL_22_SAS_REALIZED_LGDND = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_22_SAS_REALIZED_LGDND",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2204_SPL_MODEL_025_REALIZED_LGDND",
        sas_file="J_RRAP_TL10_2204_SPL_MODEL_025_REALIZED_LGDND.sas",
    )
    OW_DM_RRII_TL_22_SAS_REALIZED_LGDND.doc = "J_RRAP_TL10_2204_SPL_MODEL_025_REALIZED_LGDND , Original taskID: IW503#OW_DM_RRII_TL_22_SAS_REALIZED_LGDND"

    OW_DM_RRII_TL_22_SAS_DEFAULT_IND = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_22_SAS_DEFAULT_IND",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2205_PSNL_LOAN_MTH_ACCT_RCVRY",
        sas_file="J_RRAP_TL10_2205_PSNL_LOAN_MTH_ACCT_RCVRY.sas",
    )
    OW_DM_RRII_TL_22_SAS_DEFAULT_IND.doc = "J_RRAP_TL10_2205_PSNL_LOAN_MTH_ACCT_RCVRY , Original taskID: IW503#OW_DM_RRII_TL_22_SAS_DEFAULT_IND"

    OW_DM_RRII_TL_22_SAS_PLN_OBPT_DRVD_VAR >> OW_DM_RRII_TL_22_SAS_GATHR_REALZ_INPT
    OW_DM_RRII_TL_22_SAS_GATHR_REALZ_INPT >> OW_DM_RRII_TL_22_SAS_REALIZED_LGDD
    OW_DM_RRII_TL_22_SAS_REALIZED_LGDD >> OW_DM_RRII_TL_22_SAS_REALIZED_LGDND
    OW_DM_RRII_TL_22_SAS_REALIZED_LGDND >> OW_DM_RRII_TL_22_SAS_DEFAULT_IND
