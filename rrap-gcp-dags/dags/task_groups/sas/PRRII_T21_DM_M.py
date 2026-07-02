from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="T21_DM_M")
def T21_DM_M():
    OW_DM_RRII_TL_21_SAS_ACCT_DRVD_VARS_2 = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_ACCT_DRVD_VARS_2",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2",
        sas_file="J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2.sas",
    )
    OW_DM_RRII_TL_21_SAS_ACCT_DRVD_VARS_2.doc = "scorecard - J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_ACCT_DRVD_VARS_2"

    OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2105_SCRD_CUSTOMER_LIST_SPL",
        sas_file="J_RRAP_TL10_2105_SCRD_CUSTOMER_LIST_SPL.sas",
    )
    OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS.doc = "scorecard - J_RRAP_TL10_2105_SCRD_CUSTOMER_LIST_SPL , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS"

    OW_DM_RRII_TL_21_SAS_GET_DRVD_CUST_INPTS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_GET_DRVD_CUST_INPTS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2106_BASEL_PSNL_LOAN_CUST_DRVD_VARS",
        sas_file="J_RRAP_TL10_2106_BASEL_PSNL_LOAN_CUST_DRVD_VARS.sas",
    )
    OW_DM_RRII_TL_21_SAS_GET_DRVD_CUST_INPTS.doc = "scorecard - J_RRAP_TL10_2106_BASEL_PSNL_LOAN_CUST_DRVD_VARS , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_GET_DRVD_CUST_INPTS"

    OW_DM_RRII_TL_21_SAS_SCRCRD_VARS_ACT_INP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_SCRCRD_VARS_ACT_INP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2107_SCRD_VAR_STATS_INPUTS_SPL_ACCT",
        sas_file="J_RRAP_TL10_2107_SCRD_VAR_STATS_INPUTS_SPL_ACCT.sas",
    )
    OW_DM_RRII_TL_21_SAS_SCRCRD_VARS_ACT_INP.doc = "scorecard - J_RRAP_TL10_2107_SCRD_VAR_STATS_INPUTS_SPL_ACCT , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_SCRCRD_VARS_ACT_INP"

    OW_DM_RRII_TL_21_SAS_SCCRD_VRS_CST_INPTS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_SCCRD_VRS_CST_INPTS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2107_SCRD_VAR_STATS_INPUTS_SPL_CUST",
        sas_file="J_RRAP_TL10_2107_SCRD_VAR_STATS_INPUTS_SPL_CUST.sas",
    )
    OW_DM_RRII_TL_21_SAS_SCCRD_VRS_CST_INPTS.doc = "scorecard - J_RRAP_TL10_2107_SCRD_VAR_STATS_INPUTS_SPL_CUST , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_SCCRD_VRS_CST_INPTS"

    OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_ACCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_ACCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2108_SCRD_VAR_STATS_SPL_ACCT",
        sas_file="J_RRAP_TL10_2108_SCRD_VAR_STATS_SPL_ACCT.sas",
    )
    OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_ACCT.doc = "scorecard - J_RRAP_TL10_2108_SCRD_VAR_STATS_SPL_ACCT , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_ACCT"

    OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_CUST_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_CUST_G",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2108_SCRD_VAR_STATS_SPL_CUST",
        sas_file="J_RRAP_TL10_2108_SCRD_VAR_STATS_SPL_CUST.sas",
    )
    OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_CUST_G.doc = "scorecard - J_RRAP_TL10_2108_SCRD_VAR_STATS_SPL_CUST , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_CUST_G"

    OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_ACCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_ACCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2109_BASEL_PSNL_LN_ACCT_SC_DRVD_VAR",
        sas_file="J_RRAP_TL10_2109_BASEL_PSNL_LN_ACCT_SC_DRVD_VAR.sas",
    )
    OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_ACCT.doc = "scorecard - J_RRAP_TL10_2109_BASEL_PSNL_LN_ACCT_SC_DRVD_VAR , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_ACCT"

    OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_CUST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_CUST",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2109_BASEL_PSNL_LN_CU_SC_DRVD_VARS",
        sas_file="J_RRAP_TL10_2109_BASEL_PSNL_LN_CU_SC_DRVD_VARS.sas",
    )
    OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_CUST.doc = "scorecard - J_RRAP_TL10_2109_BASEL_PSNL_LN_CU_SC_DRVD_VARS , Original taskID: IW503#OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_CUST"

    OW_DM_RRII_TL_21_SAS_ACCT_DRVD_VARS_2 >> OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS
    OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS >> OW_DM_RRII_TL_21_SAS_GET_DRVD_CUST_INPTS
    OW_DM_RRII_TL_21_SAS_GET_CUSTOMERS >> OW_DM_RRII_TL_21_SAS_SCRCRD_VARS_ACT_INP
    OW_DM_RRII_TL_21_SAS_GET_DRVD_CUST_INPTS >> OW_DM_RRII_TL_21_SAS_SCCRD_VRS_CST_INPTS
    OW_DM_RRII_TL_21_SAS_SCRCRD_VARS_ACT_INP >> OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_ACCT
    OW_DM_RRII_TL_21_SAS_SCCRD_VRS_CST_INPTS >> OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_CUST_G
    OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_ACCT >> OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_ACCT
    OW_DM_RRII_TL_21_SAS_CR_VAR_STATS_CUST_G >> OW_DM_RRII_TL_21_SAS_LOAD_VAR_STATS_CUST
