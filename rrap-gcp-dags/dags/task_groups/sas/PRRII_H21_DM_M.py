from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SSHOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator

# from converted_informatica_workflows import wf_DM_RRAP_Load_CR_BUREAU_DELI_MTH_SNAPSHOT


@task_group(group_id="H21_DM_M")
def H21_DM_M():
    OW_DM_RRIIH21_SAS_PIT_STAT_PRE_STEP_DFLT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_PIT_STAT_PRE_STEP_DFLT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_0000_PIT_STATUS_PRE_STEP_CROSS_DFLT",
        sas_file="J_RRII_KS10_0000_PIT_STATUS_PRE_STEP_CROSS_DFLT.sas",
    )
    OW_DM_RRIIH21_SAS_PIT_STAT_PRE_STEP_DFLT.doc = "KS PIT Status STEP Change Job Addition , Original taskID: IW503#OW_DM_RRIIH21_SAS_PIT_STAT_PRE_STEP_DFLT"

    OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI = InformaticaOperator(
        ssh_conn_id="infa-dm-rrap-conn",
        task_id="OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_CR_BUREAU_DELI_MTH_SNAPSHOT",
        infa_workflow="wf_DM_RRAP_Load_CR_BUREAU_DELI_MTH_SNAPSHOT",
    )
    OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI.doc = "Calls ETL WF wf_DM_RRAP_Load_CR_BUREAU_DELI_MTH_SNAPSHOT , Original taskID: IW502#OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI"

    OW_DM_RRIIH21_SAS_BSL_CUST_TXN_SUM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_BSL_CUST_TXN_SUM",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2101_BASEL_CUST_MTH_DEP_TXN_SUM",
        sas_file="J_RRAP_KS10_2101_BASEL_CUST_MTH_DEP_TXN_SUM.sas",
    )
    OW_DM_RRIIH21_SAS_BSL_CUST_TXN_SUM.doc = "Calls SAS code to load BASEL_CUST_MTH_DEP_TXN_SUM , Original taskID: IW503#OW_DM_RRIIH21_SAS_BSL_CUST_TXN_SUM"

    OW_DM_RRIIH21_SAS_LOAD_BSL_STP_PL_BS_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_STP_PL_BS_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2102_BASEL_STEP_PLN_BASE_DRVD_VARS",
        sas_file="J_RRAP_KS10_2102_BASEL_STEP_PLN_BASE_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_STP_PL_BS_DRV.doc = "Calls SAS code to Load BASEL_STEP_PLN_BASE_DRVD_VARS  , Original taskID: IW503#OW_DM_RRIIH21_SAS_LOAD_BSL_STP_PL_BS_DRV"

    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS",
        sas_file="J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV.doc = "Calls SAS job J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV"

    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CR_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CR_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS_BASEL_REVLVNG_CR_ACCT_DRVD_VARS",
        sas_file="J_RRII_KS_BASEL_REVLVNG_CR_ACCT_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CR_DRV.doc = "J_RRII_KS_BASEL_REVLVNG_CR_ACCT_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CR_DRV"

    OW_DM_RRIIH21_SAS_LOAD_BSL_MOR_ACCT_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_MOR_ACCT_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS",
        sas_file="J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_MOR_ACCT_DRV.doc = "J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIH21_SAS_LOAD_BSL_MOR_ACCT_DRV"

    OW_DM_RRIH21_SAS_LOAD_BSL_PSNL_LOAN_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIH21_SAS_LOAD_BSL_PSNL_LOAN_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS",
        sas_file="J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS.sas",
    )
    OW_DM_RRIH21_SAS_LOAD_BSL_PSNL_LOAN_DRV.doc = "J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS , Original taskID: IW503#OW_DM_RRIH21_SAS_LOAD_BSL_PSNL_LOAN_DRV"

    OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2111_DM_RRAP_BASEL_STEP_SCORECRD_DRVD_VARS",
        sas_file="J_RRII_KS10_2111_DM_RRAP_BASEL_STEP_SCORECRD_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV.doc = "Calls SAS job to J_RRII_KS10_2111_DM_RRAP_BASEL_STEP_SCORECRD_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV"

    OW_DM_RRIIH21_INFA_LOAD_BSL_CUST_SCR_DRV = InformaticaOperator(
        task_id="OW_DM_RRIIH21_INFA_LOAD_BSL_CUST_SCR_DRV",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_CUST_SCORECRD_DRVD_VARS",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_CUST_SCORECRD_DRVD_VARS",
    )
    OW_DM_RRIIH21_INFA_LOAD_BSL_CUST_SCR_DRV.doc = "Calls ETL WF _DM_RRAP_Load_BASEL_CUST_SCORECRD_DRVD_VARS , Original taskID: IW502#OW_DM_RRIIH21_INFA_LOAD_BSL_CUST_SCR_DRV"

    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_CSDV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_CSDV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_IIAS_Load_BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS",
        sas_file="RRAP_IIAS_Load_BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_CSDV.doc = "RRAP_IIAS_Load_BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_CSDV"

    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_RCDV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_RCDV",
        # bash_command="echo dummy",
        sas_file="J_RRII_KS10_2106_BASEL_CC_LOC_REVLVNG_CR_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_RCDV.doc = (
        " , Original taskID: IW502#OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_RCDV"
    )

    OW_DM_RRIIH21_INFA_SANITY_CHECK_H21 = InformaticaOperator(
        task_id="OW_DM_RRIIH21_INFA_SANITY_CHECK_H21",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc21",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc21",
    )
    OW_DM_RRIIH21_INFA_SANITY_CHECK_H21.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc21 , Original taskID: IW502#OW_DM_RRIIH21_INFA_SANITY_CHECK_H21"

    OW_DM_RRIIDRO_INFA_SANITY_CHECK_NULL_H21 = InformaticaOperator(
        task_id="OW_DM_RRIIDRO_INFA_SANITY_CHECK_NULL_H21",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_check_data_sanity_NULL_DRVD_VARS_21",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_check_data_sanity_NULL_DRVD_VARS_21",
    )
    OW_DM_RRIIDRO_INFA_SANITY_CHECK_NULL_H21.doc = "Calls ETL WF wf_DM_RRAP_check_data_sanity_NULL_DRVD_VARS_21 , Original taskID: IW502#OW_DM_RRIIDRO_INFA_SANITY_CHECK_NULL_H21"

    OW_DM_RRIIH21_INFA_AUDIT_DRVDVARS_BASEL = InformaticaOperator(
        task_id="OW_DM_RRIIH21_INFA_AUDIT_DRVDVARS_BASEL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Audit_DrvdVars_BASEL",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Audit_DrvdVars_BASEL",
    )
    OW_DM_RRIIH21_INFA_AUDIT_DRVDVARS_BASEL.doc = "Calls ETL WF wf_STG_RRAP_Dm_PostLoad_Audit_DrvdVars_BASEL , Original taskID: IW502#OW_DM_RRIIH21_INFA_AUDIT_DRVDVARS_BASEL"

    OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD1 = SSHOperator(
        task_id="OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD1",
        ssh_conn_id="ssh-edw-conn",
        command='{{ var.value.PSCRIPTS }}/ow_db_db2_load.ksh -d OWSTARDB -g yes -t CB.CUST_XREF_WRK2 -h CB.CUST_XREF_WRK_LOAD1 -m replace',
    )
    OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD1.doc = "Call db2 load script to load CUST_XREF_WRK2 table , Original taskID: IW501#OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD1"

    OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD2 = SSHOperator(
        task_id="OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD2",
        ssh_conn_id="ssh-edw-conn",
        command='{{ var.value.PSCRIPTS }}/ow_db_db2_load.ksh -d OWSTARDB -g yes -t CB.CUST_XREF_WRK -h CB.CUST_XREF_WRK_LOAD2 -m replace',
    )
    OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD2.doc = "Call db2 load script to load CUST_XREF_WRK2 table , Original taskID: IW501#OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD2"

    OW_DM_RRIIH21_INFA_LOAD_TRANS_DELI_AUDIT = InformaticaOperator(
        task_id="OW_DM_RRIIH21_INFA_LOAD_TRANS_DELI_AUDIT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Audit_DELI_FILE",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Audit_DELI_FILE",
    )
    OW_DM_RRIIH21_INFA_LOAD_TRANS_DELI_AUDIT.doc = "load audit table , Original taskID: IW502#OW_DM_RRIIH21_INFA_LOAD_TRANS_DELI_AUDIT"

    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_1101_BASEL_CUST_MTH_POSTN_SUM_FACT",
        sas_file="J_RRAP_KS10_1101_BASEL_CUST_MTH_POSTN_SUM_FACT.sas",
    )
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM.doc = "Calls SAS job to Load_BASEL_CUST_MTH_POSTN_SUM_FACT , Original taskID: IW503#OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM"

    OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11 = InformaticaOperator(
        task_id="OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc11",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc11",
    )
    OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc11 , Original taskID: IW502#OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11"

    OW_DM_RRII_SAS_TL_21_CU_MTH_POSTN_SUM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_SAS_TL_21_CU_MTH_POSTN_SUM",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2102_BASEL_TL_CUST_MTH_POSTN_SUM"
        sas_file="J_RRAP_TL10_2102_BASEL_TL_CUST_MTH_POSTN_SUM.sas",
    )
    OW_DM_RRII_SAS_TL_21_CU_MTH_POSTN_SUM.doc = (
        "scorecard - J_RRAP_TL10_2102_BASEL_TL_CUST_MTH_POSTN_SUM"
    )

    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2104_BASEL_CC_ACCT_SCORECRD_DRVD_VARS"
        sas_file="J_RRII_KS10_2104_BASEL_CC_ACCT_SCORECRD_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV.doc = (
        "Calls SAS job J_RRII_KS10_2104_BASEL_CC_ACCT_SCORECRD_DRVD_VARS"
    )

    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_CUST_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_LOAD_BSL_CC_CUST_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2104_BASEL_CC_CUST_SCORECRD_DRVD_VARS"
        sas_file="J_RRII_KS10_2104_BASEL_CC_CUST_SCORECRD_DRVD_VARS.sas",
    )
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_CUST_DRV.doc = (
        "Calls SAS job  J_RRII_KS10_2104_BASEL_CC_CUST_SCORECRD_DRVD_VARS"
    )

    OW_DM_RRIIH21_SAS_BSL_STP_PL_BS_DRV_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_BSL_STP_PL_BS_DRV_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2102_BASEL_STEP_PLN_BASE_DRVD_VARS_CMA"
        sas_file="J_RRAP_KS10_2102_BASEL_STEP_PLN_BASE_DRVD_VARS_CMA.sas",
    )
    OW_DM_RRIIH21_SAS_BSL_STP_PL_BS_DRV_CMA.doc = (
        "Calls SAS JOB J_RRAP_KS10_2102_BASEL_STEP_PLN_BASE_DRVD_VARS_CMA "
    )

    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIIH21_SAS_BSL_STP_PL_BS_DRV_CMA

    OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS_BASEL_REVLVNG_CR_ACCT_DRVD_VARS_CMA"
        sas_file="J_RRII_KS_BASEL_REVLVNG_CR_ACCT_DRVD_VARS_CMA.sas",
    )
    OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA.doc = (
        "J_RRII_KS_BASEL_REVLVNG_CR_ACCT_DRVD_VARS_CMA"
    )

    OW_DM_RRII_SAS_TL_21_CU_MTH_DEP_TXN_SUM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_SAS_TL_21_CU_MTH_DEP_TXN_SUM",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2103_BASEL_TL_CUST_MTH_DEP_TXN_SUM"
        sas_file="J_RRAP_TL10_2103_BASEL_TL_CUST_MTH_DEP_TXN_SUM.sas",
    )
    OW_DM_RRII_SAS_TL_21_CU_MTH_DEP_TXN_SUM.doc = (
        "scorecard - J_RRAP_TL10_2103_BASEL_TL_CUST_MTH_DEP_TXN_SUM"
    )
    
    OW_DM_RRIIH21_SAS_PIT_STAT_PRE_STEP_DFLT >> OW_DM_RRII_SAS_TL_21_CU_MTH_POSTN_SUM
    OW_DM_RRII_SAS_TL_21_CU_MTH_POSTN_SUM >> OW_DM_RRII_SAS_TL_21_CU_MTH_DEP_TXN_SUM
    OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV
    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV
    OW_DM_RRII_SAS_TL_21_CU_MTH_DEP_TXN_SUM >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV
    OW_DM_RRII_SAS_TL_21_CU_MTH_POSTN_SUM >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_ACCT_DRV >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_CUST_DRV
    OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD2 >> OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIIH21_SAS_BSL_CUST_TXN_SUM
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIIH21_SAS_BSL_STP_PL_BS_DRV_CMA
    OW_DM_RRIIH21_SAS_BSL_STP_PL_BS_DRV_CMA >> OW_DM_RRIIH21_SAS_LOAD_BSL_STP_PL_BS_DRV
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV
    OW_DM_RRIH21_SAS_LOAD_BSL_PSNL_LOAN_DRV >> OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA
    OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI >> OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_RCDV >> OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA
    OW_DM_RRIIH21_SAS_LOAD_BSL_MOR_ACCT_DRV >> OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA
    OW_DM_RRIIH21_SAS_LOAD_BSL_STP_PL_BS_DRV >> OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA
    OW_DM_RRIIH21_SAS_BSL_RVL_CR_DRV_CMA >> OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CR_DRV
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIIH21_SAS_LOAD_BSL_MOR_ACCT_DRV
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIH21_SAS_LOAD_BSL_PSNL_LOAN_DRV
    OW_DM_RRIIH21_SAS_BSL_CUST_TXN_SUM >> OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV
    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CR_DRV >> OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV
    OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV >> OW_DM_RRIIH21_INFA_LOAD_BSL_CUST_SCR_DRV
    OW_DM_RRIIH21_SAS_LOAD_BSL_CUST_STP_DRV >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_CSDV
    OW_DM_RRIIH21_SAS_LOAD_BSL_RVL_CRBS_DRV >> OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_RCDV
    OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11 >> OW_DM_RRIIH21_INFA_SANITY_CHECK_H21
    OW_DM_RRIIH21_INFA_SANITY_CHECK_H21 >> OW_DM_RRIIDRO_INFA_SANITY_CHECK_NULL_H21
    OW_DM_RRIIDRO_INFA_SANITY_CHECK_NULL_H21 >> OW_DM_RRIIH21_INFA_AUDIT_DRVDVARS_BASEL
    OW_DM_RRIIH11_SAS_LOAD_BASL_CUST_SUMM >> OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD1
    OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD1 >> OW_DM_RRIIH21_INFA_LOD_CB_CUST_XREF_LD2
    OW_DM_RRIIH21_INFA_LOAD_TRANS_UNION_DELI >> OW_DM_RRIIH21_INFA_LOAD_TRANS_DELI_AUDIT
    OW_DM_RRIIH21_INFA_LOAD_BSL_CUST_SCR_DRV >> OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11
    OW_DM_RRIIH21_SAS_LOAD_BSL_CC_LOC_CSDV >> OW_DM_RRIIBSL_INFA_LOAD_SANITY_CHECK_H11
