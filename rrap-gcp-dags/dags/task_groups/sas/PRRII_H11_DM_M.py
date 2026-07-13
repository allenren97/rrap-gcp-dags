from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator

from task_groups.sas.BSL_ACT_PRFM_FACT import BSL_ACT_PRFM_FACT


@task_group(group_id='H11_DM_M')
def H11_DM_M():
    # Operators
    OW_DM_RRIIH11_INFA_PRE_LOAD = InformaticaOperator(
        task_id="OW_DM_RRIIH11_INFA_PRE_LOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PreLoad_BASEL",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PreLoad_BASEL",
    )
    OW_DM_RRIIH11_INFA_PRE_LOAD.doc = "Calls ETL WF wf_STG_RRAP_Dm_PreLoad_BASEL , Original taskID: IW502#OW_DM_RRIIH11_INFA_PRE_LOAD"

    OW_DM_RRIIH11_SAS_KS_CTRL_TABLE = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_KS_CTRL_TABLE",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_0000_DETERMINE_RUN_DATES",
        sas_file="J_RRAP_KS10_0000_DETERMINE_RUN_DATES.sas",
    )
    OW_DM_RRIIH11_SAS_KS_CTRL_TABLE.doc = "KS CONTROL TABLE UPDATE , Original taskID: IW503#OW_DM_RRIIH11_SAS_KS_CTRL_TABLE"

    OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_START_RUN_SAS_SERVER_CLEANUP",
        sas_file="J_RRII_START_RUN_SAS_SERVER_CLEANUP.sas",
    )
    OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP.doc = "Calls SAS job for deleting old datasets on SAS server before starting ME/QE run , Original taskID: IW503#OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP"

    OW_DM_RRIIH11_SAS_LOAD_ASSET_DRC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_LOAD_ASSET_DRC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_H11_002_ASST_DRC_COST_MTH_SNAPSHOT",
        sas_file="J_RRII_H11_002_ASST_DRC_COST_MTH_SNAPSHOT.sas",
    )
    OW_DM_RRIIH11_SAS_LOAD_ASSET_DRC.doc = "Loads Direct Cost J_RRII_H11_002_ASST_DRC_COST_MTH_SNAPSHOT , Original taskID: IW503#OW_DM_RRIIH11_SAS_LOAD_ASSET_DRC"

    OW_DM_RRIIH11_SAS_LOAD_MBR_INDR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_LOAD_MBR_INDR",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_H11_003_MBR_INDRCT_COST_MTH_SNAPSHOT",
        sas_file="J_RRII_H11_003_MBR_INDRCT_COST_MTH_SNAPSHOT.sas",
    )
    OW_DM_RRIIH11_SAS_LOAD_MBR_INDR.doc = "Loads Indirect Cost J_RRII_H11_003_MBR_INDRCT_COST_MTH_SNAPSHOT , Original taskID: IW503#OW_DM_RRIIH11_SAS_LOAD_MBR_INDR"

    OW_DM_RRIIH11_SAS_LOAD_TERNT_HUS_INX = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_LOAD_TERNT_HUS_INX",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_H11_004_HOUSE_PRC_INDEX",
        sas_file="J_RRII_H11_004_HOUSE_PRC_INDEX.sas",
    )
    OW_DM_RRIIH11_SAS_LOAD_TERNT_HUS_INX.doc = "Loads Housing Index J_RRII_H11_004_HOUSE_PRC_INDEX , Original taskID: IW503#OW_DM_RRIIH11_SAS_LOAD_TERNT_HUS_INX"

    OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_H11_005_DRC_INDRC_AUDIT_CHECK",
        sas_file="J_RRII_H11_005_DRC_INDRC_AUDIT_CHECK.sas",
    )
    OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC.doc = "SAS Audit Job J_RRII_H11_005_DRC_INDRC_AUDIT_CHECK , Original taskID: IW503#OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC"

    OW_DM_RRIIH11_SAS_DRC_INDRC_POST_LOAD = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_DRC_INDRC_POST_LOAD",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_H11_006_DRC_INDRC_POST_LOAD_CHECK",
        sas_file="J_RRII_H11_006_DRC_INDRC_POST_LOAD_CHECK.sas",
    )
    OW_DM_RRIIH11_SAS_DRC_INDRC_POST_LOAD.doc = "SAS Audit Job J_RRII_H11_006_DRC_INDRC_POST_LOAD_CHECK , Original taskID: IW503#OW_DM_RRIIH11_SAS_DRC_INDRC_POST_LOAD"

    OW_DM_RRIIH11_SAS_RL_SLP_CONVERT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_RL_SLP_CONVERT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRII_RL_SLP_Conversion",
        sas_file="RRII_RL_SLP_Conversion.sas",
    )
    OW_DM_RRIIH11_SAS_RL_SLP_CONVERT.doc = "Calls SAS job for RLP TO SL conversion code , Original taskID: IW503#OW_DM_RRIIH11_SAS_RL_SLP_CONVERT"

    OW_DM_RRIIH11_SAS_TERNT_HUS_INX_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH11_SAS_TERNT_HUS_INX_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_H11_004_HOUSE_PRC_INDEX_CMA"
        sas_file="J_RRII_H11_004_HOUSE_PRC_INDEX_CMA.sas",
    )
    OW_DM_RRIIH11_SAS_TERNT_HUS_INX_CMA.doc = ("Loads Housing Index J_RRII_H11_004_HOUSE_PRC_INDEX_CMA")

    # Airflow/Python replacement for SAS BSL_ACT_PRFM_FACT job that fails to connect in production
    OW_DM_RRII_BSL_ACT_PRFM_FACT = BSL_ACT_PRFM_FACT()
    
    OW_DM_RRIIH11_INFA_PRE_LOAD >> OW_DM_RRIIH11_SAS_KS_CTRL_TABLE
    OW_DM_RRIIH11_SAS_KS_CTRL_TABLE >> OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP
    OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP >> OW_DM_RRIIH11_SAS_LOAD_ASSET_DRC
    OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP >> OW_DM_RRIIH11_SAS_LOAD_MBR_INDR
    OW_DM_RRIIH11_SAS_PREVDATA_CLEANUP >> OW_DM_RRIIH11_SAS_TERNT_HUS_INX_CMA
    OW_DM_RRIIH11_SAS_TERNT_HUS_INX_CMA >> OW_DM_RRIIH11_SAS_LOAD_TERNT_HUS_INX
    OW_DM_RRIIH11_SAS_LOAD_ASSET_DRC >> OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC
    OW_DM_RRIIH11_SAS_LOAD_MBR_INDR >> OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC
    OW_DM_RRIIH11_SAS_SANITY_CHECK_DRC_INDRC >> OW_DM_RRIIH11_SAS_DRC_INDRC_POST_LOAD
    OW_DM_RRIIH11_SAS_DRC_INDRC_POST_LOAD >> OW_DM_RRIIH11_SAS_RL_SLP_CONVERT
    OW_DM_RRIIH11_SAS_RL_SLP_CONVERT >> OW_DM_RRII_BSL_ACT_PRFM_FACT
