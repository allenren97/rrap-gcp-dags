from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="FRG_MOR_M2")
def FRG_MOR_M2():

    OW_SAS_DM_RRII_MORMOD65_0607_BDBE_FT_H_G = SasOperator(
        task_id="OW_SAS_DM_RRII_MORMOD65_0607_BDBE_FT_H_G",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_65_06_TO_07_LOAD_TABLE_FOR_BD_BE_FACT",
        ssh_conn_id="sas-conn",
        sas_file="RRAP_MOR_MODEL_65_06_TO_07_LOAD_TABLE_FOR_BD_BE_FACT.sas",
    )
    OW_SAS_DM_RRII_MORMOD65_0607_BDBE_FT_H_G.doc = "RRAP_MOR_MODEL_65_06_TO_07_LOAD_TABLE_FOR_BD_BE_FACT_HIST_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD65_0607_BDBE_FT_H_G"

    OW_SAS_DM_RRII_MORMOD70_RPTG_NCR_BD_FT_G = SasOperator(
        task_id="RRAP_MOR_MODEL_70_BNS_RPTING_NCR_BD_FACT_G",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_70_BNS_RPTING_NCR_BD_FACT_G",
        ssh_conn_id="sas-conn",
        sas_file="RRAP_MOR_MODEL_70_BNS_RPTING_NCR_BD_FACT_G.sas",
    )
    OW_SAS_DM_RRII_MORMOD70_RPTG_NCR_BD_FT_G.doc = "RRAP_MOR_MODEL_70_BNS_RPTING_NCR_BD_FACT_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD70_RPTG_NCR_BD_FT_G"

    OW_SAS_TNG_II_06_07_LD_FOR_BD_BE_FACT = SasOperator(
        task_id="OW_SAS_TNG_II_06_07_LD_FOR_BD_BE_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_06_07_LOAD_TBL_FOR_BD_BE_FACT",
        ssh_conn_id="sas-conn",
        sas_file="RRAP_TNG_06_07_LOAD_TBL_FOR_BD_BE_FACT.sas",
    )
    OW_SAS_TNG_II_06_07_LD_FOR_BD_BE_FACT.doc = "RRAP_TNG_06_07_LOAD_TBL_FOR_BD_BE_FACT , Original taskID: IW503#OW_SAS_TNG_II_06_07_LD_FOR_BD_BE_FACT"

    OW_SAS_TNG_II_07_RPTING_NCR_BD_FACT = SasOperator(
        task_id="OW_SAS_TNG_II_07_RPTING_NCR_BD_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_07_RPTING_NCR_BD_FACT",
        ssh_conn_id="sas-conn",
        sas_file="RRAP_TNG_07_RPTING_NCR_BD_FACT.sas",
    )
    OW_SAS_TNG_II_07_RPTING_NCR_BD_FACT.doc = "RRAP_TNG_07_RPTING_NCR_BD_FACT , Original taskID: IW503#OW_SAS_TNG_II_07_RPTING_NCR_BD_FACT"

    OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG = InformaticaOperator(
        task_id="OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS  wf_DM_RRAP_Load_BASEL_BRIDGE_NCR_BE_BD_MOR_FRG",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_BRIDGE_NCR_BE_BD_MOR_FRG",
    )
    OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG.doc = "Load BRIDGE_NCR_BE_BD_MOR , Original taskID: IW502#OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG"

    OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_DTL_FT = InformaticaOperator(
        task_id="OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_DTL_FT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS  wf_DM_RRAP_Load_BASEL_MORT_RPTG_EXCPTN_DTL_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_MORT_RPTG_EXCPTN_DTL_FACT",
    )
    OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_DTL_FT.doc = "Load BASEL_MORT_RPTG_EXCP_DTL_FACT table , Original taskID: IW502#OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_DTL_FT"

    OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_SUM_FT = InformaticaOperator(
        task_id="OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_SUM_FT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS  wf_DM_RRAP_Load_BASEL_MORT_RPTG_EXCPTN_SUM_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_MORT_RPTG_EXCPTN_SUM_FACT",
    )
    OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_SUM_FT.doc = "Load BASEL_MORT_RPTG_EXCP_SUM_FACT , Original taskID: IW502#OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_SUM_FT"

    OW_SAS_DM_RRII_MORMOD65_0607_BDBE_FT_H_G >> OW_SAS_DM_RRII_MORMOD70_RPTG_NCR_BD_FT_G
    OW_SAS_TNG_II_06_07_LD_FOR_BD_BE_FACT >> OW_SAS_TNG_II_07_RPTING_NCR_BD_FACT
    OW_SAS_DM_RRII_MORMOD70_RPTG_NCR_BD_FT_G >> OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG
    OW_SAS_TNG_II_07_RPTING_NCR_BD_FACT >> OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG
    OW_DM_RRIIRPF_INFA_BSL_AGG_NCR_BE_F_FRG >> OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_DTL_FT
    OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_DTL_FT >> OW_DM_RRIIRPF_INFA_BSL_MOR_RPT_EX_SUM_FT
