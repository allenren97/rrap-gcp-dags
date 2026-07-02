from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id='RNCR_DM_M')
def RNCR_DM_M():

    OW_DM_RRII_RNCR_RPT_SAS_INITIAL_EXTRACT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RNCR_RPT_SAS_INITIAL_EXTRACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_dm_initial_extract",
        sas_file="rrap_dm_initial_extract.sas",
    )
    OW_DM_RRII_RNCR_RPT_SAS_INITIAL_EXTRACT.doc = "Exports necessary data from DB2 to AIX as SAS datasets which later act as source for SAS jobs , Original taskID: IW503#OW_DM_RRII_RNCR_RPT_SAS_INITIAL_EXTRACT"

    OW_DM_RRII_RNCR_RB_SAS_INS_CD_RL_N_BD_FT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RNCR_RB_SAS_INS_CD_RL_N_BD_FT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_canadn_rtl_bd_fact",
        sas_file="rrap_canadn_rtl_bd_fact.sas",
    )
    OW_DM_RRII_RNCR_RB_SAS_INS_CD_RL_N_BD_FT.doc = "Calls SAS code to Load BASEL_CANDN_RTL_NCR_BD_FACT , Original taskID: IW503#OW_DM_RRII_RNCR_RB_SAS_INS_CD_RL_N_BD_FT"

    OW_DM_RRII_RNCR_RB_SAS_INS_P1_EXTR_TABLE = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RNCR_RB_SAS_INS_P1_EXTR_TABLE",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_basel_p1_series_extract",
        sas_file="rrap_basel_p1_series_extract.sas",
    )
    OW_DM_RRII_RNCR_RB_SAS_INS_P1_EXTR_TABLE.doc = "Calls SAS code to Load BASEL_P1_*.* , Original taskID: IW503#OW_DM_RRII_RNCR_RB_SAS_INS_P1_EXTR_TABLE"

    OW_DM_RRII_RNCR_RB_SAS_INS_BCAR_ATT_FCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RNCR_RB_SAS_INS_BCAR_ATT_FCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_bcar_attesttn_fact",
        sas_file="rrap_bcar_attesttn_fact.sas",
    )
    OW_DM_RRII_RNCR_RB_SAS_INS_BCAR_ATT_FCT.doc = "Calls SAS code to Load BASEL_NCR_BCAR_ATTESTTN_FACT , Original taskID: IW503#OW_DM_RRII_RNCR_RB_SAS_INS_BCAR_ATT_FCT"

    OW_DM_RRII_RNCR_RPT_SAS_GEN_AE_BD_EXTR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RNCR_RPT_SAS_GEN_AE_BD_EXTR",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_basel_p1_series_files",
        sas_file="rrap_basel_p1_series_files.sas",
    )
    OW_DM_RRII_RNCR_RPT_SAS_GEN_AE_BD_EXTR.doc = "Calls SAS code to Generate AE_BD_EXTRACT , Original taskID: IW503#OW_DM_RRII_RNCR_RPT_SAS_GEN_AE_BD_EXTR"

    OW_DM_RRII_RNCR_RPT_VALIDATIONS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RNCR_RPT_VALIDATIONS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_ncr_validation",
        sas_file="rrap_ncr_validation.sas",
    )
    OW_DM_RRII_RNCR_RPT_VALIDATIONS.doc = (
        "rrap_ncr_validation , Original taskID: IW503#OW_DM_RRII_RNCR_RPT_VALIDATIONS"
    )

    OW_ARCHIVE_RRII_RNCR_SAS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_ARCHIVE_RRII_RNCR_SAS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_reports_archive_ncr",
        sas_file="rrap_reports_archive_ncr.sas",
    )
    OW_ARCHIVE_RRII_RNCR_SAS.doc = ("rrap_reports_archive_ncr , Original taskID: IW503#OW_ARCHIVE_RRII_RNCR_SAS")

    OW_DM_RRII_NCR_BD_ATTES_TACTICAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_NCR_BD_ATTES_TACTICAL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_BASEL_NCR_BD_ATTES_TACTICAL"
        sas_file="J_RRII_BASEL_NCR_BD_ATTES_TACTICAL.sas",
    )
    OW_DM_RRII_NCR_BD_ATTES_TACTICAL.doc = "J_RRII_BASEL_NCR_BD_ATTES_TACTICAL.sas"

    OW_DM_RRII_RNCR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RNCR_RB_SAS_INS_CD_RL_N_BD_FT
    OW_DM_RRII_RNCR_RB_SAS_INS_CD_RL_N_BD_FT >> OW_DM_RRII_RNCR_RB_SAS_INS_P1_EXTR_TABLE
    OW_DM_RRII_RNCR_RB_SAS_INS_P1_EXTR_TABLE >> OW_DM_RRII_RNCR_RB_SAS_INS_BCAR_ATT_FCT
    OW_DM_RRII_RNCR_RB_SAS_INS_BCAR_ATT_FCT >> OW_DM_RRII_RNCR_RPT_SAS_GEN_AE_BD_EXTR
    OW_DM_RRII_RNCR_RPT_SAS_GEN_AE_BD_EXTR >> OW_DM_RRII_RNCR_RPT_VALIDATIONS
    OW_DM_RRII_RNCR_RPT_VALIDATIONS >> OW_ARCHIVE_RRII_RNCR_SAS
