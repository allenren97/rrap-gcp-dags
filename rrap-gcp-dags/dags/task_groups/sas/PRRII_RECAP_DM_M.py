from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import SSHOperator


@task_group(group_id='RECAP_DM_M')
def RECAP_DM_M():

    OW_DM_RRII_RECAP_RB_SAS_INS_ECONM_CPT_EX = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RECAP_RB_SAS_INS_ECONM_CPT_EX",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_basel_econmc_captl_extract",
        sas_file="rrap_basel_econmc_captl_extract.sas",
    )
    OW_DM_RRII_RECAP_RB_SAS_INS_ECONM_CPT_EX.doc = "Calls SAS code to Load BASEL_ECONMC_CAPTL_EXTR , Original taskID: IW503#OW_DM_RRII_RECAP_RB_SAS_INS_ECONM_CPT_EX"

    OW_DM_RRII_CR9_QTR_END = SasOperator(
        task_id="OW_DM_RRII_CR9_QTR_END",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_RPTG_QTR_END_CR9",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_RPTG_QTR_END_CR9.sas",
    )
    OW_DM_RRII_CR9_QTR_END.doc = "Added by composer. , Original taskID: IW503#OW_DM_RRII_CR9_QTR_END"

    OW_DM_RRII_RECAP_SAS_INS_ECN_CPT_NCREX = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RECAP_SAS_INS_ECN_CPT_NCREX",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_basel_econmc_captl_ncr_extract",
        sas_file="rrap_basel_econmc_captl_ncr_extract.sas",
    )
    OW_DM_RRII_RECAP_SAS_INS_ECN_CPT_NCREX.doc = "Calls SAS code to Load BASEL_ECONMC_CAPTL_NCR_EXTR , Original taskID: IW503#OW_DM_RRII_RECAP_SAS_INS_ECN_CPT_NCREX"

    OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_NCR_FL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_NCR_FL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_basel_econmc_captl_files",
        sas_file="rrap_basel_econmc_captl_files.sas",
    )
    OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_NCR_FL.doc = "Calls SAS code to Generate ECO_CAPTL_NCR_BCR_AND_DEL_FILES , Original taskID: IW503#OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_NCR_FL"

    OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_basel_econmc_captl_ncr_files",
        sas_file="rrap_basel_econmc_captl_ncr_files.sas",
    )
    OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES.doc = "Calls SAS code to Generate ECO_CAPTL_BCR_AND_DEL_FILES , Original taskID: IW503#OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES"

    OW_FT_RRII_RECAP_SAS_RPT_CONDIR_EXTR = SSHOperator(
        ssh_conn_id="ssh-edw-conn",
        task_id="OW_FT_RRII_RECAP_SAS_RPT_CONDIR_EXTR",
        command='{{ var.value.PSCRIPTS }}/rrap/ow_rrap_ecap_connect_direct.ksh {{ var.value.OW_FTP }}/mv/outgoing tork162',
    )
    OW_FT_RRII_RECAP_SAS_RPT_CONDIR_EXTR.doc = "Calls AIX code to Generate ECAP_CD_EXTRACT , Original taskID: IW103#OW_FT_RRII_RECAP_SAS_RPT_CONDIR_EXTR"

    OW_EM_RRII_RECAP_SAS_RPT_CN_SEND_EMAIL = SSHOperator(
        ssh_conn_id="ssh-edw-conn",
        task_id="OW_EM_RRII_RECAP_SAS_RPT_CN_SEND_EMAIL",
        command='{{ var.value.PSCRIPTS }}/ow_send_mail_common.ksh PRRAP_RECAP_DM_M finish',
    )
    OW_EM_RRII_RECAP_SAS_RPT_CN_SEND_EMAIL.doc = "Sends an email once ECAP reports are generated , Original taskID: IW502#OW_EM_RRII_RECAP_SAS_RPT_CN_SEND_EMAIL"

    OW_ARCHIVE_RRII_RECAP_SAS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_ARCHIVE_RRII_RECAP_SAS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_reports_archive_ecap",
        sas_file="rrap_reports_archive_ecap.sas",
    )
    OW_ARCHIVE_RRII_RECAP_SAS.doc = ("ARCHIVE ECAP REPORTS , Original taskID: IW503#OW_ARCHIVE_RRII_RECAP_SAS")
    
    OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES >> OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_NCR_FL
    OW_DM_RRII_RECAP_RB_SAS_INS_ECONM_CPT_EX >> OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES
    OW_DM_RRII_RECAP_SAS_INS_ECN_CPT_NCREX >> OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_FILES
    OW_DM_RRII_RECAP_RPT_SAS_GEN_ECPT_NCR_FL >> OW_FT_RRII_RECAP_SAS_RPT_CONDIR_EXTR
    OW_FT_RRII_RECAP_SAS_RPT_CONDIR_EXTR >> [OW_EM_RRII_RECAP_SAS_RPT_CN_SEND_EMAIL,
                                             OW_ARCHIVE_RRII_RECAP_SAS]
