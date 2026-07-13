
from __future__ import annotations
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.sdk import task_group

# from airflow.operators.bash import BashOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import BashOperator
from bns.rrap.operators.empty import SSHOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="DT4_M_JOBS")
def DT4_M_JOBS():

    OW_DM_RRIIDT4_RPTG_DRVD_VARS = SasOperator(
        task_id="OW_DM_RRIIDT4_RPTG_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0200_DT4_RPTG_DRVD_VARS",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_DT4_0200_DT4_RPTG_DRVD_VARS.sas"
    )
    OW_DM_RRIIDT4_RPTG_DRVD_VARS.doc = "Loads J_RRAP_DT4_0200_DT4_RPTG_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RPTG_DRVD_VARS"

    OW_DM_RRIIDT4_RT18_EST_CCAR_VARS = SasOperator(
        task_id="OW_DM_RRIIDT4_RT18_EST_CCAR_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0340_DT4_RT18_EST_CCAR_VARS",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_DT4_0340_DT4_RT18_EST_CCAR_VARS.sas"
    )
    OW_DM_RRIIDT4_RT18_EST_CCAR_VARS.doc = "Loads J_RRAP_DT4_0340_DT4_RT18_EST_CCAR_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT18_EST_CCAR_VARS"

    OW_DM_RRIIDT4_RT18_EST_ER_VARS = SasOperator(
        task_id="OW_DM_RRIIDT4_RT18_EST_ER_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0350_DT4_RT18_EST_ER_VARS",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_DT4_0350_DT4_RT18_EST_ER_VARS.sas"
    )
    OW_DM_RRIIDT4_RT18_EST_ER_VARS.doc = "Loads J_RRAP_DT4_0350_DT4_RT18_EST_ER_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT18_EST_ER_VARS"

    OW_FT_RRII_CCAR_ACAP_PREPARE_JOFILES = SSHOperator(
        ssh_conn_id="ssh-edw-conn",
        task_id="OW_FT_RRII_CCAR_ACAP_PREPARE_JOFILES",
        command='{{ var.value.PSCRIPTS }}/rrap/ow_rrap_prepare_ccar_acap_for_sftpjo.ksh {{ var.value.OW_FTP }}/cmf/outgoing/CCAR_ACAP {{ var.value.OW_FTP }}/cmf/outgoing/jofiles/CCAR_ACAP',
    )
    OW_FT_RRII_CCAR_ACAP_PREPARE_JOFILES.doc = "Prepare Combined CCAR-ACAP Extracts for SFTPJO , Original taskID: IW103#OW_FT_RRII_CCAR_ACAP_PREPARE_JOFILES"

    OW_FT_RRII_CCAR_ACAP_CN_SFTP_TO_MAILMNGR = SSHOperator(
        ssh_conn_id="ssh-edw-conn",
        task_id="OW_FT_RRII_CCAR_ACAP_CN_SFTP_TO_MAILMNGR",
        command='{{ var.value.PSCRIPTS }}/ow_common_sftp.ksh /owprd/data ow_common_sftp_from_edw_to_mailmanager_rrap_ccar_acap.txt',
    )
    OW_FT_RRII_CCAR_ACAP_CN_SFTP_TO_MAILMNGR.doc = "Calls Common SFTP to transfer Combined CCAR-ACAP Extracts thru SFTPJO , Original taskID: IW103#OW_FT_RRII_CCAR_ACAP_CN_SFTP_TO_MAILMNGR"

    OW_ARCHIVE_RRII_CCAR_ACAP_SAS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_ARCHIVE_RRII_CCAR_ACAP_SAS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_REPORTS_ARCHIVE_CCAR_ACAP",
        sas_file="RRAP_REPORTS_ARCHIVE_CCAR_ACAP.sas",
    )
    OW_ARCHIVE_RRII_CCAR_ACAP_SAS.doc = "ARCHIVE CCAR ACAP REPORTS , Original taskID: IW503#OW_ARCHIVE_RRII_CCAR_ACAP_SAS"

    OW_DM_RRIIDT4_RT18_EST_VARS = SasOperator(
        task_id="OW_DM_RRIIDT4_RT18_EST_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0360_DT4_RT18_EST_VARS",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_DT4_0360_DT4_RT18_EST_VARS.sas"
    )
    OW_DM_RRIIDT4_RT18_EST_VARS.doc = "Loads J_RRAP_DT4_0360_DT4_RT18_EST_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT18_EST_VARS"
    
    OW_DM_RRIIDT4_RPTG_DRVD_VARS >> OW_DM_RRIIDT4_RT18_EST_CCAR_VARS
    OW_DM_RRIIDT4_RT18_EST_CCAR_VARS >> OW_DM_RRIIDT4_RT18_EST_ER_VARS
    OW_DM_RRIIDT4_RT18_EST_ER_VARS >> OW_DM_RRIIDT4_RT18_EST_VARS
    OW_DM_RRIIDT4_RT18_EST_ER_VARS >> OW_FT_RRII_CCAR_ACAP_PREPARE_JOFILES
    OW_FT_RRII_CCAR_ACAP_PREPARE_JOFILES >> OW_FT_RRII_CCAR_ACAP_CN_SFTP_TO_MAILMNGR
    OW_FT_RRII_CCAR_ACAP_CN_SFTP_TO_MAILMNGR >> OW_ARCHIVE_RRII_CCAR_ACAP_SAS