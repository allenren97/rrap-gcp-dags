from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SSHOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="RCCAR_DM_M")
def RCCAR_DM_M():

    OW_DM_RRII_RCCAR_RPT_SAS_INVALID_PRODUCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RPT_SAS_INVALID_PRODUCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_INVALID_PRODUCTS",
        sas_file="J_RRAP_INVALID_PRODUCTS.sas",
    )
    OW_DM_RRII_RCCAR_RPT_SAS_INVALID_PRODUCT.doc = "Initials email to notify the exclusion of invalid prodcuts before entering CCAR , Original taskID: IW503#OW_DM_RRII_RCCAR_RPT_SAS_INVALID_PRODUCT"

    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_dm_initial_extract",
        sas_file="rrap_dm_initial_extract.sas",
    )
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT.doc = "Exports necessary data from DB2 to AIX as SAS datasets which later act as source for SAS jobs , Original taskID: IW503#OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_FCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_FCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_ccar_expsr_fact",
        sas_file="rrap_ccar_expsr_fact.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_FCT.doc = "Calls SAS code to Load BASEL_CCAR_EXPSR_FACT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_FCT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_rptg_rvl_cr_scrd_oth_mort_fact",
        sas_file="rrap_rptg_rvl_cr_scrd_oth_mort_fact.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT.doc = "Calls SAS code to Load BASEL_RPTG_REVL_CR_SCRD_OT_MORT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_rptg_sched_2_6_fact",
        sas_file="rrap_rptg_sched_2_6_fact.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT.doc = "Calls SAS code to Load BASEL_RPTG_SCHED_2_6_FACT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_rptg_residual_maturity_fact",
        sas_file="rrap_rptg_residual_maturity_fact.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT.doc = "Calls SAS code to Load BASEL_RPTG_RESIDUAL_MAT_FACT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_rptg_scrd_oth_mort_fact",
        sas_file="rrap_rptg_scrd_oth_mort_fact.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT.doc = "Calls SAS code to Load BASEL_RPTG_SCRD_OT_MORT_FACT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_rptg_osfi_os_scrty_tp_fact",
        sas_file="rrap_rptg_osfi_os_scrty_tp_fact.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT.doc = "Calls SAS code to Load BASEL_RPTG_OSFI_OS_SCRTY_TP_FACT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_ccar_expsr_extr",
        sas_file="rrap_ccar_expsr_extr.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT.doc = "Calls SAS code to Load BASEL_CCAR_EXPSR_EXTR , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT"

    OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_ccar_pd_curve_extr",
        sas_file="rrap_ccar_pd_curve_extr.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX.doc = "Calls SAS code to Load BASEL_CCAR_PD_CURVE_EXTR , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX"

    OW_DM_RRIIDT4_CCAR_PROVISIONS_MAPPING = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_CCAR_PROVISIONS_MAPPING",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0100_CCAR_PROVISIONS_MAPPING",
        sas_file="J_RRAP_DT4_0100_CCAR_PROVISIONS_MAPPING.sas",
    )
    OW_DM_RRIIDT4_CCAR_PROVISIONS_MAPPING.doc = "Loads J_RRAP_DT4_0100_CCAR_PROVISIONS_MAPPING , Original taskID: IW503#OW_DM_RRIIDT4_CCAR_PROVISIONS_MAPPING"

    OW_DM_RRIIDT4_POST_ECL_PROVISIONS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_POST_ECL_PROVISIONS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0110_POST_ECL_PROVISIONS",
        sas_file="J_RRAP_DT4_0110_POST_ECL_PROVISIONS.sas",
    )
    OW_DM_RRIIDT4_POST_ECL_PROVISIONS.doc = "Loads J_RRAP_DT4_0110_POST_ECL_PROVISIONS , Original taskID: IW503#OW_DM_RRIIDT4_POST_ECL_PROVISIONS"

    OW_DM_RRII_RCCAR_RB_SAS_CCAR_EXP_EXT_ECL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_CCAR_EXP_EXT_ECL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_ccar_expsr_extr_ecl",
        sas_file="rrap_ccar_expsr_extr_ecl.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_CCAR_EXP_EXT_ECL.doc = "Calls SAS code to Load BASEL_CCAR_EXPSR_EXTR , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_CCAR_EXP_EXT_ECL"


    OW_DM_RRII_CCAR_ACAP_FACT_ECL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_CCAR_ACAP_FACT_ECL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_CCAR_ACAP_FACT_ECL",
        sas_file="RRAP_CCAR_ACAP_FACT_ECL.sas",
    )
    OW_DM_RRII_CCAR_ACAP_FACT_ECL.doc = "Calls SAS code to Load BASEL_CCAR_EXPSR_FACT , Original taskID: IW503#OW_DM_RRII_CCAR_ACAP_FACT_ECL"

    OW_DM_RRII_CCAR_ACAP_EXTR_ECL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_CCAR_ACAP_EXTR_ECL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_CCAR_ACAP_EXTR_ECL",
        sas_file="RRAP_CCAR_ACAP_EXTR_ECL.sas",
    )
    OW_DM_RRII_CCAR_ACAP_EXTR_ECL.doc = "Calls SAS code to Load BASEL_CCAR_EXPSR_EXTR , Original taskID: IW503#OW_DM_RRII_CCAR_ACAP_EXTR_ECL"

    OW_DM_RRII_CCAR_ACAP_FILES = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_CCAR_ACAP_FILES",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_CCAR_ACAP_FILES",
        sas_file="RRAP_CCAR_ACAP_FILES.sas",
    )
    OW_DM_RRII_CCAR_ACAP_FILES.doc = "Calls SAS code to Generate CCAR_FILES , Original taskID: IW503#OW_DM_RRII_CCAR_ACAP_FILES"

    OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_BASEL_RISK_WGHTD_AVG_FACT",
        sas_file="J_RRAP_BASEL_RISK_WGHTD_AVG_FACT.sas",
    )
    OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT.doc = "LOAD BASEL_RISK_WGHTD_AVG_FACT , Original taskID: IW503#OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT"

    OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS",
        # bash_command="echo dummy",
        sas_file="J_RRAP_CCAR_ACAP_AUDIT_CHECKS.sas",

    )
    OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS.doc = ("Added by composer. , Original taskID: IW503#OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS")

    OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_10_DATA_TRANSFER",
        sas_file="RRAP_TNG_10_DATA_TRANSFER.sas",
    )
    OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT.doc = "Calls SAS code to TRANSFER JOAAITNG FILE , Original taskID: IW503#OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT"

    OW_EM_RRII_RCCAR_SAS_RPT_CN_SEND_EMAIL = SSHOperator(
        ssh_conn_id="ssh-edw-conn",
        task_id="OW_EM_RRII_RCCAR_SAS_RPT_CN_SEND_EMAIL",
        command='{{ var.value.PSCRIPTS }}/ow_send_mail_common.ksh PRRAP_RCCAR_DM_M finish',
    )
    OW_EM_RRII_RCCAR_SAS_RPT_CN_SEND_EMAIL.doc = "Sends an email once CCAR reports are generated , Original taskID: IW502#OW_EM_RRII_RCCAR_SAS_RPT_CN_SEND_EMAIL"

    OW_ARCHIVE_RRII_RCCAR_SAS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_ARCHIVE_RRII_RCCAR_SAS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/rrap_reports_archive_ccar",
        sas_file="rrap_reports_archive_ccar.sas",
    )
    OW_ARCHIVE_RRII_RCCAR_SAS.doc = (
        "ARCHIVE CCAR REPORTS , Original taskID: IW503#OW_ARCHIVE_RRII_RCCAR_SAS"
    )

    OW_FT_RRII_TNGOUT_CN_SFTP_TO_MAILMNGR = SSHOperator(
        ssh_conn_id="ssh-edw-conn",
        task_id="OW_FT_RRII_TNGOUT_CN_SFTP_TO_MAILMNGR",
        command='{{ var.value.PSCRIPTS }}/ow_common_sftp.ksh /owprd/data ow_common_sftp_from_edw_to_mailmanager_rrap_tng.txt',
    )
    OW_FT_RRII_TNGOUT_CN_SFTP_TO_MAILMNGR.doc = "SEND TANGERINE EXTRACT VIA EMAIL , Original taskID: IW103#OW_FT_RRII_TNGOUT_CN_SFTP_TO_MAILMNGR"

    OW_DM_RRII_SPLMC_MOGAP_ADJOSBAL_SAS_EML = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_SPLMC_MOGAP_ADJOSBAL_SAS_EML",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_SPLMC_MOGAP_ADJOSBAL_SAS_EML",
        sas_file="RRAP_SPLMC_MOGAP_ADJOSBAL_SAS_EML.sas",
    )
    OW_DM_RRII_SPLMC_MOGAP_ADJOSBAL_SAS_EML.doc = "This job is to generate email for SPL Commercial Misclassification breakdown and Mortgage Gap after recon approval , Original taskID: IW503#OW_DM_RRII_SPLMC_MOGAP_ADJOSBAL_SAS_EML"

    OW_DM_RRIIDT4_RW_INSURER = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RW_INSURER",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0120_DT4_RW_INSURER"
        sas_file="J_RRAP_DT4_0120_DT4_RW_INSURER.sas",
    )
    OW_DM_RRIIDT4_RW_INSURER.doc = "Loads J_RRAP_DT4_0120_DT4_RW_INSURER"

    OW_DM_RRII_00_2_6_REPORT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_00_2_6_REPORT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_CCAR_06_RRAP_RPTG_SCHED50_2_6_FACT"
        sas_file="J_RRII_CCAR_06_RRAP_RPTG_SCHED50_2_6_FACT.sas",
    )
    OW_DM_RRII_00_2_6_REPORT.doc = "Calls SAS code to Load BASEL_RPTG_SCHED_2_6_FACT"

    OW_DM_RRII_RCCAR_RPT_SAS_INVALID_PRODUCT >> OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_FCT
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT
    OW_DM_RRII_RCCAR_RPT_SAS_INITIAL_EXTRACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT
    OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_FCT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT
    OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT
    OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT
    OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT
    OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT
    OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT
    OW_DM_RRII_CCAR_ACAP_EXTR_ECL >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX
    OW_DM_RRII_RCCAR_RB_SAS_INS_OS_SCT_TP_FT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX
    OW_DM_RRII_RCCAR_RB_SAS_INS_RC_SCR_OT_MT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX
    OW_DM_RRII_RCCAR_RB_SAS_INS_RESID_MT_FCT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX
    OW_DM_RRII_RCCAR_RB_SAS_INS_SCHD_26_FACT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX
    OW_DM_RRII_RCCAR_RB_SAS_INS_SC_OT_MR_FCT >> OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX
    OW_DM_RRII_RCCAR_RB_SAS_INS_CCAR_EXP_EXT >> OW_DM_RRIIDT4_CCAR_PROVISIONS_MAPPING
    OW_DM_RRIIDT4_CCAR_PROVISIONS_MAPPING >> OW_DM_RRIIDT4_POST_ECL_PROVISIONS
    OW_DM_RRIIDT4_POST_ECL_PROVISIONS >> OW_DM_RRII_CCAR_ACAP_FACT_ECL
    OW_DM_RRII_CCAR_ACAP_FACT_ECL >> OW_DM_RRII_CCAR_ACAP_EXTR_ECL
    OW_DM_RRII_CCAR_ACAP_EXTR_ECL >> OW_DM_RRII_CCAR_ACAP_FILES
    OW_DM_RRII_CCAR_ACAP_FILES >> OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT
    OW_DM_RRII_RCCAR_RB_SAS_INS_CCR_PD_CR_EX >> OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT
    OW_DM_RRII_RCCAR_RB_SAS_INS_BASL_RWA_FCT >> OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS
    OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS >> OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT
    OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS >> OW_DM_RRIIDT4_RW_INSURER
    OW_DM_RRIIDT4_RW_INSURER >> OW_DM_RRII_00_2_6_REPORT
    OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT >> OW_FT_RRII_TNGOUT_CN_SFTP_TO_MAILMNGR
    OW_DM_RRII_RCCAR_RPT_SAS_TNG_EXTRACT >> OW_EM_RRII_RCCAR_SAS_RPT_CN_SEND_EMAIL
    OW_DM_RRII_CCAR_ACAP_AUDIT_CHECKS >> OW_DM_RRII_SPLMC_MOGAP_ADJOSBAL_SAS_EML
