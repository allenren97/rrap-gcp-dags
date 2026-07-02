from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator

# from converted_informatica_workflows import wf_DM_RRAP_Load_BASEL_REVLVNG_CR_RPTG_DRVD_VAR

@task_group(group_id="RB2_1_DM_M")
def RB2_1_DM_M():

    OW_STG_RRIIRB21_INFA_PREPRCS_DYN_PRM = InformaticaOperator(
        task_id="OW_STG_RRIIRB21_INFA_PREPRCS_DYN_PRM",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PreLoad_Basel_Reporting_Tables",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PreLoad_Basel_Reporting_Tables",
    )
    OW_STG_RRIIRB21_INFA_PREPRCS_DYN_PRM.doc = "Calls ETL WF wf_STG_RRAP_Dm_PreLoad_Basel_Reporting_Tables , Original taskID: IW502#OW_STG_RRIIRB21_INFA_PREPRCS_DYN_PRM"

    OW_DM_RRIIRB21_INFA_BSL_RVLV_RPT_DRV = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_INFA_BSL_RVLV_RPT_DRV",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_REVLVNG_CR_RPTG_DRVD_VAR",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_REVLVNG_CR_RPTG_DRVD_VAR",
    )
    OW_DM_RRIIRB21_INFA_BSL_RVLV_RPT_DRV.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_REVLVNG_CR_RPTG_DRVD_VAR , Original taskID: IW502#OW_DM_RRIIRB21_INFA_BSL_RVLV_RPT_DRV"

    OW_DM_RRIIRB21_INFA_BSL_RLV_DRV_POST = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_INFA_BSL_RLV_DRV_POST",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_BASEL_REVLVNG_CR_RPTG_DRVD_VAR",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_BASEL_REVLVNG_CR_RPTG_DRVD_VAR",
    )
    OW_DM_RRIIRB21_INFA_BSL_RLV_DRV_POST.doc = "Calls ETL WF wf_STG_RRAP_Dm_PostLoad_BASEL_REVLVNG_CR_RPTG_DRVD_VAR , Original taskID: IW502#OW_DM_RRIIRB21_INFA_BSL_RLV_DRV_POST"

    OW_DM_RRIIRB21_SAS_KS_BSL_ANL_BL_INS_F = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIRB21_SAS_KS_BSL_ANL_BL_INS_F",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2108_BASEL_ANALYTICL_BL_INSTRMNT_FACT_KS",
        sas_file="J_RRII_KS10_2108_BASEL_ANALYTICL_BL_INSTRMNT_FACT_KS.sas",
    )
    OW_DM_RRIIRB21_SAS_KS_BSL_ANL_BL_INS_F.doc = "Calls SAS job J_RRII_KS10_2108_BASEL_ANALYTICL_BL_INSTRMNT_FACT_KS , Original taskID: IW503#OW_DM_RRIIRB21_SAS_KS_BSL_ANL_BL_INS_F"

    OW_DM_RRII_RB2_2601CL_SAS_SNAPSHOT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RB2_2601CL_SAS_SNAPSHOT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2601_CL_SECURITIZATION",
        sas_file="J_RRAP_KS10_2601_CL_SECURITIZATION.sas",
    )
    OW_DM_RRII_RB2_2601CL_SAS_SNAPSHOT.doc = "CL Process Before KS Instrument fact , Original taskID: IW503#OW_DM_RRII_RB2_2601CL_SAS_SNAPSHOT"

    OW_DM_RRII_RB2_2601CC_SAS_SNAPSHOT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_RB2_2601CC_SAS_SNAPSHOT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2601_CC_SECURITIZATION",
        sas_file="J_RRAP_KS10_2601_CC_SECURITIZATION.sas",
    )
    OW_DM_RRII_RB2_2601CC_SAS_SNAPSHOT.doc = "CC Process Before KS Instrument fact , Original taskID: IW503#OW_DM_RRII_RB2_2601CC_SAS_SNAPSHOT"

    OW_DM_RRII_RB2_2601CC_SAS_SNAPSHOT >> OW_DM_RRII_RB2_2601CL_SAS_SNAPSHOT
    OW_DM_RRIIRB21_INFA_BSL_RLV_DRV_POST >> OW_DM_RRII_RB2_2601CC_SAS_SNAPSHOT
    OW_STG_RRIIRB21_INFA_PREPRCS_DYN_PRM >> OW_DM_RRIIRB21_INFA_BSL_RVLV_RPT_DRV
    OW_DM_RRIIRB21_INFA_BSL_RVLV_RPT_DRV >> OW_DM_RRIIRB21_INFA_BSL_RLV_DRV_POST
    OW_DM_RRII_RB2_2601CL_SAS_SNAPSHOT >> OW_DM_RRIIRB21_SAS_KS_BSL_ANL_BL_INS_F
