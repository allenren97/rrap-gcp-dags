from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="RB2_1_D_M2")
def RB2_1_D_M2():

    OW_DM_RRIIRB21_BR_KS_INFA_POSTLOAD = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_BR_KS_INFA_POSTLOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Bridge_Base_Layer_KS_Audit",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Bridge_Base_Layer_KS_Audit",
    )
    OW_DM_RRIIRB21_BR_KS_INFA_POSTLOAD.doc = "Audit BASEL_ANALYTCL_BL_INSTRMNT_FACT_KS_SPL_MO for KS part  , Original taskID: IW502#OW_DM_RRIIRB21_BR_KS_INFA_POSTLOAD"

    OW_DM_RRIIRB21_BR_NCR_INFA_PRELOAD = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_BR_NCR_INFA_PRELOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PreLoad_Bridge_NCRBDBE",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PreLoad_Bridge_NCRBDBE",
    )
    OW_DM_RRIIRB21_BR_NCR_INFA_PRELOAD.doc = ("Check BRDM , Original taskID: IW502#OW_DM_RRIIRB21_BR_NCR_INFA_PRELOAD")

    OW_DM_RRIIRB21_INFA_BSL_AGG_NCR_BD_F = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_INFA_BSL_AGG_NCR_BD_F",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT_KS",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT_KS",
    )
    OW_DM_RRIIRB21_INFA_BSL_AGG_NCR_BD_F.doc = "Calls ETL WF wf_DM_RRAP_Load_BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT , Original taskID: IW502#OW_DM_RRIIRB21_INFA_BSL_AGG_NCR_BD_F"

    OW_DM_RRIIRB21_BR_NCR_INFA_POSTLOAD = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_BR_NCR_INFA_POSTLOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Bridge_NCRBDBE",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Bridge_NCRBDBE",
    )
    OW_DM_RRIIRB21_BR_NCR_INFA_POSTLOAD.doc = ("Load BR MO BRDM , Original taskID: IW502#OW_DM_RRIIRB21_BR_NCR_INFA_POSTLOAD")

    OW_DM_RRIIRB21_NCR_INFA_BD_KS_POSTLOAD = InformaticaOperator(
        task_id="OW_DM_RRIIRB21_NCR_INFA_BD_KS_POSTLOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT_ks",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT_ks",
    )
    OW_DM_RRIIRB21_NCR_INFA_BD_KS_POSTLOAD.doc = "Load BR BD KS BRDM , Original taskID: IW502#OW_DM_RRIIRB21_NCR_INFA_BD_KS_POSTLOAD"

    OW_STG_RRIIRB21_INFA_FACT_POST_PRCS = InformaticaOperator(
        task_id="OW_STG_RRIIRB21_INFA_FACT_POST_PRCS",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Basel_Reporting_Tables",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Basel_Reporting_Tables",
    )
    OW_STG_RRIIRB21_INFA_FACT_POST_PRCS.doc = "Calls ETL WF wf_STG_RRAP_Dm_PostLoad_Basel_Reporting_Tables , Original taskID: IW502#OW_STG_RRIIRB21_INFA_FACT_POST_PRCS"

    OW_DM_RRII_RB2_2602CC_SAS_SNAPSHOT = SasOperator(
        task_id="OW_DM_RRII_RB2_2602CC_SAS_SNAPSHOT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2602_SECURITIZATION_POST_LOAD",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_KS10_2602_SECURITIZATION_POST_LOAD.sas",
    )
    OW_DM_RRII_RB2_2602CC_SAS_SNAPSHOT.doc = "CC and CL Process After KS Instrument fact , Original taskID: IW503#OW_DM_RRII_RB2_2602CC_SAS_SNAPSHOT"
    
    OW_DM_RRII_RB2_2602CC_SAS_SNAPSHOT >> OW_DM_RRIIRB21_BR_KS_INFA_POSTLOAD
    OW_DM_RRIIRB21_BR_KS_INFA_POSTLOAD >> OW_DM_RRIIRB21_BR_NCR_INFA_PRELOAD
    OW_DM_RRIIRB21_BR_NCR_INFA_PRELOAD >> OW_DM_RRIIRB21_INFA_BSL_AGG_NCR_BD_F
    OW_DM_RRIIRB21_INFA_BSL_AGG_NCR_BD_F >> OW_DM_RRIIRB21_BR_NCR_INFA_POSTLOAD
    OW_DM_RRIIRB21_BR_NCR_INFA_POSTLOAD >> OW_DM_RRIIRB21_NCR_INFA_BD_KS_POSTLOAD
    OW_DM_RRIIRB21_NCR_INFA_BD_KS_POSTLOAD >> OW_STG_RRIIRB21_INFA_FACT_POST_PRCS
