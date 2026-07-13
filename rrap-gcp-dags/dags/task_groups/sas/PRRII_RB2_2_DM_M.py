from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="RB2_2_DM_M")
def RB2_2_DM_M():

    OW_DM_RRIIRB22_CCAR_AGR_INFA_PRELOAD = InformaticaOperator(
        task_id="OW_DM_RRIIRB22_CCAR_AGR_INFA_PRELOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PreLoad_Bridge_CCAR_AGR",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PreLoad_Bridge_CCAR_AGR",
    )
    OW_DM_RRIIRB22_CCAR_AGR_INFA_PRELOAD.doc = ("Check BRDM , Original taskID: IW502#OW_DM_RRIIRB22_CCAR_AGR_INFA_PRELOAD")

    OW_DM_RRIIBR22_CCAR_AGR_SAS_LOAD = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIBR22_CCAR_AGR_SAS_LOAD",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_BASEL_CCAR_BUS_AGGRTD_FACT",
        sas_file="J_RRAP_BASEL_CCAR_BUS_AGGRTD_FACT.sas",
    )
    OW_DM_RRIIBR22_CCAR_AGR_SAS_LOAD.doc = "Calls SAS job J_RRAP_BASEL_CCAR_BUS_AGGRTD_FACT  , Original taskID: IW503#OW_DM_RRIIBR22_CCAR_AGR_SAS_LOAD"

    OW_DM_RRIIBR22_CCAR_AGR_INFA_POSTLOAD = InformaticaOperator(
        task_id="OW_DM_RRIIBR22_CCAR_AGR_INFA_POSTLOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Bridge_CCAR_AGR",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Bridge_CCAR_AGR",
    )
    OW_DM_RRIIBR22_CCAR_AGR_INFA_POSTLOAD.doc = "Load BR NCR AGR BRDM , Original taskID: IW502#OW_DM_RRIIBR22_CCAR_AGR_INFA_POSTLOAD"

    OW_DM_RRIIBR22_BASEL_LOAD_CNTRL_FACT = InformaticaOperator(
        task_id="OW_DM_RRIIBR22_BASEL_LOAD_CNTRL_FACT",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_BASEL_LOAD_CNTRL_FACT",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_LOAD_CNTRL_FACT",
    )
    OW_DM_RRIIBR22_BASEL_LOAD_CNTRL_FACT.doc = "Load BASEL_LOAD_CNTRL_FACT , Original taskID: IW502#OW_DM_RRIIBR22_BASEL_LOAD_CNTRL_FACT"
    
    OW_DM_RRIIRB22_CCAR_AGR_INFA_PRELOAD >> OW_DM_RRIIBR22_CCAR_AGR_SAS_LOAD
    OW_DM_RRIIBR22_CCAR_AGR_SAS_LOAD >> OW_DM_RRIIBR22_CCAR_AGR_INFA_POSTLOAD
    OW_DM_RRIIBR22_CCAR_AGR_INFA_POSTLOAD >> OW_DM_RRIIBR22_BASEL_LOAD_CNTRL_FACT
