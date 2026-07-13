from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="FRG_MOR_M")
def FRG_MOR_M():

    OW_DM_RRIIRPF_INFA_ACCT_DIM_FRG = InformaticaOperator(
        task_id="OW_DM_RRIIRPF_INFA_ACCT_DIM_FRG",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS  wf_DM_RRAP_Load_BASEL_ACCT_DIM_TNG_FRG",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_BASEL_ACCT_DIM_TNG_FRG",
    )

    OW_DM_RRIIRPF_INFA_ACCT_DIM_FRG.doc = (
        "Load Acct Dim , Original taskID: IW502#OW_DM_RRIIRPF_INFA_ACCT_DIM_FRG"
    )

    OW_DM_RRIIRPF_SAS_BSL_ANL_BL_INS_F_FRG = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIRPF_SAS_BSL_ANL_BL_INS_F_FRG",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2109_BASEL_ANALYTICL_BL_INSTRMNT_FACT_MOR_FRG",
        sas_file="J_RRII_KS10_2109_BASEL_ANALYTICL_BL_INSTRMNT_FACT_MOR_FRG.sas",
    )

    OW_DM_RRIIRPF_SAS_BSL_ANL_BL_INS_F_FRG.doc = "Calls SAS job to J_RRII_KS10_2109_BASEL_ANALYTICL_BL_INSTRMNT_FACT_MOR_FRG , Original taskID: IW503#OW_DM_RRIIRPF_SAS_BSL_ANL_BL_INS_F_FRG"

    OW_DM_RRIIRPF_INFA_ACCT_DIM_FRG >> OW_DM_RRIIRPF_SAS_BSL_ANL_BL_INS_F_FRG
