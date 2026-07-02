from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id='H24_DM_Q')
def H24_DM_Q():

    OW_DM_RRIIH24_INFA_LOAD_SEG_HIS_AVG_VAL = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_LOAD_SEG_HIS_AVG_VAL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_SEG_HISTORIC_QTR_AVG_VAL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_SEG_HISTORIC_QTR_AVG_VAL",
    )
    OW_DM_RRIIH24_INFA_LOAD_SEG_HIS_AVG_VAL.doc = "Calls ETL WF wf_DM_RRAP_Load_SEG_HISTORIC_QTR_AVG_VAL , Original taskID: IW502#OW_DM_RRIIH24_INFA_LOAD_SEG_HIS_AVG_VAL"

    OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24_Q = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24_Q",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc24_Q",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc24_Q",
    )
    OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24_Q.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc24_Q , Original taskID: IW502#OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24_Q"

    OW_DM_RRIIH24_INFA_LOAD_SEG_HIS_AVG_VAL >> OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24_Q
