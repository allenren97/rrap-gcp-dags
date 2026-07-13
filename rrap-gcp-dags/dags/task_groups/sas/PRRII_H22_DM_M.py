from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="H22_DM_M")
def H22_DM_M():
    OW_DM_RRIIH22_INFA_LOAD_DRV_M_AST_DR_CST = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_DRV_M_AST_DR_CST",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_DRVD_MTH_ASST_DRC_COST",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_DRVD_MTH_ASST_DRC_COST",
    )
    OW_DM_RRIIH22_INFA_LOAD_DRV_M_AST_DR_CST.doc = "Calls SAS code to Load wf_DM_RRAP_Load_DRVD_MTH_ASST_DRC_COST  , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_DRV_M_AST_DR_CST"

    OW_DM_RRIIH22_INFA_LOAD_DRV_M_DRC_CST = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_DRV_M_DRC_CST",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_DRVD_MTH_DRC_COST",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_DRVD_MTH_DRC_COST",
    )
    OW_DM_RRIIH22_INFA_LOAD_DRV_M_DRC_CST.doc = "Calls ETL WF wf_DM_RRAP_Load_DRVD_MTH_DRC_COST , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_DRV_M_DRC_CST"

    OW_DM_RRIIH22_INFA_LOAD_DRV_M_AST_DR_CST >> OW_DM_RRIIH22_INFA_LOAD_DRV_M_DRC_CST
