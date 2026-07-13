from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="H24_DM_M")
def H24_DM_M():

    OW_DM_RRIIH24_INFA_LOAD_LGD_SEG_RLZVAL = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_LOAD_LGD_SEG_RLZVAL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_LGD_SEG_MTH_REALZ_VAL_LGD",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_LGD_SEG_MTH_REALZ_VAL_LGD",
    )
    OW_DM_RRIIH24_INFA_LOAD_LGD_SEG_RLZVAL.doc = "Calls ETL WF wf_DM_RRAP_Load_LGD_SEG_MTH_REALZ_VAL_LGD , Original taskID: IW502#OW_DM_RRIIH24_INFA_LOAD_LGD_SEG_RLZVAL"

    OW_DM_RRIIH24_INFA_LOAD_PD_SEG_RLZ_VAL = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_LOAD_PD_SEG_RLZ_VAL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_PD_SEG_MTH_REALZ_VAL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_PD_SEG_MTH_REALZ_VAL",
    )
    OW_DM_RRIIH24_INFA_LOAD_PD_SEG_RLZ_VAL.doc = "Calls ETL WF wf_DM_RRAP_Load_PD_SEG_MTH_REALZ_VAL , Original taskID: IW502#OW_DM_RRIIH24_INFA_LOAD_PD_SEG_RLZ_VAL"

    OW_DM_RRIIH24_INFA_LOAD_EAD_SEG_RLZ_VAL = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_LOAD_EAD_SEG_RLZ_VAL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_EAD_SEG_MTH_REALZ_VAL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_EAD_SEG_MTH_REALZ_VAL",
    )
    OW_DM_RRIIH24_INFA_LOAD_EAD_SEG_RLZ_VAL.doc = "Calls ETL WF wf_DM_RRAP_Load_EAD_SEG_MTH_REALZ_VAL , Original taskID: IW502#OW_DM_RRIIH24_INFA_LOAD_EAD_SEG_RLZ_VAL"

    OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24 = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc24",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc24",
    )
    OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc24 , Original taskID: IW502#OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24"

    OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24 = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_check_data_sanity_XREF_COUNT_CHECK_24",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_check_data_sanity_XREF_COUNT_CHECK_24",
    )
    OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24.doc = "Calls ETL WF f_DM_RRAP_check_data_sanity_XREF_COUNT_CHECK_24 , Original taskID: IW502#OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24"

    OW_DM_RRIIH24_INFA_AUDIT_H24 = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_AUDIT_H24",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Audit_2_4_BASEL",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Audit_2_4_BASEL",
    )
    OW_DM_RRIIH24_INFA_AUDIT_H24.doc = "Calls ETL WF wf_STG_RRAP_Dm_PostLoad_Audit_2_4_BASEL , Original taskID: IW502#OW_DM_RRIIH24_INFA_AUDIT_H24"

    OW_DM_RRIIH24_INFA_POST_LOAD = InformaticaOperator(
        task_id="OW_DM_RRIIH24_INFA_POST_LOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_BASEL",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_BASEL",
    )
    OW_DM_RRIIH24_INFA_POST_LOAD.doc = "Calls ETL WF wf_STG_RRAP_Dm_PostLoad_BASEL , Original taskID: IW502#OW_DM_RRIIH24_INFA_POST_LOAD"

    OW_DM_RRIIH24_INFA_LOAD_LGD_SEG_RLZVAL >> OW_DM_RRIIH24_INFA_LOAD_PD_SEG_RLZ_VAL
    OW_DM_RRIIH24_INFA_LOAD_PD_SEG_RLZ_VAL >> OW_DM_RRIIH24_INFA_LOAD_EAD_SEG_RLZ_VAL
    OW_DM_RRIIH24_INFA_LOAD_EAD_SEG_RLZ_VAL >> OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24
    OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24 >> OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24
    OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24 >> OW_DM_RRIIH24_INFA_AUDIT_H24
    OW_DM_RRIIH24_INFA_AUDIT_H24 >> OW_DM_RRIIH24_INFA_POST_LOAD
