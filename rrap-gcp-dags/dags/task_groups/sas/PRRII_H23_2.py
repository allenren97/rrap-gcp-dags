from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="H23_2")
def H23_2():

    OW_DM_CN_SAS_RRIIH23_GCP_KS_SCORE = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_GCP_KS_SCORE",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_GCP_KS_SCORE",
        sas_file="RRAP_GCP_KS_SCORE.sas",
    )
    OW_DM_CN_SAS_RRIIH23_GCP_KS_SCORE.doc = "Calls SAS code to Load RRAP_GCP_KS_SCORE , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_GCP_KS_SCORE"

    OW_DM_CN_SAS_RRIIH23_LGD_SEG_ACCT_XCEF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_LGD_SEG_ACCT_XCEF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2401_LGD_SEG_ACCT_XREF",
        sas_file="J_RRAP_KS10_2401_LGD_SEG_ACCT_XREF.sas",
    )

    OW_DM_CN_SAS_RRIIH23_LGD_SEG_ACCT_XCEF.doc = "Calls SAS code to Load LGD_SEG_ACCT_XCEF , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_LGD_SEG_ACCT_XCEF"
    
    OW_DM_CN_SAS_RRIIH23_GCP_KS_SEG = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_GCP_KS_SEG",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_GCP_KS_SEG",
        sas_file="RRAP_GCP_KS_SEG.sas",
    )
    OW_DM_CN_SAS_RRIIH23_GCP_KS_SEG.doc = "Calls SAS code to Load RRAP_GCP_KS_SEG , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_GCP_KS_SEG"    

    OW_DM_RRIIH23_SAS_LOAD_SANITY_CHECKS = InformaticaOperator(
        task_id="OW_DM_RRIIH23_SAS_LOAD_SANITY_CHECKS",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc23",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc23",
    )
    OW_DM_RRIIH23_SAS_LOAD_SANITY_CHECKS.doc = "Calls ETL WF wwf_DM_RRAP_Dm_Sanity_Check_Heloc23 , Original taskID: IW502#OW_DM_RRIIH23_SAS_LOAD_SANITY_CHECKS"

    OW_DM_RRIIH23_INFA_SNTY_CHK_SAS_LD_SCRD = InformaticaOperator(
        task_id="OW_DM_RRIIH23_INFA_SNTY_CHK_SAS_LD_SCRD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_check_data_sanity_BASEL_MODEL_SCORECRD_DTL_23",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_check_data_sanity_BASEL_MODEL_SCORECRD_DTL_23",
    )
    OW_DM_RRIIH23_INFA_SNTY_CHK_SAS_LD_SCRD.doc = "Calls ETL WF wf_DM_RRAP_check_data_sanity_BASEL_MODEL_SCORECRD_DTL_23 , Original taskID: IW502#OW_DM_RRIIH23_INFA_SNTY_CHK_SAS_LD_SCRD"


    OW_DM_RRIIH22_INFA_LOAD_RVLC_ACCT_RCVRY = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_RVLC_ACCT_RCVRY",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY",
    )
    OW_DM_RRIIH22_INFA_LOAD_RVLC_ACCT_RCVRY.doc = "Added by composer. , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_RVLC_ACCT_RCVRY"

    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCVY = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCVY",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_RCVRY",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_RCVRY",
    )
    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCVY.doc = "Calls ETL WF wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_RCVRY , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCVY"

    OW_DM_RRIIH22_SAS_LOAD_RVLC_EAD_OB_RVAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH22_SAS_LOAD_RVLC_EAD_OB_RVAL",
        sas_file="J_RRII_KS10_2610_REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL.sas",
    )
    OW_DM_RRIIH22_SAS_LOAD_RVLC_EAD_OB_RVAL.doc = "Calls SAS code to Load REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_RVLC_EAD_OB_RVAL"

    OW_DM_RRIIH22_SAS_LOAD_EAD_SEG_QTR_REALZ_VAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH22_SAS_LOAD_EAD_SEG_QTR_REALZ_VAL",
        sas_file="J_RRII_KS10_2620_EAD_SEG_QTR_REALZ_VAL.sas",
    )
    OW_DM_RRIIH22_SAS_LOAD_EAD_SEG_QTR_REALZ_VAL.doc = "Calls SAS code to Load REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_RVLC_EAD_OB_RVAL"

    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCST = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCST",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_COST",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_COST",
    )
    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCST.doc = "Calls ETL WF wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_COST , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCST"

    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RVAL = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RVAL",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_VAL",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_VAL",
    )
    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RVAL.doc = "Calls ETL WF wf_DM_RRAP_Load_REVLVNG_CR_LGD_OBSVTN_PT_REALZ_VAL , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RVAL"

    OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22 = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc22",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc22",
    )
    OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc22 , Original taskID: IW502#OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22"

    OW_DM_RRIIH22_INFA_SANITY_CHECK_NULL_H22 = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_SANITY_CHECK_NULL_H22",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_check_data_sanity_NULL_DRVD_VARS_22",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_check_data_sanity_NULL_DRVD_VARS_22",
    )
    OW_DM_RRIIH22_INFA_SANITY_CHECK_NULL_H22.doc = "Calls ETL WF wf_DM_RRAP_check_data_sanity_NULL_DRVD_VARS_22 , Original taskID: IW502#OW_DM_RRIIH22_INFA_SANITY_CHECK_NULL_H22"

    OW_DM_RRIIH22_INFA_SANITY_CHK_MISS_MTHS = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_SANITY_CHK_MISS_MTHS",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc22_FOR_MISSING_MONTHS",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc22_FOR_MISSING_MONTHS",
    )
    OW_DM_RRIIH22_INFA_SANITY_CHK_MISS_MTHS.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc22_FOR_MISSING_MONTHS , Original taskID: IW502#OW_DM_RRIIH22_INFA_SANITY_CHK_MISS_MTHS"

    OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Dm_Sanity_Check_Heloc22_FOR_WRONG_DATA_LOAD",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Dm_Sanity_Check_Heloc22_FOR_WRONG_DATA_LOAD",
    )
    OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD.doc = "Calls ETL WF wf_DM_RRAP_Dm_Sanity_Check_Heloc22_FOR_MISSING_MONTHS , Original taskID: IW502#OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD"

    OW_DM_RRIIH22_INFA_AUDIT_H22 = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_AUDIT_H22",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh STG_RRAP_IIAS wf_STG_RRAP_Dm_PostLoad_Audit_2_2_BASEL",
        ssh_conn_id="infa-stg-rrap-conn",
        infa_workflow="wf_STG_RRAP_Dm_PostLoad_Audit_2_2_BASEL",
    )
    OW_DM_RRIIH22_INFA_AUDIT_H22.doc = "Calls ETL WF wf_STG_RRAP_Dm_PostLoad_Audit_2_2_BASEL , Original taskID: IW502#OW_DM_RRIIH22_INFA_AUDIT_H22"

    OW_DM_RRIIH22_SAS_LOAD_RVLC_CROB_PT_DRV = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIH22_SAS_LOAD_RVLC_CROB_PT_DRV",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR"
        sas_file="J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas",
    )
    OW_DM_RRIIH22_SAS_LOAD_RVLC_CROB_PT_DRV.doc = (
        "Calls SAS JOB J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR"
    )


    OW_DM_CN_SAS_RRIIH23_GCP_KS_SCORE >> OW_DM_CN_SAS_RRIIH23_LGD_SEG_ACCT_XCEF
    OW_DM_CN_SAS_RRIIH23_LGD_SEG_ACCT_XCEF >> OW_DM_CN_SAS_RRIIH23_GCP_KS_SEG
    OW_DM_CN_SAS_RRIIH23_GCP_KS_SEG >> OW_DM_RRIIH23_SAS_LOAD_SANITY_CHECKS
    OW_DM_RRIIH23_SAS_LOAD_SANITY_CHECKS >> OW_DM_RRIIH23_INFA_SNTY_CHK_SAS_LD_SCRD    
    OW_DM_RRIIH23_INFA_SNTY_CHK_SAS_LD_SCRD >> OW_DM_RRIIH22_INFA_LOAD_RVLC_ACCT_RCVRY
    OW_DM_RRIIH22_INFA_LOAD_RVLC_ACCT_RCVRY >> OW_DM_RRIIH22_SAS_LOAD_RVLC_CROB_PT_DRV
    OW_DM_RRIIH22_SAS_LOAD_RVLC_CROB_PT_DRV >> OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCVY
    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCVY >> OW_DM_RRIIH22_SAS_LOAD_RVLC_EAD_OB_RVAL
    OW_DM_RRIIH22_SAS_LOAD_RVLC_EAD_OB_RVAL >> OW_DM_RRIIH22_SAS_LOAD_EAD_SEG_QTR_REALZ_VAL
    OW_DM_RRIIH22_SAS_LOAD_EAD_SEG_QTR_REALZ_VAL >> OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCST
    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RCST >> OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RVAL
    OW_DM_RRIIH22_INFA_LOAD_RVLC_LGD_OB_RVAL >> OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22
    OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22 >> OW_DM_RRIIH22_INFA_SANITY_CHECK_NULL_H22
    OW_DM_RRIIH22_INFA_SANITY_CHECK_NULL_H22 >> OW_DM_RRIIH22_INFA_SANITY_CHK_MISS_MTHS
    OW_DM_RRIIH22_INFA_SANITY_CHK_MISS_MTHS >> OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD
    OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD >> OW_DM_RRIIH22_INFA_AUDIT_H22
