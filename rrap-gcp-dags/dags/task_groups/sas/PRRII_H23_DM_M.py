from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="H23_DM_M")
def H23_DM_M():
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_HC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_HC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC",
        sas_file="J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_HC.doc = "Calls SAS code to Load RVLVCR_SCR_DTL_SS- HELOC , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_HC"

    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC",
        sas_file="J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CC.doc = "Calls SAS code to Load RVLVCR_SCR_DTL_SS -cc , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CC"

    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_LC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_LC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC",
        sas_file="J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_LC.doc = "Calls SAS code to Load RVLVCR_SCR_DTL_SS -loc , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_LC"

    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN",
        # bash_command="echo "^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE"",
        sas_file="J_RRAP_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN.doc = "Combine data from RRAP jobs for BASEL_RCA_SCORE_DTL_SNAPSHOT , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN"

    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC",
        sas_file="J_RRAP_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC.doc = "Calls SAS code to Load RVLV_CR_SCR_SS -HELOC , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC"

    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC",
        sas_file="J_RRAP_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC.doc = "Calls SAS code to Load RVLV_CR_SCR_SS --CC , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC"

    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC",
        sas_file="J_RRAP_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC.sas",
    )
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC.doc = "Calls SAS code to Load RVLV_CR_SCR_SS -LOC , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC"



    OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2401_EAD_SEG_ACCT_XREF",
        sas_file="J_RRAP_KS10_2401_EAD_SEG_ACCT_XREF.sas",
    )

    OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF.doc = "Calls SAS code to Load EAD_SEG_ACCT_XCEF  , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF"


    OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_KS10_2401_PD_SEG_ACCT_XREF",
        sas_file="J_RRAP_KS10_2401_PD_SEG_ACCT_XREF.sas",
    )
    OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF.doc = "Calls SAS code to Load PD_SEG_ACCT_XCEF  , Original taskID: IW503#OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF"
    


    OW_DM_RRIIH22_INFA_LOAD_DRV_M_IND_CST_SM = InformaticaOperator(
        task_id="OW_DM_RRIIH22_INFA_LOAD_DRV_M_IND_CST_SM",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_DRVD_MTH_INDRCT_COST_SUM",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_DRVD_MTH_INDRCT_COST_SUM",
    )
    OW_DM_RRIIH22_INFA_LOAD_DRV_M_IND_CST_SM.doc = "Calls ETL WF wf_DM_RRAP_Load_DRVD_MTH_INDRCT_COST_SUM , Original taskID: IW502#OW_DM_RRIIH22_INFA_LOAD_DRV_M_IND_CST_SM"


    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CC >> OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_HC >> OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_LC >> OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN
    OW_DM_CN_SAS_RRIIH23_RCA_SCR_DTL_SS_CMBN >> OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC >> OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC >> OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC >> OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC >> OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC >> OW_DM_CN_SAS_RRIIH23_EAD_SEG_ACCT_XCEF   
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_CC >> OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_HC >> OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF
    OW_DM_CN_SAS_RRIIH23_RVLV_CR_SCR_SS_LC >> OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF
    OW_DM_CN_SAS_RRIIH23_PD_SEG_ACCT_XCEF >> OW_DM_RRIIH22_INFA_LOAD_DRV_M_IND_CST_SM