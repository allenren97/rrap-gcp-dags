from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="T24_2_DM_M")
def T24_2_DM_M():

    OW_DM_RRIIT24_SAS_LGD_ACCT_XREF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_LGD_ACCT_XREF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF",
        sas_file="J_RRAP_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF.sas",
    )
    OW_DM_RRIIT24_SAS_LGD_ACCT_XREF.doc = "J_RRAP_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF , Original taskID: IW503#OW_DM_RRIIT24_SAS_LGD_ACCT_XREF"

    OW_RRII_LGD_UPDATE_IND_COST_MOR_AND_SPL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_IND_COST_MOR_AND_SPL",
        # bash_command="echo ^SCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_IND_COST_MOR_AND_SPL",
        sas_file="RRAP_LGD_UPDATE_IND_COST_MOR_AND_SPL.sas",
    )
    OW_RRII_LGD_UPDATE_IND_COST_MOR_AND_SPL.doc = "Creates IND_COST used by MOR and SPL , Original taskID: IW503#OW_RRII_LGD_UPDATE_IND_COST_MOR_AND_SPL"

    OW_RRII_LGD_UPDATE_SPL_LGD_D = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_SPL_LGD_D",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_SPL_LGD_D",
        sas_file="RRAP_LGD_UPDATE_SPL_LGD_D.sas",
    )
    OW_RRII_LGD_UPDATE_SPL_LGD_D.doc = (
        "Update LGDD SPL , Original taskID: IW503#OW_RRII_LGD_UPDATE_SPL_LGD_D"
    )

    OW_RRII_LGD_UPDATE_SPL_LGD_ND = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_SPL_LGD_ND",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_SPL_LGD_ND",
        sas_file="RRAP_LGD_UPDATE_SPL_LGD_ND.sas",
    )
    OW_RRII_LGD_UPDATE_SPL_LGD_ND.doc = (
        "Update LGDND SPL , Original taskID: IW503#OW_RRII_LGD_UPDATE_SPL_LGD_ND"
    )

    OW_DM_RRIIT24_SAS_LGDD_RLZ_VAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_LGDD_RLZ_VAL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2402_BASEL_PNL_LN_LGDD_SEG_MTH_RLZ_VAL",
        sas_file="J_RRAP_TL10_2402_BASEL_PNL_LN_LGDD_SEG_MTH_RLZ_VAL.sas",
    )
    OW_DM_RRIIT24_SAS_LGDD_RLZ_VAL.doc = "J_RRAP_TL10_2402_BASEL_PNL_LN_LGDD_SEG_MTH_RLZ_VAL , Original taskID: IW503#OW_DM_RRIIT24_SAS_LGDD_RLZ_VAL"

    OW_DM_RRIIT24_SAS_LGDND_RLZ_VAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_LGDND_RLZ_VAL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL",
        sas_file="J_RRAP_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL.sas",
    )
    OW_DM_RRIIT24_SAS_LGDND_RLZ_VAL.doc = "J_RRAP_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL , Original taskID: IW503#OW_DM_RRIIT24_SAS_LGDND_RLZ_VAL"

    OW_DM_RRIIT24_SAS_PD_RLZ_VAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_PD_RLZ_VAL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2402_BASEL_PNL_LN_PD_SEG_MTH_RLZ_VAL",
        sas_file="J_RRAP_TL10_2402_BASEL_PNL_LN_PD_SEG_MTH_RLZ_VAL.sas",
    )
    OW_DM_RRIIT24_SAS_PD_RLZ_VAL.doc = "J_RRAP_TL10_2402_BASEL_PNL_LN_PD_SEG_MTH_RLZ_VAL , Original taskID: IW503#OW_DM_RRIIT24_SAS_PD_RLZ_VAL"

    OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2405_BASEL_PNL_LN_PD_SEG_HIS_QTR_AVG_VAL",
        sas_file="J_RRAP_TL10_2405_BASEL_PNL_LN_PD_SEG_HIS_QTR_AVG_VAL.sas",
    )
    OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL.doc = "J_RRAP_TL10_2405_BASEL_PNL_LN_PD_SEG_HIS_QTR_AVG_VAL , Original taskID: IW503#OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL"

    OW_DM_RRIIT24_SAS_LGD_HIS_QTR_AVG_VAL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIT24_SAS_LGD_HIS_QTR_AVG_VAL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2404_BASEL_PNL_LN_LGD_SEG_HIS_QTR_AVG_VAL",
        sas_file="J_RRAP_TL10_2404_BASEL_PNL_LN_LGD_SEG_HIS_QTR_AVG_VAL.sas",
    )
    OW_DM_RRIIT24_SAS_LGD_HIS_QTR_AVG_VAL.doc = "J_RRAP_TL10_2404_BASEL_PNL_LN_LGD_SEG_HIS_QTR_AVG_VAL , Original taskID: IW503#OW_DM_RRIIT24_SAS_LGD_HIS_QTR_AVG_VAL"

    OW_DM_RRIIT24_SAS_LGD_ACCT_XREF >> OW_RRII_LGD_UPDATE_IND_COST_MOR_AND_SPL
    OW_RRII_LGD_UPDATE_IND_COST_MOR_AND_SPL >> OW_RRII_LGD_UPDATE_SPL_LGD_D
    OW_RRII_LGD_UPDATE_SPL_LGD_D >> OW_RRII_LGD_UPDATE_SPL_LGD_ND
    OW_RRII_LGD_UPDATE_SPL_LGD_ND >> OW_DM_RRIIT24_SAS_LGDD_RLZ_VAL
    OW_DM_RRIIT24_SAS_LGDD_RLZ_VAL >> OW_DM_RRIIT24_SAS_LGDND_RLZ_VAL
    OW_DM_RRIIT24_SAS_LGD_ACCT_XREF >> OW_DM_RRIIT24_SAS_PD_RLZ_VAL
    OW_DM_RRIIT24_SAS_LGDND_RLZ_VAL >> OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL
    OW_DM_RRIIT24_SAS_PD_RLZ_VAL >> OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL
    OW_DM_RRIIT24_SAS_PD_HIS_QTR_AVG_VAL >> OW_DM_RRIIT24_SAS_LGD_HIS_QTR_AVG_VAL
