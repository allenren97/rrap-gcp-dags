from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="MO_BNSTG_3_2")
def MO_BNSTG_3_2():
    
    OW_SAS_DM_RRII_GCP_MOR_SCORE_SEG = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_GCP_MOR_SCORE_SEG",
        sas_file="RRAP_GCP_MOR_SCORE_SEG.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_GCP_MOR_SCORE_SEG.sas",
    )
    OW_SAS_DM_RRII_GCP_MOR_SCORE_SEG.doc = "RRAP_GCP_MOR_SCORE_SEG.sas , Original taskID: IW503#OW_SAS_DM_RRII_GCP_MOR_SCORE_SEG"  
    
    OW_RRII_LGD_UPDATE_MOR_LGD_ND = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_MOR_LGD_ND",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_MOR_LGD_ND",
        sas_file="RRAP_LGD_UPDATE_MOR_LGD_ND.sas",
    )
    OW_RRII_LGD_UPDATE_MOR_LGD_ND.doc = (
        "Update LGDND MOR , Original taskID: IW503#OW_RRII_LGD_UPDATE_MOR_LGD_ND"
    )

    OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD",
        sas_file="RRAP_MOR_MODEL_15_BNS_MOR_LGD_ND_REALIZED_LGD.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_15_BNS_MOR_LGD_ND_REALIZED_LGD",
    )
    OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD.doc = "RRAP_MOR_MODEL_15_BNS_MOR_LGD_ND_REALIZED_LGD , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD" 

    OW_RRII_LGD_UPDATE_MOR_LGD_D = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_MOR_LGD_D",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_MOR_LGD_D",
        sas_file="RRAP_LGD_UPDATE_MOR_LGD_D.sas",
    )
    OW_RRII_LGD_UPDATE_MOR_LGD_D.doc = (
        "Update LGDD MOR , Original taskID: IW503#OW_RRII_LGD_UPDATE_MOR_LGD_D"
    )
    
    OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD",
        sas_file="RRAP_MOR_MODEL_25_BNS_MOR_LGD_D_REALIZED_LGD.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_25_BNS_MOR_LGD_D_REALIZED_LGD",
    )
    OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD.doc = "RRAP_MOR_MODEL_25_BNS_MOR_LGD_D_REALIZED_LGD , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD"

    OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN",
        sas_file="RRAP_MOR_MODEL_55_LOAD_PRE_06_LARGE_JOIN.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_55_LOAD_PRE_06_LARGE_JOIN",
    )
    OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN.doc = "RRAP_MOR_MODEL_55_LOAD_PRE_06_LARGE_JOIN , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN"

    OW_SAS_DM_RRII_MORMOD57_DEFAULT_MONTHS_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD57_DEFAULT_MONTHS_G",
        sas_file="RRAP_MOR_MODEL_57_DEFAULT_MONTHS_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_57_DEFAULT_MONTHS_G",
    )
    OW_SAS_DM_RRII_MORMOD57_DEFAULT_MONTHS_G.doc = "RRAP_MOR_MODEL_57_DEFAULT_MONTHS_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD57_DEFAULT_MONTHS_G"
    

    OW_SAS_DM_RRII_GCP_MOR_SCORE_SEG >> OW_RRII_LGD_UPDATE_MOR_LGD_D
    OW_SAS_DM_RRII_GCP_MOR_SCORE_SEG >>  OW_RRII_LGD_UPDATE_MOR_LGD_ND
    OW_RRII_LGD_UPDATE_MOR_LGD_D >> OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD
    OW_RRII_LGD_UPDATE_MOR_LGD_ND >> OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD
    OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD >> OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN
    OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD >> OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN
    
    [OW_SAS_DM_RRII_MORMOD25_LGDD_REALZ_LGD,OW_SAS_DM_RRII_MORMOD15_LGDND_REALZ_LGD] >> OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN
    OW_SAS_DM_RRII_MORMOD55_PRE_06_LRGE_JOIN >> OW_SAS_DM_RRII_MORMOD57_DEFAULT_MONTHS_G
                                                    