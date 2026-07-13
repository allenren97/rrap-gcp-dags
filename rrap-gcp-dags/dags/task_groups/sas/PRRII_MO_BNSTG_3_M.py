from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="MO_BNSTG_3_M")
def MO_BNSTG_3_M():
    OW_SAS_DM_RRII_MORSCRD55_LGD_COSTS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD55_LGD_COSTS",
        sas_file="RRAP_MOR_SCRCD_55_LGD_COSTS.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_55_LGD_COSTS",
    )
    OW_SAS_DM_RRII_MORSCRD55_LGD_COSTS.doc = "RRAP_MOR_SCRCD_55_LGD_COSTS , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD55_LGD_COSTS"

    OW_SAS_DM_RRII_MORSCRD60_LOAD_SCRD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD60_LOAD_SCRD_VARS",
        sas_file="RRAP_MOR_SCRCD_60_LOAD_SCORECARD_VARS.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_60_LOAD_SCORECARD_VARS",
    )
    OW_SAS_DM_RRII_MORSCRD60_LOAD_SCRD_VARS.doc = "RRAP_MOR_SCRCD_60_LOAD_SCORECARD_VARS , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD60_LOAD_SCRD_VARS"

    OW_SAS_DM_RRII_MORSCRD65_UNLOAD_SCRD_V_F = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD65_UNLOAD_SCRD_V_F",
        sas_file="RRAP_MOR_SCRCD_65_UNLOAD_SCORECARD_VARS_FINAL.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_65_UNLOAD_SCORECARD_VARS_FINAL",
    )
    OW_SAS_DM_RRII_MORSCRD65_UNLOAD_SCRD_V_F.doc = "RRAP_MOR_SCRCD_65_UNLOAD_SCORECARD_VARS_FINAL , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD65_UNLOAD_SCRD_V_F"

    OW_SAS_DM_RRII_MORMOD01_DEFINE_STATUS_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD01_DEFINE_STATUS_G",
        sas_file="RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_01_DEFINE_STATUS_G",
    )
    OW_SAS_DM_RRII_MORMOD01_DEFINE_STATUS_G.doc = "RRAP_MOR_MODEL_01_DEFINE_STATUS_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD01_DEFINE_STATUS_G"

    OW_SAS_DM_RRII_MOR_01_BASELAYER_SRC_CHK = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_01_BASELAYER_SRC_CHK",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_01_BASELAYER_MOR_SOURCE_CHECK",
        sas_file="RRAP_MOR_01_BASELAYER_MOR_SOURCE_CHECK.sas",
    )
    OW_SAS_DM_RRII_MOR_01_BASELAYER_SRC_CHK.doc = "RRAP_MOR_01_BASELAYER_MOR_SOURCE_CHECK , Original taskID: IW503#OW_SAS_DM_RRII_MOR_01_BASELAYER_SRC_CHK"

    OW_SAS_DM_RRII_MORMOD02_BNS_MOR_PD_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD02_BNS_MOR_PD_G",
        sas_file="RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_02_BNS_MOR_PD_G",
    )
    OW_SAS_DM_RRII_MORMOD02_BNS_MOR_PD_G.doc = "RRAP_MOR_MODEL_02_BNS_MOR_PD_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD02_BNS_MOR_PD_G"

    OW_SAS_DM_RRII_MORMOD01A_GATHER_LOAD_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD01A_GATHER_LOAD_G",
        sas_file="RRAP_MOR_MODEL_01A_GATHER_LOAD_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_01A_GATHER_LOAD_G",
    )
    OW_SAS_DM_RRII_MORMOD01A_GATHER_LOAD_G.doc = "RRAP_MOR_MODEL_01A_GATHER_LOAD_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD01A_GATHER_LOAD_G"

    OW_SAS_DM_RRII_MORMOD50_LOAD_BRDM_TO_NZ = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD50_LOAD_BRDM_TO_NZ",
        sas_file="RRAP_MOR_MODEL_50_LOAD_BRDM_TO_NETEZZA.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_50_LOAD_BRDM_TO_NETEZZA",
    )
    OW_SAS_DM_RRII_MORMOD50_LOAD_BRDM_TO_NZ.doc = "RRAP_MOR_MODEL_50_LOAD_BRDM_TO_NETEZZA , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD50_LOAD_BRDM_TO_NZ"

    OW_SAS_DM_RRII_MORMOD03_PD_SCORE_SEG = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD03_PD_SCORE_SEG",
        sas_file="RRAP_MOR_MODEL_03_BNS_MOR_PD_SCORE_SEGMENT.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_03_BNS_MOR_PD_SCORE_SEGMENT",
    )
    OW_SAS_DM_RRII_MORMOD03_PD_SCORE_SEG.doc = "RRAP_MOR_MODEL_03_BNS_MOR_PD_SCORE_SEGMENT , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD03_PD_SCORE_SEG"

    OW_SAS_DM_RRII_MORMOD13_LGD_ND_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD13_LGD_ND_G",
        sas_file="RRAP_MOR_MODEL_13_BNS_MOR_LGD_ND_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_13_BNS_MOR_LGD_ND_G",
    )
    OW_SAS_DM_RRII_MORMOD13_LGD_ND_G.doc = "RRAP_MOR_MODEL_13_BNS_MOR_LGD_ND_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD13_LGD_ND_G"

    OW_SAS_DM_RRII_MORMOD14_LGDND_SCRE_SEG_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD14_LGDND_SCRE_SEG_G",
        sas_file="RRAP_MOR_MODEL_14_BNS_MOR_LGD_ND_SCORE_SEGMENT_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_14_BNS_MOR_LGD_ND_SCORE_SEGMENT_G",
    )
    OW_SAS_DM_RRII_MORMOD14_LGDND_SCRE_SEG_G.doc = "RRAP_MOR_MODEL_14_BNS_MOR_LGD_ND_SCORE_SEGMENT_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD14_LGDND_SCRE_SEG_G"

    OW_SAS_DM_RRII_MORMOD23_LGD_D_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD23_LGD_D_G",
        sas_file="RRAP_MOR_MODEL_23_BNS_MOR_LGD_D_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_23_BNS_MOR_LGD_D_G",
    )
    OW_SAS_DM_RRII_MORMOD23_LGD_D_G.doc = "RRAP_MOR_MODEL_23_BNS_MOR_LGD_D_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD23_LGD_D_G"

    OW_SAS_DM_RRII_MORMOD04_REALIZED_DR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD04_REALIZED_DR",
        sas_file="RRAP_MOR_MODEL_04_BNS_MOR_REALIZED_DR.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_04_BNS_MOR_REALIZED_DR",
    )
    OW_SAS_DM_RRII_MORMOD04_REALIZED_DR.doc = "RRAP_MOR_MODEL_04_BNS_MOR_REALIZED_DR , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD04_REALIZED_DR"

    OW_SAS_DM_RRII_MORMOD24_LGDD_SCORE_SEG_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD24_LGDD_SCORE_SEG_G",
        sas_file="RRAP_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT_G",
    )
    OW_SAS_DM_RRII_MORMOD24_LGDD_SCORE_SEG_G.doc = "RRAP_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD24_LGDD_SCORE_SEG_G"

    OW_RRII_LGD_UPDT_MOR_ACCT_LVL_DATA_RCVRY = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDT_MOR_ACCT_LVL_DATA_RCVRY",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_MOR_ACCT_LVL_DATA_AND_RECOVERY_CALCS",
        sas_file="RRAP_LGD_UPDATE_MOR_ACCT_LVL_DATA_AND_RECOVERY_CALCS.sas",
    )
    OW_RRII_LGD_UPDT_MOR_ACCT_LVL_DATA_RCVRY.doc = "Update LGD MOR ACCT LVL DATA AND RECOVERY CALCULATION , Original taskID: IW503#OW_RRII_LGD_UPDT_MOR_ACCT_LVL_DATA_RCVRY"
    
    OW_DM_RRII_BSL_ACCT_SAS_GCP_APND_MOR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_BSL_ACCT_SAS_GCP_APND_MOR",
        sas_file="J_RRAP_GCP_APPEND_BASEL_ACCT_ID_MOR.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_GCP_APPEND_BASEL_ACCT_ID_MOR",
    )
    OW_DM_RRII_BSL_ACCT_SAS_GCP_APND_MOR.doc = "Adds BASEL_ACCT_ID to MOR Tables for GCP , Original taskID: IW503#OW_DM_RRII_BSL_ACCT_SAS_GCP_APND_MOR"
    
    OW_SAS_10_LGD_INGRESS_AUDIT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_10_LGD_INGRESS_AUDIT",
        sas_file="J_RRII_10_LGD_INGRESS_AUDIT.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_10_LGD_INGRESS_AUDIT.sas",
    )
    OW_SAS_10_LGD_INGRESS_AUDIT.doc = "Counts number of records to be replicated into GCP for LGD scoring and segmentation , Original taskID: NEW!" 
    
    



    

    OW_SAS_DM_RRII_MORSCRD55_LGD_COSTS >> OW_SAS_DM_RRII_MORSCRD60_LOAD_SCRD_VARS
    OW_SAS_DM_RRII_MORSCRD60_LOAD_SCRD_VARS >> OW_SAS_DM_RRII_MORSCRD65_UNLOAD_SCRD_V_F
    OW_SAS_DM_RRII_MORSCRD65_UNLOAD_SCRD_V_F >> [OW_SAS_DM_RRII_MORMOD01_DEFINE_STATUS_G, 
                                                 OW_SAS_DM_RRII_MOR_01_BASELAYER_SRC_CHK]

    OW_SAS_DM_RRII_MORMOD01_DEFINE_STATUS_G >> [OW_SAS_DM_RRII_MORMOD02_BNS_MOR_PD_G, 
                                                OW_SAS_DM_RRII_MORMOD01A_GATHER_LOAD_G]
    OW_SAS_DM_RRII_MORMOD02_BNS_MOR_PD_G >> OW_SAS_DM_RRII_MORMOD03_PD_SCORE_SEG
    OW_SAS_DM_RRII_MORMOD03_PD_SCORE_SEG >> [OW_SAS_DM_RRII_MORMOD13_LGD_ND_G, 
                                             OW_SAS_DM_RRII_MORMOD04_REALIZED_DR]
    OW_SAS_DM_RRII_MORMOD13_LGD_ND_G >> [OW_SAS_DM_RRII_MORMOD23_LGD_D_G, 
                                         OW_SAS_DM_RRII_MORMOD14_LGDND_SCRE_SEG_G]
    OW_SAS_DM_RRII_MORMOD23_LGD_D_G >> OW_SAS_DM_RRII_MORMOD24_LGDD_SCORE_SEG_G
    OW_SAS_DM_RRII_MORMOD14_LGDND_SCRE_SEG_G >> OW_RRII_LGD_UPDT_MOR_ACCT_LVL_DATA_RCVRY
    [OW_SAS_DM_RRII_MORMOD24_LGDD_SCORE_SEG_G, OW_RRII_LGD_UPDT_MOR_ACCT_LVL_DATA_RCVRY] >> OW_DM_RRII_BSL_ACCT_SAS_GCP_APND_MOR
    OW_DM_RRII_BSL_ACCT_SAS_GCP_APND_MOR >> OW_SAS_10_LGD_INGRESS_AUDIT
    OW_SAS_DM_RRII_MOR_01_BASELAYER_SRC_CHK >> OW_SAS_DM_RRII_MORMOD50_LOAD_BRDM_TO_NZ

                                                    