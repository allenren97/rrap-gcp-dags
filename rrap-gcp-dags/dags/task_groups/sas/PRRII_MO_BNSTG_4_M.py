from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="MO_BNSTG_4_M")
def MO_BNSTG_4_M():
    
    OW_SAS_DM_RRII_MORMOD_59_BASLMO_INST_ADJ = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD_59_BASLMO_INST_ADJ",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_59_BASEL_MORT_INSTRMNT_ADJ",
        sas_file="RRAP_MOR_MODEL_59_BASEL_MORT_INSTRMNT_ADJ.sas",
    )
    OW_SAS_DM_RRII_MORMOD_59_BASLMO_INST_ADJ.doc = "RRAP_MOR_MODEL_59_BASEL_MORT_INSTRMNT_ADJ , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD_59_BASLMO_INST_ADJ"

    OW_SAS_DM_RRII_MORMOD60_RPTG_INST_FACT_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD60_RPTG_INST_FACT_G",
        sas_file="RRAP_MOR_MODEL_60_BNS_REPORTING_INSTRUMENT_FACT_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_60_BNS_REPORTING_INSTRUMENT_FACT_G",
    )
    OW_SAS_DM_RRII_MORMOD60_RPTG_INST_FACT_G.doc = "RRAP_MOR_MODEL_60_BNS_REPORTING_INSTRUMENT_FACT_G , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD60_RPTG_INST_FACT_G"

    OW_SAS_TNG_II_06_RPTING_INSTRMNT_FACT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_06_RPTING_INSTRMNT_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_06_RPTING_INSTRMNT_FACT",
        sas_file="RRAP_TNG_06_RPTING_INSTRMNT_FACT.sas",
    )
    OW_SAS_TNG_II_06_RPTING_INSTRMNT_FACT.doc = "RRAP_TNG_06_RPTING_INSTRMNT_FACT , Original taskID: IW503#OW_SAS_TNG_II_06_RPTING_INSTRMNT_FACT"

    OW_DM_RRII_MIDRUN_SAS_SRVR_CLNUP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_MIDRUN_SAS_SRVR_CLNUP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MID_RUN_SAS_SERVER_CLEANUP",
        sas_file="RRAP_MID_RUN_SAS_SERVER_CLEANUP.sas",
    )
    OW_DM_RRII_MIDRUN_SAS_SRVR_CLNUP.doc = "JOB DELETES TEMPORARY DATASETS FROM SAS PROD SERVER , Original taskID: IW503#OW_DM_RRII_MIDRUN_SAS_SRVR_CLNUP"


    OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS",
        sas_file="RRAP_MOR_MODEL_80_STACK_TNG_AND_BNS.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_MODEL_80_STACK_TNG_AND_BNS",
    )
    OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS.doc = "RRAP_MOR_MODEL_80_STACK_TNG_AND_BNS , Original taskID: IW503#OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS"


    OW_DM_RRII_MTH_SRC_IND_COST = SasOperator(
        task_id="OW_DM_RRII_MTH_SRC_IND_COST",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MTH_SRC_IND_COST",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_MTH_SRC_IND_COST.sas",
    )
    OW_DM_RRII_MTH_SRC_IND_COST.doc = "This is monthly job which combines EDRTLRP1D.MBR_INDRCT_COST_MTH_SNAPSHOT and /owpftp/TOT_NON_INT_EXP_TRANSIT.csv  , Original taskID: IW503#OW_DM_RRII_MTH_SRC_IND_COST"



    OW_SAS_DM_RRII_MORMOD_59_BASLMO_INST_ADJ >> OW_SAS_DM_RRII_MORMOD60_RPTG_INST_FACT_G
    OW_SAS_TNG_II_06_RPTING_INSTRMNT_FACT >> OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS
    OW_SAS_DM_RRII_MORMOD60_RPTG_INST_FACT_G >> OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS
    OW_SAS_DM_RRII_MORMOD80_STACK_TNG_N_BNS >> [OW_DM_RRII_MTH_SRC_IND_COST,
                                                OW_DM_RRII_MIDRUN_SAS_SRVR_CLNUP]
    
