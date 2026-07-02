from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="DB2_LOAD")
def DB2_LOAD():

    OW_DM_RRII_2700_BSL_ANA_BL_INST_FACT = SasOperator(
        task_id="OW_DM_RRII_2700_BSL_ANA_BL_INST_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_2700_BASEL_ANALYTCL_BL_INSTRMNT_FACT",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_2700_BASEL_ANALYTCL_BL_INSTRMNT_FACT.sas",
    )
    OW_DM_RRII_2700_BSL_ANA_BL_INST_FACT.doc = "Loads J_RRAP_2700_BASEL_ANALYTCL_BL_INSTRMNT_FACT , Original taskID: IW503#OW_DM_RRII_2700_BSL_ANA_BL_INST_FACT"

    OW_DM_RRII_2710_INS_FACT_WRITE_BACK = SasOperator(
        task_id="OW_DM_RRII_2710_INS_FACT_WRITE_BACK",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_2710_INSTRMNT_FACT_WRITE_BACK",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_2710_INSTRMNT_FACT_WRITE_BACK.sas",
    )
    OW_DM_RRII_2710_INS_FACT_WRITE_BACK.doc = "Loads J_RRAP_2710_INSTRMNT_FACT_WRITE_BACK , Original taskID: IW503#OW_DM_RRII_2710_INS_FACT_WRITE_BACK"

    OW_DM_RRII_KS_2720_INS_FACT_DB2_COPY = SasOperator(
        task_id="OW_DM_RRII_KS_2720_INS_FACT_DB2_COPY",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_2720_INSTRMNT_FACT_KS_DB2_COPY",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_2720_INSTRMNT_FACT_KS_DB2_COPY.sas",
    )
    OW_DM_RRII_KS_2720_INS_FACT_DB2_COPY.doc = "Loads J_RRAP_2720_INSTRMNT_FACT_KS_DB2_COPY , Original taskID: IW503#OW_DM_RRII_KS_2720_INS_FACT_DB2_COPY"

    OW_DM_RRII_SPL_2730_INS_FACT_DB2_COPY = SasOperator(
        task_id="OW_DM_RRII_SPL_2730_INS_FACT_DB2_COPY",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_2730_INSTRMNT_FACT_SPL_DB2_COPY",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_2730_INSTRMNT_FACT_SPL_DB2_COPY.sas",
    )
    OW_DM_RRII_SPL_2730_INS_FACT_DB2_COPY.doc = "Loads J_RRAP_2730_INSTRMNT_FACT_SPL_DB2_COPY , Original taskID: IW503#OW_DM_RRII_SPL_2730_INS_FACT_DB2_COPY"

    OW_DM_RRII_MOR_2740_INS_FACT_DB2_COPY = SasOperator(
        task_id="OW_DM_RRII_MOR_2740_INS_FACT_DB2_COPY",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_2740_INSTRMNT_FACT_MOR_DB2_COPY",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_2740_INSTRMNT_FACT_MOR_DB2_COPY.sas",
    )
    OW_DM_RRII_MOR_2740_INS_FACT_DB2_COPY.doc = "Loads J_RRAP_2740_INSTRMNT_FACT_MOR_DB2_COPY , Original taskID: IW503#OW_DM_RRII_MOR_2740_INS_FACT_DB2_COPY"

    OW_DM_RRII_TNG_2750_INS_FACT_DB2_COPY = SasOperator(
        task_id="OW_DM_RRII_TNG_2750_INS_FACT_DB2_COPY",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_2750_INSTRMNT_FACT_TNG_DB2_COPY",
        ssh_conn_id="sas-conn",
        sas_file="J_RRAP_2750_INSTRMNT_FACT_TNG_DB2_COPY.sas",
    )
    OW_DM_RRII_TNG_2750_INS_FACT_DB2_COPY.doc = "Loads J_RRAP_2750_INSTRMNT_FACT_TNG_DB2_COPY , Original taskID: IW503#OW_DM_RRII_TNG_2750_INS_FACT_DB2_COPY"
    
    OW_DM_RRII_2700_BSL_ANA_BL_INST_FACT >> OW_DM_RRII_2710_INS_FACT_WRITE_BACK
    OW_DM_RRII_2710_INS_FACT_WRITE_BACK >> OW_DM_RRII_KS_2720_INS_FACT_DB2_COPY
    OW_DM_RRII_KS_2720_INS_FACT_DB2_COPY >> OW_DM_RRII_SPL_2730_INS_FACT_DB2_COPY
    OW_DM_RRII_SPL_2730_INS_FACT_DB2_COPY >> OW_DM_RRII_MOR_2740_INS_FACT_DB2_COPY
    OW_DM_RRII_MOR_2740_INS_FACT_DB2_COPY >> OW_DM_RRII_TNG_2750_INS_FACT_DB2_COPY
