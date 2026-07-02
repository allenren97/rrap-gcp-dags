from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="MO_BNSTG_1_M")
def MO_BNSTG_1_M():
    OW_SAS_DM_RRII_MOR_00_DETERMINE_RUNDATES = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_00_DETERMINE_RUNDATES",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_00_DETERMINE_RUN_DATES",
        sas_file="RRAP_MOR_00_DETERMINE_RUN_DATES.sas",
    )
    OW_SAS_DM_RRII_MOR_00_DETERMINE_RUNDATES.doc = "RRAP_MOR_00_DETERMINE_RUN_DATES , Original taskID: IW503#OW_SAS_DM_RRII_MOR_00_DETERMINE_RUNDATES"

    OW_RRII_TNG_ACT_MO_MCAP_FLTR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_TNG_ACT_MO_MCAP_FLTR",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_ACCT_MO_MCAP_FILTER",
        sas_file="RRAP_TNG_ACCT_MO_MCAP_FILTER.sas",
    )
    OW_RRII_TNG_ACT_MO_MCAP_FLTR.doc = "FILTERS AND EXCLUDES TNG_ACCT_MO TABLE FOR MCAP ACCOUNTS , Original taskID: IW503#OW_RRII_TNG_ACT_MO_MCAP_FLTR"


    OW_SAS_DM_RRII_MOR_00_DETERMINE_RUNDATES >> OW_RRII_TNG_ACT_MO_MCAP_FLTR