from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.db2 import DuplicateCheckOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import DuplicateCheckOperator


@task_group("TB_DM_M")
def TB_DM_M():

    OW_DM_RRIITB_SAS_TL_RB_RPTING_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIITB_SAS_TL_RB_RPTING_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS",
        sas_file="J_RRAP_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS.sas",
    )
    OW_DM_RRIITB_SAS_TL_RB_RPTING_DRVD_VARS.doc = "J_RRAP_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS , Original taskID: IW503#OW_DM_RRIITB_SAS_TL_RB_RPTING_DRVD_VARS"

    OW_DM_RRIITB_SAS_AUTO_SEC = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIITB_SAS_AUTO_SEC",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2601_AUTO_SECURITIZATION",
        sas_file="J_RRAP_TL10_2601_AUTO_SECURITIZATION.sas",
    )
    OW_DM_RRIITB_SAS_AUTO_SEC.doc = "Auto Loan Process Before SPL Instrument fact , Original taskID: IW503#OW_DM_RRIITB_SAS_AUTO_SEC"

    dupe_check_cc_sec = DuplicateCheckOperator(
        task_id='AUTO_SEC_DUPE_CHECK',
        db2_conn_id="db2-conn-legacy",
        table_name='{{ params.EDW_schema_EDRRAPT  }}.BASEL_CC_SEC_ACCT_MTH_SNAP',
        dupe_checks=[
            ("acct_num_recvd",),
            ("basel_acct_id_ccar_matched",)
        ]
    )

    OW_DM_RRIITB_SAS_TL_RB_INSTRUMENT_FACT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIITB_SAS_TL_RB_INSTRUMENT_FACT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT",
        sas_file="J_RRAP_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT.sas",
    )
    OW_DM_RRIITB_SAS_TL_RB_INSTRUMENT_FACT.doc = "J_RRAP_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT , Original taskID: IW503#OW_DM_RRIITB_SAS_TL_RB_INSTRUMENT_FACT"

    OW_DM_RRIITB_SAS_TL_RB_RPTING_DRVD_VARS >> OW_DM_RRIITB_SAS_AUTO_SEC
    OW_DM_RRIITB_SAS_AUTO_SEC >> dupe_check_cc_sec >> OW_DM_RRIITB_SAS_TL_RB_INSTRUMENT_FACT
