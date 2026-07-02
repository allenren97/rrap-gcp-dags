from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id='DT4_DM_M')
def DT4_DM_M():


    OW_DM_RRIIDT4_SEGMENT_XREF_BKUP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_SEGMENT_XREF_BKUP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0205_DT4_SEGMENT_XREF_BKUP",
        sas_file="J_RRAP_DT4_0205_DT4_SEGMENT_XREF_BKUP.sas",
    )
    OW_DM_RRIIDT4_SEGMENT_XREF_BKUP.doc = "Loads J_RRAP_DT4_0205_DT4_SEGMENT_XREF_BKUP , Original taskID: IW503#OW_DM_RRIIDT4_SEGMENT_XREF_BKUP"

    OW_DM_RRIIDT4_SEGMENT_XREF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_SEGMENT_XREF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0210_DT4_SEGMENT_XREF",
        sas_file="J_RRAP_DT4_0210_DT4_SEGMENT_XREF.sas",
    )
    OW_DM_RRIIDT4_SEGMENT_XREF.doc = "Loads J_RRAP_DT4_0210_DT4_SEGMENT_XREF , Original taskID: IW503#OW_DM_RRIIDT4_SEGMENT_XREF"

    OW_DM_RRIIDT4_RLZ_DATA_PREP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RLZ_DATA_PREP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0220_DT4_RLZ_DATA_PREP",
        sas_file="J_RRAP_DT4_0220_DT4_RLZ_DATA_PREP.sas",
    )
    OW_DM_RRIIDT4_RLZ_DATA_PREP.doc = "Loads J_RRAP_DT4_0220_DT4_RLZ_DATA_PREP , Original taskID: IW503#OW_DM_RRIIDT4_RLZ_DATA_PREP"

    OW_DM_RRIIDT4_RT18_RLZ_PDEAD_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT18_RLZ_PDEAD_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0310_DT4_RT18_RLZ_PDEAD_DRVD_VARS",
        sas_file="J_RRAP_DT4_0310_DT4_RT18_RLZ_PDEAD_DRVD_VARS.sas",
    )
    OW_DM_RRIIDT4_RT18_RLZ_PDEAD_DRVD_VARS.doc = "Loads J_RRAP_DT4_0310_DT4_RT18_RLZ_PDEAD_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT18_RLZ_PDEAD_DRVD_VARS"

    OW_DM_RRIIDT4_RT18_RLZ_LGD_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT18_RLZ_LGD_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0320_DT4_RT18_RLZ_LGD_DRVD_VARS",
        sas_file="J_RRAP_DT4_0320_DT4_RT18_RLZ_LGD_DRVD_VARS.sas",
    )
    OW_DM_RRIIDT4_RT18_RLZ_LGD_DRVD_VARS.doc = "Loads J_RRAP_DT4_0320_DT4_RT18_RLZ_LGD_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT18_RLZ_LGD_DRVD_VARS"

    OW_DM_RRIIDT4_RT18_RLZ_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT18_RLZ_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0330_DT4_RT18_RLZ_VARS",
        sas_file="J_RRAP_DT4_0330_DT4_RT18_RLZ_VARS.sas",
    )
    OW_DM_RRIIDT4_RT18_RLZ_VARS.doc = "Loads J_RRAP_DT4_0330_DT4_RT18_RLZ_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT18_RLZ_VARS"

    OW_DM_RRIIDT4_T18_FINAL_RPTG_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_T18_FINAL_RPTG_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0370_DT4_RT18_FINAL_RPTG_VARS",
        sas_file="J_RRAP_DT4_0370_DT4_RT18_FINAL_RPTG_VARS.sas",
    )
    OW_DM_RRIIDT4_T18_FINAL_RPTG_VARS.doc = "Loads J_RRAP_DT4_0370_DT4_RT18_FINAL_RPTG_VARS , Original taskID: IW503#OW_DM_RRIIDT4_T18_FINAL_RPTG_VARS"

    OW_DM_RRIIDT4_PD12_RLZ_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_PD12_RLZ_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0410_DT4_PD12_RLZ_DRVD_VARS",
        sas_file="J_RRAP_DT4_0410_DT4_PD12_RLZ_DRVD_VARS.sas",
    )
    OW_DM_RRIIDT4_PD12_RLZ_DRVD_VARS.doc = "Loads J_RRAP_DT4_0410_DT4_PD12_RLZ_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_PD12_RLZ_DRVD_VARS"

    OW_DM_RRIIDT4_PD12_FINAL_RPTG_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_PD12_FINAL_RPTG_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0420_DT4_PD12_FINAL_RPTG_VARS",
        sas_file="J_RRAP_DT4_0420_DT4_PD12_FINAL_RPTG_VARS.sas",
    )
    OW_DM_RRIIDT4_PD12_FINAL_RPTG_VARS.doc = "Loads J_RRAP_DT4_0420_DT4_PD12_FINAL_RPTG_VARS , Original taskID: IW503#OW_DM_RRIIDT4_PD12_FINAL_RPTG_VARS"

    OW_DM_RRIIDT4_RT20_RLZ_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT20_RLZ_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0510_DT4_RT20_RLZ_DRVD_VARS",
        sas_file="J_RRAP_DT4_0510_DT4_RT20_RLZ_DRVD_VARS.sas",
    )
    OW_DM_RRIIDT4_RT20_RLZ_DRVD_VARS.doc = "Loads J_RRAP_DT4_0510_DT4_RT20_RLZ_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT20_RLZ_DRVD_VARS"

    OW_DM_RRIIDT4_RT20_FINAL_RPTG_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT20_FINAL_RPTG_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0520_DT4_RT20_FINAL_RPTG_VARS",
        sas_file="J_RRAP_DT4_0520_DT4_RT20_FINAL_RPTG_VARS.sas",
    )
    OW_DM_RRIIDT4_RT20_FINAL_RPTG_VARS.doc = "Loads J_RRAP_DT4_0520_DT4_RT20_FINAL_RPTG_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT20_FINAL_RPTG_VARS"

    OW_DM_RRIIDT4_RT30_RLZ_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT30_RLZ_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0610_DT4_RT30_RLZ_DRVD_VARS",
        sas_file="J_RRAP_DT4_0610_DT4_RT30_RLZ_DRVD_VARS.sas",
    )
    OW_DM_RRIIDT4_RT30_RLZ_DRVD_VARS.doc = "Loads J_RRAP_DT4_0610_DT4_RT30_RLZ_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT30_RLZ_DRVD_VARS"

    OW_DM_RRIIDT4_RT30_FINAL_RPTG_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT30_FINAL_RPTG_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0620_DT4_RT30_FINAL_RPTG_VARS",
        sas_file="J_RRAP_DT4_0620_DT4_RT30_FINAL_RPTG_VARS.sas",
    )
    OW_DM_RRIIDT4_RT30_FINAL_RPTG_VARS.doc = "Loads J_RRAP_DT4_0620_DT4_RT30_FINAL_RPTG_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT30_FINAL_RPTG_VARS"

    OW_DM_RRIIDT4_RT40_RLZ_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT40_RLZ_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0710_DT4_RT40_RLZ_DRVD_VARS",
        sas_file="J_RRAP_DT4_0710_DT4_RT40_RLZ_DRVD_VARS.sas",
    )
    OW_DM_RRIIDT4_RT40_RLZ_DRVD_VARS.doc = "Loads J_RRAP_DT4_0710_DT4_RT40_RLZ_DRVD_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT40_RLZ_DRVD_VARS"

    OW_DM_RRIIDT4_RT40_FINAL_RPTG_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_RT40_FINAL_RPTG_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0720_DT4_RT40_FINAL_RPTG_VARS",
        sas_file="J_RRAP_DT4_0720_DT4_RT40_FINAL_RPTG_VARS.sas",
    )
    OW_DM_RRIIDT4_RT40_FINAL_RPTG_VARS.doc = "Loads J_RRAP_DT4_0720_DT4_RT40_FINAL_RPTG_VARS , Original taskID: IW503#OW_DM_RRIIDT4_RT40_FINAL_RPTG_VARS"

    OW_DM_RRIIDT4_DT4_RT05_DECLARATION = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_DT4_RT05_DECLARATION",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0810_DT4_RT05_DECLARATION",
        sas_file="J_RRAP_DT4_0810_DT4_RT05_DECLARATION.sas",
    )
    OW_DM_RRIIDT4_DT4_RT05_DECLARATION.doc = "Loads J_RRAP_DT4_0810_DT4_RT05_DECLARATION , Original taskID: IW503#OW_DM_RRIIDT4_DT4_RT05_DECLARATION"

    OW_DM_RRIIDT4_DT4_RT10_DECLARATION = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_DT4_RT10_DECLARATION",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0820_DT4_RT10_DECLARATION",
        sas_file="J_RRAP_DT4_0820_DT4_RT10_DECLARATION.sas",
    )
    OW_DM_RRIIDT4_DT4_RT10_DECLARATION.doc = "Loads J_RRAP_DT4_0820_DT4_RT10_DECLARATION , Original taskID: IW503#OW_DM_RRIIDT4_DT4_RT10_DECLARATION"

    OW_DM_RRIIDT4_DT4_RT11_DECLARATION = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_DT4_RT11_DECLARATION",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0830_DT4_RT11_DECLARATION",
        sas_file="J_RRAP_DT4_0830_DT4_RT11_DECLARATION.sas",
    )
    OW_DM_RRIIDT4_DT4_RT11_DECLARATION.doc = "Loads J_RRAP_DT4_0830_DT4_RT11_DECLARATION , Original taskID: IW503#OW_DM_RRIIDT4_DT4_RT11_DECLARATION"

    OW_DM_RRIIDT4_DT4_RT12_DECLARATION = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_DT4_RT12_DECLARATION",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0840_DT4_RT12_DECLARATION",
        sas_file="J_RRAP_DT4_0840_DT4_RT12_DECLARATION.sas",
    )
    OW_DM_RRIIDT4_DT4_RT12_DECLARATION.doc = "Loads J_RRAP_DT4_0840_DT4_RT12_DECLARATION , Original taskID: IW503#OW_DM_RRIIDT4_DT4_RT12_DECLARATION"

    OW_DM_RRIIDT4_BRDR_SEG_LVL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_BRDR_SEG_LVL",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0910_DT4_BRDR_SEG_LVL",
        sas_file="J_RRAP_DT4_0910_DT4_BRDR_SEG_LVL.sas",
    )
    OW_DM_RRIIDT4_BRDR_SEG_LVL.doc = "Loads J_RRAP_DT4_0910_DT4_BRDR_SEG_LVL , Original taskID: IW503#OW_DM_RRIIDT4_BRDR_SEG_LVL"

    OW_DM_RRIIDT4_DT4_BRDR_REC_UNREC_BRCH = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_DT4_BRDR_REC_UNREC_BRCH",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_0920_DT4_BRDR_REC_UNREC_BRCH",
        sas_file="J_RRAP_DT4_0920_DT4_BRDR_REC_UNREC_BRCH.sas",
    )
    OW_DM_RRIIDT4_DT4_BRDR_REC_UNREC_BRCH.doc = "Loads J_RRAP_DT4_0920_DT4_BRDR_REC_UNREC_BRCH , Original taskID: IW503#OW_DM_RRIIDT4_DT4_BRDR_REC_UNREC_BRCH"

    OW_DM_RRIIDT4_DT4_XML_OUTPUT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRIIDT4_DT4_XML_OUTPUT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DT4_1000_DT4_XML_OUTPUT",
        sas_file="J_RRAP_DT4_1000_DT4_XML_OUTPUT.sas",
    )
    OW_DM_RRIIDT4_DT4_XML_OUTPUT.doc = "Loads J_RRAP_DT4_1000_DT4_XML_OUTPUT , Original taskID: IW503#OW_DM_RRIIDT4_DT4_XML_OUTPUT"
    
    OW_DM_RRIIDT4_SEGMENT_XREF_BKUP >> OW_DM_RRIIDT4_SEGMENT_XREF
    OW_DM_RRIIDT4_SEGMENT_XREF >> OW_DM_RRIIDT4_RLZ_DATA_PREP
    OW_DM_RRIIDT4_RLZ_DATA_PREP >> OW_DM_RRIIDT4_RT18_RLZ_PDEAD_DRVD_VARS
    OW_DM_RRIIDT4_RT18_RLZ_PDEAD_DRVD_VARS >> OW_DM_RRIIDT4_RT18_RLZ_LGD_DRVD_VARS
    OW_DM_RRIIDT4_RT18_RLZ_LGD_DRVD_VARS >> OW_DM_RRIIDT4_RT18_RLZ_VARS
    OW_DM_RRIIDT4_RT18_RLZ_VARS >> OW_DM_RRIIDT4_T18_FINAL_RPTG_VARS
    OW_DM_RRIIDT4_T18_FINAL_RPTG_VARS >> OW_DM_RRIIDT4_PD12_RLZ_DRVD_VARS
    OW_DM_RRIIDT4_PD12_RLZ_DRVD_VARS >> OW_DM_RRIIDT4_PD12_FINAL_RPTG_VARS
    OW_DM_RRIIDT4_PD12_FINAL_RPTG_VARS >> OW_DM_RRIIDT4_RT20_RLZ_DRVD_VARS
    OW_DM_RRIIDT4_RT20_RLZ_DRVD_VARS >> OW_DM_RRIIDT4_RT20_FINAL_RPTG_VARS
    OW_DM_RRIIDT4_RT20_FINAL_RPTG_VARS >> OW_DM_RRIIDT4_RT30_RLZ_DRVD_VARS
    OW_DM_RRIIDT4_RT30_RLZ_DRVD_VARS >> OW_DM_RRIIDT4_RT30_FINAL_RPTG_VARS
    OW_DM_RRIIDT4_RT30_FINAL_RPTG_VARS >> OW_DM_RRIIDT4_RT40_RLZ_DRVD_VARS
    OW_DM_RRIIDT4_RT40_RLZ_DRVD_VARS >> OW_DM_RRIIDT4_RT40_FINAL_RPTG_VARS
    OW_DM_RRIIDT4_RT40_FINAL_RPTG_VARS >> OW_DM_RRIIDT4_DT4_RT05_DECLARATION
    OW_DM_RRIIDT4_DT4_RT05_DECLARATION >> OW_DM_RRIIDT4_DT4_RT10_DECLARATION
    OW_DM_RRIIDT4_DT4_RT10_DECLARATION >> OW_DM_RRIIDT4_DT4_RT11_DECLARATION
    OW_DM_RRIIDT4_DT4_RT11_DECLARATION >> OW_DM_RRIIDT4_DT4_RT12_DECLARATION
    OW_DM_RRIIDT4_DT4_RT12_DECLARATION >> OW_DM_RRIIDT4_BRDR_SEG_LVL
    OW_DM_RRIIDT4_BRDR_SEG_LVL >> OW_DM_RRIIDT4_DT4_BRDR_REC_UNREC_BRCH
    OW_DM_RRIIDT4_DT4_BRDR_REC_UNREC_BRCH >> OW_DM_RRIIDT4_DT4_XML_OUTPUT
