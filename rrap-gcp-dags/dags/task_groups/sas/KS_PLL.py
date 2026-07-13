from datetime import datetime, date
import pyarrow as pa

import duckdb as ddb
import logging

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator

logger = logging.getLogger("airflow.task")

@task_group(group_id='KS_PLL')
def KS_PLL():

    J_PLL_BASEL_SEG_RPTG_PARM = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_BASEL_SEG_RPTG_PARM",
        sas_file="J_PLL_BASEL_SEG_RPTG_PARM.sas",
    )

    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC",
        sas_file="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC.sas",
    )

    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC",
        sas_file="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC.sas",
    )

    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC",
        sas_file="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC.sas",
    )

    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE",
        sas_file="J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE.sas",
    )

    J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC",
        sas_file="J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC.sas",
    )

    J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC",
        sas_file="J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC.sas",
    )

    J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC",
        sas_file="J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC.sas",
    )

    J_PLL_KS10_2401_EAD_SEG_ACCT_XREF = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2401_EAD_SEG_ACCT_XREF",
        sas_file="J_PLL_KS10_2401_EAD_SEG_ACCT_XREF.sas",
    )

    J_PLL_KS10_2401_LGD_SEG_ACCT_XREF = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2401_LGD_SEG_ACCT_XREF",
        sas_file="J_PLL_KS10_2401_LGD_SEG_ACCT_XREF.sas",
    )

    J_PLL_KS10_2401_PD_SEG_ACCT_XREF = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2401_PD_SEG_ACCT_XREF",
        sas_file="J_PLL_KS10_2401_PD_SEG_ACCT_XREF.sas",
    )

    J_PLL_exception_report_1_KS_Scorecard_Model_Variables = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_exception_report_1_KS_Scorecard_Model_Variables",
        sas_file="J_PLL_exception_report_1_KS_Scorecard_Model_Variables.sas",
    )

    J_PLL_exception_report_2_KS_Scorecard_Distribution = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_exception_report_2_KS_Scorecard_Distribution",
        sas_file="J_PLL_exception_report_2_KS_Scorecard_Distribution.sas",
    )

    J_PLL_exception_report_3_KS_Segmentation_Distribution = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_exception_report_3_KS_Segmentation_Distribution",
        sas_file="J_PLL_exception_report_3_KS_Segmentation_Distribution.sas",
    )

    J_PLL_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR",
        sas_file="J_PLL_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas",
    )

    wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY_PLL = InformaticaOperator(
        task_id="wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY_PLL",
        ssh_conn_id="infa-dm-rrap-conn-pll",
        infa_workflow="wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY_PLL",
    )

    J_PLL_KS_BASEL_ANALYTICL_BL_INSTRMNT_FACT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_KS_BASEL_ANALYTICL_BL_INSTRMNT_FACT",
        sas_file="J_PLL_KS_BASEL_ANALYTICL_BL_INSTRMNT_FACT.sas",
    )

    J_PLL_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT",
        sas_file="J_PLL_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT.sas",
    )

    J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT",
        sas_file="J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT.sas",
    )

    J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT",
        sas_file="J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT.sas",
    )

    J_rrap_exception_report_1_MOR_Scorecard_Model_Variables_PLL = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_rrap_exception_report_1_MOR_Scorecard_Model_Variables_PLL",
        sas_file="J_rrap_exception_report_1_MOR_Scorecard_Model_Variables_PLL.sas",
    )

    J_rrap_exception_report_2_MOR_Scorecard_Distribution_PLL = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_rrap_exception_report_2_MOR_Scorecard_Distribution_PLL",
        sas_file="J_rrap_exception_report_2_MOR_Scorecard_Distribution_PLL.sas",
    )

    J_rrap_exception_report_3_MOR_Segmentation_Distribution_PLL = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_rrap_exception_report_3_MOR_Segmentation_Distribution_PLL",
        sas_file="J_rrap_exception_report_3_MOR_Segmentation_Distribution_PLL.sas",
    )

    J_PLL_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT",
        sas_file="J_PLL_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT.sas",
    )
    
    J_PLL_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT",
        sas_file="J_PLL_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT.sas",
    )

    J_PLL_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF",
        sas_file="J_PLL_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF.sas",
    )

    J_PLL_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL",
        sas_file="J_PLL_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL.sas",
    )

    J_PLL_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS",
        sas_file="J_PLL_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS.sas",
    )

    J_PLL_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT",
        sas_file="J_PLL_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT.sas",
    )

    J_PLL_EXCEPTION_REPORT_1_TL_SCORECARD_MODEL_VARIABLES = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_EXCEPTION_REPORT_1_TL_SCORECARD_MODEL_VARIABLES",
        sas_file="J_PLL_EXCEPTION_REPORT_1_TL_SCORECARD_MODEL_VARIABLES.sas",
    )

    J_PLL_EXCEPTION_REPORT_2_TL_SCORECARD_DISTRIBUTION = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_EXCEPTION_REPORT_2_TL_SCORECARD_DISTRIBUTION",
        sas_file="J_PLL_EXCEPTION_REPORT_2_TL_SCORECARD_DISTRIBUTION.sas",
    )
    
    J_PLL_EXCEPTION_REPORT_3_TL_SEGMENTATION_DISTRIBUTION = SasOperator(
        ssh_conn_id="sas-conn-pll",
        task_id="J_PLL_EXCEPTION_REPORT_3_TL_SEGMENTATION_DISTRIBUTION",
        sas_file="J_PLL_EXCEPTION_REPORT_3_TL_SEGMENTATION_DISTRIBUTION.sas",
    )
    
    J_PLL_BASEL_SEG_RPTG_PARM >> J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC
    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC >> J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC
    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_LOC >> J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC
    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_HELOC >> J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE
    J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_COMBINE >> J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC

    J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_CC >> J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC
    J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_LOC >> J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC
    J_PLL_KS10_2302_BASEL_RCA_SCORE_SNAPSHOT_HELOC >> J_PLL_KS10_2401_EAD_SEG_ACCT_XREF

    J_PLL_KS10_2401_EAD_SEG_ACCT_XREF >> J_PLL_KS10_2401_LGD_SEG_ACCT_XREF
    J_PLL_KS10_2401_LGD_SEG_ACCT_XREF >> J_PLL_KS10_2401_PD_SEG_ACCT_XREF
    J_PLL_KS10_2401_PD_SEG_ACCT_XREF >> wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY_PLL

    wf_DM_RRAP_Load_REVLVNG_CR_MTH_ACCT_RCVRY_PLL >> J_PLL_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR
    J_PLL_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR >> J_PLL_exception_report_1_KS_Scorecard_Model_Variables
    J_PLL_exception_report_1_KS_Scorecard_Model_Variables >> J_PLL_exception_report_2_KS_Scorecard_Distribution
    J_PLL_exception_report_2_KS_Scorecard_Distribution >> J_PLL_exception_report_3_KS_Segmentation_Distribution

    J_PLL_exception_report_3_KS_Segmentation_Distribution >> J_PLL_KS_BASEL_ANALYTICL_BL_INSTRMNT_FACT
    J_PLL_KS_BASEL_ANALYTICL_BL_INSTRMNT_FACT >> J_PLL_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT
    J_PLL_MOR_MODEL_24_BNS_MOR_LGD_D_SCORE_SEGMENT >> J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT
    J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT >> J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT
    J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT >> J_rrap_exception_report_1_MOR_Scorecard_Model_Variables_PLL
    J_rrap_exception_report_1_MOR_Scorecard_Model_Variables_PLL >> J_rrap_exception_report_2_MOR_Scorecard_Distribution_PLL
    J_rrap_exception_report_2_MOR_Scorecard_Distribution_PLL >> J_rrap_exception_report_3_MOR_Segmentation_Distribution_PLL

    J_PLL_BASEL_SEG_RPTG_PARM >> J_PLL_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT
    J_PLL_TL10_2301_BASEL_PNL_LN_SCORE_DTL_SNAPSHOT >> J_PLL_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT
    J_PLL_TL10_2302_BASEL_PNL_LN_SCORE_SNAPSHOT >> J_PLL_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF
    J_PLL_TL10_2401_BASEL_PNL_LN_LGD_SEG_ACCT_XREF >> J_PLL_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL
    J_PLL_TL10_2403_BASEL_PNL_LN_LGDND_SEG_MTH_RLZ_VAL >> J_PLL_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS
    J_PLL_TL10_2601_BASEL_PSNL_LOAN_RPTG_DRVD_VARS >> J_PLL_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT
    J_PLL_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT >> J_PLL_EXCEPTION_REPORT_1_TL_SCORECARD_MODEL_VARIABLES
    J_PLL_TL10_2602_BASEL_PSNL_LN_ANL_BL_INST_FACT >> J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT
    J_PLL_EXCEPTION_REPORT_1_TL_SCORECARD_MODEL_VARIABLES >> J_PLL_EXCEPTION_REPORT_2_TL_SCORECARD_DISTRIBUTION
    J_PLL_EXCEPTION_REPORT_2_TL_SCORECARD_DISTRIBUTION >> J_PLL_EXCEPTION_REPORT_3_TL_SEGMENTATION_DISTRIBUTION
