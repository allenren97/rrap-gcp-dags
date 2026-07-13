from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator

 
@task_group(group_id="DLGD_1_DM_M")
def DLGD_1_DM_M():

    OW_SAS_II_DLGD_METRPL_CITY_LKP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_METRPL_CITY_LKP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0030_METRPL_CITY_LKP",
        sas_file="J_RRAP_DLGD_0030_METRPL_CITY_LKP.sas",
    )
    OW_SAS_II_DLGD_METRPL_CITY_LKP.doc = "J_RRAP_DLGD_0030_METRPL_CITY_LKP , Original taskID: IW503#OW_SAS_II_DLGD_METRPL_CITY_LKP"

    OW_SAS_II_DLGD_TERANET_ADDR_LKP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_TERANET_ADDR_LKP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0040_TERANET_ADDR_LKP",
        sas_file="J_RRAP_DLGD_0040_TERANET_ADDR_LKP.sas",
    )
    OW_SAS_II_DLGD_TERANET_ADDR_LKP.doc = "J_RRAP_DLGD_0040_TERANET_ADDR_LKP , Original taskID: IW503#OW_SAS_II_DLGD_TERANET_ADDR_LKP"

    OW_SAS_II_DLGD_METRPL_SCALNG_FACTR_DIM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_METRPL_SCALNG_FACTR_DIM",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0050_DLGD_METRPL_SCALNG_FACTR_DIM",
        sas_file="J_RRAP_DLGD_0050_DLGD_METRPL_SCALNG_FACTR_DIM.sas",
    )
    OW_SAS_II_DLGD_METRPL_SCALNG_FACTR_DIM.doc = "J_RRAP_DLGD_0050_DLGD_METRPL_SCALNG_FACTR_DIM , Original taskID: IW503#OW_SAS_II_DLGD_METRPL_SCALNG_FACTR_DIM"

    OW_SAS_II_DLGD_METRPL_THRSHD_VAL_DIM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_METRPL_THRSHD_VAL_DIM",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0060_DLGD_METRPL_THRSHD_VAL_DIM",
        sas_file="J_RRAP_DLGD_0060_DLGD_METRPL_THRSHD_VAL_DIM.sas",
    )
    OW_SAS_II_DLGD_METRPL_THRSHD_VAL_DIM.doc = "J_RRAP_DLGD_0060_DLGD_METRPL_THRSHD_VAL_DIM , Original taskID: IW503#OW_SAS_II_DLGD_METRPL_THRSHD_VAL_DIM"

    OW_SAS_II_DLGD_SCRI_QTR_DRVD_VARS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_SCRI_QTR_DRVD_VARS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0070_DLGD_SCRI_QTR_DRVD_VARS",
        sas_file="J_RRAP_DLGD_0070_DLGD_SCRI_QTR_DRVD_VARS.sas",
    )
    OW_SAS_II_DLGD_SCRI_QTR_DRVD_VARS.doc = "J_RRAP_DLGD_0070_DLGD_SCRI_QTR_DRVD_VARS , Original taskID: IW503#OW_SAS_II_DLGD_SCRI_QTR_DRVD_VARS"

    OW_DM_RRII_SAS_TL_CTRL_TABLE = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_DM_RRII_SAS_TL_CTRL_TABLE",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TL10_0000_DETERMINE_RUN_DATES"
        sas_file="J_RRAP_TL10_0000_DETERMINE_RUN_DATES.sas",
    )
    OW_DM_RRII_SAS_TL_CTRL_TABLE.doc = "J_RRAP_TL10_0000_DETERMINE_RUN_DATES"

    # OW_DM_RRII_SAS_TRNET_HPI_CMA_12M = SasOperator(
    #     ssh_conn_id="sas-conn",
    #     task_id="OW_DM_RRII_SAS_TRNET_HPI_CMA_12M",
    #     # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TERA_0020_TERANET_HOUSE_PRC_INDEX_CMA_12M"
    #     sas_file="J_RRAP_TERA_0020_TERANET_HOUSE_PRC_INDEX_CMA_12M.sas",
    # )
    # OW_DM_RRII_SAS_TRNET_HPI_CMA_12M.doc = ("Calls SAS job  J_RRAP_TERA_0020_TERANET_HOUSE_PRC_INDEX_CMA_12M")

    # OW_DM_RRII_SAS_TRNET_0030_HPI_CMA = SasOperator(
    #     ssh_conn_id="sas-conn",
    #     task_id="OW_DM_RRII_SAS_TRNET_0030_HPI_CMA",
    #     # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_TERA_0030_TERANET_HOUSE_PRC_INDEX_CMA"
    #     sas_file="J_RRAP_TERA_0030_TERANET_HOUSE_PRC_INDEX_CMA.sas",
    # )
    # OW_DM_RRII_SAS_TRNET_0030_HPI_CMA.doc = ("Calls SAS job  J_RRAP_TERA_0030_TERANET_HOUSE_PRC_INDEX_CMA")

    OW_SAS_II_DLGD_METRPL_CITY_LKP_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_METRPL_CITY_LKP_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0030_METRPL_CITY_LKP_CMA"
        sas_file="J_RRAP_DLGD_0030_METRPL_CITY_LKP_CMA.sas",
    )
    OW_SAS_II_DLGD_METRPL_CITY_LKP_CMA.doc = "J_RRAP_DLGD_0030_METRPL_CITY_LKP_CMA"

    OW_SAS_II_DLGD_TERANET_ADDR_LKP_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_II_DLGD_TERANET_ADDR_LKP_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_DLGD_0040_TERANET_ADDR_LKP_CMA"
        sas_file="J_RRAP_DLGD_0040_TERANET_ADDR_LKP_CMA.sas",
    )
    OW_SAS_II_DLGD_TERANET_ADDR_LKP_CMA.doc = "J_RRAP_DLGD_0040_TERANET_ADDR_LKP_CMA"

    OW_SAS_II_DLGD_METRPL_CITY_LKP >> OW_SAS_II_DLGD_TERANET_ADDR_LKP
    OW_SAS_II_DLGD_TERANET_ADDR_LKP >> OW_SAS_II_DLGD_METRPL_SCALNG_FACTR_DIM    
    OW_SAS_II_DLGD_METRPL_SCALNG_FACTR_DIM >> OW_SAS_II_DLGD_METRPL_THRSHD_VAL_DIM
    OW_SAS_II_DLGD_METRPL_THRSHD_VAL_DIM >> OW_SAS_II_DLGD_SCRI_QTR_DRVD_VARS
    OW_SAS_II_DLGD_SCRI_QTR_DRVD_VARS >> OW_DM_RRII_SAS_TL_CTRL_TABLE
    OW_DM_RRII_SAS_TL_CTRL_TABLE >> OW_SAS_II_DLGD_METRPL_CITY_LKP_CMA
    # OW_DM_RRII_SAS_TL_CTRL_TABLE >> OW_DM_RRII_SAS_TRNET_HPI_CMA_12M
    # OW_DM_RRII_SAS_TRNET_HPI_CMA_12M >> OW_DM_RRII_SAS_TRNET_0030_HPI_CMA
    # OW_DM_RRII_SAS_TRNET_0030_HPI_CMA >> OW_SAS_II_DLGD_METRPL_CITY_LKP_CMA
    OW_SAS_II_DLGD_METRPL_CITY_LKP_CMA >> OW_SAS_II_DLGD_TERANET_ADDR_LKP_CMA

