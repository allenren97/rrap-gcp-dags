from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator


@task_group(group_id="MO_BNSTG_2_M")
def MO_BNSTG_2_M():

    OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_00_SOURCE_CHECK",
        sas_file="RRAP_MOR_00_SOURCE_CHECK.sas",
    )
    OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK.doc = "RRAP_MOR_00_SOURCE_CHECK , Original taskID: IW503#OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK"

    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G",
        sas_file="RRAP_MOR_ACCT_01_LOAD_MORTGAGE_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_ACCT_01_LOAD_MORTGAGE_G",
    )
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G.doc = "RRAP_MOR_ACCT_01_LOAD_MORTGAGE_G , Original taskID: IW503#OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G"

    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR",
        sas_file="RRAP_MOR_ACCT_01_LOAD_MORTGAGE_GATHER.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_ACCT_01_LOAD_MORTGAGE_GATHER",
    )
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR.doc = "RRAP_MOR_ACCT_01_LOAD_MORTGAGE_GATHER , Original taskID: IW503#OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR"

    OW_SAS_DM_RRII_MORACCT02_UNLOAD_MORT_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORACCT02_UNLOAD_MORT_G",
        sas_file="RRAP_MOR_ACCT_02_UNLOAD_MORTGAGE_G.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_ACCT_02_UNLOAD_MORTGAGE_G",
    )
    OW_SAS_DM_RRII_MORACCT02_UNLOAD_MORT_G.doc = "RRAP_MOR_ACCT_02_UNLOAD_MORTGAGE_G , Original taskID: IW503#OW_SAS_DM_RRII_MORACCT02_UNLOAD_MORT_G"

    OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G",
        sas_file="RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G",
    )
    OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G.doc = "RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G"

    OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA",
        sas_file="RRAP_MOR_SCRCD_01_LOAD_INPUT_DATA.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_01_LOAD_INPUT_DATA",
    )
    OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA.doc = "RRAP_MOR_SCRCD_01_LOAD_INPUT_DATA , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA"

    OW_SAS_DM_RRII_MORSCRD02_LOAD_MOR_DATA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD02_LOAD_MOR_DATA",
        sas_file="RRAP_MOR_SCRCD_02_LOAD_MOR_DATA.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_02_LOAD_MOR_DATA",
    )
    OW_SAS_DM_RRII_MORSCRD02_LOAD_MOR_DATA.doc_md = """RRAP_MOR_SCRCD_02_LOAD_MOR_DATA , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD02_LOAD_MOR_DATA
    ## Depends on FRG_USER_DATA.MORTGAGE_HIST """

    OW_SAS_DM_RRII_MORSCRD03_LOAD_MOR_DATA09 = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD03_LOAD_MOR_DATA09",
        sas_file="RRAP_MOR_SCRCD_03_LOAD_MOR_DATA_09.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_03_LOAD_MOR_DATA_09",
    )
    OW_SAS_DM_RRII_MORSCRD03_LOAD_MOR_DATA09.doc = "RRAP_MOR_SCRCD_03_LOAD_MOR_DATA_09 , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD03_LOAD_MOR_DATA09"

    OW_SAS_DM_RRII_MORSCRD05_CUST_TO_MOR_09 = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD05_CUST_TO_MOR_09",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_05_CUST_DATA_TO_MOR_09",
        sas_file="RRAP_MOR_SCRCD_05_CUST_DATA_TO_MOR_09.sas",
    )
    OW_SAS_DM_RRII_MORSCRD05_CUST_TO_MOR_09.doc = "RRAP_MOR_SCRCD_05_CUST_DATA_TO_MOR_09 , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD05_CUST_TO_MOR_09"

    OW_SAS_DM_RRII_MORSCRD06_DET_ITVL_MOR_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD06_DET_ITVL_MOR_G",
        sas_file="RRAP_MOR_SCRCD_06_DETERMINE_INTERVALS_SORT_MOR_G.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_06_DETERMINE_INTERVALS_SORT_MOR_G",
    )
    OW_SAS_DM_RRII_MORSCRD06_DET_ITVL_MOR_G.doc = "RRAP_MOR_SCRCD_06_DETERMINE_INTERVALS_SORT_MOR_G , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD06_DET_ITVL_MOR_G"

    OW_SAS_DM_RRII_MORSCRD04_LOAD_CUST_T_MOR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD04_LOAD_CUST_T_MOR",
        sas_file="RRAP_MOR_SCRCD_04_LOAD_CUST_DATA_TO_MOR.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_04_LOAD_CUST_DATA_TO_MOR",
    )
    OW_SAS_DM_RRII_MORSCRD04_LOAD_CUST_T_MOR.doc = "RRAP_MOR_SCRCD_04_LOAD_CUST_DATA_TO_MOR , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD04_LOAD_CUST_T_MOR"

    OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_08_LOAD_MOR_DATA_TO_NETEZZA",
        sas_file="RRAP_MOR_SCRCD_08_LOAD_MOR_DATA_TO_NETEZZA.sas",
    )
    OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ.doc = "RRAP_MOR_SCRCD_08_LOAD_MOR_DATA_TO_NETEZZA , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ"

    OW_SAS_DM_RRII_MORSCRD09_LOAD_ACCT_LIST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD09_LOAD_ACCT_LIST",
        sas_file="RRAP_MOR_SCRCD_09_LOAD_ACCT_LIST.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_09_LOAD_ACCT_LIST",
    )
    OW_SAS_DM_RRII_MORSCRD09_LOAD_ACCT_LIST.doc = "RRAP_MOR_SCRCD_09_LOAD_ACCT_LIST , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD09_LOAD_ACCT_LIST"

    OW_SAS_DM_RRII_MORSCRD13_LOAD_CUST_LIST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD13_LOAD_CUST_LIST",
        sas_file="RRAP_MOR_SCRCD_13_LOAD_CUST_LIST.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_13_LOAD_CUST_LIST",
    )
    OW_SAS_DM_RRII_MORSCRD13_LOAD_CUST_LIST.doc = "RRAP_MOR_SCRCD_13_LOAD_CUST_LIST , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD13_LOAD_CUST_LIST"

    OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F",
        sas_file="RRAP_MOR_SCRCD_17_LOAD_CUST_LIST_FINAL.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_17_LOAD_CUST_LIST_FINAL",
    )
    OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F.doc = "RRAP_MOR_SCRCD_17_LOAD_CUST_LIST_FINAL , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F"

    OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F",
        sas_file="RRAP_MOR_SCRCD_12_LOAD_ACCT_LIST_FINAL.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_12_LOAD_ACCT_LIST_FINAL",
    )
    OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F.doc = "RRAP_MOR_SCRCD_12_LOAD_ACCT_LIST_FINAL , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F"

    OW_SAS_DM_RRII_MORSCRD18_LOAD_BSL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD18_LOAD_BSL",
        sas_file="RRAP_MOR_SCRCD_18_LOAD_BSL.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_18_LOAD_BSL",
    )
    OW_SAS_DM_RRII_MORSCRD18_LOAD_BSL.doc = "RRAP_MOR_SCRCD_18_LOAD_BSL , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD18_LOAD_BSL"

    OW_SAS_DM_RRII_MORSCRD25_LOAD_CUST_PRODS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD25_LOAD_CUST_PRODS",
        sas_file="RRAP_MOR_SCRCD_25_LOAD_CUST_PRODS.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_25_LOAD_CUST_PRODS",
    )
    OW_SAS_DM_RRII_MORSCRD25_LOAD_CUST_PRODS.doc = "RRAP_MOR_SCRCD_25_LOAD_CUST_PRODS , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD25_LOAD_CUST_PRODS"

    OW_SAS_DM_RRII_MORSCRD31_LOAD_MORTGAGES = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD31_LOAD_MORTGAGES",
        sas_file="RRAP_MOR_SCRCD_31_LOAD_MORTGAGES.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_31_LOAD_MORTGAGES",
    )
    OW_SAS_DM_RRII_MORSCRD31_LOAD_MORTGAGES.doc = "RRAP_MOR_SCRCD_31_LOAD_MORTGAGES , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD31_LOAD_MORTGAGES"

    OW_SAS_DM_RRII_MORSCRD37_LOAD_CUST_SUMM = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD37_LOAD_CUST_SUMM",
        sas_file="RRAP_MOR_SCRCD_37_LOAD_CUSTOMER_SUMMARY.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_37_LOAD_CUSTOMER_SUMMARY",
    )
    OW_SAS_DM_RRII_MORSCRD37_LOAD_CUST_SUMM.doc = "RRAP_MOR_SCRCD_37_LOAD_CUSTOMER_SUMMARY , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD37_LOAD_CUST_SUMM"

    OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU",
        sas_file="RRAP_MOR_SCRCD_19_LOAD_CREDIT_BUREAU.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_19_LOAD_CREDIT_BUREAU",
    )
    OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU.doc = "RRAP_MOR_SCRCD_19_LOAD_CREDIT_BUREAU , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU"

    OW_SAS_DM_RRII_MORSCRD38_LOAD_CUST_PROF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD38_LOAD_CUST_PROF",
        sas_file="RRAP_MOR_SCRCD_38_LOAD_CUST_PROFILE.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_38_LOAD_CUST_PROFILE",
    )
    OW_SAS_DM_RRII_MORSCRD38_LOAD_CUST_PROF.doc = "RRAP_MOR_SCRCD_38_LOAD_CUST_PROFILE , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD38_LOAD_CUST_PROF"

    OW_SAS_DM_RRII_MORSCRD20_LOAD_CUST_TU = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD20_LOAD_CUST_TU",
        sas_file="RRAP_MOR_SCRCD_20_LOAD_CUST_TU.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_20_LOAD_CUST_TU",
    )
    OW_SAS_DM_RRII_MORSCRD20_LOAD_CUST_TU.doc = "RRAP_MOR_SCRCD_20_LOAD_CUST_TU , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD20_LOAD_CUST_TU"

    OW_SAS_DM_RRII_MORSCRD23_LOAD_D2D_TRANS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD23_LOAD_D2D_TRANS",
        sas_file="RRAP_MOR_SCRCD_23_LOAD_D2D_TRANS.sas",
        
            
            # How to deal with _{year} ?
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_23_LOAD_D2D_TRANS",
    )
    OW_SAS_DM_RRII_MORSCRD23_LOAD_D2D_TRANS.doc = "RRAP_MOR_SCRCD_23_LOAD_D2D_TRANS , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD23_LOAD_D2D_TRANS"

    OW_SAS_DM_RRII_MORSCRD21_LOAD_CUST_PRODS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD21_LOAD_CUST_PRODS",
        sas_file="RRAP_MOR_SCRCD_21_LOAD_CUST_PRODUCTS.sas",
        
            
            # TODO Can dataset names contain templates? Or is this best handled with an extra_dict ?
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_21_LOAD_CUST_PRODUCTS",
    )
    OW_SAS_DM_RRII_MORSCRD21_LOAD_CUST_PRODS.doc = "RRAP_MOR_SCRCD_21_LOAD_CUST_PRODUCTS , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD21_LOAD_CUST_PRODS"

    OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL",
        sas_file="RRAP_MOR_SCRCD_40_LOAD_COMBINE_ALL.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_40_LOAD_COMBINE_ALL",
    )
    OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL.doc = "RRAP_MOR_SCRCD_40_LOAD_COMBINE_ALL , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL"

    OW_SAS_DM_RRII_MORSCRD45_CR_VAR_STATS_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD45_CR_VAR_STATS_G",
        sas_file="RRAP_MOR_SCRCD_45_CREATE_VAR_STATS_G.sas",
        
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_45_CREATE_VAR_STATS_G",
    )
    OW_SAS_DM_RRII_MORSCRD45_CR_VAR_STATS_G.doc = "RRAP_MOR_SCRCD_45_CREATE_VAR_STATS_G , Original taskID: IW503#OW_SAS_DM_RRII_MORSCRD45_CR_VAR_STATS_G"

    OW_SAS_TNG_II_00_MORT_SOURCE_CHECK = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_00_MORT_SOURCE_CHECK",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_00_MORT_SOURCE_CHECK",
        sas_file="RRAP_TNG_00_MORT_SOURCE_CHECK.sas",
    )
    OW_SAS_TNG_II_00_MORT_SOURCE_CHECK.doc = "RRAP_TNG_00_MORT_SOURCE_CHECK , Original taskID: IW503#OW_SAS_TNG_II_00_MORT_SOURCE_CHECK"

    OW_SAS_TNG_II_01_DEFINE_STATUS_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_01_DEFINE_STATUS_G",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_01_DEFINE_STATUS_G",
        sas_file="RRAP_TNG_01_DEFINE_STATUS_G.sas",
    )
    OW_SAS_TNG_II_01_DEFINE_STATUS_G.doc = "RRAP_TNG_01_DEFINE_STATUS_G , Original taskID: IW503#OW_SAS_TNG_II_01_DEFINE_STATUS_G"

    OW_SAS_TNG_II_02_MOR_PD_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_02_MOR_PD_G",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_02_MOR_PD_G",
        sas_file="RRAP_TNG_02_MOR_PD_G.sas",
    )
    OW_SAS_TNG_II_02_MOR_PD_G.doc = (
        "RRAP_TNG_02_MOR_PD_G , Original taskID: IW503#OW_SAS_TNG_II_02_MOR_PD_G"
    )

    OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G",
        # bash_command="^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_02_MOR_SCORE_VAR_CREATION_G"
        sas_file="RRAP_TNG_02_MOR_SCORE_VAR_CREATION_G.sas",
    )
    OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G.doc = "RRAP_TNG_02_MOR_SCORE_VAR_CREATION_G, Original taskID: IW503:OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G"

    OW_SAS_TNG_II_03_LGD_ND_CALCULATION = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_03_LGD_ND_CALCULATION",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_03_LGD_ND_CALCULATION",
        sas_file="RRAP_TNG_03_LGD_ND_CALCULATION.sas",
    )
    OW_SAS_TNG_II_03_LGD_ND_CALCULATION.doc = "RRAP_TNG_03_LGD_ND_CALCULATION , Original taskID: IW503#OW_SAS_TNG_II_03_LGD_ND_CALCULATION"

    OW_RRII_LGD_UPDATE_TNG_IND_COST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_TNG_IND_COST",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_TNG_IND_COST",
        sas_file="RRAP_LGD_UPDATE_TNG_IND_COST.sas",
    )
    OW_RRII_LGD_UPDATE_TNG_IND_COST.doc = "Update LGD TNG IND COST CALCULATION , Original taskID: IW503#OW_RRII_LGD_UPDATE_TNG_IND_COST"

    OW_RRII_LGD_UPDATE_TNG_LGD_ND = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_TNG_LGD_ND",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_TNG_LGD_ND",
        sas_file="RRAP_LGD_UPDATE_TNG_LGD_ND.sas",
    )
    OW_RRII_LGD_UPDATE_TNG_LGD_ND.doc = (
        "Update LGDND TNG , Original taskID: IW503#OW_RRII_LGD_UPDATE_TNG_LGD_ND"
    )

    OW_SAS_TNG_II_03_MOR_LGD_D = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_03_MOR_LGD_D",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_03_MOR_LGD_D",
        sas_file="RRAP_TNG_03_MOR_LGD_D.sas",
    )
    OW_SAS_TNG_II_03_MOR_LGD_D.doc = (
        "RRAP_TNG_03_MOR_LGD_D , Original taskID: IW503#OW_SAS_TNG_II_03_MOR_LGD_D"
    )

    OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_03_MOR_PD_SCORE_SEGMENT",
        sas_file="RRAP_TNG_03_MOR_PD_SCORE_SEGMENT.sas",
    )
    OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT.doc = "RRAP_TNG_03_MOR_PD_SCORE_SEGMENT , Original taskID: IW503#OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT"

    OW_SAS_TNG_II_03_MOR_PD_SCORE_SEG_IFRS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_03_MOR_PD_SCORE_SEG_IFRS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRII_TNG_03_MOR_PD_SCORE_SEG_IFRS",
        sas_file="RRII_TNG_03_MOR_PD_SCORE_SEG_IFRS.sas",
    )
    OW_SAS_TNG_II_03_MOR_PD_SCORE_SEG_IFRS.doc = "RRAP_TNG_03_MOR_PD_SCORE_SEG_IFRS , Original taskID: IW503#OW_SAS_TNG_II_03_MOR_PD_SCORE_SEG_IFRS"

    OW_SAS_TNG_II_04_LGD_ND_SEGMENT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_04_LGD_ND_SEGMENT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_04_LGD_ND_SEGMENT",
        sas_file="RRAP_TNG_04_LGD_ND_SEGMENT.sas",
    )
    OW_SAS_TNG_II_04_LGD_ND_SEGMENT.doc = "RRAP_TNG_04_LGD_ND_SEGMENT , Original taskID: IW503#OW_SAS_TNG_II_04_LGD_ND_SEGMENT"

    OW_SAS_TNG_II_04_LGD_ND_SEG_IFRS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_04_LGD_ND_SEG_IFRS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRII_TNG_04_LGD_ND_SEG_IFRS",
        sas_file="RRII_TNG_04_LGD_ND_SEG_IFRS.sas",
    )
    OW_SAS_TNG_II_04_LGD_ND_SEG_IFRS.doc = "RRII_TNG_04_LGD_ND_SEG_IFRS , Original taskID: IW503#OW_SAS_TNG_II_04_LGD_ND_SEG_IFRS"

    OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_04_MOR_LGD_SCORE_SEGMENT",
        sas_file="RRAP_TNG_04_MOR_LGD_SCORE_SEGMENT.sas",
    )
    OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT.doc = "RRAP_TNG_04_MOR_LGD_SCORE_SEGMENT , Original taskID: IW503#OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT"

    OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEG_IFRS = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEG_IFRS",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRII_TNG_04_MOR_LGD_SCORE_SEG_IFRS",
        sas_file="RRII_TNG_04_MOR_LGD_SCORE_SEG_IFRS.sas",
    )
    OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEG_IFRS.doc = "RRAP_TNG_04_MOR_LGD_SCORE_SEGM_IFRS , Original taskID: IW503#OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEG_IFRS"

    OW_RRII_LGD_UPDATE_TNG_LGD_D = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_RRII_LGD_UPDATE_TNG_LGD_D",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_LGD_UPDATE_TNG_LGD_D",
        sas_file="RRAP_LGD_UPDATE_TNG_LGD_D.sas",
    )
    OW_RRII_LGD_UPDATE_TNG_LGD_D.doc = (
        "Update LGDD TNG , Original taskID: IW503#OW_RRII_LGD_UPDATE_TNG_LGD_D"
    )

    OW_SAS_TNG_II_04_MOR_REALIZED_DR = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_04_MOR_REALIZED_DR",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_04_MOR_REALIZED_DR",
        sas_file="RRAP_TNG_04_MOR_REALIZED_DR.sas",
    )
    OW_SAS_TNG_II_04_MOR_REALIZED_DR.doc = "RRAP_TNG_04_MOR_REALIZED_DR , Original taskID: IW503#OW_SAS_TNG_II_04_MOR_REALIZED_DR"

    OW_SAS_TNG_II_05_LGD_D_REALIZED = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_05_LGD_D_REALIZED",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_05_LGD_D_REALIZED",
        sas_file="RRAP_TNG_05_LGD_D_REALIZED.sas",
    )
    OW_SAS_TNG_II_05_LGD_D_REALIZED.doc = "RRAP_TNG_05_LGD_D_REALIZED , Original taskID: IW503#OW_SAS_TNG_II_05_LGD_D_REALIZED"

    OW_SAS_TNG_II_05_LGD_ND_REALIZED = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_05_LGD_ND_REALIZED",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_05_LGD_ND_REALIZED",
        sas_file="RRAP_TNG_05_LGD_ND_REALIZED.sas",
    )
    OW_SAS_TNG_II_05_LGD_ND_REALIZED.doc = "RRAP_TNG_05_LGD_ND_REALIZED , Original taskID: IW503#OW_SAS_TNG_II_05_LGD_ND_REALIZED"

    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_01_HPI_CONVERSION_BNS_TNG",
        sas_file="RRAP_MOR_01_HPI_CONVERSION_BNS_TNG.sas",
    )
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG.doc = "RRAP_MOR_01_HPI_CONVERSION_BNS_TNG , Original taskID: IW503#OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG"

    OW_SAS_DM_RRII_MOR_01_GEN_LKP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_01_GEN_LKP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_01_GEN_LKP",
        sas_file="RRAP_MOR_01_GEN_LKP.sas",
    )
    OW_SAS_DM_RRII_MOR_01_GEN_LKP.doc = (
        "RRAP_MOR_01_GEN_LKP , Original taskID: IW503#OW_SAS_DM_RRII_MOR_01_GEN_LKP"
    )

    OW_SAS_DM_RRII_MOR_SRC_0040_TNG_ACCT_DEP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0040_TNG_ACCT_DEP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0040_TNG_ACCT_DEPOSIT",
        sas_file="J_RRAP_MOR_SRC_0040_TNG_ACCT_DEPOSIT.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0040_TNG_ACCT_DEP.doc = "J_RRAP_MOR_SRC_0040_TNG_ACCT_DEPOSIT , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0040_TNG_ACCT_DEP"

    OW_SAS_DM_RRII_MOR_SRC_0050_TNG_CUST_DEP = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0050_TNG_CUST_DEP",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0050_TNG_CUST_DEPOSIT",
        sas_file="J_RRAP_MOR_SRC_0050_TNG_CUST_DEPOSIT.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0050_TNG_CUST_DEP.doc = "J_RRAP_MOR_SRC_0050_TNG_CUST_DEPOSIT , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0050_TNG_CUST_DEP"

    OW_SAS_DM_RRII_MOR_SRC_0070_TNG_ACC_COST = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0070_TNG_ACC_COST",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0070_TNG_ACCT_CLLCTCOST",
        sas_file="J_RRAP_MOR_SRC_0070_TNG_ACCT_CLLCTCOST.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0070_TNG_ACC_COST.doc = "J_RRAP_MOR_SRC_0070_TNG_ACCT_CLLCTCOST , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0070_TNG_ACC_COST"

    OW_SAS_DM_RRII_MOR_SRC_0100_TNG_PRD_D = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0100_TNG_PRD_D",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0100_TNG_TNG_PRD_D",
        sas_file="J_RRAP_MOR_SRC_0100_TNG_TNG_PRD_D.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0100_TNG_PRD_D.doc = "J_RRAP_MOR_SRC_0100_TNG_TNG_PRD_D , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0100_TNG_PRD_D"

    OW_SAS_DM_RRII_MOR_SRC_0120_BNS_SPL_NEW = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0120_BNS_SPL_NEW",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0120_BNS_SPL_NEW",
        sas_file="J_RRAP_MOR_SRC_0120_BNS_SPL_NEW.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0120_BNS_SPL_NEW.doc = "J_RRAP_MOR_SRC_0120_BNS_SPL_NEW , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0120_BNS_SPL_NEW"

    OW_SAS_DM_RRII_MOR_SRC_0130_BNS_CIS_NEW = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0130_BNS_CIS_NEW",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0130_BNS_CIS_DATA_NEW_FINAL",
        sas_file="J_RRAP_MOR_SRC_0130_BNS_CIS_DATA_NEW_FINAL.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0130_BNS_CIS_NEW.doc = "J_RRAP_MOR_SRC_0130_BNS_CIS_DATA_NEW_FINAL , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0130_BNS_CIS_NEW"

    OW_SAS_DM_RRII_MOR_SRC_0150_BNS_CST_XREF = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MOR_SRC_0150_BNS_CST_XREF",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRAP_MOR_SRC_0150_BNS_CUST_XREF2",
        sas_file="J_RRAP_MOR_SRC_0150_BNS_CUST_XREF2.sas",
    )
    OW_SAS_DM_RRII_MOR_SRC_0150_BNS_CST_XREF.doc = "J_RRAP_MOR_SRC_0150_BNS_CUST_XREF2 , Original taskID: IW503#OW_SAS_DM_RRII_MOR_SRC_0150_BNS_CST_XREF"


    OW_SAS_TNG_II_SRC_COMPT_INS_TRG_BRDM = InformaticaOperator(
        task_id="OW_SAS_TNG_II_SRC_COMPT_INS_TRG_BRDM",
        # bash_command="echo ^PSCRIPTS^ow_infa_call_etl.ksh DM_RRAP_IIAS wf_DM_RRAP_Load_RRAP_JobTgr_Recon_TNG",
        ssh_conn_id="infa-dm-rrap-conn",
        infa_workflow="wf_DM_RRAP_Load_RRAP_JobTgr_Recon_TNG",
    )
    OW_SAS_TNG_II_SRC_COMPT_INS_TRG_BRDM.doc = "TRIGGER TO BRDM INDICATING TANGERINE SOURCE CHECK COMPLETE , Original taskID: IW502#OW_SAS_TNG_II_SRC_COMPT_INS_TRG_BRDM"

    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0120_BNS_SPL_NEW >> OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0130_BNS_CIS_NEW >> OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0150_BNS_CST_XREF >> OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK
 
    OW_SAS_DM_RRII_MOR_00_SOURCE_CHECK >> OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G >> OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_G >> OW_SAS_DM_RRII_MORACCT02_UNLOAD_MORT_G
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR >> OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G
    OW_SAS_DM_RRII_MORACCT02_UNLOAD_MORT_G >> OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G
  
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR >> OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR >> OW_SAS_DM_RRII_MORSCRD02_LOAD_MOR_DATA
    OW_SAS_DM_RRII_MORACCT01_LOAD_MORT_GATHR >> OW_SAS_DM_RRII_MORSCRD03_LOAD_MOR_DATA09
    OW_SAS_DM_RRII_MORSCRD03_LOAD_MOR_DATA09 >> OW_SAS_DM_RRII_MORSCRD05_CUST_TO_MOR_09
    OW_SAS_DM_RRII_MORSCRD05_CUST_TO_MOR_09 >> OW_SAS_DM_RRII_MORSCRD06_DET_ITVL_MOR_G
    OW_SAS_DM_RRII_MORSCRD02_LOAD_MOR_DATA >> OW_SAS_DM_RRII_MORSCRD04_LOAD_CUST_T_MOR
    OW_SAS_DM_RRII_MORSCRD04_LOAD_CUST_T_MOR >> OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ
    OW_SAS_DM_RRII_MORSCRD06_DET_ITVL_MOR_G >> OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ
    OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ >> OW_SAS_DM_RRII_MORSCRD09_LOAD_ACCT_LIST
    OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ >> OW_SAS_DM_RRII_MORSCRD13_LOAD_CUST_LIST
    OW_SAS_DM_RRII_MORSCRD13_LOAD_CUST_LIST >> OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F
    OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA >> OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F
    OW_SAS_DM_RRII_MORSCRD09_LOAD_ACCT_LIST >> OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F
    OW_SAS_DM_RRII_MORSCRD12_LOAD_ACCT_LST_F >> OW_SAS_DM_RRII_MORSCRD18_LOAD_BSL
    OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F >> OW_SAS_DM_RRII_MORSCRD25_LOAD_CUST_PRODS
    OW_SAS_DM_RRII_MORSCRD25_LOAD_CUST_PRODS >> OW_SAS_DM_RRII_MORSCRD31_LOAD_MORTGAGES
    OW_SAS_DM_RRII_MORSCRD31_LOAD_MORTGAGES >> OW_SAS_DM_RRII_MORSCRD37_LOAD_CUST_SUMM
    OW_SAS_DM_RRII_MORSCRD17_LOAD_CUST_LST_F >> OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU
    OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU >> OW_SAS_DM_RRII_MORSCRD38_LOAD_CUST_PROF
    OW_SAS_DM_RRII_MORSCRD38_LOAD_CUST_PROF >> OW_SAS_DM_RRII_MORSCRD20_LOAD_CUST_TU
    OW_SAS_DM_RRII_MORSCRD19_LOAD_CRED_BUREU >> OW_SAS_DM_RRII_MORSCRD23_LOAD_D2D_TRANS
    OW_SAS_DM_RRII_MORSCRD23_LOAD_D2D_TRANS >> OW_SAS_DM_RRII_MORSCRD21_LOAD_CUST_PRODS
    OW_SAS_DM_RRII_MORSCRD01_LOAD_INPUT_DATA >> OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL
    OW_SAS_DM_RRII_MORSCRD18_LOAD_BSL >> OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL
    OW_SAS_DM_RRII_MORSCRD20_LOAD_CUST_TU >> OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL
    OW_SAS_DM_RRII_MORSCRD21_LOAD_CUST_PRODS >> OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL
    OW_SAS_DM_RRII_MORSCRD40_LOAD_COMBINE_AL >> OW_SAS_DM_RRII_MORSCRD45_CR_VAR_STATS_G
    
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_TNG_II_00_MORT_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0040_TNG_ACCT_DEP >> OW_SAS_TNG_II_00_MORT_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0050_TNG_CUST_DEP >> OW_SAS_TNG_II_00_MORT_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0070_TNG_ACC_COST >> OW_SAS_TNG_II_00_MORT_SOURCE_CHECK
    OW_SAS_DM_RRII_MOR_SRC_0100_TNG_PRD_D >> OW_SAS_TNG_II_00_MORT_SOURCE_CHECK
    OW_SAS_TNG_II_00_MORT_SOURCE_CHECK >> OW_SAS_TNG_II_01_DEFINE_STATUS_G
    OW_SAS_TNG_II_01_DEFINE_STATUS_G >> OW_SAS_TNG_II_02_MOR_PD_G
    OW_SAS_TNG_II_02_MOR_PD_G >> OW_SAS_TNG_II_03_LGD_ND_CALCULATION
    OW_SAS_TNG_II_03_LGD_ND_CALCULATION >> OW_RRII_LGD_UPDATE_TNG_IND_COST
    OW_RRII_LGD_UPDATE_TNG_IND_COST >> OW_RRII_LGD_UPDATE_TNG_LGD_ND
    OW_RRII_LGD_UPDATE_TNG_IND_COST >> OW_RRII_LGD_UPDATE_TNG_LGD_D
    OW_SAS_TNG_II_02_MOR_PD_G >> OW_SAS_TNG_II_03_MOR_LGD_D
    OW_SAS_TNG_II_02_MOR_PD_G >> OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT
    OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT >> OW_SAS_TNG_II_03_MOR_PD_SCORE_SEG_IFRS
    OW_RRII_LGD_UPDATE_TNG_LGD_ND >> OW_SAS_TNG_II_04_LGD_ND_SEGMENT
    OW_SAS_TNG_II_04_LGD_ND_SEGMENT >> OW_SAS_TNG_II_04_LGD_ND_SEG_IFRS
    OW_SAS_TNG_II_03_MOR_LGD_D >> OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT
    OW_RRII_LGD_UPDATE_TNG_LGD_D >> OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEG_IFRS
    OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT >> OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEG_IFRS
    OW_SAS_TNG_II_04_MOR_LGD_SCORE_SEGMNT >> OW_RRII_LGD_UPDATE_TNG_LGD_D
    OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT >> OW_SAS_TNG_II_04_MOR_REALIZED_DR
    OW_RRII_LGD_UPDATE_TNG_LGD_D >> OW_SAS_TNG_II_05_LGD_D_REALIZED
    OW_SAS_TNG_II_04_LGD_ND_SEGMENT >> OW_SAS_TNG_II_05_LGD_ND_REALIZED

    OW_SAS_DM_RRII_MOR_01_GEN_LKP >> OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0040_TNG_ACCT_DEP
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0050_TNG_CUST_DEP
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0070_TNG_ACC_COST
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0100_TNG_PRD_D
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0120_BNS_SPL_NEW
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0130_BNS_CIS_NEW
    OW_SAS_DM_RRII_MOR_01_HPI_CONV_BNS_TNG >> OW_SAS_DM_RRII_MOR_SRC_0150_BNS_CST_XREF
    OW_SAS_TNG_II_00_MORT_SOURCE_CHECK >> OW_SAS_TNG_II_SRC_COMPT_INS_TRG_BRDM

    OW_SAS_DM_RRII_MORSCRD50_LTV_DATA_G_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_DM_RRII_MORSCRD50_LTV_DATA_G_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G_CMA"
        sas_file="RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G_CMA.sas",
    )
    OW_SAS_DM_RRII_MORSCRD50_LTV_DATA_G_CMA.doc = (
        "RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G_CMA"
    )
    OW_SAS_DM_RRII_MORSCRD50_LOAD_LTV_DATA_G >> OW_SAS_DM_RRII_MORSCRD50_LTV_DATA_G_CMA

    OW_SAS_TNG_II_02_MOR_SCR_VAR_CREAT_G_CMA = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_TNG_II_02_MOR_SCR_VAR_CREAT_G_CMA",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/RRAP_TNG_02_MOR_SCORE_VAR_CREATION_G_CMA"
        sas_file="RRAP_TNG_02_MOR_SCORE_VAR_CREATION_G_CMA.sas",
    )
    OW_SAS_TNG_II_02_MOR_SCR_VAR_CREAT_G_CMA.doc = (
        "RRAP_TNG_02_MOR_SCORE_VAR_CREATION_G_CMA"
    )
    OW_SAS_TNG_II_01_DEFINE_STATUS_G >> OW_SAS_TNG_II_02_MOR_SCR_VAR_CREAT_G_CMA
    OW_SAS_TNG_II_02_MOR_SCR_VAR_CREAT_G_CMA >> OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G
    OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G >> OW_SAS_TNG_II_03_LGD_ND_CALCULATION
    OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G >> OW_SAS_TNG_II_03_MOR_LGD_D
    OW_SAS_TNG_II_02_MOR_SCRE_VAR_CREAT_G >> OW_SAS_TNG_II_03_MOR_PD_SCORE_SEGMENT
