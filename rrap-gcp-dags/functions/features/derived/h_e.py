from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_ACCT_PRFM_FACT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM",
                  "features.SRC_SYS_CD",
                  "features.HELOC_F",
                  "features.ASST_CL_NUM",
                  "features.BASEL_PRD_TP_CD",
                  "reference.RPTG_LGD_HC_LKP",]

DOWNSTREAM_ASSET = "features.H_E"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_h_e"],
    "export_spl": ["duckdb_delete_h_e"],
    "export_mor": ["duckdb_delete_h_e"],
    "export_tng": ["duckdb_delete_h_e"],
    "duckdb_delete_h_e": ["duckdb_load_h_e"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE WHEN hc.H_E IS NOT NULL THEN hc.H_E
        ELSE NULL
        END AS H_E
    FROM {UPSTREAM_ASSET[5]} dim
    LEFT JOIN {UPSTREAM_ASSET[0]} ks ON
        dim.BASEL_ACCT_ID = ks.BASEL_ACCT_ID
    LEFT JOIN {UPSTREAM_ASSET[6]} sys_cd ON
        dim.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
        AND sys_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[7]} heloc ON
        dim.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
        AND heloc.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[8]} cl_num ON
        dim.BASEL_ACCT_ID = cl_num.BASEL_ACCT_ID
        AND cl_num.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[9]} tp_cd ON
        dim.BASEL_ACCT_ID = tp_cd.BASEL_ACCT_ID
        AND tp_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[10]} hc ON
        case 
            when sys_cd.SRC_SYS_CD = 'KS' 
            and ks.PRD_CD = 'SCL' 
            and ks.SUB_PRD_CD = 'CS' 
            and heloc.HELOC_F = 'N' 
            and cl_num.ASST_CL_NUM = 2 then 'Financial'
            when sys_cd.SRC_SYS_CD = 'SPL' and cl_num.ASST_CL_NUM = 2 and  
            tp_cd.BASEL_PRD_TP_CD LIKE 'ITL_AUTO%' then 'Other Physical'
            else NULL
        end = hc.COLLATERAL_TYPE
        AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN CAST(EFF_FROM_YR_MTH AS INTEGER) AND CAST(EFF_TO_YR_MTH AS INTEGER)
    WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        CASE WHEN hc.H_E IS NOT NULL THEN hc.H_E
        ELSE NULL
        END AS H_E
    FROM {UPSTREAM_ASSET[2]} spl
    LEFT JOIN {UPSTREAM_ASSET[6]} sys_cd ON
        spl.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
        AND sys_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[7]} heloc ON
        spl.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
        AND heloc.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[8]} cl_num ON
        spl.BASEL_ACCT_ID = cl_num.BASEL_ACCT_ID
        AND cl_num.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[9]} tp_cd ON
        spl.BASEL_ACCT_ID = tp_cd.BASEL_ACCT_ID
        AND tp_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[10]} hc ON
        case 
            when sys_cd.SRC_SYS_CD = 'KS' 
            and heloc.HELOC_F = 'N' 
            and cl_num.ASST_CL_NUM = 2 then 'Financial'
            when sys_cd.SRC_SYS_CD = 'SPL' and cl_num.ASST_CL_NUM = 2 and  
            tp_cd.BASEL_PRD_TP_CD LIKE 'ITL_AUTO%' then 'Other Physical'
            else NULL
        end = hc.COLLATERAL_TYPE
        AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN CAST(EFF_FROM_YR_MTH AS INTEGER) AND CAST(EFF_TO_YR_MTH AS INTEGER)
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        NULL AS H_E
    FROM {UPSTREAM_ASSET[3]} mor
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        NULL AS H_E
    FROM {UPSTREAM_ASSET[4]} tng
    INNER JOIN {UPSTREAM_ASSET[5]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_h_e(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_h_e(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                H_E
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__h_e.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__h_e.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__h_e.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__h_e.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass

