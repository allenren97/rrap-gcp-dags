import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
        "features.BASEL_ACCT_ID",
        "features.ASST_CL_NUM",
        "features.TOT_EXPSR_ABOVE_1500K_LMT_F",
        "features.BASEL_PRD_TP_CD",
        "features.PRD_CD",
        "features.SUB_PRD_CD",
        "features.RNTL_PRPTY_F_IFRS9",
        "features.CLP_FLAG",
        "features.TRANSACTOR_FLAG_QRR",
        "reference.RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP"
    ]

DOWNSTREAM_ASSET = "features.BCAR_SCHED_NUM_50"
DEPENDENCIES = {
    'duckdb_clear': ['duckdb_load'],
}


def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH get_pop as (
        SELECT
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD
        FROM {UPSTREAM_ASSET[0]} acct
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
        )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}' as OBSN_DT,
        acct.BASEL_ACCT_ID,
        acct.SRC_SYS_CD,
        COALESCE(c.BCAR_SCHED_NUM_50, d.BCAR_SCHED_NUM_50) AS BCAR_SCHED_NUM_50
    FROM get_pop acct
    LEFT JOIN {UPSTREAM_ASSET[1]} asst ON
        acct.BASEL_ACCT_ID = asst.BASEL_ACCT_ID
        AND asst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[2]} tot_ex ON
        acct.BASEL_ACCT_ID = tot_ex.BASEL_ACCT_ID
        AND tot_ex.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[3]} prd_tp ON
        acct.BASEL_ACCT_ID = prd_tp.BASEL_ACCT_ID
        AND prd_tp.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[4]} prd_cd ON
        acct.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
        AND prd_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[5]} sub_prd ON
        acct.BASEL_ACCT_ID = sub_prd.BASEL_ACCT_ID
        AND sub_prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    -- MAY NEED TO USE IFRS9 VERSION OF RNTL_PRPTY_F
    LEFT JOIN {UPSTREAM_ASSET[6]} rntl ON
        acct.BASEL_ACCT_ID = rntl.BASEL_ACCT_ID
        AND rntl.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[7]} clp ON
        acct.BASEL_ACCT_ID = clp.BASEL_ACCT_ID
        AND clp.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[8]} qrr ON
        acct.BASEL_ACCT_ID = qrr.BASEL_ACCT_ID
        AND qrr.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    LEFT JOIN {UPSTREAM_ASSET[9]} c ON
        UPPER(acct.SRC_SYS_CD) = UPPER(c.SRC_SYS_CD) 
        AND asst.ASST_CL_NUM = c.ASST_CL_NUM 
        AND UPPER(tot_ex.TOT_EXPSR_ABOVE_1500K_LMT_F) = UPPER(c.TOT_EXPSR_ABOVE_LMT_F)
        AND UPPER(c.TOT_EXPSR_ABOVE_LMT_F) = 'Y'
        AND ('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN CAST(c.EFF_FROM_YR_MTH AS INTEGER) AND CAST(c.EFF_TO_YR_MTH AS INTEGER))
    LEFT JOIN {UPSTREAM_ASSET[9]} d ON
        UPPER(acct.SRC_SYS_CD) = UPPER(d.SRC_SYS_CD) 
        AND UPPER(prd_tp.BASEL_PRD_TP_CD) = UPPER(d.BASEL_PRD_TP_CD) 
        AND COALESCE(UPPER(prd_cd.PRD_CD), '_') = COALESCE(UPPER(d.PRD_CD), '_')
        AND COALESCE(UPPER(sub_prd.SUB_PRD_CD), '_') = COALESCE(UPPER(d.SUB_PRD_CD), '_')
        AND COALESCE(UPPER(rntl.RNTL_PRPTY_F_IFRS9), '_') = COALESCE(UPPER(d.RNTL_PRPTY_F), '_')
        AND COALESCE(UPPER(clp.CLP_FLAG), '_') = COALESCE(UPPER(d.CLP_FLAG), '_')
        AND COALESCE(UPPER(qrr.TRANSACTOR_FLAG_QRR), '_') = COALESCE(UPPER(d.TRANSACTOR_FLAG_QRR), '_')
        AND d.TOT_EXPSR_ABOVE_LMT_F IS NULL
        AND ('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN CAST(d.EFF_FROM_YR_MTH AS INTEGER) AND CAST(d.EFF_TO_YR_MTH AS INTEGER))
    """
):
    pass