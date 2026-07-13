from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.CANDN_POPN_MTH_SNAPSHOT",
    "ingestion.HH_DSPSBL_INCM_QTR",
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",
    "ingestion.TM_DIM",
    "reference.DLGD_METRPL_SCALNG_FACTR_DIM",]

DOWNSTREAM_ASSET = "features.METRPL_SCRI_VAL"
DEPENDENCIES = {
    "branch_decide": ["export_qtrly_pop"],
    "export_qtrly_pop": ["export_hh_disp_income"],
    "export_hh_disp_income": ["export_smoothed_index"],
    "export_smoothed_index": ["export_candn_per_capita_incm_amt"],
    "export_candn_per_capita_incm_amt": ["export_metrpl_scri_val"],
    "export_metrpl_scri_val": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
    "duckdb_load": ['empty_task']
}

def branch_decide():
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    month = int(rundate.split('-')[1])
    if month not in (1,4,7,10):
        return ["derived__metrpl_scri_val.export_qtrly_pop"]
    
    return 'empty_task'

def export_qtrly_pop(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
      SELECT
        AVG(CANDN_POPN_THSNDTH_VAL) as QTRLY_POP
      FROM {UPSTREAM_ASSET[0]}
      WHERE MTH_TM_ID between {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*9 and {{{{task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} -40 * 7
    """
): 
  pass

def export_hh_disp_income(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
      SELECT HH_DSPSBL_INCM_MILLNTH_AMT as HH_DISP_INCOME
      FROM {UPSTREAM_ASSET[1]}
      WHERE MTH_TM_ID =  {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*7
      AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' >= EFF_FROM_YR_MTH
      AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' < EFF_TO_YR_MTH
    """
):
  pass

def export_smoothed_index(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
      SELECT 
        trim(
            regexp_replace(
            translate(
                lower(coalesce(LABEL_2, '')),
                'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                'aaaaaaceeeeiiiinooooouuuuyy'
            ),
            '[^a-z0-9]+',
            ' ',
            'g'
            )
        ) as METRPL_AREA_NM,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} as MTH_TM_ID,
        AVG(INDEX) as SMOOTHED_INDEX
        FROM {UPSTREAM_ASSET[2]}
        WHERE MTH_TM_ID between {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*(12-1+5) 
        and {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40 * 5
        and LABEL_2 <> '6'
        group by LABEL_2
        order by LABEL_2
    """
):
  pass

def export_candn_per_capita_incm_amt(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
        SELECT
            ind.METRPL_AREA_NM as METRPL_AREA_NM,
            ind.SMOOTHED_INDEX,
            ind.MTH_TM_ID,
            qp.QTRLY_POP,
            hh.HH_DISP_INCOME,
            strftime(tm.tm_lvl_end_dt, '%Y%m') as YRMTH,
            (1000 * HH_DISP_INCOME/QTRLY_POP) as CANDN_PER_CAPITA_INCM_AMT
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__metrpl_scri_val.export_smoothed_index", key="parquet")}}}}' ind
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__metrpl_scri_val.export_hh_disp_income", key="parquet")}}}}' hh ON 1=1
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__metrpl_scri_val.export_qtrly_pop", key="parquet")}}}}' qp ON 1=1
        LEFT JOIN {UPSTREAM_ASSET[3]} tm ON tm.TM_ID = ind.MTH_TM_ID and tm.TM_LVL = 'Month'
      """
):
  pass

def export_metrpl_scri_val(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
      SELECT 
            *,
            ((SMOOTHED_INDEX * SCALNG_FACTR_VAL) / (CANDN_PER_CAPITA_INCM_AMT)) as METRPL_SCRI_VAL
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__metrpl_scri_val.export_candn_per_capita_incm_amt", key="parquet")}}}}' cc
        LEFT JOIN {UPSTREAM_ASSET[4]} scal
            ON trim(
                regexp_replace(
                translate(
                    lower(coalesce(scal.METRPL_AREA_NM, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
            ) = cc.METRPL_AREA_NM
            AND scal.CRNT_F = 'Y'
            AND YRMTH >= scal.EFF_FROM_YR_MTH and YRMTH < scal.EFF_TO_YR_MTH
    """
):
  pass

def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
      INSERT INTO {DOWNSTREAM_ASSET} BY NAME
      FROM (
        SELECT
        TRUNC(METRPL_SCRI_VAL,4) as METRPL_SCRI_VAL,
        YRMTH,
        METRPL_AREA_NM,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__metrpl_scri_val.export_metrpl_scri_val", key="parquet")}}}}')
        )


    """
):
  pass

def empty_task(trigger_rule='all_done'):
   pass