from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'features.PRD_ID',
    'reference.RPTG_PRD_LKP_KS',
    'reference.RPTG_PRD_LKP_SPL',
    'reference.RPTG_PRD_LKP_MOR'
]
DOWNSTREAM_ASSET = 'features.PD_BAND_EXPSR_CL_KEY_VAL'
DEPENDENCIES = {
    'duckdb_delete': ['duckdb_load'],
}


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            WITH prd AS (
                SELECT * FROM {UPSTREAM_ASSET[0]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            )
            SELECT
                prd.BASEL_ACCT_ID,
                prd.OBSN_DT,
                prd.SRC_SYS_CD,
                COALESCE(
                    lkp1.PD_BAND_EXPSR_CL_KEY_VAL,
                    lkp2.PD_BAND_EXPSR_CL_KEY_VAL,
                    lkp3.PD_BAND_EXPOSURE_CLASS_KEY_VALUE
                ) AS PD_BAND_EXPSR_CL_KEY_VAL
            FROM prd
            LEFT JOIN {UPSTREAM_ASSET[1]} lkp1
                ON TRIM(UPPER(prd.PRD_ID)) = TRIM(UPPER(lkp1.PRD_ID))
                AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN lkp1.EFF_FROM_YR_MTH AND lkp1.EFF_TO_YR_MTH
            LEFT JOIN {UPSTREAM_ASSET[2]} lkp2
                ON TRIM(UPPER(prd.PRD_ID)) = TRIM(UPPER(lkp2.PRD_ID))
                AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN lkp2.EFF_FROM_YR_MTH AND lkp2.EFF_TO_YR_MTH
            LEFT JOIN {UPSTREAM_ASSET[3]} lkp3
                ON TRIM(UPPER(prd.PRD_ID)) = TRIM(UPPER(lkp3.PRODUCT_ID))
                AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN lkp3.EFF_FROM_YR_MTH AND lkp3.EFF_TO_YR_MTH
        )    
    """
):
    pass


