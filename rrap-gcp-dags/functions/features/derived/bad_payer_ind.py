from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT", ]
DOWNSTREAM_ASSET = "features.BAD_PAYER_IND"
DEPENDENCIES = {
    "duckdb_delete_bad_payer_ind": ["duckdb_load_bad_payer_ind"],
}


def duckdb_delete_bad_payer_ind(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_bad_payer_ind(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_CUST_ID,
                CAST(CASE WHEN (
	                COALESCE(BNKRPY_CNT, 0) +  
		            COALESCE(COLCTN_CNT, 0) + 
		            COALESCE(NOT_SUFFICENT_FUNDS_CHQ_CNT, 0)  + 
		            COALESCE(DERGTRY_PUB_RECD_CNT, 0)  + 
		            COALESCE(LEGAL_CNT, 0)
		             ) >= 1 
                    THEN 1 
                    ELSE 0 
                END AS CHARACTER) AS BAD_PAYER_IND
            FROM
                {UPSTREAM_ASSET[0]}
            WHERE
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """,
):
    pass


