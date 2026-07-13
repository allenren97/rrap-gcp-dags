import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 
    'reference.IWD_CHNL_RM', 
    'ingestion.IWF_CUST_ACTY_RLP',
    'ingestion.IWF_CUST_ACCT',
    'ingestion.CUST_XREF',
    'ingestion.BASEL_CUST_DIM',
]

DOWNSTREAM_ASSET = 'features.OD_NUM'

DEPENDENCIES = {
    'duckdb_clear_OD_NUM': ['duckdb_derive_OD_NUM'],
}


def duckdb_clear_OD_NUM(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_OD_NUM(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    WITH raw_data AS (
		select d.basel_cust_id, a.TXN_GRP_KEY_RM, b.ACCT_KEY, a.dlvy_key
		from { UPSTREAM_ASSET[0] } a
		inner join (select distinct 
								dlvy_key, 
								CUST_BASE_KEY, 
								ACCT_KEY, 
								time_key 
							from 
								{ UPSTREAM_ASSET[1] } 
							where 
								time_key = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}) b
		on b.time_key = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
		and a.dlvy_key = b.dlvy_key
		inner join { UPSTREAM_ASSET[2] } c
		ON b.CUST_BASE_KEY = c.CUST_BASE_KEY AND b.ACCT_KEY = c.ACCT_KEY
		AND b.TIME_KEY = c.TIME_KEY AND c.ACCT_LCST IN ('A', 'I', 'D') AND c.PRIM_CUST_F = 'P'
		inner join { UPSTREAM_ASSET[3] } x
		on c.CUST_BASE_KEY = x.CUST_BASE_KEY
		right join { UPSTREAM_ASSET[4] } d
		on trim(x.CUST_ID) = trim(d.CUST_CID)
		where b.cust_base_key is not null and a.TXN_GRP_KEY_RM is not null
	),

    base AS (
        SELECT DISTINCT 
            BASEL_CUST_ID,
            TXN_GRP_KEY_RM,
            dlvy_key
        FROM
            raw_data
        )

    SELECT
        BASEL_CUST_ID,
        SUM(CASE WHEN TXN_GRP_KEY_RM = 'O' THEN 1 ELSE 0 END) AS OD_NUM,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
    FROM
        base
    GROUP BY 
        BASEL_CUST_ID
    )
    """,
    pool="duckdb_pool",
    pool_slots=96,
    resource_tier="MED",
):
    pass



