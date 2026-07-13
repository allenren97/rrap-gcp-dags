from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    'ingestion.IWF_ACTY_ROLLUP',
    'ingestion.IWF_CUST_ACCT',
    'ingestion.IWD_CHNL',
    'ingestion.CUST_XREF',
    'ingestion.BASEL_CUST_DIM',
]
DOWNSTREAM_ASSET = "features.WTHDRWL_CNT"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        with cust_acct_trxn as (select c.cust_base_key,
                                    c.CUST_BASE_KEY,
                                    c.acct_base_key,
                                    c.time_key,
                                    a.dlvy_key,
                                    a.acty_cnt,
                                    a.acty_tot_amt,
                                    d.txn_grp_key,
                                    d.txn_grp_desc
                            from {UPSTREAM_ASSET[0]} as a,
                                {UPSTREAM_ASSET[1]} as c,
                                {UPSTREAM_ASSET[2]} as d
                            where c.time_key = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                                and c.sum_srvc_code in ('CHQ','SAV')
                                and c.acct_lcst in ('A','I','D')
                                and c.acct_base_key = a.acct_base_key
                                and c.time_key = a.time_key
                                and a.dlvy_key=d.dlvy_key),

        cust_acct_trxn_aggr AS (
            SELECT
                CUST_BASE_KEY,
                SUM(
                    CASE
                        WHEN TXN_GRP_KEY IN (1100,1800,2300,2700,16600,17400,21000,22100,22400,23100,23800,25400,26400,29000,30200,30700,31400,
                            32500,33400, 34100,44100,44500,48000,50200,72500, 8300,15000,16300,50300) THEN ACTY_CNT
                        ELSE 0
                    END
                ) AS WTHDRWL_CNT
            FROM
                cust_acct_trxn
            GROUP BY
                CUST_BASE_KEY
        ),

        cust_id_lookup AS (
            SELECT DISTINCT cust_base_key, CUST_ID
            FROM {UPSTREAM_ASSET[3]}
        ),

        basel_id_lookup AS (
            SELECT basel_cust_id, cust_cid
            FROM {UPSTREAM_ASSET[4]}
        )

        SELECT
            b_id.basel_cust_id,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            agg.WTHDRWL_CNT
        FROM
            cust_acct_trxn_aggr AS agg
        JOIN
            cust_id_lookup AS c_id ON agg.CUST_BASE_KEY = c_id.CUST_BASE_KEY
        JOIN
            basel_id_lookup AS b_id ON c_id.CUST_ID = b_id.cust_cid
    )
    """,
):
    pass


