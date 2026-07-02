from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.STEP_FLAG"
DEPENDENCIES = {
    "duckdb_delete": ["export_ks", "export_mor", "export_spl"],
    "export_ks": ["duckdb_load"],
    "export_mor": ["duckdb_load"],
    "export_spl": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from { DOWNSTREAM_ASSET } where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    select 
        basel_acct_id, 
        (case 
            when step_pln_agrmnt_num is not null 
                and trim(step_pln_agrmnt_num) != '' 
                and cast(step_pln_agrmnt_num as bigint) > 0 
            then 'STEP' 
            else 'STANDALONE' 
        end) as step_flag,
        'KS' as SRC_SYS_CD
        from {UPSTREAM_ASSET[0]}
        where mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select 
        basel_acct_id, 
        (case 
            when step_pln_agrmnt_num is not null 
                and trim(step_pln_agrmnt_num) != '' 
                and cast(step_pln_agrmnt_num as bigint) > 0 
            then 'STEP' 
            else 'STANDALONE' 
        end) as step_flag,
        'MOR' as SRC_SYS_CD
        from {UPSTREAM_ASSET[1]}
        where mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select 
        basel_acct_id, 
        (case
            when step_pln_agrmnt_num is not null 
                and trim(step_pln_agrmnt_num) != '' 
                and cast(step_pln_agrmnt_num as bigint) > 0 
            then 'STEP' 
            else 'STANDALONE'
        end) as step_flag,
        'SPL' as SRC_SYS_CD
        from {UPSTREAM_ASSET[2]}
        where mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } by name
    FROM (
        SELECT *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__step_flag.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__step_flag.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__step_flag.export_spl", key="parquet") }}}}',
        ], union_by_name = true)
    )
    """,
):
    pass


