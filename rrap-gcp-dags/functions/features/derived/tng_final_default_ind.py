from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}


UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "ingestion.TNG_ACCT_WRITEOFF",
    "features.TNG_ADJ_DEFAULT_IND",
]
DOWNSTREAM_ASSET = "features.TNG_FINAL_DEFAULT_IND"
DEPENDENCIES = {
    "export_adj_ind_2": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_adj_ind_2(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH
            writeoff AS (
                SELECT
                    account_id,
                    MIN(month_end_dt) AS month_end_dt,
                    MIN(writeoff_date) AS writeoff_dt,
                    SUM(writeoff_amt) AS writeoff_amt
                FROM
                    (
                        SELECT
                            CASE
                                WHEN SUBSTRING(TRIM(mtg_num), 1, 1) = 'F'
                                OR SUBSTRING(TRIM(mtg_num), 1, 1) = 'M'
                                OR SUBSTRING(TRIM(mtg_num), 1, 1) = 'R' THEN TRIM(mtg_num)
                                WHEN Provider = 'FNAL' THEN concat ('FNAL~', TRIM(mtg_num), '~1')
                                WHEN Provider = 'MCAP' THEN concat ('MCAP~', TRIM(mtg_num), '~1')
                                WHEN Provider = 'DIR'
                                AND SUBSTRING(TRIM(mtg_num), 1, 1) != '7' THEN concat ('MCAP~', TRIM(mtg_num), '~1')
                                ELSE concat ('MBS~', TRIM(mtg_num))
                            END AS account_id,
                            month_end_dt,
                            writeoff_date,
                            writeoff_amt
                        FROM
                            ingestion.TNG_ACCT_WRITEOFF AS main
                    )
                WHERE
                    TRIM(account_id) NOT IN (
                        'MBS~7000174901',
                        'MBS~7000225553',
                        'MBS~7700206059',
                        'MCAP~8207220~1',
                        'MBS~7000068947',
                        'MBS~7000034722',
                        'MBS~203566',
                        'MBS~7000039018',
                        'MBS~7000124332',
                        'MCAP~8268926~1',
                        'MBS~7700174435',
                    )
                GROUP BY
                    account_id
            )
        SELECT
            main.account_id,
            TRIM(default_ind) AS def_ind,
            CASE
                WHEN writeoff_dt <= main.month_end_dt THEN 'Y'
                ELSE TRIM(default_ind)
            END AS wo_def_ind
        FROM
            ingestion.TNG_ACCT_MO AS main
            INNER JOIN writeoff ON main.account_id = writeoff.account_id
        WHERE
            main.month_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    """,
):
    pass


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
        SELECT
            main.month_end_dt as obsn_dt,
            bad.basel_acct_id,
            TRIM(main.account_id) AS account_id,
            CASE
                WHEN end_principal_balance = 0
                AND COALESCE(b.wo_def_ind, '') != 'Y' THEN 'N'
                WHEN COALESCE(a.TNG_ADJ_DEFAULT_IND, '') = 'Y'
                OR COALESCE(b.wo_def_ind, '') = 'Y' THEN 'Y'
                ELSE TRIM(main.default_ind)
            END AS TNG_FINAL_DEFAULT_IND
        FROM
            { UPSTREAM_ASSET[1] } AS main
        inner join { UPSTREAM_ASSET[0] } bad on
            bad.src_app_cd ='TNG-MOR'
            and bad.src_sys_del_f != 'Y'
            and trim(main.account_id) = trim(bad.src_app_id)
        left join features.TNG_ADJ_DEFAULT_IND AS a ON
            trim(main.account_id) = trim(a.account_id)
            and main.month_end_dt = a.obsn_dt
        left join '{{{{ task_instance.xcom_pull(task_ids="derived__tng_final_default_ind.export_adj_ind_2", key="parquet") }}}}' AS b ON
            trim(main.account_id) = trim(b.account_id)
        where
            main.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


