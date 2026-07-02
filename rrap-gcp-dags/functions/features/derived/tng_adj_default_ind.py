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
]
DOWNSTREAM_ASSET = "features.TNG_ADJ_DEFAULT_IND"
DEPENDENCIES = {
    "export_adj_ind_1": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


### DEPRECATED LOOP
# def export_tng_acct(
#     duckdb_conn_id="duckdb-conn",
#     sql=r"""
# SELECT
#     trim(account_id) as account_id,
#     trim(default_ind) as def_ind,
#     lag(trim(default_ind),1) over (partition by account_id order by month_end_dt) as prev_def_ind,
#     NULL as adj_ind,
#     month_end_dt,
#     days_arrears_cnt,
#     tot_arrears_amt,
#     pymts_arrears_cnt,
#     sundry_balance
# FROM
#     ingestion.TNG_ACCT_MO
# WHERE trim(account_id) IN (SELECT DISTINCT trim(account_id) FROM ingestion.TNG_ACCT_MO WHERE trim(default_ind)='Y')
# order by
#     account_id, month_end_dt
#     """,
# ):
#     pass

# def adj_ind_1():
#     """Calculates default indicator for adjustment #1 for all of history"""
#     context = get_current_context()
#     tng_acct = ddb.sql(
#         f"select * from '{context['ti'].xcom_pull(task_ids='export_acc_list', key='parquet')}'"
#     ).df()
#     tng_acct["adj_ind"] = tng_acct["adj_ind"].astype(str)
#     for i in range(0, len(tng_acct)):
#         if i == 0:
#             tng_acct.loc[i, "adj_ind"] = tng_acct["def_ind"][i]
#         elif (
#             (
#                 str(tng_acct["account_id"][i - 1]).strip()
#                 == str(tng_acct["account_id"][i]).strip()
#                 and str(tng_acct["adj_ind"][i - 1]).strip() == "Y"
#             )
#             and str(tng_acct["def_ind"][i]).strip() == "N"
#             and (
#                 (tng_acct["DAYS_ARREARS_CNT"][i]) > 0
#                 or (tng_acct["TOT_ARREARS_AMT"][i]) > 0
#                 or (tng_acct["PYMTS_ARREARS_CNT"])[i] > 0
#                 or (tng_acct["SUNDRY_BALANCE"][i]) > 250
#             )
#         ):
#             delta = relativedelta.relativedelta(
#                 datetime.strptime(str(tng_acct["MONTH_END_DT"][i])[:10], "%Y-%m-%d"),
#                 datetime.strptime(tng_acct["MONTH_END_DT"][i - 1], "%Y-%m-%d"),
#             )
#             if delta.months + delta.years * 12 == 1:
#                 tng_acct.loc[i, "adj_ind"] = "Y"
#             else:
#                 tng_acct.loc[i, "adj_ind"] = tng_acct["def_ind"][i]
#         else:
#             tng_acct.loc[i, "adj_ind"] = tng_acct["def_ind"][i]

# table = pa.table(tng_acct)
# pq.write_table(table, 'tng_status_1.parquet')
### DEPRECATED LOOP


def export_adj_ind_1(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            TRIM(main.account_id) AS account_id,
            CASE
                WHEN COALESCE(TRIM(TNG_ADJ_DEFAULT_IND), '') = 'Y'
                AND COALESCE(TRIM(default_ind), '') = 'N'
                AND (
                    days_arrears_cnt > 0
                    OR tot_arrears_amt > 0
                    OR pymts_arrears_cnt > 0
                    OR sundry_balance > 250
                )
                --AND date_diff ('month', dv.obsn_dt, main.month_end_dt) = 1
                THEN 'Y'
                ELSE TRIM(default_ind)
            END AS TNG_ADJ_DEFAULT_IND
        FROM
            ingestion.TNG_ACCT_MO AS main
            LEFT JOIN features.TNG_ADJ_DEFAULT_IND AS dv ON TRIM(dv.account_id) = TRIM(main.account_id)
            AND dv.obsn_dt = last_day (DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 1 MONTH)
        WHERE
            TRIM(main.account_id) IN (
                SELECT DISTINCT
                    TRIM(account_id)
                FROM
                    ingestion.TNG_ACCT_MO
                WHERE
                    TRIM(default_ind) = 'Y'
            )
            AND main.month_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ORDER BY
            main.account_id,
            main.month_end_dt
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
            a.month_end_dt as obsn_dt,
            b.basel_acct_id,
            a.account_id,
            c.TNG_ADJ_DEFAULT_IND
        from { UPSTREAM_ASSET[1] } a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
        left outer join '{{{{ task_instance.xcom_pull(task_ids="derived__tng_adj_default_ind.export_adj_ind_1", key="parquet") }}}}' c on
            trim(a.account_id) = trim(c.account_id)
        where
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


