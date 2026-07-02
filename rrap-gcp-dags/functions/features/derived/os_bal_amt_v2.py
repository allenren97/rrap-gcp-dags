from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "ingestion.TM_DIM",
]

DOWNSTREAM_ASSET = "features.OS_BAL_AMT_V2"

DEPENDENCIES = {
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_spl(
    duckdb_conn_id="duckdb-conn",
    resource_tier="MED",
    pool_slots=32,
    sql=r"""
        WITH
            main_curr AS (
                SELECT
                    TRIM(main.loan_num) AS loan_num,
                    TRIM(main.crnt_br_loctn_trnst) AS cab,
                    main.basel_acct_id,
                    main.tot_crnt_bal_amt,
                    main.add_on_bal_amt,
                    main.accr_intr
                FROM
                    ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS main
                WHERE
                    main.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                    AND main.recd_stat_cd IN (4, 5, 6, 7, 8)
            ),
            pit_curr AS (
                SELECT
                    pit.basel_acct_id,
                    pit.pit_status_cross_default_orig
                FROM
                    features.PIT_STATUS_CROSS_DEFAULT_ORIG AS pit
                WHERE
                    pit.obsn_dt = DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            spl_distinct_default_accts AS (
                SELECT DISTINCT
                    m.loan_num,
                    m.cab
                FROM
                    main_curr AS m
                    INNER JOIN pit_curr AS p ON p.basel_acct_id = m.basel_acct_id
                WHERE
                    UPPER(TRIM(p.pit_status_cross_default_orig)) IN ('DEF', 'CHG')
            ),
            main_hist AS (
                SELECT
                    TRIM(main.loan_num) AS loan_num,
                    TRIM(main.crnt_br_loctn_trnst) AS cab,
                    main.basel_acct_id,
                    main.accr_intr,
                    tm.tm_lvl_end_dt
                FROM
                    ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS main
                    INNER JOIN ingestion.TM_DIM AS tm ON tm.tm_id = main.mth_tm_id
                    INNER JOIN spl_distinct_default_accts AS d ON d.loan_num = TRIM(main.loan_num)
                    AND d.cab = TRIM(main.crnt_br_loctn_trnst)
                WHERE
                    main.mth_tm_id <= {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                    AND main.recd_stat_cd IN (4, 5, 6, 7, 8)
            ),
            pit_hist AS (
                SELECT
                    pit.basel_acct_id,
                    pit.obsn_dt,
                    pit.pit_status_cross_default_orig
                FROM
                    features.PIT_STATUS_CROSS_DEFAULT_ORIG AS pit
                WHERE
                    pit.obsn_dt <= DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            spl_default_accts_hist AS (
                SELECT
                    h.loan_num,
                    h.cab,
                    h.tm_lvl_end_dt,
                    p.pit_status_cross_default_orig,
                    h.accr_intr
                FROM
                    main_hist AS h
                    LEFT JOIN pit_hist AS p ON p.basel_acct_id = h.basel_acct_id
                    AND p.obsn_dt = h.tm_lvl_end_dt
            ),
            ranked_hist AS (
                SELECT
                    loan_num,
                    cab,
                    tm_lvl_end_dt,
                    pit_status_cross_default_orig,
                    accr_intr,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            loan_num,
                            cab
                        ORDER BY
                            loan_num,
                            cab,
                            tm_lvl_end_dt DESC
                    ) AS rn
                FROM
                    spl_default_accts_hist
            ),
            min_dates AS (
                SELECT
                    loan_num,
                    cab,
                    MIN(tm_lvl_end_dt) AS earliest_dt
                FROM
                    spl_default_accts_hist
                GROUP BY
                    loan_num,
                    cab
            ),
            with_prior AS (
                SELECT
                    curr.loan_num,
                    curr.cab,
                    curr.tm_lvl_end_dt,
                    curr.pit_status_cross_default_orig,
                    curr.accr_intr,
                    prev.pit_status_cross_default_orig AS lag_pit,
                    prev.accr_intr AS lag_accr_intr,
                    m.earliest_dt AS last_tm_lvl_end_dt
                FROM
                    ranked_hist AS curr
                    LEFT JOIN ranked_hist AS prev ON curr.loan_num = prev.loan_num
                    AND prev.rn = curr.rn - 1
                    AND curr.cab = prev.cab
                    LEFT JOIN min_dates AS m ON curr.loan_num = m.loan_num
                    AND curr.cab = m.cab
            ),
            int_at_def AS (
                SELECT
                    loan_num,
                    cab,
                    tm_lvl_end_dt,
                    CASE
                    -- Grabs the interest accrued when the account/loan first turned to DEF
                        WHEN (
                            TRIM(lag_pit) IN ('DEF')
                            AND TRIM(pit_status_cross_default_orig) IN ('CUR')
                        )
                        -- Accounts for when the loan/account was always DEF/NULL/CHG
                        OR (
                            tm_lvl_end_dt = last_tm_lvl_end_dt
                            AND (
                                TRIM(pit_status_cross_default_orig) IN ('DEF')
                                -- OR pit_status_cross_default_orig IS NULL
                            )
                        ) THEN CASE
                            WHEN TRIM(lag_pit) IN ('DEF')
                            AND TRIM(pit_status_cross_default_orig) IN ('CUR') THEN lag_accr_intr
                            ELSE accr_intr
                        END
                        ELSE NULL
                    END AS int_at_default
                FROM
                    with_prior
            ),
            int_at_def_recent AS (
                SELECT
                    loan_num,
                    cab,
                    MAX(tm_lvl_end_dt) AS tm_lvl_end_dt
                FROM
                    int_at_def
                WHERE
                    int_at_default IS NOT NULL
                GROUP BY
                    loan_num,
                    cab
            ),
            os_bal_amtv2 AS (
                SELECT
                    m.basel_acct_id,
                    CASE
                        WHEN p.pit_status_cross_default_orig IN ('DEF', 'CHG') THEN COALESCE(
                            m.tot_crnt_bal_amt + i.int_at_default + m.add_on_bal_amt,
                            0
                        )
                        WHEN p.pit_status_cross_default_orig IN ('CUR', 'CLO') THEN COALESCE(
                            m.tot_crnt_bal_amt + m.accr_intr + m.add_on_bal_amt,
                            0
                        )
                    END AS os_bal_amt_v2
                FROM
                    main_curr AS m
                    LEFT JOIN int_at_def_recent AS r ON r.loan_num = m.loan_num
                    AND r.cab = m.cab
                    LEFT JOIN int_at_def AS i ON i.loan_num = m.loan_num
                    AND i.cab = m.cab
                    AND i.int_at_default IS NOT NULL
                    AND i.tm_lvl_end_dt = r.tm_lvl_end_dt
                    LEFT JOIN pit_curr AS p ON p.basel_acct_id = m.basel_acct_id
            )
        SELECT
            basel_acct_id,
            os_bal_amt_v2
        FROM
            os_bal_amtv2
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
         select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt, from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_v2.export_spl", key="parquet") }}}}'])
    )
    """,
):
    pass
