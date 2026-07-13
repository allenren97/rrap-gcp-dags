from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "reference.SRC_PRD_LKP",
    "features.CONSM_PRD_TREATMNT_CD",
]
DOWNSTREAM_ASSET = "features.STEP_WORST_DLQDAYS_KS_AVG12M"
DEPENDENCIES = {
    "export": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        select
            trim(main.STEP_PLN_AGRMNT_NUM) as STEP_PLN_AGRMNT_NUM,
            max(
                case
                    when (coalesce(main.BNS_DLQNT_DAY, 0) -30) < 0 then 0
                    else (coalesce(main.BNS_DLQNT_DAY, 0) -30)
                end
            ) as bns_dlqnt_day
        from
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT as main
        left join (select
                    SRC_PRD_CD,
                    SRC_SUB_PRD_CD,
                    SML_BUS_F,
                    PRD_SYS_CD
                from reference.SRC_PRD_LKP
                where '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN EFF_FROM_YR_MTH and EFF_TO_YR_MTH and trim(PRD_SYS_CD)='KS') as sp on trim(main.PRD_CD) = trim(sp.SRC_PRD_CD) and trim(main.SUB_PRD_CD) = trim(sp.SRC_SUB_PRD_CD)
        left join (select tm_id,tm_lvl_end_dt from ingestion.TM_DIM where trim(tm_lvl)='Month') tm on main.mth_tm_id=tm.tm_id
        left join (select basel_acct_id, CONSM_PRD_TREATMNT_CD, obsn_dt from features.CONSM_PRD_TREATMNT_CD where  obsn_dt > last_day(date '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - interval 12 month) and obsn_dt <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}')  as lkp on main.basel_acct_id = lkp.basel_acct_id and lkp.obsn_dt = tm.tm_lvl_end_dt
        where
            coalesce(trim(main.STEP_PLN_AGRMNT_NUM), '') != '' and
            main.MTH_TM_ID > {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 12 * 40 and
            main.MTH_TM_ID <= {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} and
            trim(sp.SML_BUS_F) = 'N' and --
            trim(lkp.CONSM_PRD_TREATMNT_CD) = 'A' --
        group by
            trim(main.STEP_PLN_AGRMNT_NUM),
            main.MTH_TM_ID
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
        with
            step_cnt as (
                select
                    max(step_pln_snapshot_id) as step_pln_snapshot_id,
                    count(1) as CNT_STEP_PLN_AGRMNT_NUM,
                    trim(STEP_PLN_AGRMNT_NUM) as STEP_PLN_AGRMNT_NUM
                from
                    (
                        select
                            step_pln_snapshot_id,
                            MTH_TM_ID,
                            trim(STEP_PLN_AGRMNT_NUM) as STEP_PLN_AGRMNT_NUM
                        from
                            ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT
                        where
                            MTH_TM_ID > {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12*40 and
                            MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} and
                            coalesce(trim(STEP_PLN_AGRMNT_NUM), '') != ''
                        group by
                            step_pln_snapshot_id,
                            MTH_TM_ID,
                            trim(STEP_PLN_AGRMNT_NUM)
                    )
                group by
                    trim(STEP_PLN_AGRMNT_NUM)
            )
        select
            max(step_cnt.step_pln_snapshot_id) as step_pln_snapshot_id,
            nullif(trim(step_cnt.step_pln_agrmnt_num), '') as STEP_PLN_AGRMNT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as obsn_dt,
            case
                when sum(coalesce(main.bns_dlqnt_day,0)) = 0 or
                    max(coalesce(step_cnt.CNT_STEP_PLN_AGRMNT_NUM,0)) = 0
                then 0
                else (sum(main.bns_dlqnt_day) / max(step_cnt.CNT_STEP_PLN_AGRMNT_NUM))::decimal(20,8)
            end as STEP_WORST_DLQDAYS_KS_AVG12M
        from
            step_cnt
            left join '{{{{ task_instance.xcom_pull(task_ids="derived__step_worst_dlqdays_ks_avg12m.export", key="parquet") }}}}' as main on step_cnt.STEP_PLN_AGRMNT_NUM = main.STEP_PLN_AGRMNT_NUM
        group by
            step_cnt.step_pln_snapshot_id,
            step_cnt.STEP_PLN_AGRMNT_NUM
    )
    """,
):
    pass
