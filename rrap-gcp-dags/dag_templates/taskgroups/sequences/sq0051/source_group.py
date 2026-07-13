import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq0051_rundir():
    """Create sq0051 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq0051_rundir = f"{rundir}/sq0051"
    os.makedirs(sq0051_rundir, exist_ok=True)


@task.beeline(
    task_id="make_airb_ifrs9_ecl_profile_fact",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            cntry_cd,
            trim(acct_num) as acct_num,
            cpp_entity_folio_cd,
            cpp_prd_folio_cd,
            cpp_quali_sub_cd,
            cpp_quanti_sub_cd,
            pit_stat_cd,
            stg3_ind,
            os_bal_amt,
            final_ecl_stage,
            final_ecl_cad,
            final_ecl_cad_drawn,
            final_ecl_cad_undrawn,
            crnt_auth_lmt_amt,
            undrawn_amt,
            scored_unscored_ind,
            from_unixtime(unix_timestamp(cast(proc_mth_id as string), 'yyyyMMdd'), 'yyyy-MM-dd') as mth_end_dt,
            case src_sys_cd
                when 'GZ' then 'MOR'
                when 'KQ' then 'KS'
                when 'SL' then 'SPL'
                when 'TNG_MTG' then 'TNG-MOR'
                else src_sys_cd
            end as src_sys_cd
        from {{ var.value.CRZ_IFRS9_RTL_SCHEMA }}.ifrs9_acct_ecl_profile_fact_ext
        where
            cast(proc_mth_id as string) = regexp_replace('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}', '-', '')
            and cntry_cd = 'CA'
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="airb_ifrs9_ecl_profile_fact.parquet",
    schema=pa.schema([
        ('cntry_cd', pa.string()),
        ('acct_num', pa.string()),
        ('cpp_entity_folio_cd', pa.string()),
        ('cpp_prd_folio_cd', pa.string()),
        ('cpp_quali_sub_cd', pa.string()),
        ('cpp_quanti_sub_cd', pa.string()),
        ('pit_stat_cd', pa.string()),
        ('stg3_ind', pa.int32()),
        ('os_bal_amt', pa.float64()),
        ('final_ecl_stage', pa.int32()),
        ('final_ecl_cad', pa.float64()),
        ('final_ecl_cad_drawn', pa.float64()),
        ('final_ecl_cad_undrawn', pa.float64()),
        ('crnt_auth_lmt_amt', pa.float64()),
        ('undrawn_amt', pa.float64()),
        ('scored_unscored_ind', pa.string()),
        ('mth_end_dt', pa.string()),
        ('src_sys_cd', pa.string()),
    ]),
)
def make_airb_ifrs9_ecl_profile_fact():
    """Extract account-level IFRS9 ECL attributes for prior month-end."""
    pass


rundir_task = create_sq0051_rundir()
extract_task = make_airb_ifrs9_ecl_profile_fact()

rundir_task >> extract_task
