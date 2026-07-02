with tng as (
    {% set upstream_asset = [ 
      'features.SRC_SYS_CD',
      'features.BASEL_PRD_TP_CD',

      'instruments.PD_FINAL_RPTG_RTO',
      'instruments.EAD_FLR',
      'instruments.EAD_FINAL_RPTG_RTO',

      'ingestion.TNG_ACCT_MO',
      'ingestion.BASEL_ACCT_DIM',
      'ingestion.MORT_MTH_SNAPSHOT',
      'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
      'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT'
    ]%}
            SELECT
            BASEL_ACCT_ID
            FROM {{upstream_asset[5]}} tng
            INNER JOIN {{upstream_asset[6]}} dim ON
                dim.SRC_APP_CD = 'TNG-MOR'
                AND dim.SRC_SYS_DEL_F != 'Y'
                AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
            WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ), mor as (
            select 
            BASEL_ACCT_ID
            from {{upstream_asset[7]}}
            where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ), spl as (
            select 
            BASEL_ACCT_ID
            from {{upstream_asset[8]}} 
            where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ), ks as (
            select
            BASEL_ACCT_ID	
            from {{upstream_asset[9]}}
            where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ), acct_list as (
            select basel_acct_id from tng
            union all
            select basel_acct_id from mor 
            union all 
            select basel_acct_id from spl
            union all 
            select basel_acct_id from ks 
        ), get_src_sys_cd as (
            select a.basel_acct_id, src_sys_cd 
            from acct_list a 
            left join {{upstream_asset[0]}} b
            on a.basel_acct_id=b.basel_acct_id
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ), get_ead_final_rptg_rto as (
            select a.basel_acct_id, ead_final_rptg_rto
            from acct_list a 
            left join {{upstream_asset[4]}} b
            on a.basel_acct_id=b.basel_acct_id
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        ), get_pd_final_rptg_rto as (
            select a.basel_acct_id, pd_final_rptg_rto
            from acct_list a 
            left join {{upstream_asset[2]}} b
            on a.basel_acct_id=b.basel_acct_id
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        ), get_basel_prd_tp_cd as (
            select a.basel_acct_id, basel_prd_tp_cd
            from acct_list a 
            left join {{upstream_asset[1]}} b
            on a.basel_acct_id=b.basel_acct_id
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        ), get_ead_flr as (
                select a.basel_acct_id, ead_flr
                    from acct_list a 
                    left join {{upstream_asset[3]}} b
                    on a.basel_acct_id=b.basel_acct_id
                    where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        ), combo as (
            select a.basel_acct_id, src_sys_cd,ead_final_rptg_rto,pd_final_rptg_rto,basel_prd_tp_cd,ead_flr
            from acct_list a left join get_src_sys_cd b on a.basel_acct_id=b.basel_acct_id
            left join get_ead_final_rptg_rto c on a.basel_acct_id=c.basel_acct_id
            left join get_pd_final_rptg_rto d on a.basel_acct_id=d.basel_acct_id
            left join get_basel_prd_tp_cd e on a.basel_acct_id=e.basel_acct_id
            left join get_ead_flr f on a.basel_acct_id=f.basel_acct_id 
        ), final as(
            select 
            a.basel_acct_id,
            case 
            when ead_final_rptg_rto is NULL then null
            when trim(src_sys_cd)='KS' and (pd_final_rptg_rto < 1 or pd_final_rptg_rto is null)  and trim(basel_prd_tp_cd) = 'SLT' then GREATEST(ead_final_rptg_rto,0)
            when trim(src_sys_cd)='KS' and (pd_final_rptg_rto < 1 or pd_final_rptg_rto is null)  and (basel_prd_tp_cd is NULL or  trim(basel_prd_tp_cd) != 'SLT') then GREATEST(ead_final_rptg_rto,ead_flr)
            when trim(src_sys_cd)='KS' and pd_final_rptg_rto > 1 then 1
            else 1
            end as EAD_FLRD_RPTG_RTO

            from combo a
        )
            select 
            basel_acct_id,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt, 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream,
            EAD_FLRD_RPTG_RTO
            from final