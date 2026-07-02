with tng as (
{% set upstream_asset = [ 
    'ingestion.TNG_ACCT_MO',
    'ingestion.BASEL_ACCT_DIM',
    'ingestion.MORT_MTH_SNAPSHOT',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    
    'features.SRC_SYS_CD',
    'instruments.EAD_FINAL_RPTG_RTO',
    'instruments.PD_FINAL_RPTG_RTO',
    'features.BASEL_PRD_TP_CD',

    'features.CCF',
    'features.DRAWN',
    'instruments.UNDRAWN',
    'features.UNDRAWN_EXPSR_PCT'
    ]
%}
SELECT
			BASEL_ACCT_ID
			FROM {{upstream_asset[0]}} tng
			INNER JOIN {{upstream_asset[1]}} dim ON
			dim.SRC_APP_CD = 'TNG-MOR'
			AND dim.SRC_SYS_DEL_F != 'Y'
			AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
			WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
		), mor as (
			select 
			BASEL_ACCT_ID
			from {{upstream_asset[2]}}
			where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
		), spl as (
			select 
			BASEL_ACCT_ID
			from {{upstream_asset[3]}} 
			where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
		), ks as (
			select
			BASEL_ACCT_ID	
			from {{upstream_asset[4]}}
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
			left join {{upstream_asset[5]}} b
			on a.basel_acct_id=b.basel_acct_id
			where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
		), get_ead_final_rptg_rto as (
			select a.basel_acct_id, ead_final_rptg_rto
			from acct_list a 
			left join {{upstream_asset[6]}} b
			on a.basel_acct_id=b.basel_acct_id
			where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
			and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
		), get_pd_final_rptg_rto as (
			select a.basel_acct_id, pd_final_rptg_rto
			from acct_list a 
			left join {{upstream_asset[7]}} b
			on a.basel_acct_id=b.basel_acct_id
			where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
			and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
		), get_basel_prd_tp_cd as (
			select a.basel_acct_id, basel_prd_tp_cd
			from acct_list a 
			left join {{upstream_asset[8]}} b
			on a.basel_acct_id=b.basel_acct_id
			where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
		), combo as (
			select a.basel_acct_id, src_sys_cd,ead_final_rptg_rto,pd_final_rptg_rto,basel_prd_tp_cd
			from acct_list a left join get_src_sys_cd b on a.basel_acct_id=b.basel_acct_id
			left join get_ead_final_rptg_rto c on a.basel_acct_id=c.basel_acct_id
			left join get_pd_final_rptg_rto d on a.basel_acct_id=d.basel_acct_id
			left join get_basel_prd_tp_cd e on a.basel_acct_id=e.basel_acct_id
		), get_calc_nums as (
			select a.basel_acct_id, ccf, drawn, undrawn, undrawn_expsr_pct 
			from acct_list a 
			left join (select basel_acct_id, ccf from {{upstream_asset[9]}} where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') b 
			on a.basel_acct_id=b.basel_acct_id
			left join ( select basel_acct_id, drawn from {{upstream_asset[10]}} where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}')c
			on a.basel_acct_id=c.basel_acct_id
			left join ( select basel_acct_id, undrawn from {{upstream_asset[11]}} 
			where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
			and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
			)d
			on a.basel_acct_id=d.basel_acct_id
			left join ( select basel_acct_id, undrawn_expsr_pct from {{upstream_asset[12]}} where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}')e
			on a.basel_acct_id=e.basel_acct_id	 
		)
			select 
			a.basel_acct_id,
			'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt, 
			'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream,
			case 
				when ead_final_rptg_rto is NULL then null
				when trim(src_sys_cd)='KS' and (pd_final_rptg_rto < 1 or pd_final_rptg_rto is null)  and trim(basel_prd_tp_cd) = 'SLT' then 0
				when trim(src_sys_cd)='KS' and (pd_final_rptg_rto < 1 or pd_final_rptg_rto is null) and (basel_prd_tp_cd is NULL or  trim(basel_prd_tp_cd) != 'SLT') then (drawn + ccf * undrawn_expsr_pct * undrawn)/(drawn+undrawn)
				when src_sys_cd='KS' and pd_final_rptg_rto > 1 then 0
				else 0
				end as EAD_FLR

			from combo a inner join get_calc_nums b on a.basel_acct_id=b.basel_acct_id