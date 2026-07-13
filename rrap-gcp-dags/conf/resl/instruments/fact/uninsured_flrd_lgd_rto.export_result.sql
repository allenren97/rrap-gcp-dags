{% set upstream_asset = [
        'ingestion.TNG_ACCT_MO',
        'ingestion.BASEL_ACCT_DIM',
        'ingestion.MORT_MTH_SNAPSHOT',
        'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
        'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',

        'features.BASEL_PRD_TP_CD',
        'instruments.LGD_FLR',
        'instruments.UNINSURED_LGD_RTO'
] %}

with tng as (
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
), get_basel_prd_tp_cd as (
	select a.basel_acct_id, basel_prd_tp_cd
	from acct_list a 
	left join {{upstream_asset[5]}} b
	on a.basel_acct_id=b.basel_acct_id
	where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
), get_lgd_flr as (
	select a.basel_acct_id, lgd_flr
	from acct_list a 
	left join {{upstream_asset[6]}} b
	on a.basel_acct_id=b.basel_acct_id
	where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' and stream ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
), get_uninsured_lgd_rto as (
	select a.basel_acct_id, uninsured_lgd_rto
	from acct_list a 
	left join {{upstream_asset[7]}} b
	on a.basel_acct_id=b.basel_acct_id
	where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' and stream ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
), combo as (
	select a.basel_acct_id, lgd_flr,uninsured_lgd_rto,basel_prd_tp_cd
	from acct_list a 
	left join get_lgd_flr c on a.basel_acct_id=c.basel_acct_id
	left join get_uninsured_lgd_rto d on a.basel_acct_id=d.basel_acct_id
	left join get_basel_prd_tp_cd e on a.basel_acct_id=e.basel_acct_id
),final as(
   select 
   a.basel_acct_id,
   case 
   when trim(basel_prd_tp_cd) not like 'GENW%' and trim(basel_prd_tp_cd) not like  'GUAR%' then null
   when uninsured_lgd_rto is null then null
   else greatest(lgd_flr,uninsured_lgd_rto)
   end as UNINSURED_FLRD_LGD_RTO
   from combo a
)
   select 
    basel_acct_id,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt, 
    UNINSURED_FLRD_LGD_RTO
    from final