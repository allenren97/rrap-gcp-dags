with mor as (
   {% 
    set upstream_asset = [ 
    'features.BASEL_ACCT_ID',
    'models.CC_EAD_SCORE',
    'models.HELOC_EAD_SCORE',
    'models.LOC_EAD_SCORE',
    ]
    %}
    select
    basel_acct_id, 
    CAST(NULL AS INTEGER) AS ead_acct_score
    from {{upstream_asset[0]}}
    where src_sys_cd ='MOR'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
),
tng as (
    select
    basel_acct_id, 
    null as ead_acct_score
    from {{upstream_asset[0]}}
    where src_sys_cd ='TNG-MOR'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
),
spl as (
    select
    basel_acct_id, 
    0 as ead_acct_score 
    from {{upstream_asset[0]}}
    where src_sys_cd = 'SPL'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
),
 SCORE_SNAPSHOT as (
    select basel_acct_id, VAR_SCORE from {{upstream_asset[1]}} 
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union all
    select basel_acct_id, VAR_SCORE from {{upstream_asset[2]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

       union all
    select basel_acct_id, VAR_SCORE from {{upstream_asset[3]}} 
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

 ), ks as (
    select
    a.basel_acct_id, 
    VAR_SCORE as ead_acct_score
    from {{upstream_asset[0]}} a 
    left join SCORE_SNAPSHOT b 
    on a.basel_acct_id=b.basel_acct_id
    where src_sys_cd = 'KS'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
 ), combo as (
    select basel_acct_id, ead_acct_score from tng

    union all 
    select basel_acct_id, ead_acct_score from mor

    union all 
    select basel_acct_id, ead_acct_score from spl

    union all 
    select basel_acct_id, ead_acct_score from ks 

 )
 select 
 basel_acct_id,
 '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt,
 '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream, 
 ead_acct_score
 from combo