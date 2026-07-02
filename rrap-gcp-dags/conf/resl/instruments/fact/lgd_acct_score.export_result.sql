with tng as (
    {%
    set upstream_asset = [ 
    'features.BASEL_ACCT_ID',
    'instruments.PIT_STAT_CD',
    
    'models.STEP_MIX_MOR_LGDND_SCORE',
    'models.STEP_MIX_MOR_LGDD_SCORE',
    
    'models.CC_LGDND_SCORE',
    
    'models.HELOC_LGDND_SCORE',
    'models.HELOC_LGDD_SCORE',
    
    'models.LOC_LGDND_SCORE',
    'models.LOC_LGDD_SCORE',
    
    'models.DTL_LGDD_SCORE',
    
    'models.ITL_LGDND_SCORE',
    'models.ITL_LGDD_SCORE',
    ]
    %}
    select 
    basel_acct_id, 
    null as lgd_acct_score
    from 
    {{upstream_asset[0]}}
    where 
    src_sys_cd='TNG-MOR'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
),get_mor_status as (
    select 
    a.basel_acct_id,
    b.pit_stat_cd 
    from 
    {{upstream_asset[0]}} a 
    left join {{upstream_asset[1]}} b 
    on a.basel_acct_id=b.basel_acct_id
    and a.obsn_dt=b.obsn_dt
    where 
    a.obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' 
    and a.src_sys_cd='MOR'
), mor as (
    select
    a.basel_acct_id,
    b.VAR_SCORE as lgdnd_score,
    c.VAR_SCORE as lgdd_score,
    case 
        when pit_stat_cd = 'CUR' then lgdnd_score
        when pit_stat_cd = 'DEF' then lgdd_score 
    end 
    as lgd_acct_score
    from get_mor_status a 
    left join 
    ( 
        select * from {{upstream_asset[2]}}  
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        and VAR_NAME='SCORE'
    ) b 
    on a.basel_acct_id=b.basel_acct_id

    left join 
    (
        select * from {{upstream_asset[3]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'  
        and VAR_NAME='SCORE'   
    )c 
    on a.basel_acct_id=c.basel_acct_id
         
), ks_score_snapshot as (
    select basel_acct_id, VAR_SCORE 
    from {{upstream_asset[4]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union 
    select basel_acct_id, VAR_SCORE 
    from  {{upstream_asset[5]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union 
    select basel_acct_id, VAR_SCORE 
    from  {{upstream_asset[6]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union 
    select basel_acct_id, VAR_SCORE 
    from  {{upstream_asset[7]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union 
    select basel_acct_id, VAR_SCORE 
    from  {{upstream_asset[8]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

 ), ks as (
    select
    a.basel_acct_id, 
    VAR_SCORE as lgd_acct_score
    from  {{upstream_asset[0]}} a 
    left join ks_score_snapshot b 
    on a.basel_acct_id=b.basel_acct_id
    where src_sys_cd = 'KS'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
 ), spl_score_snapshot as (
    select basel_acct_id, 
    VAR_SCORE 
    from {{upstream_asset[9]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union 
    select basel_acct_id, 
    VAR_SCORE 
    from  {{upstream_asset[10]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    union 
    select basel_acct_id, 
    VAR_SCORE 
    from  {{upstream_asset[11]}}
    where 
    VAR_NAME='SCORE' 
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
    and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'    
 ), spl as (
    select
    a.basel_acct_id, 
    VAR_SCORE as lgd_acct_score
    from  {{upstream_asset[0]}} a 
    left join spl_score_snapshot b 
    on a.basel_acct_id=b.basel_acct_id
    where src_sys_cd = 'SPL'
    and obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
 ), combo as (
    select basel_acct_id, lgd_acct_score from tng

    union  
    select basel_acct_id, lgd_acct_score from mor

    union  
    select basel_acct_id, lgd_acct_score from spl

    union  
    select basel_acct_id, lgd_acct_score from ks 

 )
 select 
 basel_acct_id, 
 lgd_acct_score, 
 '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream, 
 '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt
 from combo
