{% set upstream_asset = [
      'instruments.EXPOSURE_UNSECURED',
      'instruments.EXPOSURE_SECURED_MAXIMUM'
    ]
%}

with base as (
                select a.basel_acct_id, EXPOSURE_UNSECURED,EXPOSURE_SECURED_MAXIMUM  
                from 
                ( 
                    select basel_acct_id, EXPOSURE_UNSECURED from {{upstream_asset[0]}} 
                    where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                    and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                ) a 
                inner join 
                ( 
                    select basel_acct_id, EXPOSURE_SECURED_MAXIMUM from {{upstream_asset[1]}}
                    where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                ) b
                on a.basel_acct_id=b.basel_acct_id
            ), final as (
                select
                basel_acct_id,
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
                (EXPOSURE_UNSECURED/EXPOSURE_SECURED_MAXIMUM) as WEIGHT_UNSECURED
                from base
            ) 
                select 
                basel_acct_id,
                obsn_dt,
                stream,
                case 
                    when isnan(WEIGHT_UNSECURED)then NULL
                    else WEIGHT_UNSECURED end
                as WEIGHT_UNSECURED
                from final