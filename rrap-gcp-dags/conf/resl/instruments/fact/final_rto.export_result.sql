with basel_parm_segment as (
        {% set upstream_asset = [
            'reference.BASEL_SEG',
            'reference.BASEL_SEG_RPTG_PARM'
        ] %}
            select  
            a.BASEL_SEG_ID,
            a.BASEL_MODEL_ID,
            b.SEG_NUM,
            STREAM,
            FINAL_RTO
            from {{upstream_asset[0]}} a left join {{upstream_asset[1]}} b 
            on trim(a.BASEL_MODEL_ID)=trim(b.BASEL_MODEL_ID) and a.seg_num=b.seg_num 
            where EFF_FROM_DT < '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and EFF_TO_DT > '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' 
            and crnt_f = 'Y' 
            order by BASEL_MODEL_ID desc 
        ) 

        select 
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
        BASEL_MODEL_ID,
        SEG_NUM,
        FINAL_RTO,
        STREAM
        from basel_parm_segment