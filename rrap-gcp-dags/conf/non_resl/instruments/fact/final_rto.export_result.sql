with basel_parm_segment as (
        {% set upstream_asset = [
            'reference.BASEL_SEG'
        ] %}
            select  
            a.BASEL_SEG_ID,
            a.BASEL_MODEL_ID,
            b.SEG_NUM,
            STREAM,
            FINAL_RTO
            from {{upstream_asset[0]}} a left join '/bns/rrap/homes/rraprun/srozaidi/BASEL_SEG_RPTG_PARM_streamed.parquet' b 
            on a.BASEL_MODEL_ID=b.NEW_ID and a.seg_num=b.seg_num 
            where EFF_FROM_DT < '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and EFF_TO_DT > '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            --and crnt_f = 'Y' -- excluding for now
            order by BASEL_MODEL_ID desc 
        ) 

        select 
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
        BASEL_MODEL_ID,
        SEG_NUM,
        FINAL_RTO,
        STREAM
        from basel_parm_segment