WITH get_segments as (
        {% set upstream_asset = [
            "instruments.LGD_BASEL_SEG_NUM",
            "instruments.FINAL_RTO",
        ] %}
            select 
            OBSN_DT,
            basel_acct_id,
            model,
            stream, 
            LGD_BASEL_SEG_NUM 
            from {{upstream_asset[0]}}
            where stream ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' 
            and OBSN_DT='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),get_ratio as (
            SELECT 
            a.basel_acct_id,
            a.model,
            b.final_rto as lgd_rto,
            a.stream
            FROM get_segments a
            LEFT JOIN {{upstream_asset[1]}} b
            ON a.model = b.basel_model_id
            AND a.LGD_BASEL_SEG_NUM = b.seg_num 
            and a.OBSN_DT=b.OBSN_DT
            AND b.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        ),final as (
            SELECT 
            basel_acct_id, 
            stream,
            max(lgd_rto) as LGD_FINAL_RPTG_RTO
            from get_ratio 
            group by basel_acct_id,stream
            order by basel_acct_id
        )
            select 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
            BASEL_ACCT_ID,
            LGD_FINAL_RPTG_RTO,
            STREAM
            from final