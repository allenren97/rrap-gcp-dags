with KS as (
    {% set 
    UPSTREAM_ASSET = [ 
        'ingestion.MORT_MTH_SNAPSHOT', 
        'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
        'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
        'ingestion.TNG_ACCT_MO',
        'ingestion.BASEL_ACCT_DIM',

        'instruments.LGD_BASEL_SEG_NUM',
        'instruments.CCAR_BASEL_PRD_TP_NM', 
        'instruments.LGD_MODEL_NM',  

        'reference.BASEL_MODEL', 
        'reference.BASEL_SEG_RPTG_PARM',
        'reference.BASEL_SEG' 
    ]
    
    %}
            select basel_acct_id 
            from {{UPSTREAM_ASSET[1]}}
            where mth_tm_id = {{task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}
        ),
        spl as (
            select basel_acct_id
            from {{UPSTREAM_ASSET[2]}}
            where mth_tm_id = {{task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}
        ), 
        tng as (
            WITH ACCT_LIST AS (
                SELECT
                a.ACCOUNT_ID,
                b.basel_acct_id
                FROM {{UPSTREAM_ASSET[3]}}  a
                INNER JOIN {{UPSTREAM_ASSET[4]}}  b ON
                a.ACCOUNT_ID = b.SRC_APP_ID
                AND b.SRC_APP_CD ='TNG-MOR'
                AND b.SRC_SYS_DEL_F != 'Y'
                WHERE a.month_end_dt ='{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            ), get_seg_nums as (
                select 
                a.basel_acct_id,
                LGD_BASEL_SEG_NUM 
                from ACCT_LIST a left join 
                (
                    select basel_acct_id, LGD_BASEL_SEG_NUM  
                    from {{UPSTREAM_ASSET[5]}}
                    where obsn_dt='{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                ) b 
                on a.basel_acct_id=b.basel_acct_id
            ), get_ccar_basel_prd_tp_nm as (
                select basel_acct_id, 
                CCAR_BASEL_PRD_TP_NM 
                from {{UPSTREAM_ASSET[6]}}
                where 
                obsn_dt='{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                and stream='{{task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            
            )
            select 
            a.basel_acct_id,
            case 
                WHEN (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%') then LGD_BASEL_SEG_NUM 
                else NULL end
                as UNINSURED_LGD_SEG_NUM,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt, 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream
            from get_seg_nums a left join get_ccar_basel_prd_tp_nm b 
            on a.basel_acct_id=b.basel_acct_id
        ),
        final as (
            select 
            basel_acct_id, 
            NULL as UNINSURED_LGD_SEG_NUM, 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'  as OBSN_DT,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'  as STREAM
            from KS

            UNION ALL 
            select
            basel_acct_id, 
            NULL as UNINSURED_LGD_SEG_NUM, 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'  as OBSN_DT,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'  as STREAM
            from spl

            UNION ALL 
            select
            basel_acct_id, 
            UNINSURED_LGD_SEG_NUM, 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'  as OBSN_DT,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'  as STREAM
            from tng

        )

        select 
        basel_acct_id,
        UNINSURED_LGD_SEG_NUM,
        OBSN_DT,
        STREAM 
        from final