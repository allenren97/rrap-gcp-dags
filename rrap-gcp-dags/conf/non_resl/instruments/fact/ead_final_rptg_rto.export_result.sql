with tng as (
    {% set upstream_asset = [
    'instruments.FINAL_RTO',
    'instruments.EAD_BASEL_SEG_NUM',

    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT"
        ] %}

            SELECT
            BASEL_ACCT_ID,
			1 as EAD_RTO
            FROM {{upstream_asset[2]}} tng
            INNER JOIN {{upstream_asset[3]}} dim ON
                dim.SRC_APP_CD = 'TNG-MOR'
                AND dim.SRC_SYS_DEL_F != 'Y'
                AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
            WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ), mor as (
            select 
            BASEL_ACCT_ID,
			1 as EAD_RTO
            from {{upstream_asset[4]}}
            where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ), spl as (
            select 
            BASEL_ACCT_ID,
			1 as EAD_RTO
            from {{upstream_asset[5]}}
            where mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ), ks_get_EAD_BASEL_SEG_NUM as (
            select
            a.BASEL_ACCT_ID,
			MODEL,
			EAD_BASEL_SEG_NUM,
			obsn_dt
            from {{upstream_asset[6]}} a 
            left join {{upstream_asset[1]}} b
            on a.basel_acct_id=b.basel_acct_id
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            and mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),   ks_get_ratios as (
			select 
			basel_acct_id, 
			final_rto as EAD_RTO
			from ks_get_EAD_BASEL_SEG_NUM a 
			left join (
                select * from {{upstream_asset[0]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                ) b
			on a.model=b.basel_model_id
			and a.ead_basel_seg_num=b.seg_num
			and a.obsn_dt=b.obsn_dt
		), combo as (
			select basel_acct_id, EAD_RTO from tng 
			union all
			
			select basel_acct_id, EAD_RTO from mor 
			union all
			
			select basel_acct_id, EAD_RTO from spl 
			union all
			
			select basel_acct_id, EAD_RTO from ks_get_ratios 
		)
			select 
			basel_acct_id,
			ead_rto as EAD_FINAL_RPTG_RTO,
			'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt,
			'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream  
			from combo 
			order by basel_acct_id