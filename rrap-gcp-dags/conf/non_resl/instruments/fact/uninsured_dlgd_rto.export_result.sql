with tng as (
{% set upstream_asset = [ 
        'ingestion.TNG_ACCT_MO',
        'ingestion.BASEL_ACCT_DIM',
        'ingestion.MORT_MTH_SNAPSHOT',
        'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
        'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',

        'instruments.DLGD_F',
        'features.SCRTY_TP_DESC',
        'instruments.LNG_RUN_LGD_ADD_ON_RTO',
        'instruments.LGD_FLR',
        'instruments.UNINSURED_LGD_RTO',
        'instruments.PMI_LGD_INSURED_RPTG_RTO',
        'instruments.PMI_LGD_UNADJUSTED_RPTG_RTO'
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
            where mth_tm_id = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'
        ), spl as (
            select 
            BASEL_ACCT_ID
            from {{upstream_asset[3]}} 
            where mth_tm_id = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'
        ), ks as (
            select
            BASEL_ACCT_ID	
            from {{upstream_asset[4]}}
            where mth_tm_id = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'
        ), acct_list as (
            select basel_acct_id from tng
            union all
            select basel_acct_id from mor 
            union all 
            select basel_acct_id from spl
            union all 
            select basel_acct_id from ks 
        ), get_values as (
		select 
		a.basel_acct_id,
		b.DLGD_F,
		c.SCRTY_TP_DESC,
		d.LNG_RUN_LGD_ADD_ON_RTO,
		e.LGD_FLR,
		f.UNINSURED_LGD_RTO,
		g.PMI_LGD_INSURED_RPTG_RTO,
		h.PMI_LGD_UNADJUSTED_RPTG_RTO
		from acct_list a 
		
		left join (select basel_acct_id, DLGD_F from {{upstream_asset[5]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' ) b 
		on a.basel_acct_id=b.basel_acct_id
		
		left join (select basel_acct_id, SCRTY_TP_DESC from {{upstream_asset[6]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') c
		on a.basel_acct_id=c.basel_acct_id
		
		left join (select basel_acct_id, LNG_RUN_LGD_ADD_ON_RTO from {{upstream_asset[7]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') d
		on a.basel_acct_id=d.basel_acct_id
		
		left join (select basel_acct_id, LGD_FLR from {{upstream_asset[8]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') e
		on a.basel_acct_id=e.basel_acct_id
		
		left join (select basel_acct_id, UNINSURED_LGD_RTO from {{upstream_asset[9]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') f 
		on a.basel_acct_id=f.basel_acct_id
		
		left join (select basel_acct_id, PMI_LGD_INSURED_RPTG_RTO from {{upstream_asset[10]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') g
		on a.basel_acct_id=g.basel_acct_id
		
		left join (select basel_acct_id, PMI_LGD_UNADJUSTED_RPTG_RTO from {{upstream_asset[11]}} 
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
        and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}')h 
		on a.basel_acct_id=h.basel_acct_id
		)
		select 
		'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt, 
		'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream,
		basel_acct_id,
		case 
			when UNINSURED_LGD_RTO is null then null 
			when DLGD_F='Y' and trim(SCRTY_TP_DESC)='Insured' and LNG_RUN_LGD_ADD_ON_RTO not null then 
				GREATEST(LGD_FLR,UNINSURED_LGD_RTO, LEAST( 1,(PMI_LGD_INSURED_RPTG_RTO + LNG_RUN_LGD_ADD_ON_RTO)))
				
			when DLGD_F='Y' and (SCRTY_TP_DESC is null or trim(SCRTY_TP_DESC)!='Insured') and LNG_RUN_LGD_ADD_ON_RTO not null then 
				GREATEST(LGD_FLR,UNINSURED_LGD_RTO, LEAST( 1,(PMI_LGD_UNADJUSTED_RPTG_RTO + LNG_RUN_LGD_ADD_ON_RTO)))
			ELSE GREATEST(UNINSURED_LGD_RTO, LGD_FLR)
		END AS UNINSURED_DLGD_RTO
		 from get_values