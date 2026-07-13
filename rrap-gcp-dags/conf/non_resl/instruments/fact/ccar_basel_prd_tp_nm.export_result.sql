with export_ks as (
    {% set upstream_asset = [ 
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'features.PRD_CD',
    'features.SUB_PRD_CD',
    'features.HELOC_F',

    'features.REVISED_EXPSR_OV_125K_F',
    'features.BASEL_PRD_CD',

    'reference.BASEL_RPTG_PRD_LKP',
    'reference.BASEL_EGL_LKP_NZ ',

    'instruments.PD_BASEL_SEG_NUM',
    'instruments.LGD_BASEL_SEG_NUM',
    'instruments.EAD_BASEL_SEG_NUM',

    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'features.PRD_ID',
    'reference.PSNL_LOAN_RPTG_PRD_LKP',

    'ingestion.MORT_MTH_SNAPSHOT',
    'features.BULK_IND',
    'reference.MORT_RPTG_PRD_LKP',

    'ingestion.TNG_ACCT_MO',
    'ingestion.BASEL_ACCT_DIM',
    'features.SRC_SYS_CD',
   
    ]

    %}
		with MERG02 as  (
            SELECT
            mth_tm_id,
            a.BASEL_ACCT_ID,
            b.PRD_CD, 
            c.SUB_PRD_CD as SUB_PRD_CD0, 
            d.HELOC_F 

            from {{upstream_asset[0]}} a
            left join ( 
                select basel_acct_id, prd_cd from {{upstream_asset[1]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )b
            on a.basel_acct_id = b.basel_acct_id

            left join ( 
                select basel_acct_id, sub_prd_cd from {{upstream_asset[2]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )c
            on a.basel_acct_id=c.basel_acct_id

            left join (
                select basel_acct_id, heloc_f from {{upstream_asset[3]}}
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )d
            on a.basel_acct_id=d.basel_acct_id	
                   
            where mth_tm_id={{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

        ), REVLVNG_CR_RPTG_DRVD_VAR as (
            SELECT 
            mth_tm_id,
            a.BASEL_ACCT_ID,
            b.REVISED_EXPSR_OV_125K_F,
            c.BASEL_PRD_CD AS BDV_BASEL_PRD_CD
            
            from {{upstream_asset[0]}} a
            left join ( 
                select basel_acct_id, REVISED_EXPSR_OV_125K_F from {{upstream_asset[4]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )b
            on a.basel_acct_id = b.basel_acct_id

            left join (
                select basel_acct_id, BASEL_PRD_CD from {{upstream_asset[5]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )c
            on a.basel_acct_id=c.basel_acct_id
            
            where mth_tm_id={{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} 

        ), RPTG_PRD_LKP1 as (
            SELECT 
            PL.PRD_CD,
            SUB_PRD_CD,
            TRIM(CCAR_BASEL_PRD_TP_NM) AS CCAR_BASEL_PRD_TP_NM0,
            TRIM(SUB_PRD_CD) AS SUB_PRD_CD0,
            TRIM(REVISED_EXPSR_OV_125K_F) AS REVISED_EXPSR_OV_125K_F,
            TRIM(HELOC_F) AS HELOC_F,
            TRIM(BASEL_PRD_CD) AS BASEL_PRD_CD
            FROM {{upstream_asset[6]}} PL 
            left join {{upstream_asset[7]}} egl 
            on LTRIM(RTRIM(PL.PRD_ID))=LTRIM(RTRIM(egl.PRD_CD))
            WHERE TRIM(SRC_SYS_CD)='KS'  
            AND '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' 
            between eff_from_yr_mth AND eff_to_yr_mth 

        ),CCAR_BASEL_PRD_TP_NM0 as (
            SELECT 	
            CM.BASEL_ACCT_ID,
            PL1.CCAR_BASEL_PRD_TP_NM0		
            FROM MERG02 CM
            LEFT JOIN REVLVNG_CR_RPTG_DRVD_VAR DV ON CM.BASEL_ACCT_ID=DV.BASEL_ACCT_ID AND CM.MTH_TM_ID=DV.MTH_TM_ID
            left join RPTG_PRD_LKP1 PL1 ON CM.PRD_CD = PL1.PRD_CD AND CM.SUB_PRD_CD0 = PL1.SUB_PRD_CD0 
            AND DV.REVISED_EXPSR_OV_125K_F = PL1.REVISED_EXPSR_OV_125K_F 
            AND cm.HELOC_F = PL1.HELOC_F AND BDV_BASEL_PRD_CD = PL1.BASEL_PRD_CD
        
        ), get_seg_nums as (
            select
            a.basel_acct_id,
            CASE 
		    when b.model is null and c.model is null and d.model is null then ''
		    else CCAR_BASEL_PRD_TP_NM0
		    end 
	        as CCAR_BASEL_PRD_TP_NM0,
            b.PD_BASEL_SEG_NUM as v_P_SEG_NUM,
            c.LGD_BASEL_SEG_NUM as v_L_SEG_NUM,
            d.EAD_BASEL_SEG_NUM as v_E_SEG_NUM
            
            from CCAR_BASEL_PRD_TP_NM0 a 
            left join ( 
                select * from {{upstream_asset[8]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )b
            on a.basel_acct_id = b.basel_acct_id

            left join (
                select * from {{upstream_asset[9]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )c
            on a.basel_acct_id=c.basel_acct_id

            left join (
                select * from {{upstream_asset[10]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )d
            on a.basel_acct_id=d.basel_acct_id

        ), set_seg_nums_as_var as( 
            select 
            basel_acct_id,
            CCAR_BASEL_PRD_TP_NM0,
            v_P_SEG_NUM,
            v_L_SEG_NUM,
            v_E_SEG_NUM,
            LPAD(COALESCE(v_P_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_P_SEG_NUM,
            LPAD(COALESCE(v_L_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_L_SEG_NUM,
            LPAD(COALESCE(v_E_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_E_SEG_NUM
            from get_seg_nums

        ),final as (
            select 
            basel_acct_id, 
            COALESCE(TRIM(CCAR_BASEL_PRD_TP_NM0), '') || '_' ||
                RIGHT(v_v_P_SEG_NUM, 2) || '_' ||
                RIGHT(v_v_E_SEG_NUM, 2) || '_' ||
                RIGHT(v_v_L_SEG_NUM, 2) AS CCAR_BASEL_PRD_TP_NM
            ,*
            from set_seg_nums_as_var

        )
            select 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
            basel_acct_id, 
            CCAR_BASEL_PRD_TP_NM,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM
            from final    
),
export_spl as (
		with BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 as (
            select
            I.BASEL_ACCT_ID,
            PRD_ID
            from {{upstream_asset[11]}} I 
            left outer join (select BASEL_ACCT_ID, PRD_ID from {{upstream_asset[12]}} 
            where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') III 
            on I.BASEL_ACCT_ID=III.BASEL_ACCT_ID
            where mth_tm_id= {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ), 	CCAR_BASEL_PRD_TP_NM_A as (
            select
            III.BASEL_ACCT_ID, 
            TRIM(LKP_PRD.CCAR_BASEL_PRD_TP_NM) AS CCAR_BASEL_PRD_TP_NM_A
            from BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 III 
            LEFT OUTER JOIN {{upstream_asset[13]}} LKP_PRD 
            ON III.PRD_ID = LKP_PRD.PRD_ID AND LKP_PRD.SRC_SYS_CD = 'SPL'

        ), get_seg_nums as (
            select
            a.basel_acct_id,
            CCAR_BASEL_PRD_TP_NM_A,
            b.PD_BASEL_SEG_NUM as v_P_SEG_NUM,
            c.LGD_BASEL_SEG_NUM as v_L_SEG_NUM,
            
            from CCAR_BASEL_PRD_TP_NM_A a 
            left join ( 
                select basel_acct_id, PD_BASEL_SEG_NUM from {{upstream_asset[8]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )b
            on a.basel_acct_id = b.basel_acct_id

            left join (
                select basel_acct_id, LGD_BASEL_SEG_NUM from {{upstream_asset[9]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )c
            on a.basel_acct_id=c.basel_acct_id

        ), set_seg_nums_as_var as( 
            select 
            basel_acct_id,
            CCAR_BASEL_PRD_TP_NM_A,
            v_P_SEG_NUM,
            v_L_SEG_NUM,
            LPAD(COALESCE(v_P_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_P_SEG_NUM,
            LPAD(COALESCE(v_L_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_L_SEG_NUM
            from get_seg_nums
        ), final as (
        select 
            basel_acct_id, 
            COALESCE(TRIM(CCAR_BASEL_PRD_TP_NM_A), '') || '_' || RIGHT(v_v_P_SEG_NUM, 2) || '_01_' || RIGHT(v_v_L_SEG_NUM, 2) AS CCAR_BASEL_PRD_TP_NM
        ,*
        from set_seg_nums_as_var
        )
            select 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
            basel_acct_id, 
            trim(CCAR_BASEL_PRD_TP_NM) as CCAR_BASEL_PRD_TP_NM,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM
            from final
),
export_mor as (
		 with base as (                        
            select 
            a.basel_acct_id, 
            a.INSUR_GRP,
            b.BULK_IND
            FROM {{upstream_asset[14]}} a 
            left join {{upstream_asset[15]}} b 
            on a.mort_num=b.mort_num
            where MTH_TM_ID={{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}  
            and obsn_dt= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
		),CCAR_BASEL_PRD_TP_NM as (
            select 
            basel_acct_id,
            CCAR_BASEL_PRD_TP_NM
            from base a
            left join {{upstream_asset[16]}} c
            ON 
            ('MOR') = UPPER(SRC_SYS_CD)
            AND a.INSUR_GRP = c.basel_mort_insurer_grp_desc 
            AND a.BULK_IND = c.BULK_F
		), get_seg_nums as (
            select
            a.basel_acct_id,
            CCAR_BASEL_PRD_TP_NM,
            b.PD_BASEL_SEG_NUM as v_P_SEG_NUM,
            c.LGD_BASEL_SEG_NUM as v_L_SEG_NUM,
            
            from CCAR_BASEL_PRD_TP_NM a 
            left join ( 
                select basel_acct_id, PD_BASEL_SEG_NUM from {{upstream_asset[8]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )b
            on a.basel_acct_id = b.basel_acct_id

            left join (
                select basel_acct_id, LGD_BASEL_SEG_NUM from {{upstream_asset[9]}} 
                where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
                and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )c
            on a.basel_acct_id=c.basel_acct_id

		), set_seg_nums_as_var as( 
			select 
			basel_acct_id,
			CCAR_BASEL_PRD_TP_NM,
			v_P_SEG_NUM,
			v_L_SEG_NUM,
			LPAD(COALESCE(v_P_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_P_SEG_NUM,
			LPAD(COALESCE(v_L_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_L_SEG_NUM
			from get_seg_nums
		), final as (
		select 
			basel_acct_id, 
			COALESCE(TRIM(CCAR_BASEL_PRD_TP_NM), '') || '_' || RIGHT(v_v_P_SEG_NUM, 2) || '_01_' || RIGHT(v_v_L_SEG_NUM, 2) AS CCAR_BASEL_PRD_TP_NM
		    ,*
		    from set_seg_nums_as_var
        )
          select 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
            basel_acct_id, 
            trim(CCAR_BASEL_PRD_TP_NM) as CCAR_BASEL_PRD_TP_NM,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM
            from final
),
export_tng as (
		WITH accts as (
        SELECT
        BASEL_ACCT_ID,
        MONTH_END_DT, 
        account_id,
        case 
        when UPPER(bulk_nsurer_desc)='BULKINSURED' then 'Y'
        else 'N'
        end as bulk_ind,
        INSURER_DESC
        FROM {{upstream_asset[17]}} a 
        INNER JOIN {{upstream_asset[18]}} b 
        ON 
        a.ACCOUNT_ID = b.SRC_APP_ID
        AND b.SRC_APP_CD ='TNG-MOR'
        AND b.SRC_SYS_DEL_F != 'Y' 
        and MONTH_END_DT='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),base as (
        select a.*, 
        SRC_SYS_CD 
        from accts a 
        left join {{upstream_asset[19]}} b 
        on a.basel_acct_id=b.basel_acct_id
        where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),CCAR_BASEL_PRD_TP_NM as (
        select 
        BASEL_ACCT_ID, 
        CCAR_BASEL_PRD_TP_NM
        from base a 
        left join 
        {{upstream_asset[16]}} b
        on (UPPER(a.src_sys_cd) = UPPER(b.SRC_SYS_CD) and
        UPPER(a.INSURER_DESC) = UPPER(b.BASEL_MORT_INSURER_GRP_DESC) and
        UPPER(a.bulk_ind) = UPPER(b.BULK_F))
    ), get_seg_nums as (
        select
        a.basel_acct_id,
        CCAR_BASEL_PRD_TP_NM,
        b.PD_BASEL_SEG_NUM as v_P_SEG_NUM,
        c.LGD_BASEL_SEG_NUM as v_L_SEG_NUM,

        from CCAR_BASEL_PRD_TP_NM a 
        left join (
            select basel_acct_id, PD_BASEL_SEG_NUM from {{upstream_asset[8]}} 
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        )b
        on a.basel_acct_id = b.basel_acct_id

        left join (
            select basel_acct_id, LGD_BASEL_SEG_NUM from {{upstream_asset[9]}} 
            where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
            and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        )c
        on a.basel_acct_id=c.basel_acct_id

    ), set_seg_nums_as_var as( 
        select 
        basel_acct_id,
        CCAR_BASEL_PRD_TP_NM,
        v_P_SEG_NUM,
        v_L_SEG_NUM,
        LPAD(COALESCE(v_P_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_P_SEG_NUM,
        LPAD(COALESCE(v_L_SEG_NUM, 0)::VARCHAR, 4, '0') AS v_v_L_SEG_NUM
        from get_seg_nums
    ), final as (
        select 
        basel_acct_id, 
        COALESCE(TRIM(CCAR_BASEL_PRD_TP_NM), '') || '_' || RIGHT(v_v_P_SEG_NUM, 2) || '_01_' || RIGHT(v_v_L_SEG_NUM, 2) AS CCAR_BASEL_PRD_TP_NM
        ,*
        from set_seg_nums_as_var
    )
        select 
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
        basel_acct_id, 
        trim(CCAR_BASEL_PRD_TP_NM) as CCAR_BASEL_PRD_TP_NM,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM
        from final
),
final as (
		SELECT OBSN_DT, BASEL_ACCT_ID, CCAR_BASEL_PRD_TP_NM, STREAM
          FROM export_ks

        UNION ALL 
        SELECT OBSN_DT, BASEL_ACCT_ID, CCAR_BASEL_PRD_TP_NM, STREAM
          FROM export_spl

        UNION ALL 
        SELECT OBSN_DT, BASEL_ACCT_ID, CCAR_BASEL_PRD_TP_NM, STREAM
          FROM export_mor

        UNION ALL 
        SELECT OBSN_DT, BASEL_ACCT_ID, CCAR_BASEL_PRD_TP_NM, STREAM
          FROM export_tng
)
select 
OBSN_DT, 
BASEL_ACCT_ID, 
CCAR_BASEL_PRD_TP_NM, 
STREAM 
from final
