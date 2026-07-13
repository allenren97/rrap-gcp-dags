{% set upstream_asset = [
  "ingestion.TNG_ACCT_MO",
  "ingestion.BASEL_ACCT_DIM",
  "instruments.DLGD_F",
  "ingestion.TERANET_ADDR_LKP_CMA"
  ] %}

WITH province AS (
    SELECT 
        month_end_dt,
        trim(account_id) AS account_id,
        trim(FSA) AS FSA,
        CASE 
            WHEN prop_province_code IS NULL OR prop_province_code='' OR prop_province_code='?' THEN 
                CASE 
                    WHEN substr(FSA,1,1)='A' THEN 'NL'
                    WHEN substr(FSA,1,1)='B' THEN 'NS'
                    WHEN substr(FSA,1,1)='C' THEN 'PE'
                    WHEN substr(FSA,1,1)='E' THEN 'NB'
                    WHEN substr(FSA,1,1)='G' THEN 'QC'
                    WHEN substr(FSA,1,1)='H' THEN 'QC'
                    WHEN substr(FSA,1,1)='J' THEN 'QC'
                    WHEN substr(FSA,1,1)='R' THEN 'MB'
                    WHEN substr(FSA,1,1)='S' THEN 'SK'
                    WHEN substr(FSA,1,1)='T' THEN 'AB'
                    WHEN substr(FSA,1,1)='V' THEN 'BC'
                    WHEN substr(FSA,1,1)='X' THEN 'NT'
                    WHEN substr(FSA,1,1)='Y' THEN 'YT'
                    WHEN TRIM(account_id)='MBS~370528' THEN 'MB'
                    WHEN TRIM(account_id)='MBS~370579' THEN 'BC'
                    ELSE 'ON'
                END
            ELSE prop_province_code
        END AS province
    FROM {{upstream_asset[0]}}
    WHERE 
        month_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
)


SELECT dim.basel_acct_id,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
    d.stream,
    'TNG-MOR' AS SRC_SYS_CD,
    prov.province as PROV,
    coalesce(loctn_label_2, '11') as cma,
    CASE
        WHEN d.DLGD_F = 'N' THEN NULL
        ELSE coalesce(loctn_label_2, '11')
    END AS metrpl_area_nm
    FROM {{upstream_asset[1]}} dim
    LEFT JOIN {{upstream_asset[0]}} tng
        ON dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN {{upstream_asset[2]}} d 
      ON dim.BASEL_ACCT_ID = d.BASEL_ACCT_ID
      AND d.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
      AND d.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN province as prov
      ON TRIM(dim.SRC_APP_ID) = TRIM(prov.account_id)
    LEFT JOIN {{upstream_asset[3]}} lkp
      ON tng.FSA = lkp.PRPTY_LOCTN_NM
      and prov.PROVINCE = lkp.LOCTN_LABEL_1
    WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
 