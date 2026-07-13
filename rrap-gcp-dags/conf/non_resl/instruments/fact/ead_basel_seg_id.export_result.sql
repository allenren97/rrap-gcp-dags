SELECT 
    BASEL_ACCT_ID,
    OBSN_DT,
    MODEL,
    CASE
        WHEN MODEL = 'heloc_ead' AND EAD_BASEL_SEG_NUM IN (1, 2) THEN EAD_BASEL_SEG_NUM + 16010
        WHEN MODEL = 'heloc_ead' AND EAD_BASEL_SEG_NUM = 98 THEN 16015 
        WHEN MODEL = 'heloc_ead' AND EAD_BASEL_SEG_NUM = 99 THEN 16016 

        WHEN MODEL = 'cc_ead' AND EAD_BASEL_SEG_NUM IN (2, 3, 4) THEN EAD_BASEL_SEG_NUM + 16036
        WHEN MODEL = 'cc_ead' AND EAD_BASEL_SEG_NUM = 98 THEN 16043
        WHEN MODEL = 'cc_ead' AND EAD_BASEL_SEG_NUM = 99 THEN 16044
        WHEN MODEL = 'cc_ead' AND EAD_BASEL_SEG_NUM = 1 THEN 16236

        WHEN MODEL = 'loc_ead' AND EAD_BASEL_SEG_NUM IN (1, 2, 3, 4) THEN EAD_BASEL_SEG_NUM + 16067
        WHEN MODEL = 'loc_ead' AND EAD_BASEL_SEG_NUM = 98 THEN 16073
        WHEN MODEL = 'loc_ead' AND EAD_BASEL_SEG_NUM = 99 THEN 16074
        WHEN MODEL = 'loc_ead' AND EAD_BASEL_SEG_NUM = 5 THEN 16240

        WHEN MODEL = 'ssla_ead' AND EAD_BASEL_SEG_NUM = 1 THEN 16097
        WHEN MODEL = 'ssla_ead' AND EAD_BASEL_SEG_NUM = 98 THEN 16098
        WHEN MODEL = 'ssla_ead' AND EAD_BASEL_SEG_NUM = 99 THEN 16099

        WHEN MODEL = 'sslb_ead' AND EAD_BASEL_SEG_NUM = 2 THEN 16100
        WHEN MODEL = 'sslb_ead' AND EAD_BASEL_SEG_NUM = 98 THEN 16101
        WHEN MODEL = 'sslb_ead' AND EAD_BASEL_SEG_NUM = 99 THEN 16102
        
        ELSE NULL
    END AS EAD_BASEL_SEG_ID,
    STREAM
FROM
    instruments.EAD_BASEL_SEG_NUM
WHERE
    STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    AND OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'