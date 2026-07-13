SELECT 
    BASEL_ACCT_ID,
    OBSN_DT,
    MODEL,
    CASE
        WHEN MODEL = 'cc_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8, 9) THEN PD_BASEL_SEG_NUM + 16026
        WHEN MODEL = 'cc_pd' AND PD_BASEL_SEG_NUM = 98 THEN 16036
        WHEN MODEL = 'cc_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16037
    
        WHEN MODEL = 'ssla_pd' AND PD_BASEL_SEG_NUM IN (1, 2) THEN PD_BASEL_SEG_NUM + 16086
        WHEN MODEL = 'ssla_pd' AND PD_BASEL_SEG_NUM = 98 THEN 16090
        WHEN MODEL = 'ssla_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16091

        WHEN MODEL = 'sslb_pd' AND PD_BASEL_SEG_NUM = 4 THEN 16092
        WHEN MODEL = 'sslb_pd' AND PD_BASEL_SEG_NUM = 5 THEN 16093
        WHEN MODEL = 'sslb_pd' AND PD_BASEL_SEG_NUM = 98 THEN 16095
        WHEN MODEL = 'sslb_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16096

        WHEN MODEL = 'itl_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) THEN PD_BASEL_SEG_NUM + 16136
        WHEN MODEL = 'itl_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16148

        WHEN MODEL = 'dtl_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 7) THEN PD_BASEL_SEG_NUM + 16112
        WHEN MODEL = 'dtl_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16120

        WHEN MODEL = 'loc_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8, 9) THEN PD_BASEL_SEG_NUM + 16056
        WHEN MODEL = 'loc_pd' AND PD_BASEL_SEG_NUM = 98 THEN 16066
        WHEN MODEL = 'loc_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16067
        WHEN MODEL = 'loc_pd' AND PD_BASEL_SEG_NUM = 10 THEN 16235

        WHEN MODEL = 'tng_mor_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4) THEN PD_BASEL_SEG_NUM + 16203
        WHEN MODEL = 'tng_mor_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16212

        WHEN MODEL = 'heloc_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8) THEN PD_BASEL_SEG_NUM + 16000
        WHEN MODEL = 'heloc_pd' AND PD_BASEL_SEG_NUM = 98 THEN 16009
        WHEN MODEL = 'heloc_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16010

        WHEN MODEL = 'mor_pd' AND PD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8) THEN PD_BASEL_SEG_NUM + 16164
        WHEN MODEL = 'mor_pd' AND PD_BASEL_SEG_NUM = 98 THEN 16174
        WHEN MODEL = 'mor_pd' AND PD_BASEL_SEG_NUM = 99 THEN 16173

        ELSE NULL
    END AS PD_BASEL_SEG_ID,
    STREAM
FROM
    instruments.PD_BASEL_SEG_NUM
WHERE
    STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    AND OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'