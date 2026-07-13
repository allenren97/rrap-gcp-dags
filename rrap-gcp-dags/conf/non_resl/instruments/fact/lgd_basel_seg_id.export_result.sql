SELECT 
    BASEL_ACCT_ID,
    OBSN_DT,
    MODEL,
    CASE

        WHEN MODEL = 'tng_mor_lgdnd' AND LGD_BASEL_SEG_NUM = 1 THEN 16213
        WHEN MODEL = 'tng_mor_lgdnd' AND LGD_BASEL_SEG_NUM = 10 THEN 16222

        WHEN MODEL = 'tng_mor_lgdd' AND LGD_BASEL_SEG_NUM = 1 THEN 16223
        WHEN MODEL = 'tng_mor_lgdd' AND LGD_BASEL_SEG_NUM = 10 THEN 16232
        WHEN MODEL = 'tng_mor_lgdd' AND LGD_BASEL_SEG_NUM = 11 THEN 16233

        WHEN MODEL = 'sslb_lgdnd' AND LGD_BASEL_SEG_NUM = 2 THEN 16105
        WHEN MODEL = 'sslb_lgdnd' AND LGD_BASEL_SEG_NUM = 98 THEN 16106

        WHEN MODEL = 'sslb_lgdd' AND LGD_BASEL_SEG_NUM = 2 THEN 16110
        WHEN MODEL = 'sslb_lgdd' AND LGD_BASEL_SEG_NUM = 90 THEN 16111 
        WHEN MODEL = 'sslb_lgdd' AND LGD_BASEL_SEG_NUM = 98 THEN 16112

        WHEN MODEL = 'ssla_lgdnd' AND LGD_BASEL_SEG_NUM = 1 THEN 16103
        WHEN MODEL = 'ssla_lgdnd' AND LGD_BASEL_SEG_NUM = 98 THEN 16104

        WHEN MODEL = 'ssla_lgdd' AND LGD_BASEL_SEG_NUM = 1 THEN 16107
        WHEN MODEL = 'ssla_lgdd' AND LGD_BASEL_SEG_NUM = 90 THEN 16108
        WHEN MODEL = 'ssla_lgdd' AND LGD_BASEL_SEG_NUM = 98 THEN 16109

        WHEN MODEL = 'mor_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) THEN LGD_BASEL_SEG_NUM + 16174
        WHEN MODEL = 'mor_lgdnd' AND LGD_BASEL_SEG_NUM = 98 THEN 16188

        WHEN MODEL = 'mor_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14) THEN LGD_BASEL_SEG_NUM + 16188
        WHEN MODEL = 'mor_lgdd' AND LGD_BASEL_SEG_NUM = 98 THEN 16203 

        WHEN MODEL = 'loc_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5) THEN LGD_BASEL_SEG_NUM + 16074
        WHEN MODEL = 'loc_lgdnd' AND LGD_BASEL_SEG_NUM = 98 THEN 16080

        WHEN MODEL = 'loc_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4) THEN LGD_BASEL_SEG_NUM + 16080
        WHEN MODEL = 'loc_lgdd' AND LGD_BASEL_SEG_NUM = 90 THEN 16085
        WHEN MODEL = 'loc_lgdd' AND LGD_BASEL_SEG_NUM = 98 THEN 16086

        WHEN MODEL = 'itl_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 7, 8) THEN LGD_BASEL_SEG_NUM + 16148

        WHEN MODEL = 'itl_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8) THEN LGD_BASEL_SEG_NUM + 16156

        WHEN MODEL = 'heloc_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3) THEN LGD_BASEL_SEG_NUM + 16016
        WHEN MODEL = 'heloc_lgdnd' AND LGD_BASEL_SEG_NUM = 98 THEN 16020

        WHEN MODEL = 'heloc_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4) THEN LGD_BASEL_SEG_NUM + 16020
        WHEN MODEL = 'heloc_lgdd' AND LGD_BASEL_SEG_NUM = 90 THEN 16025
        WHEN MODEL = 'heloc_lgdd' AND LGD_BASEL_SEG_NUM = 98 THEN 16026

        WHEN MODEL = 'dtl_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 7, 8) THEN LGD_BASEL_SEG_NUM + 16120

        WHEN MODEL = 'dtl_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4, 5, 6, 7, 8) THEN LGD_BASEL_SEG_NUM + 16128

        WHEN MODEL = 'cc_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3, 4) THEN LGD_BASEL_SEG_NUM + 16044
        WHEN MODEL = 'cc_lgdnd' AND LGD_BASEL_SEG_NUM = 98 THEN 16050

        WHEN MODEL = 'cc_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 2) THEN LGD_BASEL_SEG_NUM + 16050
        WHEN MODEL = 'cc_lgdd' AND LGD_BASEL_SEG_NUM = 90 THEN 16055
        WHEN MODEL = 'cc_lgdd' AND LGD_BASEL_SEG_NUM = 98 THEN 16056

        ELSE NULL
    END AS LGD_BASEL_SEG_ID,
    STREAM
FROM
    instruments.LGD_BASEL_SEG_NUM
WHERE
    STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    AND OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'