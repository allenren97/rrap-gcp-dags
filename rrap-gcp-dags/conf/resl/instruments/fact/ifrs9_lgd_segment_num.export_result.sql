{% set UPSTREAM_ASSET = [
  "features.SRC_SYS_CD",
  "instruments.LGD_BASEL_SEG_NUM"
  ]%}

SELECT 
  dim.BASEL_ACCT_ID,
  model.STREAM,
  '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
  CASE 
    IF SRC_SYS_CD = 'MOR'
      THEN CASE 
        IF STEP_PLNT_AGRMNT_NUM IS NOT NULL
          THEN CASE 
            IF LGD_BASEL_SEG_NUM IN (1, 4, 7) THEN 1
            IF LGD_BASEL_SEG_NUM IN (2, 5, 8) THEN 2
            IF LGD_BASEL_SEG_NUM IN (3, 6, 9) THEN 3
            ELSE LGD_BASEL_SEG_NUM
          END
        ELSE CASE
          IF MODEL = 'standalone_mor_lgdd' AND LGD_BASEL_SEG_NUM IN (1, 3, 5) THEN 1
          IF MODEL = 'standalone_mor_lgdd' AND LGD_BASEL_SEG_NUM IN (2, 4, 6) THEN 2
          IF MODEL = 'standalone_mor_lgdnd' AND LGD_BASEL_SEG_NUM IN (1, 2, 3) THEN 1
          ELSE LGD_BASEL_SEG_NUM
        END
      END
    ELSE LGD_BASEL_SEG_NUM
  END AS IFRS9_LGD_SEGMENT_NUM

FROM {{UPSTREAM_ASSET[0]}} acct
LEFT JOIN {{UPSTREAM_ASSET[1]}} num
ON num.BASEL_ACCT_ID = acct.BASEL_ACCT_ID
AND num.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
AND num.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'