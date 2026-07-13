{% set upstream_asset = [
    'ingestion.MORT_MTH_SNAPSHOT',
    'instruments.DLGD_F',
    'instruments.DLGD_FLR',
    'instruments.LGD_FINAL_RPTG_RTO',
    'instruments.LGD_FLR',
        ] %}

SELECT
    mor.BASEL_ACCT_ID,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
    CASE
      WHEN DLGD_F = 'Y'
      THEN GREATEST(LGD_FINAL_RPTG_RTO, DLGD_FLR, LGD_FLR)
      ELSE GREATEST(LGD_FINAL_RPTG_RTO, LGD_FLR)
    END AS DLGD_RPTG_RTO

FROM {{upstream_asset[0]}} mor
LEFT JOIN {{upstream_asset[1]}} dlgd
    ON dlgd.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
    and dlgd.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    and dlgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN {{upstream_asset[3]}} lgd_rto
    on lgd_rto.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
    and lgd_rto.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    and lgd_rto.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN {{upstream_asset[4]}} lgd_flr
    on lgd_flr.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
    and lgd_flr.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    and lgd_flr.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN {{upstream_asset[2]}} dlgd_flr
    on dlgd_flr.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
    and dlgd_flr.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    and dlgd_flr.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE mor.MTH_TM_Id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

