WITH
            seg_num AS (
                SELECT
                    main.BASEL_ACCT_ID,
                    seg.SEG_VER
                FROM
                    instruments.EAD_BASEL_SEG_NUM AS main
                    LEFT JOIN reference.BASEL_SEG AS seg ON main.EAD_BASEL_SEG_NUM = seg.SEG_NUM
                    AND main.model = seg.BASEL_MODEL_ID
                WHERE
                    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    AND main.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            )
        SELECT DISTINCT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS stream,
            CASE
                WHEN TRIM(acct.SRC_SYS_CD) = 'KS' THEN seg.SEG_VER
                ELSE NULL
            END AS EAD_SEG_VER
        FROM features.BASEL_ACCT_ID acct
        LEFT JOIN seg_num seg ON
            acct.BASEL_ACCT_ID = seg.BASEL_ACCT_ID
        WHERE
            acct.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND TRIM(acct.SRC_SYS_CD) IN ('KS','SPL')

        UNION ALL

        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS stream,
            NULL AS EAD_SEG_VER
        FROM features.BASEL_ACCT_ID acct
        WHERE
            acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND TRIM(acct.SRC_SYS_CD) IN ('MOR','TNG-MOR')