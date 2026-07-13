(WITH
            mapping AS (
                SELECT
                    seg.basel_seg_id,
                    parm.UNADJUSTED_RPTG_RTO
                FROM
                    reference.BASEL_SEG_RPTG_PARM AS parm
                    LEFT JOIN reference.BASEL_SEG AS seg ON parm.basel_seg_id = seg.basel_seg_id
                    LEFT JOIN reference.BASEL_MODEL AS MOD ON MOD.basel_model_id = parm.basel_model_id
                WHERE
                    parm.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                    AND parm.EFF_TO_DT >= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    AND parm.EFF_FROM_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            seg_num AS (
                SELECT
                    main.*,
                    seg.basel_seg_id
                FROM
                    instruments.EAD_BASEL_SEG_NUM AS main
                    LEFT JOIN reference.BASEL_SEG AS seg ON main.EAD_BASEL_SEG_NUM = seg.SEG_NUM
                    AND main.model = seg.BASEL_MODEL_ID
                WHERE
                    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    AND main.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            ),
            dv_pop AS (
            SELECT
                main.basel_acct_id,
                mapping.UNADJUSTED_RPTG_RTO AS EAD_UNADJUSTED_RPTG_RTO,
            FROM
                seg_num AS main
                LEFT JOIN mapping ON main.basel_seg_id = mapping.basel_seg_id
            WHERE
                main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )
            SELECT
                ks.BASEL_ACCT_ID,
                pop.EAD_UNADJUSTED_RPTG_RTO,
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS stream
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
            LEFT JOIN dv_pop pop ON
                ks.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
            WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        )

        UNION ALL

        SELECT
            acct.BASEL_ACCT_ID,
            0 AS EAD_UNADJUSTED_RPTG_RTO,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS stream
        FROM features.BASEL_ACCT_ID acct
        WHERE acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND acct.SRC_SYS_CD IN ('SPL', 'TNG-MOR', 'MOR')