UPSTREAM_ASSET = [
    "instruments.LGD_BASEL_SEG_NUM",
    "reference.BASEL_SEG_RPTG_PARM",
    "reference.BASEL_SEG",
    "reference.BASEL_MODEL",
    "features.BASEL_ACCT_ID"
]
DOWNSTREAM_ASSET = "instruments.LGD_SEG_VER"
DEPENDENCIES = {
    "duckdb_clear": ["duckdb_load"],
}


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    
    """,
):
    pass


def duckdb_load(
    trigger_rule="none_failed_min_one_success",
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        WITH
            seg_num AS (
                SELECT
                    main.BASEL_ACCT_ID,
                    seg.SEG_VER
                FROM
                    instruments.LGD_BASEL_SEG_NUM AS main
                    LEFT JOIN reference.BASEL_SEG AS seg ON main.LGD_BASEL_SEG_NUM = seg.SEG_NUM
                    AND main.model = seg.BASEL_MODEL_ID
                WHERE
                    main.obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND main.stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            )
        SELECT DISTINCT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS stream,
            seg.SEG_VER AS LGD_SEG_VER
        FROM features.BASEL_ACCT_ID acct
        LEFT JOIN seg_num seg ON
            acct.BASEL_ACCT_ID = seg.BASEL_ACCT_ID
        WHERE
            acct.obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND TRIM(acct.SRC_SYS_CD) IN ('KS','SPL')

        UNION ALL

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS stream,
            '3.0' AS LGD_SEG_VER
        FROM features.BASEL_ACCT_ID acct
        WHERE
            acct.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND TRIM(acct.SRC_SYS_CD) IN ('MOR','TNG-MOR')
        )
    """,
):
    pass
