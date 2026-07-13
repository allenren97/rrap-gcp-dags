import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT",
    "instruments.METRPL_AREA_NM",
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",
    "features.PROP_VAL_NEW_CMA",
    "ingestion.PROVNCL_HOUSE_INDEX_SUM_CMA",
    "instruments.INDEX_TERANETV_CMA",
    "instruments.DLGD_F",
    "ingestion.TM_DIM",
    "reference.METRPL_AREA_NM_LKP"
]
DOWNSTREAM_ASSET = "instruments.PREV_12_QTR_PRPTY_VAL_AMT"
DEPENDENCIES = {
    "export_prev_12_qtr_prpty_val_amt": ["export_ks", "export_mor", "export_spl", "export_tng"],
    "export_ks": ["duckdb_clear"],
    "export_mor": ["duckdb_clear"],
    "export_spl": ["duckdb_clear"],
    "export_tng": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}

def export_prev_12_qtr_prpty_val_amt(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            m.PROV,
            METRPL_AREA_NM_CLEAN as METRPL_AREA_NM
        FROM {UPSTREAM_ASSET[6]} m
        LEFT JOIN {UPSTREAM_ASSET[13]} lkp
        ON m.METRPL_AREA_NM = lkp.METRPL_AREA_NM
        AND m.PROV IS NOT DISTINCT FROM lkp.PROV
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    ), 
    tera_index AS (
        SELECT DISTINCT
            MTH_TM_ID,
            LABEL_1,
            trim(
                regexp_replace(
                    translate(
                        lower(coalesce(LABEL_2, '')),
                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                        'aaaaaaceeeeiiiinooooouuuuyy'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ) AS LABEL_2,
            LAST_VALUE(
                CASE WHEN "INDEX" = 0 THEN NULL ELSE "INDEX" END
                IGNORE NULLS
            ) OVER (
                PARTITION BY LABEL_1, 
                            trim(
                                regexp_replace(
                                    translate(
                                        lower(coalesce(LABEL_2, '')),
                                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                                        'aaaaaaceeeeiiiinooooouuuuyy'
                                    ),
                                    '[^a-z0-9]+',
                                    ' ',
                                    'g'
                                )
                            )
                ORDER BY MTH_TM_ID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "INDEX"
        FROM {UPSTREAM_ASSET[7]}
    ),
    prov_sum AS (
        SELECT
            MTH_TM_ID,
            PROV_CD,
            LAST_VALUE(
                CASE WHEN HOUSE_INDEX_RTO = 0 THEN NULL ELSE HOUSE_INDEX_RTO END
                IGNORE NULLS
            ) OVER (
                PARTITION BY PROV_CD
                ORDER BY MTH_TM_ID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS HOUSE_INDEX_RTO
        FROM ingestion.PROVNCL_HOUSE_INDEX_SUM_CMA
    ),
    base AS (
        SELECT
            COALESCE(terA."INDEX", provA.HOUSE_INDEX_RTO, fallbackA."INDEX") AS PREV_METRPL_TERANET_INDEX,
            COALESCE(terB."INDEX", provB.HOUSE_INDEX_RTO, fallbackB."INDEX") AS APRSD_DT_INDEX,
            step.APRSD_VAL,
            step.STEP_PLN_AGRMNT_NUM,
            step.STEP_PLN_SNAPSHOT_ID,
            acct.BASEL_ACCT_ID
        FROM {UPSTREAM_ASSET[5]} AS step
        LEFT JOIN (
            SELECT
                BASEL_ACCT_ID,
                STEP_PLN_AGRMNT_NUM
            FROM {UPSTREAM_ASSET[0]}
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            UNION
            SELECT
                BASEL_ACCT_ID,
                STEP_PLN_AGRMNT_NUM
            FROM {UPSTREAM_ASSET[2]}
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ) AS acct
            ON acct.STEP_PLN_AGRMNT_NUM = step.STEP_PLN_AGRMNT_NUM
        LEFT JOIN prpty_nm AS m
            ON acct.BASEL_ACCT_ID = m.BASEL_ACCT_ID
        LEFT JOIN tera_index AS terA
            ON m.METRPL_AREA_NM = terA.LABEL_2
            AND m.PROV = terA.LABEL_1
            AND terA.mth_tm_id = step.MTH_TM_ID - 40*12*3
        LEFT JOIN prov_sum AS provA
            ON m.PROV = provA.PROV_CD
            AND provA.mth_tm_id = step.MTH_TM_ID - 40*12*3
        LEFT JOIN tera_index AS fallbackA
            ON fallbackA.LABEL_2 = '11'
            AND fallbackA.LABEL_1 = 'COMPOSITE'
            AND fallbackA.mth_tm_id = step.MTH_TM_ID - 40*12*3
        LEFT JOIN {UPSTREAM_ASSET[12]} AS tm
            ON COALESCE(step.APRSD_DT, step.CR_LMT_DT) = tm.day_dt
        LEFT JOIN tera_index AS terB
            ON m.METRPL_AREA_NM = terB.LABEL_2
            AND m.PROV = terB.LABEL_1
            AND terB.mth_tm_id = tm.FNCL_MTH_KEY
        LEFT JOIN prov_sum AS provB
            ON m.PROV = provB.PROV_CD
            AND provB.mth_tm_id = tm.FNCL_MTH_KEY
        LEFT JOIN tera_index AS fallbackB
            ON fallbackB.LABEL_2 = '11'
            AND fallbackB.LABEL_1 = 'COMPOSITE'
            AND fallbackB.mth_tm_id = tm.FNCL_MTH_KEY
        WHERE step.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    ),
    appraised AS (
        SELECT
            STEP_PLN_AGRMNT_NUM,
            STEP_PLN_SNAPSHOT_ID,
            PREV_METRPL_TERANET_INDEX,
            APRSD_DT_INDEX,
            APRSD_VAL,
            BASEL_ACCT_ID,
            ROUND(APRSD_VAL / APRSD_DT_INDEX, 3) AS APPRSL_DT_PRPTY_VAL_AMT
        FROM base
    )
    SELECT
        STEP_PLN_AGRMNT_NUM,
        STEP_PLN_SNAPSHOT_ID,
        PREV_METRPL_TERANET_INDEX,
        APRSD_DT_INDEX,
        APRSD_VAL,    
        APPRSL_DT_PRPTY_VAL_AMT,
        a.BASEL_ACCT_ID,
        CASE
            WHEN DLGD_F = 'N' THEN NULL
            WHEN dlgd.DLGD_F = 'Y' THEN ROUND(APPRSL_DT_PRPTY_VAL_AMT * PREV_METRPL_TERANET_INDEX, 3)
            ELSE NULL
        END AS PREV_12_QTR_PRPTY_VAL_AMT
    FROM appraised a
    LEFT JOIN {UPSTREAM_ASSET[11]} dlgd
    on a.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
    and stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    and OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    
    """,
):
    pass

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            prpty.PREV_12_QTR_PRPTY_VAL_AMT
        FROM {UPSTREAM_ASSET[0]} AS ks
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="fact__prev_12_qtr_prpty_val_amt.export_prev_12_qtr_prpty_val_amt", key="parquet") }}}}' prpty
            ON prpty.BASEL_ACCT_ID = ks.BASEL_ACCT_ID
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            PROV_COMP as PROV,
            METRPL_AREA_NM_CLEAN AS METRPL_AREA_NM
        FROM {UPSTREAM_ASSET[6]} m
        LEFT JOIN {UPSTREAM_ASSET[13]} lkp
        ON m.METRPL_AREA_NM = lkp.METRPL_AREA_NM
        and m.PROV IS NOT DISTINCT FROM lkp.PROV
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    ), 
    tera_index AS (
        SELECT DISTINCT
            MTH_TM_ID,
            LABEL_1,
            trim(
                regexp_replace(
                    translate(
                        lower(coalesce(LABEL_2, '')),
                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                        'aaaaaaceeeeiiiinooooouuuuyy'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ) AS LABEL_2,
            LAST_VALUE(
                CASE WHEN "INDEX" = 0 THEN NULL ELSE "INDEX" END
                IGNORE NULLS
            ) OVER (
                PARTITION BY LABEL_1, 
                            trim(
                                regexp_replace(
                                    translate(
                                        lower(coalesce(LABEL_2, '')),
                                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                                        'aaaaaaceeeeiiiinooooouuuuyy'
                                    ),
                                    '[^a-z0-9]+',
                                    ' ',
                                    'g'
                                )
                            )
                ORDER BY MTH_TM_ID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "INDEX"
        FROM {UPSTREAM_ASSET[7]}
    )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            MOR.BASEL_ACCT_ID,
            CASE
                WHEN DLGD_F = 'N' THEN NULL
                WHEN DLGD_F = 'Y' THEN IND.INDEX_TERANETV/NULLIF(indx1."INDEX",0) * indx2."INDEX"
                ELSE NULL
            END AS PREV_12_QTR_PRPTY_VAL_AMT,
            'MOR' as SRC_SYS_CD
        FROM {UPSTREAM_ASSET[1]} MOR
        LEFT JOIN {UPSTREAM_ASSET[10]} IND
        ON IND.MORT_NUM = MOR.MORT_NUM
        AND IND.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND IND.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
        LEFT JOIN prpty_nm metrpl
            ON MOR.BASEL_ACCT_ID = metrpl.BASEL_ACCT_ID
        LEFT JOIN tera_index AS indx1
            ON TRIM(indx1.LABEL_2) = TRIM(metrpl.METRPL_AREA_NM)
            AND TRIM(indx1.LABEL_1) = TRIM(metrpl.PROV)
            AND indx1.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40
        LEFT JOIN tera_index AS indx2
            ON TRIM(indx2.LABEL_2) = TRIM(metrpl.METRPL_AREA_NM)
            AND TRIM(indx2.LABEL_1) = TRIM(metrpl.PROV)
            AND indx2.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*12*3
        LEFT JOIN {UPSTREAM_ASSET[11]} dlgd
            on MOR.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
            and dlgd.stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            and dlgd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE MOR.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """ 
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            prpty.PREV_12_QTR_PRPTY_VAL_AMT
        FROM {UPSTREAM_ASSET[2]} AS spl
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="fact__prev_12_qtr_prpty_val_amt.export_prev_12_qtr_prpty_val_amt", key="parquet") }}}}' prpty
            ON prpty.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            PROV_COMP as PROV,
            METRPL_AREA_NM_CLEAN AS METRPL_AREA_NM
        FROM {UPSTREAM_ASSET[6]} m
        LEFT JOIN {UPSTREAM_ASSET[13]} lkp
        ON m.METRPL_AREA_NM = lkp.METRPL_AREA_NM
        and m.PROV IS NOT DISTINCT FROM lkp.PROV
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    ), 
    tera_index AS (
        SELECT DISTINCT
            MTH_TM_ID,
            LABEL_1,
            trim(
                regexp_replace(
                    translate(
                        lower(coalesce(LABEL_2, '')),
                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                        'aaaaaaceeeeiiiinooooouuuuyy'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ) AS LABEL_2,
            LAST_VALUE(
                CASE WHEN "INDEX" = 0 THEN NULL ELSE "INDEX" END
                IGNORE NULLS
            ) OVER (
                PARTITION BY LABEL_1, 
                            trim(
                                regexp_replace(
                                    translate(
                                        lower(coalesce(LABEL_2, '')),
                                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                                        'aaaaaaceeeeiiiinooooouuuuyy'
                                    ),
                                    '[^a-z0-9]+',
                                    ' ',
                                    'g'
                                )
                            )
                ORDER BY MTH_TM_ID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "INDEX"
        FROM {UPSTREAM_ASSET[7]}
    )
        SELECT
            tng.MONTH_END_DT AS OBSN_DT,
            dim.BASEL_ACCT_ID,
            CASE
            WHEN DLGD_F = 'N'
            THEN NULL
            ELSE prop.PROP_VAL_NEW/NULLIF(indx1."INDEX",0) * indx2."INDEX" 
            END AS PREV_12_QTR_PRPTY_VAL_AMT,
            'TNG-MOR' as SRC_SYS_CD
        FROM {UPSTREAM_ASSET[3]} dim
        LEFT JOIN {UPSTREAM_ASSET[4]} tng
            ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        LEFT JOIN {UPSTREAM_ASSET[8]} AS prop
            ON prop.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
            AND prop.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN prpty_nm metrpl
            ON dim.BASEL_ACCT_ID = metrpl.BASEL_ACCT_ID
        LEFT JOIN tera_index AS indx1
            ON TRIM(indx1.LABEL_2) = TRIM(metrpl.METRPL_AREA_NM)
            AND TRIM(indx1.LABEL_1) = TRIM(metrpl.PROV)
            AND indx1.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        LEFT JOIN tera_index AS indx2
            ON TRIM(indx2.LABEL_2) = TRIM(metrpl.METRPL_AREA_NM)
            AND TRIM(indx2.LABEL_1) = TRIM(metrpl.PROV)
            AND indx2.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*12*3
        LEFT JOIN {UPSTREAM_ASSET[11]} dlgd
            on dim.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
            and dlgd.stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            and dlgd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            PREV_12_QTR_PRPTY_VAL_AMT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__prev_12_qtr_prpty_val_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__prev_12_qtr_prpty_val_amt.export_tng", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__prev_12_qtr_prpty_val_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__prev_12_qtr_prpty_val_amt.export_spl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass