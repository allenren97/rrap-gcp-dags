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
    "ingestion.TERANET_ADDR_LKP_CMA",
]
DOWNSTREAM_ASSET = "instruments.CRNT_PRPTY_VAL_AMT"
DEPENDENCIES = {
    "export_crnt_prpty_val_amt": ["export_ks", "export_mor", "export_spl", "export_tng"],
    "export_ks": ["duckdb_clear"],
    "export_mor": ["duckdb_clear"],
    "export_spl": ["duckdb_clear"],
    "export_tng": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


"""
TODO: ingestion.TERANET_HOUSE_PRC_INDEX_CMA stores values for each month in the PREVIOUS month table. (If you're looking for 21076 data, you need to use mth_tm_id = 21037).
Hopefully this is eventually fixed, but for now we are subtracting 40 for the mth_tm_id when joining tera_index for the current month. When it is fixed, removing the '- 40' should
fix it.
"""
def export_crnt_prpty_val_amt(
    duckdb_conn_id="duckdb-conn",
    sql=f"""    
    WITH prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            --STEP_PLN_AGRMNT_NUM,
            PROV,
            trim(
                regexp_replace(
                translate(
                    lower(coalesce(CMA, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
            ) AS METRPL_AREA_NM
        FROM --best_match
        instruments.METRPL_AREA_NM
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    ), 
    tera_index AS (
        SELECT
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
        FROM ingestion.TERANET_HOUSE_PRC_INDEX_CMA
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
            COALESCE(terA."INDEX", provA.HOUSE_INDEX_RTO, fallbackA."INDEX") AS CRNT_METRPL_TERANET_INDEX,
            COALESCE(terB."INDEX", provB.HOUSE_INDEX_RTO, fallbackB."INDEX") AS APRSD_DT_INDEX,
            step.APRSD_VAL,
            step.STEP_PLN_AGRMNT_NUM,
            step.STEP_PLN_SNAPSHOT_ID,
            acct.BASEL_ACCT_ID
        FROM ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT AS step
        LEFT JOIN (
            SELECT
                BASEL_ACCT_ID,
                STEP_PLN_AGRMNT_NUM
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            UNION
            SELECT
                BASEL_ACCT_ID,
                STEP_PLN_AGRMNT_NUM
            FROM ingestion.MORT_MTH_SNAPSHOT
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            UNION
            SELECT
                BASEL_ACCT_ID,
                STEP_PLN_AGRMNT_NUM
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ) AS acct
            ON acct.STEP_PLN_AGRMNT_NUM = step.STEP_PLN_AGRMNT_NUM
        LEFT JOIN prpty_nm AS m
            ON acct.BASEL_ACCT_ID = m.BASEL_ACCT_ID
            --ON step.STEP_PLN_AGRMNT_NUM = m.STEP_PLN_AGRMNT_NUM
        LEFT JOIN tera_index AS terA
            ON m.METRPL_AREA_NM = terA.LABEL_2
            AND m.PROV = terA.LABEL_1
            AND terA.mth_tm_id = step.MTH_TM_ID - 40 
        LEFT JOIN prov_sum AS provA
            ON m.PROV = provA.PROV_CD
            AND provA.mth_tm_id = step.MTH_TM_ID
        LEFT JOIN tera_index AS fallbackA
            ON fallbackA.LABEL_2 = '11'
            AND fallbackA.LABEL_1 = 'COMPOSITE'
            AND fallbackA.mth_tm_id = step.MTH_TM_ID - 40 
        LEFT JOIN ingestion.TM_DIM AS tm
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
            CRNT_METRPL_TERANET_INDEX,
            APRSD_DT_INDEX,
            APRSD_VAL,
            BASEL_ACCT_ID,
            ROUND(APRSD_VAL / APRSD_DT_INDEX, 3) AS APPRSL_DT_PRPTY_VAL_AMT
        FROM base
    )
    SELECT
        STEP_PLN_AGRMNT_NUM,
        STEP_PLN_SNAPSHOT_ID,
        CRNT_METRPL_TERANET_INDEX,
        APRSD_DT_INDEX,
        APRSD_VAL,    
        APPRSL_DT_PRPTY_VAL_AMT,
        BASEL_ACCT_ID,
        ROUND(APPRSL_DT_PRPTY_VAL_AMT * CRNT_METRPL_TERANET_INDEX, 3) AS CRNT_PRPTY_VAL_AMT
    FROM appraised
    """,
):
    pass

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT DISTINCT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            prpty.CRNT_PRPTY_VAL_AMT
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS ks
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_crnt_prpty_val_amt", key="parquet") }}}}' prpty
            ON prpty.BASEL_ACCT_ID = ks.BASEL_ACCT_ID
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT DISTINCT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            prpty.CRNT_PRPTY_VAL_AMT
        FROM ingestion.MORT_MTH_SNAPSHOT AS mor
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_crnt_prpty_val_amt", key="parquet") }}}}' prpty
            ON prpty.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """ 
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT DISTINCT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            prpty.CRNT_PRPTY_VAL_AMT
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
        LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_crnt_prpty_val_amt", key="parquet") }}}}' prpty
            ON prpty.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT DISTINCT
            tng.MONTH_END_DT AS OBSN_DT,
            dim.BASEL_ACCT_ID,
            prop.PROP_VAL_NEW AS CRNT_PRPTY_VAL_AMT
        FROM ingestion.BASEL_ACCT_DIM dim
        LEFT JOIN ingestion.TNG_ACCT_MO tng
            ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        LEFT JOIN features.PROP_VAL_NEW_CMA AS prop
            ON prop.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
            AND prop.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
            CRNT_PRPTY_VAL_AMT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_tng", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__crnt_prpty_val_amt.export_spl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass