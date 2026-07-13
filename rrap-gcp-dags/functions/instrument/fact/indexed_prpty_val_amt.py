import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",                 # 0
    "ingestion.BASEL_ACCT_DIM",                    # 1
    "ingestion.TNG_ACCT_MO",                       # 2
    "features.PROP_VAL_NEW_CMA",                   # 3
    "instruments.INDEX_TERANETV_CMA",              # 4
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",      # 5
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",     # 6
    "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT",       # 7
    "instruments.METRPL_AREA_NM",                  # 8
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",       # 9
    "ingestion.PROVNCL_HOUSE_INDEX_SUM_CMA",       # 10
]

DOWNSTREAM_ASSET = "instruments.INDEXED_PRPTY_VAL_AMT"

DEPENDENCIES = {
    "duckdb_clear": [
        "export_ks",
        "export_spl",
        "export_mor",
        "export_tng",
    ],
    "export_ks": ["duckdb_load"],
    "export_spl": ["duckdb_load"],
    "export_mor": ["duckdb_load"],
    "export_tng": ["duckdb_load"],
}


# KS
def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            PROV,
            TRIM(
                REGEXP_REPLACE(
                    TRANSLATE(
                        LOWER(COALESCE(CMA, '')),
                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                        'aaaaaaceeeeiiiinooooouuuuyy'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ) AS METRPL_AREA_NM
        FROM {UPSTREAM_ASSET[8]}
        WHERE OBSN_DT =
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
          AND STREAM =
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    ),

    tera_index AS (
        SELECT
            MTH_TM_ID,
            LABEL_1,
            TRIM(
                REGEXP_REPLACE(
                    TRANSLATE(
                        LOWER(COALESCE(LABEL_2, '')),
                        'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                        'aaaaaaceeeeiiiinooooouuuuyy'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ) AS LABEL_2,
            LAST_VALUE(
                CASE
                    WHEN "INDEX" = 0 THEN NULL
                    ELSE "INDEX"
                END
                IGNORE NULLS
            ) OVER (
                PARTITION BY
                    LABEL_1,
                    TRIM(
                        REGEXP_REPLACE(
                            TRANSLATE(
                                LOWER(COALESCE(LABEL_2, '')),
                                'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                                'aaaaaaceeeeiiiinooooouuuuyy'
                            ),
                            '[^a-z0-9]+',
                            ' ',
                            'g'
                        )
                    )
                ORDER BY MTH_TM_ID DESC
            ) AS "INDEX"
        FROM {UPSTREAM_ASSET[9]}
    ),

    prov_sum AS (
        SELECT
            MTH_TM_ID,
            PROV_CD,
            LAST_VALUE(
                CASE
                    WHEN HOUSE_INDEX_RTO = 0 THEN NULL
                    ELSE HOUSE_INDEX_RTO
                END
                IGNORE NULLS
            ) OVER (
                PARTITION BY PROV_CD
                ORDER BY MTH_TM_ID DESC
            ) AS HOUSE_INDEX_RTO
        FROM {UPSTREAM_ASSET[10]}
    ),

    base AS (
        SELECT
            ks.BASEL_ACCT_ID,
            step.APRSD_VAL,

            COALESCE(
                terA."INDEX",
                provA.HOUSE_INDEX_RTO,
                fallbackA."INDEX"
            ) AS CRNT_METRPL_TERANET_INDEX,

            COALESCE(
                terB."INDEX",
                provB.HOUSE_INDEX_RTO,
                fallbackB."INDEX"
            ) AS APRSD_DT_INDEX

        FROM {UPSTREAM_ASSET[6]} ks

        INNER JOIN {UPSTREAM_ASSET[7]} step
            ON ks.STEP_PLN_AGRMNT_NUM = step.STEP_PLN_AGRMNT_NUM
           AND step.MTH_TM_ID =
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}

        LEFT JOIN prpty_nm m
            ON ks.BASEL_ACCT_ID = m.BASEL_ACCT_ID

        LEFT JOIN tera_index terA
            ON m.METRPL_AREA_NM = terA.LABEL_2
           AND m.PROV = terA.LABEL_1
           AND terA.MTH_TM_ID = step.MTH_TM_ID - 40

        LEFT JOIN prov_sum provA
            ON m.PROV = provA.PROV_CD
           AND provA.MTH_TM_ID = step.MTH_TM_ID

        LEFT JOIN tera_index fallbackA
            ON fallbackA.LABEL_1 = 'COMPOSITE'
           AND fallbackA.LABEL_2 = '11'
           AND fallbackA.MTH_TM_ID = step.MTH_TM_ID - 40

        LEFT JOIN ingestion.TM_DIM tm
            ON COALESCE(step.APRSD_DT, step.CR_LMT_DT) = tm.DAY_DT

        LEFT JOIN tera_index terB
            ON m.METRPL_AREA_NM = terB.LABEL_2
           AND m.PROV = terB.LABEL_1
           AND terB.MTH_TM_ID = tm.FNCL_MTH_KEY

        LEFT JOIN prov_sum provB
            ON m.PROV = provB.PROV_CD
           AND provB.MTH_TM_ID = tm.FNCL_MTH_KEY

        LEFT JOIN tera_index fallbackB
            ON fallbackB.LABEL_1 = 'COMPOSITE'
           AND fallbackB.LABEL_2 = '11'
           AND fallbackB.MTH_TM_ID = tm.FNCL_MTH_KEY

        WHERE ks.MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
          AND step.APRSD_VAL IS NOT NULL
          AND step.APRSD_DT IS NOT NULL
    ),

    ks_calc AS (
        SELECT
            BASEL_ACCT_ID,
            CASE
                WHEN APRSD_VAL IS NULL
                  OR APRSD_DT_INDEX IS NULL
                  OR APRSD_DT_INDEX = 0
                  OR CRNT_METRPL_TERANET_INDEX IS NULL
                THEN NULL
                ELSE ROUND(
                    APRSD_VAL
                    * CRNT_METRPL_TERANET_INDEX
                    / APRSD_DT_INDEX,
                    3
                )
            END AS INDEXED_PRPTY_VAL_AMT
        FROM base
    ),

    ks_dedup AS (
        SELECT
            BASEL_ACCT_ID,
            INDEXED_PRPTY_VAL_AMT,
            ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID
                ORDER BY INDEXED_PRPTY_VAL_AMT DESC NULLS LAST
            ) AS rn
        FROM ks_calc
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        INDEXED_PRPTY_VAL_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
    FROM ks_dedup
    WHERE rn = 1
    """
):
    pass


# SPL
def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT DISTINCT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        0 AS INDEXED_PRPTY_VAL_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
    FROM {UPSTREAM_ASSET[5]}
    WHERE MTH_TM_ID =
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


# MOR
def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH latest_mort_val AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY MORT_NUM
                       ORDER BY OBSN_DT DESC
                   ) AS rn
            FROM {UPSTREAM_ASSET[4]}
        )
        WHERE rn = 1
    ),
    mor_joined AS (
        SELECT
            mor.BASEL_ACCT_ID,
            f.INDEX_TERANETV,
            ROW_NUMBER() OVER (
                PARTITION BY mor.BASEL_ACCT_ID
                ORDER BY f.INDEX_TERANETV DESC NULLS LAST,
                         f.OBSN_DT DESC
            ) AS rn
        FROM {UPSTREAM_ASSET[0]} mor
        LEFT JOIN latest_mort_val f
            ON mor.MORT_NUM = f.MORT_NUM
        WHERE mor.MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        INDEX_TERANETV AS INDEXED_PRPTY_VAL_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
    FROM mor_joined
    WHERE rn = 1
    """
):
    pass


# TNG
def export_tng(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH tng_joined AS (
        SELECT
            dim.BASEL_ACCT_ID,
            prop.PROP_VAL_NEW,
            tng.MONTH_END_DT,
            ROW_NUMBER() OVER (
                PARTITION BY dim.BASEL_ACCT_ID
                ORDER BY prop.PROP_VAL_NEW DESC NULLS LAST,
                         tng.MONTH_END_DT DESC
            ) AS rn
        FROM {UPSTREAM_ASSET[1]} dim
        LEFT JOIN {UPSTREAM_ASSET[2]} tng
            ON dim.SRC_APP_CD = 'TNG-MOR'
           AND dim.SRC_SYS_DEL_F != 'Y'
           AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        LEFT JOIN {UPSTREAM_ASSET[3]} prop
            ON prop.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
           AND prop.OBSN_DT =
               '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE tng.MONTH_END_DT =
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        PROP_VAL_NEW AS INDEXED_PRPTY_VAL_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
    FROM tng_joined
    WHERE rn = 1
    """
):
    pass


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT =
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM =
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            INDEXED_PRPTY_VAL_AMT,
            STREAM
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_prpty_val_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_prpty_val_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_prpty_val_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_prpty_val_amt.export_tng", key="parquet") }}}}'
        ], union_by_name = true)
    )
    """
):
    pass