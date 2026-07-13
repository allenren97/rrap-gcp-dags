from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM",
                  "features.TRNST_NUM",
                  "reference.ORG_UNIT_HIERARCHY_DIM_NZ",
                  "ingestion.BASELAYER_MOR",
                  "reference.ORG_UNIT_HIERARCHY_DIM"]

DOWNSTREAM_ASSET = "features.RGNL_OFFC_CD"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_rgnl_offc_cd"],
    "export_spl": ["duckdb_delete_rgnl_offc_cd"],
    "export_mor": ["duckdb_delete_rgnl_offc_cd"],
    "export_tng": ["duckdb_delete_rgnl_offc_cd"],
    "duckdb_delete_rgnl_offc_cd": ["duckdb_load_rgnl_offc_cd"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    WITH dv_pop AS (
    SELECT
        ks.BASEL_ACCT_ID,
        rgnl.RGNL_OFFC_CD
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[5]} trnst ON
        ks.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
        AND trnst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[8]} rgnl ON
        TRIM(trnst.TRNST_NUM) = TRIM(rgnl.TRNST_NUM)
    WHERE
        ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' BETWEEN rgnl.EFF_FROM_TMSTMP AND rgnl.EFF_TO_TMSTMP
    GROUP BY
        ks.BASEL_ACCT_ID,
        rgnl.RGNL_OFFC_CD
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'KS' AS SRC_SYS_CD,
        ks.BASEL_ACCT_ID,
        RGNL_OFFC_CD
        FROM {UPSTREAM_ASSET[0]} ks
        LEFT JOIN dv_pop pop ON
            ks.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        rgnl.RGNL_OFFC_CD
    FROM {UPSTREAM_ASSET[1]} spl
    LEFT JOIN {UPSTREAM_ASSET[5]} trnst ON
        spl.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
        AND trnst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[6]} rgnl ON
        TRIM(trnst.TRNST_NUM) = TRIM(rgnl.TRNST_NUM)
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID,
        rgnl.RGNL_OFFC_CD
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        base.TNIF_REGION_CODE AS RGNL_OFFC_CD
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[7]} base ON
        mor.MORT_NUM = base.MORT_NUM
        AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        '0' AS RGNL_OFFC_CD
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_rgnl_offc_cd(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_rgnl_offc_cd(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                RGNL_OFFC_CD
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__rgnl_offc_cd.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__rgnl_offc_cd.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__rgnl_offc_cd.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__rgnl_offc_cd.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass