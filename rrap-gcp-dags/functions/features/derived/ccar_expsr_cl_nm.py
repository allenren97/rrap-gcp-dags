import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',  # 0
    'ingestion.MORT_MTH_SNAPSHOT',              # 1
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',   # 2
    'ingestion.TNG_ACCT_MO',                    # 3
    'ingestion.BASEL_ACCT_DIM',                 # 4   
    'reference.BASEL_EXPSR_CL_DIM_NZ',          # 5
    'features.CONSM_PRD_TREATMNT_CD',           # 6
    'features.SML_BUS_F',                       # 7
    'features.PIT_STATUS_ACCOUNT_ORIG',         # 8
    'features.TRNST_EXCLSN_F',                  # 9
    'features.NCR_EXPSR_CL_KEY_VAL',            # 10
    'features.TREATMENT_F',                     # 11
]

DOWNSTREAM_ASSET = 'features.CCAR_EXPSR_CL_NM'

DEPENDENCIES = {
    'duckdb_clear_ccar_expsr_cl_nm': ['export_ks', 'export_mor', 'export_spl', 'export_tng'],
    'export_ks': ['duckdb_derive_ccar_expsr_cl_nm'],
    'export_mor': ['duckdb_derive_ccar_expsr_cl_nm'],
    'export_spl': ['duckdb_derive_ccar_expsr_cl_nm'],
    'export_tng': ['duckdb_derive_ccar_expsr_cl_nm'],
}


def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        with revl as (
            SELECT * FROM {UPSTREAM_ASSET[0]} WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        consm as (
            SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        sml_bus_f as (
            SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        pit as (
            SELECT * FROM {UPSTREAM_ASSET[8]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        trnst as (
            SELECT * FROM {UPSTREAM_ASSET[9]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        expsr_cl as (
            SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        SELECT 
            revl.BASEL_ACCT_ID,
            bec.CCAR_EXPSR_CL_NM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'KS' as SRC_SYS_CD
        FROM revl
        LEFT JOIN consm ON revl.BASEL_ACCT_ID = consm.BASEL_ACCT_ID AND TRIM(UPPER(consm.CONSM_PRD_TREATMNT_CD)) = 'A'
        LEFT JOIN sml_bus_f ON revl.BASEL_ACCT_ID = sml_bus_f.BASEL_ACCT_ID AND TRIM(UPPER(sml_bus_f.SML_BUS_F)) = 'N'
        LEFT JOIN pit ON revl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND TRIM(UPPER(pit.PIT_STATUS_ACCOUNT_ORIG)) IN ('CUR', 'DEF')
        LEFT JOIN trnst ON revl.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID AND TRIM(UPPER(trnst.TRNST_EXCLSN_F)) = 'N'
        LEFT JOIN expsr_cl ON revl.BASEL_ACCT_ID = expsr_cl.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[5]} bec  -- reference.BASEL_EXPSR_CL_DIM_NZ
        ON '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN bec.EFF_FROM_YR_MTH AND bec.EFF_TO_YR_MTH
        AND TRIM(expsr_cl.NCR_EXPSR_CL_KEY_VAL) = TRIM(bec.NCR_EXPSR_CL_KEY_VAL)
    """
):
    pass


def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        with mor as (
            SELECT * FROM {UPSTREAM_ASSET[1]} WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        expsr_cl as (
            SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        SELECT 
            mor.BASEL_ACCT_ID,
            bec.CCAR_EXPSR_CL_NM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'MOR' as SRC_SYS_CD
        FROM mor
        LEFT JOIN expsr_cl ON mor.BASEL_ACCT_ID = expsr_cl.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[5]} bec -- reference.BASEL_EXPSR_CL_DIM_NZ
        ON '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN bec.EFF_FROM_YR_MTH AND bec.EFF_TO_YR_MTH
        AND TRIM(expsr_cl.NCR_EXPSR_CL_KEY_VAL) = TRIM(bec.NCR_EXPSR_CL_KEY_VAL)
    """
):
    pass


def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        with psnl as (
            SELECT * FROM {UPSTREAM_ASSET[2]} WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        expsr_cl as (
            SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        pit as (
            SELECT * FROM {UPSTREAM_ASSET[8]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        trnst as (
            SELECT * FROM {UPSTREAM_ASSET[9]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        treatment_f as (
            SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        SELECT 
            psnl.BASEL_ACCT_ID,
            bec.CCAR_EXPSR_CL_NM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'SPL' as SRC_SYS_CD
        FROM psnl
        LEFT JOIN pit ON psnl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND TRIM(UPPER(pit.PIT_STATUS_ACCOUNT_ORIG)) IN ('CUR', 'DEF')
        LEFT JOIN trnst ON psnl.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID AND TRIM(UPPER(trnst.TRNST_EXCLSN_F)) = 'N'
        LEFT JOIN treatment_f ON psnl.BASEL_ACCT_ID = treatment_f.BASEL_ACCT_ID AND TRIM(UPPER(treatment_f.TREATMENT_F)) = 'A'
        LEFT JOIN expsr_cl ON psnl.BASEL_ACCT_ID = expsr_cl.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[5]} bec -- reference.BASEL_EXPSR_CL_DIM_NZ
        ON '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN bec.EFF_FROM_YR_MTH AND bec.EFF_TO_YR_MTH
        AND TRIM(expsr_cl.NCR_EXPSR_CL_KEY_VAL) = TRIM(bec.NCR_EXPSR_CL_KEY_VAL)
"""
):
    pass


def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        with tng as (
            SELECT * FROM {UPSTREAM_ASSET[3]} WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ),
        expsr_cl as (
            SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        SELECT
            dim.BASEL_ACCT_ID,
             bec.CCAR_EXPSR_CL_NM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'TNG-MOR' as SRC_SYS_CD
        FROM tng
        INNER JOIN {UPSTREAM_ASSET[4]} dim -- ingestion.BASEL_ACCT_DIM
        ON dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        LEFT JOIN expsr_cl ON dim.BASEL_ACCT_ID = expsr_cl.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[5]} bec -- reference.BASEL_EXPSR_CL_DIM_NZ
        ON '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN bec.EFF_FROM_YR_MTH AND bec.EFF_TO_YR_MTH
        AND TRIM(expsr_cl.NCR_EXPSR_CL_KEY_VAL) = TRIM(bec.NCR_EXPSR_CL_KEY_VAL)
    """
):
    pass


def duckdb_clear_ccar_expsr_cl_nm(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET }
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_ccar_expsr_cl_nm(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT *
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccar_expsr_cl_nm.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccar_expsr_cl_nm.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccar_expsr_cl_nm.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccar_expsr_cl_nm.export_tng", key="parquet") }}}}'
        ], union_by_name = true)   
    )
    """
):
    pass
