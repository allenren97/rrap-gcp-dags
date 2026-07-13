import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT", # 0
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",  # 1
    "ingestion.MORT_MTH_SNAPSHOT",             # 2
    "ingestion.BASEL_ACCT_DIM",                # 3
    "ingestion.BASELAYER_MOR",                 # 4
    "ingestion.ORG_UNIT_DIM",                  # 5
    "ingestion.TNG_ACCT_MO",                   # 6
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",  # 7
    "features.TRNST_EXCLSN_F",                 # 8
    "features.CONSM_PRD_TREATMNT_CD",          # 9
    "features.SML_BUS_F",                      # 10
    "reference.PROVINCE_REF",                  # 11
    "reference.BASEL_NCR_GEO_DIM",             # 12
    "features.PRD_ID",                         # 13
    "features.TREATMENT_F",                    # 14
    "reference.PSNL_LOAN_RPTG_PRD_LKP"         # 15
]

DOWNSTREAM_ASSET = 'features.NCR_GEO_KEY_VAL'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_ncr_geo_key_val'],
    'export_spl': ['duckdb_clear_ncr_geo_key_val'],
    'export_mor': ['duckdb_clear_ncr_geo_key_val'],
    'export_tng': ['duckdb_clear_ncr_geo_key_val'],
    'duckdb_clear_ncr_geo_key_val': ['duckdb_derive_ncr_geo_key_val']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        WITH ks AS (
            SELECT TRNST_OU_ID, BASEL_ACCT_ID, MTH_TM_ID
            FROM {UPSTREAM_ASSET[0]}
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        geo AS (
            SELECT
                ks.BASEL_ACCT_ID,
                CASE
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='T' THEN '0302'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='V' THEN '0303'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='R' THEN '0304'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='E' THEN '0305'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='A' THEN '0306'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='X' THEN '0307'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='B' THEN '0308'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1) IN ('P','K','L','M','N') THEN '0310'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='C' THEN '0311'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1) IN ('J','G','H') THEN '0312'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='S' THEN '0313'
                    WHEN SUBSTR(ou.POSTAL_ZIP_CD,1,1)='Y' THEN '0314'
                    WHEN UPPER(ou.PROV_STATE)='ONTARIO' THEN '0310'
                    ELSE '0319'
                END AS NCR_GEO_KEY_VAL
            FROM ks
            LEFT JOIN {UPSTREAM_ASSET[5]} ou
                ON ks.TRNST_OU_ID = ou.ORG_UNIT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[9]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') cd
                ON ks.BASEL_ACCT_ID = cd.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') bus
                ON ks.BASEL_ACCT_ID = bus.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit
                ON ks.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[8]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') trn
                ON ks.BASEL_ACCT_ID = trn.BASEL_ACCT_ID
            WHERE trn.TRNST_EXCLSN_F='N'
            AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR','DEF')
            AND bus.SML_BUS_F='N'
            AND cd.CONSM_PRD_TREATMNT_CD='A'
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            geo.NCR_GEO_KEY_VAL
        FROM geo
        RIGHT JOIN ks
            ON ks.BASEL_ACCT_ID = geo.BASEL_ACCT_ID
"""
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        WITH spl AS (
            SELECT
                spl.BASEL_ACCT_ID,
                orig.POSTAL_ZIP_CD,
                orig.PROV_STATE,
                spl.MTH_TM_ID
            FROM {UPSTREAM_ASSET[1]} spl
            LEFT JOIN {UPSTREAM_ASSET[5]} orig
                ON spl.TRNST_NUM = orig.TRNST_NUM
            WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        geo AS (
            SELECT
                spl.BASEL_ACCT_ID,
                CASE
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='T' THEN '0302'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='V' THEN '0303'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='R' THEN '0304'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='E' THEN '0305'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='A' THEN '0306'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='X' THEN '0307'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='B' THEN '0308'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1) IN ('P','K','L','M','N') THEN '0310'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='C' THEN '0311'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1) IN ('J','G','H') THEN '0312'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='S' THEN '0313'
                    WHEN SUBSTR(spl.POSTAL_ZIP_CD,1,1)='Y' THEN '0314'
                    WHEN UPPER(spl.PROV_STATE)='ALBERTA' THEN '0302'
                    WHEN UPPER(spl.PROV_STATE)='BRITISH COLUMBIA' THEN '0303'
                    WHEN UPPER(spl.PROV_STATE)='MANITOBA' THEN '0304'
                    WHEN UPPER(spl.PROV_STATE)='NEW BRUNSWICK' THEN '0305'
                    WHEN UPPER(spl.PROV_STATE)='NEWFOUNDLAND' THEN '0306'
                    WHEN UPPER(spl.PROV_STATE)='NORTHWEST TERRITORIES' THEN '0307'
                    WHEN UPPER(spl.PROV_STATE)='NOVA SCOTIA' THEN '0308'
                    WHEN UPPER(spl.PROV_STATE)='NUNAVUT' THEN '0309'
                    WHEN UPPER(spl.PROV_STATE)='ONTARIO' THEN '0310'
                    WHEN UPPER(spl.PROV_STATE)='PRINCE EDWARD' THEN '0311'
                    WHEN UPPER(spl.PROV_STATE)='QUEBEC' THEN '0312'
                    WHEN UPPER(spl.PROV_STATE)='SASKATCHEWAN' THEN '0313'
                    WHEN UPPER(spl.PROV_STATE)='YUKON' THEN '0314'
                    ELSE ''
                END AS NCR_GEO_KEY_VAL
            FROM spl
            INNER JOIN {UPSTREAM_ASSET[8]} tef
                ON spl.BASEL_ACCT_ID = tef.BASEL_ACCT_ID
                AND tef.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND tef.TRNST_EXCLSN_F = 'N'
            INNER JOIN {UPSTREAM_ASSET[13]} prd
                ON spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
                AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            INNER JOIN {UPSTREAM_ASSET[14]} tf
                ON spl.BASEL_ACCT_ID = tf.BASEL_ACCT_ID
                AND tf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND tf.TREATMENT_F = 'A'
            INNER JOIN {UPSTREAM_ASSET[7]} pit
                ON spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
                AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('DEF','CUR')
            LEFT JOIN {UPSTREAM_ASSET[15]} lkp
                ON prd.PRD_ID = lkp.PRD_ID
                AND TRIM(lkp.SRC_SYS_CD) = 'SPL'
            WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD,
            geo.NCR_GEO_KEY_VAL
        FROM geo
        RIGHT JOIN spl
            ON spl.BASEL_ACCT_ID = geo.BASEL_ACCT_ID
        """   
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            TRIM(base.NCR_GEOGRAPHY) AS NCR_GEO_KEY_VAL
        FROM {UPSTREAM_ASSET[2]} mor
        INNER JOIN {UPSTREAM_ASSET[4]} base
            ON mor.MORT_NUM = base.MORT_NUM
            AND mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        WITH tng AS (
            SELECT *,
                CASE
                    WHEN (PROP_PROVINCE_CODE='' OR PROP_PROVINCE_CODE='?' OR PROP_PROVINCE_CODE IS NULL) THEN
                        CASE
                            WHEN SUBSTR(FSA,1,1)='A' THEN 'NL'
                            WHEN SUBSTR(FSA,1,1)='B' THEN 'NS'
                            WHEN SUBSTR(FSA,1,1)='C' THEN 'PE'
                            WHEN SUBSTR(FSA,1,1)='E' THEN 'NB'
                            WHEN SUBSTR(FSA,1,1) IN ('G','H','J') THEN 'QC'
                            WHEN SUBSTR(FSA,1,1)='R' THEN 'MB'
                            WHEN SUBSTR(FSA,1,1)='S' THEN 'SK'
                            WHEN SUBSTR(FSA,1,1)='T' THEN 'AB'
                            WHEN SUBSTR(FSA,1,1)='V' THEN 'BC'
                            WHEN SUBSTR(FSA,1,1)='X' THEN 'NT'
                            WHEN SUBSTR(FSA,1,1)='Y' THEN 'YT'
                            ELSE 'ON'
                        END
                    ELSE PROP_PROVINCE_CODE
                END AS PROP_PROVINCE_CD
            FROM {UPSTREAM_ASSET[6]}
        )
        SELECT
            '2026-07-31' AS OBSN_DT,
            'TNG-MOR' AS SRC_SYS_CD,
            dim.BASEL_ACCT_ID,
            TRIM(geo.NCR_GEO_KEY_VAL) AS NCR_GEO_KEY_VAL
        FROM tng
        INNER JOIN {UPSTREAM_ASSET[3]} dim
            ON dim.SRC_APP_CD='TNG-MOR'
            AND dim.SRC_SYS_DEL_F!='Y'
            AND TRIM(tng.ACCOUNT_ID)=TRIM(dim.SRC_APP_ID)
        LEFT JOIN {UPSTREAM_ASSET[11]} ref
            ON tng.PROP_PROVINCE_CD = ref.PROVINCE_CD
        LEFT JOIN {UPSTREAM_ASSET[12]} geo
            ON ref.PROVINCE_NM = geo.NCR_GEO_DESC
        WHERE MONTH_END_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        """
):
    pass

def duckdb_clear_ncr_geo_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_ncr_geo_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            NCR_GEO_KEY_VAL
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_geo_key_val.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_geo_key_val.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_geo_key_val.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_geo_key_val.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
