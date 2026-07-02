from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

"""
TODO: Note for both LTV_TERANET_CMA and LTV_TERANET
Consider refactoring this DV into

prop_val feature first order
index_lookup feature first order

prop_val_new second order
LTV_teranet_cma second order

Currently these second orders are being split into two separate tables from this one DV.
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "ingestion.TM_DIM",
    "ingestion.TERANET_ADDR_LKP_CMA",
    "ingestion.TERANET_HOUSE_PRC_INDEX_CMA",
    "ingestion.PROVNCL_HOUSE_INDEX_SUM_CMA",
]
DOWNSTREAM_ASSET = ["features.LTV_TERANET_CMA", "features.PROP_VAL_NEW_CMA"]
DEPENDENCIES = {
    "export_ltv": ["duckdb_delete", "duckdb_delete_prop_val_new_cma"],
    "duckdb_delete": ["duckdb_load"],
    "duckdb_delete_prop_val_new_cma": ["duckdb_load_prop_val_new_cma"]
}


def export_ltv(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
WITH
params AS (
    SELECT
        tm_id mth_tm_id,
        tm_lvl_end_dt month_end_dt
    FROM { UPSTREAM_ASSET[2] }
    WHERE
        tm_lvl = 'Month'
        AND tm_id <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
)
, teranet_lkp AS (
    SELECT
        prpty_loctn_nm
        , trim(translate(lower(prpty_loctn_nm),'àâäçèéêëîïôùûüÿ,-''/','aaaceeeeiiouuuy    ')) prpty_loctn_nm2
        , loctn_label_1 prov
        , loctn_label_2 cma
    FROM { UPSTREAM_ASSET[3] }
    WHERE
        eff_from_yr_mth <= strftime(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', '%Y%m')
        AND eff_to_yr_mth >= strftime(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', '%Y%m')
)
, teranet_idx AS (
    SELECT
        a.mth_tm_id
        , a.month_end_dt
        , b.label_1 prov
        , b.label_2 cma
        , LAST_VALUE(b."index" IGNORE NULLS) OVER (PARTITION BY b.label_1, b.label_2 ORDER BY a.mth_tm_id DESC ROWS UNBOUNDED PRECEDING) as "INDEX"
        , LAST_VALUE(NULLIF(c.house_index_rto, 0) IGNORE NULLS) OVER (PARTITION BY b.label_1, b.label_2 ORDER BY a.mth_tm_id DESC ROWS UNBOUNDED PRECEDING) AS PROVNCL_INDEX
    FROM params a
    LEFT OUTER JOIN { UPSTREAM_ASSET[4] } b ON
        a.mth_tm_id = b.mth_tm_id
    LEFT OUTER JOIN { UPSTREAM_ASSET[5] } c ON
        a.mth_tm_id = c.mth_tm_id
        AND b.label_1 = (CASE WHEN c.prov_cd='CO' THEN 'COMPOSITE' ELSE c.prov_cd END)
)
, snapshot AS (
    SELECT
        month_end_dt,
        trim(account_id) AS account_id,
        end_principal_balance,
        CASE
            WHEN PROP_PURCHASE_DT IS NOT NULL AND PROP_PURCHASE_AMT IS NOT NULL THEN PROP_PURCHASE_AMT
            WHEN ORIG_PROP_APPRAISAL_DT IS NOT NULL AND ORIG_PROP_APPRAISAL_VAL IS NOT NULL THEN ORIG_PROP_APPRAISAL_VAL
            WHEN LST_PROP_APPRAISAL_DT IS NOT NULL AND LST_PROP_APPRAISAL_VAL IS NOT NULL THEN LST_PROP_APPRAISAL_VAL
            WHEN ORIG_PROP_APPRAISAL_VAL IS NOT NULL AND ORIG_PROP_APPRAISAL_DT IS NULL THEN ORIG_PROP_APPRAISAL_VAL
            WHEN PROP_PURCHASE_AMT IS NOT NULL AND PROP_PURCHASE_DT IS NULL THEN PROP_PURCHASE_AMT
            WHEN LST_PROP_APPRAISAL_VAL IS NOT NULL AND LST_PROP_APPRAISAL_DT IS NULL THEN LST_PROP_APPRAISAL_VAL
        END AS prop_val,
        CASE
            WHEN PROP_PURCHASE_DT IS NOT NULL AND PROP_PURCHASE_AMT IS NOT NULL THEN LAST_DAY(PROP_PURCHASE_DT)
            WHEN ORIG_PROP_APPRAISAL_DT IS NOT NULL AND ORIG_PROP_APPRAISAL_VAL IS NOT NULL THEN LAST_DAY(ORIG_PROP_APPRAISAL_DT)
            WHEN LST_PROP_APPRAISAL_DT IS NOT NULL AND LST_PROP_APPRAISAL_VAL IS NOT NULL THEN LAST_DAY(LST_PROP_APPRAISAL_DT)
            WHEN ORIG_PROP_APPRAISAL_VAL IS NOT NULL AND ORIG_PROP_APPRAISAL_DT IS NULL THEN LAST_DAY(OPEN_DT)
            WHEN PROP_PURCHASE_AMT IS NOT NULL AND PROP_PURCHASE_DT IS NULL THEN LAST_DAY(OPEN_DT)
            WHEN LST_PROP_APPRAISAL_VAL IS NOT NULL AND LST_PROP_APPRAISAL_DT IS NULL THEN LAST_DAY(OPEN_DT)
        END AS prop_val_date,
        fsa,
        trim(translate(lower(fsa),'àâäçèéêëîïôùûüÿ,-''/','aaaceeeeiiouuuy    ')) fsa_lkp,
        prop_city,
        trim(translate(lower(prop_city),'àâäçèéêëîïôùûüÿ,-''/','aaaceeeeiiouuuy    ')) city_lkp,
        prop_province_code,
        CASE
            WHEN prop_province_code IS NULL OR prop_province_code='' OR prop_province_code='?' THEN
                CASE
                    WHEN substr(FSA,1,1)='A' THEN 'NL'
                    WHEN substr(FSA,1,1)='B' THEN 'NS'
                    WHEN substr(FSA,1,1)='C' THEN 'PE'
                    WHEN substr(FSA,1,1)='E' THEN 'NB'
                    WHEN substr(FSA,1,1)='G' THEN 'QC'
                    WHEN substr(FSA,1,1)='H' THEN 'QC'
                    WHEN substr(FSA,1,1)='J' THEN 'QC'
                    WHEN substr(FSA,1,1)='R' THEN 'MB'
                    WHEN substr(FSA,1,1)='S' THEN 'SK'
                    WHEN substr(FSA,1,1)='T' THEN 'AB'
                    WHEN substr(FSA,1,1)='V' THEN 'BC'
                    WHEN substr(FSA,1,1)='X' THEN 'NT'
                    WHEN substr(FSA,1,1)='Y' THEN 'YT'
                    WHEN TRIM(account_id)='MBS~370528' THEN 'MB'
                    WHEN TRIM(account_id)='MBS~370579' THEN 'BC'
                    ELSE 'ON'
                END
            ELSE prop_province_code
        END AS province_lkp
    FROM { UPSTREAM_ASSET[1] }
    WHERE
        month_end_dt = (SELECT max(month_end_dt) FROM params)
)
, snapshot_cma1 AS (
    SELECT
        s.*
        , COALESCE(t1.cma, t2.cma) as cma
        , COALESCE(t1.prpty_loctn_nm, t2.prpty_loctn_nm) as property
    FROM snapshot s
    LEFT OUTER JOIN teranet_lkp t1 ON
        s.fsa_lkp = t1.prpty_loctn_nm2
    LEFT OUTER JOIN teranet_lkp t2 ON
        s.city_lkp = t1.prpty_loctn_nm2
)
, snapshot_cma2 AS (
    SELECT
        a.*,
        COALESCE(b1.INDEX, b2.INDEX) AS monthly_HPI,
        COALESCE(c1.INDEX, c2.INDEX) AS appraisal_HPI,
    FROM snapshot_cma1 a
    LEFT OUTER JOIN teranet_idx b1 ON
        a.month_end_dt = b1.month_end_dt
        AND UPPER(a.province_lkp) = UPPER(b1.prov)
        AND UPPER(a.cma) = UPPER(b1.cma)
    LEFT OUTER JOIN teranet_idx b2 ON
        a.month_end_dt = b2.month_end_dt
        AND UPPER(b2.prov) = 'COMPOSITE'
        AND UPPER(b2.cma) = '11'
    LEFT OUTER JOIN teranet_idx c1 ON
        a.prop_val_date = c1.month_end_dt
        AND UPPER(a.province_lkp) = UPPER(c1.prov)
        AND UPPER(a.cma) = UPPER(c1.cma)
    LEFT OUTER JOIN teranet_idx c2 ON
        a.prop_val_date = c2.month_end_dt
        AND UPPER(c2.prov) = 'COMPOSITE'
        AND UPPER(c2.cma) = '11'
)
, LTV_teranet AS (
    SELECT
        *,
        prop_val*(monthly_HPI/appraisal_HPI) AS prop_val_new,
        end_principal_balance/(prop_val*(monthly_HPI/appraisal_HPI)) AS LTV_teranet_cma
    FROM snapshot_cma2
)
select * from LTV_teranet
    """,
):
    pass

def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from { DOWNSTREAM_ASSET[0] } where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_delete_prop_val_new_cma(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from { DOWNSTREAM_ASSET[1] } where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET[0] } by name
    FROM (
        select
            a.month_end_dt obsn_dt,
            b.basel_acct_id,
            trim(a.account_id) account_id,
            a.LTV_teranet_cma
        from read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__ltv_teranet_cma.export_ltv", key="parquet") }}}}') a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
    )
    """,
):
    pass

def duckdb_load_prop_val_new_cma(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET[1] } by name
    FROM (
        select
            a.month_end_dt obsn_dt,
            b.basel_acct_id,
            trim(a.account_id) account_id,
            a.prop_val_new
        from read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__ltv_teranet_cma.export_ltv", key="parquet") }}}}') a
        inner join { UPSTREAM_ASSET[0] } b on
            b.src_app_cd ='TNG-MOR'
            and b.src_sys_del_f != 'Y'
            and trim(a.account_id) = trim(b.src_app_id)
    )
    """,  
): 
    pass


