import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.WRITTEN_OUT_F",
    "features.MODEL_EXCL_F",
    "features.TREATMENT_F",
    "features.PIT_STATUS_STEP",
    "features.OS_BAL_AMT_V2",
    "features.TOTAL_BALANCE",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT",
    "features.STEP_SUB_PORT",
    "features.STEP_PRIM_CUST_ID",
]
DOWNSTREAM_ASSET = "features.STEP_DELQ_DAYS_AVG12M"

DEPENDENCIES = {
    "duckdb_clear_step_delq_days_avg12m": ["export_all"],
    "export_all": ["duckdb_derive_step_delq_days_avg12m"],
}


def duckdb_clear_step_delq_days_avg12m(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET} 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def export_all(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
    WITH

        /* ============================================================
        ### FIX #1: Build OBSN_DT (obs-month) flag slices / dedupe
        Why: flags are time-stamped by OBSN_DT and keyed by BASEL_ACCT_ID.
            ============================================================ */
        w_f AS (
        SELECT BASEL_ACCT_ID, MAX(TRIM(WRITTEN_OUT_F)) AS WRITTEN_OUT_F
        FROM features.WRITTEN_OUT_F
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        me_f AS (
        SELECT BASEL_ACCT_ID, MAX(TRIM(MODEL_EXCL_F)) AS MODEL_EXCL_F
        FROM features.MODEL_EXCL_F
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        tf_f AS (
        SELECT BASEL_ACCT_ID, MAX(TRIM(TREATMENT_F)) AS TREATMENT_F
        FROM features.TREATMENT_F
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        pit_f AS (
        SELECT BASEL_ACCT_ID, MAX(TRIM(PIT_STATUS_STEP)) AS PIT_STATUS_STEP
        FROM features.PIT_STATUS_STEP
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        os_f AS (
        SELECT BASEL_ACCT_ID, MAX(OS_BAL_AMT_V2) AS OS_BAL_AMT_V2
        FROM features.OS_BAL_AMT_V2
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        tot_f AS (
        SELECT BASEL_ACCT_ID, MAX(TOTAL_BALANCE) AS TOTAL_BALANCE
        FROM features.TOTAL_BALANCE
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        cr_f AS (
        SELECT BASEL_ACCT_ID, MAX(CR_LMT_AMT) AS CR_LMT_AMT
        FROM features.CR_LMT_AMT
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        nb_f AS (
        SELECT BASEL_ACCT_ID, MAX(TOT_NEW_BAL_AMT) AS TOT_NEW_BAL_AMT
        FROM features.TOT_NEW_BAL_AMT
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        ssp_f AS (
        SELECT BASEL_ACCT_ID,
                MAX(TRIM(STEP_SUB_PORT)) AS STEP_SUB_PORT
        FROM features.STEP_SUB_PORT
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        GROUP BY BASEL_ACCT_ID
        ),
        spci AS (
        SELECT TRY_CAST(STEP_PLN_AGRMNT_NUM AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
                STEP_PRIM_CUST_ID
        FROM features.STEP_PRIM_CUST_ID
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        /* ============================================================
        ### FIX #2: Pre-filter each source snapshot ONCE (this STEP + 12M window)
        ============================================================ */
        base_spl AS (
        SELECT
            a.BASEL_ACCT_ID,
            TRY_CAST(NULLIF(TRIM(a.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            a.LOAN_NUM,
            TRY_CAST(a.PRIM_BASEL_CUST_ID AS BIGINT) AS PRIM_BASEL_CUST_ID,
            a.MTH_TM_ID,
            CAST(a.DAY_ODUE AS DOUBLE) AS DAY_ODUE,
            a.TOT_CRNT_BAL_AMT
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
        
        WHERE a.MTH_TM_ID BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 440 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND a.PRIM_BASEL_CUST_ID <> -1
            AND a.TOT_CRNT_BAL_AMT > 0
        ),
        base_mor AS (
        SELECT
            m.BASEL_ACCT_ID,
            m.MORT_NUM,
            TRY_CAST(NULLIF(TRIM(m.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            TRY_CAST(m.PRIM_BASEL_CUST_ID AS BIGINT) AS PRIM_BASEL_CUST_ID,
            m.MTH_TM_ID,
            CAST(m.DLQNT_DAY AS DOUBLE) AS DLQNT_DAY
        FROM ingestion.MORT_MTH_SNAPSHOT m
        
        WHERE m.MTH_TM_ID BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 440 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND UPPER(TRIM(m.COMM_TP)) = 'RESIDENTIAL'
            AND m.CRNT_BAL_AMT > 0
            AND TRIM(m.PD_OFF_F) = 'N'
        ),
        base_ks AS (
        SELECT
            k.BASEL_ACCT_ID,
            TRY_CAST(NULLIF(TRIM(k.STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            TRY_CAST(k.PRIM_BASEL_CUST_ID AS BIGINT) AS PRIM_BASEL_CUST_ID,
            k.MTH_TM_ID,
            CAST(k.BNS_DLQNT_DAY AS DOUBLE) AS BNS_DLQNT_DAY
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT k
        
        WHERE k.MTH_TM_ID BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 440 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND k.PRIM_BASEL_CUST_ID <> -1
        ),

        /* ============================================================
        ### FIX #3: Build ELIGIBLE ACCOUNTS at OBS MONTH ONLY (MTH_TM_ID = MTH_TM_OBS)
        Why: applying PIT/OS_BAL/etc across all 12 months will delete history.
        PIT is month-derived; flags are OBSN_DT based.
        ============================================================ */
        eligible_spl AS (
        SELECT DISTINCT b.STEP_PLN_AGRMNT_NUM, coalesce(spci.STEP_PRIM_CUST_ID, b.prim_basel_cust_id) as prim_basel_cust_id, b.BASEL_ACCT_ID, b.LOAN_NUM
        FROM base_spl b
        
        JOIN ssp_f ssp ON ssp.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND ssp.STEP_SUB_PORT='STEP_MIX'
        JOIN w_f  w  ON w.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND w.WRITTEN_OUT_F='N'
        JOIN me_f me ON me.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND me.MODEL_EXCL_F='N'
        JOIN tf_f tf ON tf.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND tf.TREATMENT_F='A'
        JOIN pit_f pit ON pit.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND pit.PIT_STATUS_STEP='CUR'
        JOIN os_f os ON os.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND os.OS_BAL_AMT_V2 > 0
        LEFT JOIN spci on spci.STEP_PLN_AGRMNT_NUM=b.STEP_PLN_AGRMNT_NUM
        WHERE b.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),
        eligible_mor AS (
        SELECT DISTINCT b.STEP_PLN_AGRMNT_NUM, coalesce(spci.STEP_PRIM_CUST_ID, b.prim_basel_cust_id) as prim_basel_cust_id, b.mort_num
        FROM base_mor b
        
        JOIN ssp_f ssp ON ssp.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND ssp.STEP_SUB_PORT='STEP_MIX'
        JOIN w_f  w  ON w.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND w.WRITTEN_OUT_F='N'
        JOIN me_f me ON me.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND me.MODEL_EXCL_F='N'
        JOIN tf_f tf ON tf.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND tf.TREATMENT_F='A'
        JOIN pit_f pit ON pit.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND pit.PIT_STATUS_STEP='CUR'
        JOIN tot_f tot ON tot.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND tot.TOTAL_BALANCE > 0
        LEFT JOIN spci on spci.STEP_PLN_AGRMNT_NUM=b.STEP_PLN_AGRMNT_NUM
        WHERE b.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}  and b.PRIM_BASEL_CUST_ID > 0
        ),
        eligible_ks AS (
        SELECT DISTINCT b.STEP_PLN_AGRMNT_NUM, coalesce(spci.STEP_PRIM_CUST_ID, b.prim_basel_cust_id) as prim_basel_cust_id, b.BASEL_ACCT_ID
        FROM base_ks b
        
        JOIN ssp_f ssp ON ssp.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND ssp.STEP_SUB_PORT='STEP_MIX'
        JOIN w_f  w  ON w.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND w.WRITTEN_OUT_F='N'
        JOIN me_f me ON me.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND me.MODEL_EXCL_F='N'
        JOIN tf_f tf ON tf.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND tf.TREATMENT_F='A'
        JOIN pit_f pit ON pit.BASEL_ACCT_ID=b.BASEL_ACCT_ID AND pit.PIT_STATUS_STEP='CUR'
        LEFT JOIN cr_f cr ON cr.BASEL_ACCT_ID=b.BASEL_ACCT_ID
        LEFT JOIN nb_f nb ON nb.BASEL_ACCT_ID=b.BASEL_ACCT_ID
        LEFT JOIN spci on spci.STEP_PLN_AGRMNT_NUM=b.STEP_PLN_AGRMNT_NUM
        WHERE b.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND (COALESCE(cr.CR_LMT_AMT,0) > 0 OR COALESCE(nb.TOT_NEW_BAL_AMT,0) > 0)
        ),

        /* ============================================================
        ### FIX #4: History pulls (12M) now only join to eligible_* by BASEL_ACCT_ID
        IMPORTANT: no more "w.obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'" in the history WHERE.
        ============================================================ */
        spl_data_pull AS (
        SELECT
            e.STEP_PLN_AGRMNT_NUM,
            e.PRIM_BASEL_CUST_ID,
            {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} AS MTH_TM_OBS,
            b.MTH_TM_ID,
            b.DAY_ODUE
        FROM eligible_spl e
        JOIN base_spl b ON b.LOAN_NUM=e.LOAN_NUM and b.basel_acct_id=e.basel_acct_id
        ),
        SPL_ACCT_LEVEL_AGG AS (
        SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID,
                MAX(DAY_ODUE) AS HGST_DAY_ODUE_SPL
        FROM spl_data_pull
        GROUP BY STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID
        ),

        mor_data_pull AS (
        SELECT
            e.STEP_PLN_AGRMNT_NUM,
            e.PRIM_BASEL_CUST_ID,
            {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} AS MTH_TM_OBS,
            b.MTH_TM_ID,
            b.DLQNT_DAY
        FROM eligible_mor e
        JOIN base_mor b ON b.mort_num=e.mort_num
        ),
        MOR_ACCT_TO_CUST AS (
        SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID,
                MAX(DLQNT_DAY) AS HGST_DLQNT_DAY_MTG
        FROM mor_data_pull
        GROUP BY STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID
        ),

        ks_data_pull AS (
        SELECT
            e.STEP_PLN_AGRMNT_NUM,
            e.PRIM_BASEL_CUST_ID,
            {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} AS MTH_TM_OBS,
            b.MTH_TM_ID,
            b.BNS_DLQNT_DAY
        FROM eligible_ks e
        JOIN base_ks b ON b.basel_acct_id=e.basel_acct_id
        ),
        KS_STEP_AGG AS (
        SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID,
                MAX(BNS_DLQNT_DAY) AS STEP_BNS_DLQNT_DAY_GP_KSA
        FROM ks_data_pull
        GROUP BY STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID
        ),

        STEP_PD_AGG_POP AS (
        SELECT DISTINCT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID
        FROM (
            SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID FROM SPL_ACCT_LEVEL_AGG
            UNION
            SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID FROM MOR_ACCT_TO_CUST
            UNION
            SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS, MTH_TM_ID FROM KS_STEP_AGG
        )
        ),

        FULL_STEP_AGG2 AS (
        SELECT
            a.STEP_PLN_AGRMNT_NUM,
            a.PRIM_BASEL_CUST_ID,
            a.MTH_TM_OBS,
            a.MTH_TM_ID,
            GREATEST(
            COALESCE(m1.HGST_DLQNT_DAY_MTG, 0),
            COALESCE(m2.STEP_BNS_DLQNT_DAY_GP_KSA, 0),
            COALESCE(z.HGST_DAY_ODUE_SPL, 0)
            ) AS STEP_DELQ_DAYS
        FROM STEP_PD_AGG_POP a
        LEFT JOIN SPL_ACCT_LEVEL_AGG z
            ON a.STEP_PLN_AGRMNT_NUM=z.STEP_PLN_AGRMNT_NUM and a.PRIM_BASEL_CUST_ID=z.PRIM_BASEL_CUST_ID AND a.MTH_TM_ID=z.MTH_TM_ID
        LEFT JOIN MOR_ACCT_TO_CUST m1
            ON a.STEP_PLN_AGRMNT_NUM=m1.STEP_PLN_AGRMNT_NUM and a.PRIM_BASEL_CUST_ID=m1.PRIM_BASEL_CUST_ID AND a.MTH_TM_ID=m1.MTH_TM_ID
        LEFT JOIN KS_STEP_AGG m2
            ON a.STEP_PLN_AGRMNT_NUM=m2.STEP_PLN_AGRMNT_NUM and a.PRIM_BASEL_CUST_ID=m2.PRIM_BASEL_CUST_ID AND a.MTH_TM_ID=m2.MTH_TM_ID
        ),

        FULL_AGG_12M AS (
        SELECT
            STEP_PLN_AGRMNT_NUM,
            PRIM_BASEL_CUST_ID,
            MTH_TM_OBS,
            AVG(STEP_DELQ_DAYS) AS STEP_DELQ_DAYSavg12M
        FROM FULL_STEP_AGG2
        WHERE MTH_TM_ID BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 440 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        GROUP BY STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, MTH_TM_OBS
        ),

        AVG_DELQ_DAYS AS(
        SELECT
        a.STEP_PLN_AGRMNT_NUM,
        a.PRIM_BASEL_CUST_ID,
        a.MTH_TM_OBS,
        a.STEP_DELQ_DAYSavg12M,
        FROM FULL_AGG_12M a
        ), 

        FINAL AS (
        SELECT 
        STEP_PLN_AGRMNT_NUM,
        MAX(STEP_DELQ_DAYSavg12M) AS STEP_DELQ_DAYS_AVG12M
        FROM AVG_DELQ_DAYS
        GROUP BY STEP_PLN_AGRMNT_NUM
        )

        SELECT
        STEP_PLN_AGRMNT_NUM,
        STEP_DELQ_DAYS_AVG12M
        FROM FINAL
            """,
):
    pass


def duckdb_derive_step_delq_days_avg12m(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
                STEP_PLN_AGRMNT_NUM,
                STEP_DELQ_DAYS_AVG12M
            FROM 
                '{{{{ task_instance.xcom_pull(task_ids="derived__step_delq_days_avg12m.export_all", key="parquet") }}}}'
        )
    """,
):
    pass
