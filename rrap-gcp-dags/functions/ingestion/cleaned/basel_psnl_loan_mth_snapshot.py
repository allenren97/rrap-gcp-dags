import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT_PRE_SL', 'ingestion.RLP_TO_SL_ACCT_LIST' ]
DOWNSTREAM_ASSET = "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT"
DEPENDENCIES = {
    'export_rlp_to_sl_acct_list': ['export_basel_psnl_loan_mth_snapshot_pre_sl'],
    'export_basel_psnl_loan_mth_snapshot_pre_sl': ['duckdb_clear_basel_psnl_loan_mth_snapshot'],
    'duckdb_clear_basel_psnl_loan_mth_snapshot': ['duckdb_load_into_ducklake'],
    'duckdb_load_into_ducklake': ['duckdb_clear_sda_accounts'],
}


def export_basel_psnl_loan_mth_snapshot_pre_sl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT * FROM { UPSTREAM_ASSET[0] }
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def export_rlp_to_sl_acct_list(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""SELECT * FROM { UPSTREAM_ASSET[1] }"""
):
    pass


def duckdb_clear_basel_psnl_loan_mth_snapshot(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def duckdb_load_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    SELECT 
        a.* EXCLUDE(BASEL_ACCT_ID, TRNST_NUM, LOAN_NUM, MOTOR_VEHCL_VAL, LOAN_VAL_OTH, PRPS_CD, CRNT_BR_LOCTN_TRNST, SCRTY_CD, UPDT_PROCESS_TMSTMP),
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN CAST(b.ORIG_BASEL_ACCT_ID AS BIGINT) ELSE a.BASEL_ACCT_ID END) AS BASEL_ACCT_ID,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN SUBSTRING(TRIM(b.SL_LOAN_NO), 1, 5) ELSE a.TRNST_NUM END) AS TRNST_NUM,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN SUBSTRING(TRIM(b.SL_LOAN_NO), 6, 7) ELSE a.LOAN_NUM END) AS LOAN_NUM,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN b.MOTOR_VEHCL_VAL ELSE a.MOTOR_VEHCL_VAL END) AS MOTOR_VEHCL_VAL,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN b.LOAN_VAL_OTH ELSE a.LOAN_VAL_OTH END) AS LOAN_VAL_OTH,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN b.PRPS_CD ELSE a.PRPS_CD END) AS PRPS_CD,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN b.CRNT_BR_LOCTN_TRNST ELSE a.CRNT_BR_LOCTN_TRNST END) AS CRNT_BR_LOCTN_TRNST,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN b.SCRTY_CD ELSE a.SCRTY_CD END) AS SCRTY_CD,
        (CASE WHEN b.RLP_LOAN_NO IS NOT NULL AND a.CRNT_BR_LOCTN_TRNST <> 32730 THEN current_timestamp ELSE a.UPDT_PROCESS_TMSTMP END) AS UPDT_PROCESS_TMSTMP,
    FROM '{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_psnl_loan_mth_snapshot.export_basel_psnl_loan_mth_snapshot_pre_sl", key="parquet")}}}}' a
    LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_psnl_loan_mth_snapshot.export_rlp_to_sl_acct_list", key="parquet")}}}}' b
    ON LTRIM(a.TRNST_NUM || a.LOAN_NUM) = TRIM(b.RLP_LOAN_NO)
    AND a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass


def duckdb_clear_sda_accounts(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET }
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND CRNT_BR_LOCTN_TRNST = 32730
    """
):
    pass


