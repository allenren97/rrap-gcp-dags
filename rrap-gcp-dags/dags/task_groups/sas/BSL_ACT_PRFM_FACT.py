import pyarrow as pa
import os
import duckdb as ddb
import logging

from airflow.sdk import task_group
# from airflow.operators.python import PythonOperator

# from bns.rrap.operators.beeline import BeelineParquetExportOperator
# from bns.rrap.hooks.db2 import Db2ClpHook
# from bns.rrap.operators.db2 import Db2ParquetExportOperator
# from bns.rrap.operators.duckdb import DuckDbUpdateParquetOperator
# from bns.rrap.operators.db2 import DuplicateCheckOperator

from bns.rrap.operators.empty import PythonOperator
from bns.rrap.operators.empty import BeelineParquetExportOperator
from bns.rrap.operators.empty import Db2ClpHook
from bns.rrap.operators.empty import Db2ParquetExportOperator
from bns.rrap.operators.empty import DuckDbUpdateParquetOperator
from bns.rrap.operators.empty import DuplicateCheckOperator


def _clear_data_on_rerun(**context):
    """
    Delete records from BASEL_ACCT_PRFM_FACT for current month end run
    Enables reruns of this task_group in case of data issues
    """
    db2_hook = Db2ClpHook(db2_conn_id='db2-conn')
    # TODO: Parameterize schema if necessary down the line
    logging.warning(f"Removal from {context['params']['EDW_schema_EDRTLRP1D']}.BASEL_ACCT_PRFRM_FACT for current month end date.")
    db2_hook.exec_sql(f"DELETE FROM {context['params']['EDW_schema_EDRTLRP1D']}.BASEL_ACCT_PRFM_FACT WHERE MTH_END_DT = '{context['var']['value'].get('MTH_END_DT')}';")

    logging.warning(f"Removal from {context['params']['EDW_schema_EDRTLRP1D']}.BASEL_KS_ACCT_TRANSACTOR_ROLE for current month end date.")
    db2_hook.exec_sql(f"DELETE FROM {context['params']['EDW_schema_EDRTLRP1D']}.BASEL_KS_ACCT_TRANSACTOR_ROLE WHERE MTH_TM_ID = {context['var']['value'].get('MTH_TM_ID')};")

def _load_basel_acct_pfrm_fact(**context):
    """
    Load BASEL_ACCT_PRFM_FACT from parquet into IIAS
    """
    wd = context['var']['value'].get('RUNDIR')
    tsvf = os.path.join(wd, 'BASEL_ACCT_PFRM_FACT.tsv')

    cols = [
        'MTH_TM_ID',
        'MTH_END_DT',
        'SRC_SYS_CD',
        'BASEL_ACCT_ID',
        'ACCT_NUM',
        'PROC_TRANSIT',
        'SERV_TRANSIT',
        'PRD_CD',
        'CURRENCY_CD',
        'HELOC_IND',
        'LTV_BCKT_CD',
        'LTV_RTO',
        'LOAN_AMT_AT_INSURED_DATE',
        'RNTL_INCM_DPNDCY_F',
        'TRNSCTR_IND',
        'CURRENCY_MISMATCH_F',
        'TOT_EXPSR_ABOVE_1500K_LMT_F',
        'RNTL_INCM_AMT',
        'GRS_INCM_AMT',
        'PRCH_PRC_AMT',
        'APRSD_VAL_AMT',
        'PRPTY_VAL_AMT',
        'UNDRAWN_AMT',
        'CRNT_AUTH_LMT_AMT',
        'OCCUPANCY_TYPE_CD',
        'OS_BAL_AMT',
        'CRNT_LTV_RTO',
        'CLP_FLAG',
        'CRNT_PRPTY_VAL_AMT',
        'LNK_TO_STEP',
        'INSRT_PROCESS_TMSTMP',
        'UPDT_PROCESS_TMSTMP',
    ]

    # Convert parquet to tsv
    ddb.connect(":memory:")
    ddb.sql(f"""
    COPY (
        SELECT
            {",".join(cols)}
        FROM
            '{wd}/UPDATE_KS_BSL_ACCT_ID.parquet'
    ) TO '{tsvf}' (DELIMITER '\t', HEADER false, TIMESTAMP_FORMAT '%Y-%m-%d')
    """)

    db2hook = Db2ClpHook(db2_conn_id='db2-conn')
    # Load generated parquet into BASEL_ACCT_PRFM_FACT
    db2hook.from_tsv(
        tsvf,
        f"{ context['params']['EDW_schema_EDRTLRP1D'] }.BASEL_ACCT_PRFM_FACT",
        cols
    )

def _insert_basel_ks_acct_transactor_role(**context):
    """
    Once BSL_ACCT_PRFM_FACT table is loaded with recent data, insert into BASEL_KS_ACCT_TRANSACTOR_ROLE
    """

    db2hook = Db2ClpHook(db2_conn_id='db2-conn')

    # Use existing SQL statement to instert into from BASEL_REVLVNG_CR_MTH_SNAPSHOT
    db2hook.exec_sql(f"""
        INSERT INTO
        { context['params']['EDW_schema_EDRTLRP1D'] }.BASEL_KS_ACCT_TRANSACTOR_ROLE
        SELECT
            DISTINCT SNAP.MTH_TM_ID,
            SNAP.BASEL_ACCT_ID,
            CASE WHEN MAX(0, SNAP.BNS_DLQNT_DAY -30) > 0 THEN 'D' WHEN FACT.TRNSCTR_IND = 'T' THEN 'T' WHEN FACT.TRNSCTR_IND = 'N' THEN 'R' ELSE '' END AS ROLE_IND,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM
            { context['params']['EDW_schema_EDRTLRP1D'] }.BASEL_REVLVNG_CR_MTH_SNAPSHOT SNAP
            LEFT JOIN (
                SELECT
                    DISTINCT tm_id AS mth_tm_id,
                    LPAD(a.ACCT_NUM, 23, '0') AS acct_num,
                    TRNSCTR_IND
                FROM
                    { context['params']['EDW_schema_EDRTLRP1D'] }.BASEL_ACCT_PRFM_FACT a,
                    { context['params']['EDW_schema_EDRTLRP1D'] }.TM_DIM b
                WHERE
                    a.mth_end_dt = b.TM_LVL_END_DT
                    AND tm_lvl = 'Month'
                    AND tm_id = { context['var']['value'].get('MTH_TM_ID') }
                    AND src_sys_cd = 'KQ'
            ) FACT ON SNAP.MTH_TM_ID = FACT.MTH_TM_ID
            AND SNAP.ACCT_NUM = FACT.ACCT_NUM
        WHERE
            SNAP.MTH_TM_ID = { context['var']['value'].get('MTH_TM_ID') };
    """)


@task_group(group_id='BSL_ACT_PRFM_FACT')
def BSL_ACT_PRFM_FACT():
    """
    Airflow/Python replacement for BSL_ACT_PRFM_FACT job in SAS due to monthly failures stemming
    from EDL/EDL-R connection failing consistently and being unable to extract data.
    """

    # Ensure idempotence, delete rows for current MTH_END_DT before reloading
    CLEAR_DATA_ON_RERUN = PythonOperator(
        task_id='CLEAR_DATA_ON_RERUN',
        python_callable=_clear_data_on_rerun,
    )

    # 1. Load source data from EDL-R into parquet
    EXTRACT_BASEL_ACCT_PRFM_FACT = BeelineParquetExportOperator(
        task_id='EXTRACT_BASEL_ACCT_PRFM_FACT',
        beeline_conn_id='edlr-conn',
        target='BASEL_ACCT_PRFM_FACT.parquet',
        sql="""
            SELECT
                CAST(null as decimal(11,0)) mth_tm_id,
                a.mth_end_dt,
                CAST(CASE WHEN a.src_sys_cd = 'KQ_TSYS' THEN 'KQ' ELSE a.src_sys_cd end as varchar(30)) src_sys_cd,
                CAST(null as decimal(20,0)) basel_acct_id,
                CAST(COALESCE(c.bcm_acct_num, a.acct_num) as varchar(80)) acct_num,
                CAST(a.proc_transit as varchar(5)) proc_transit,
                CAST(a.serv_transit as varchar(5)) serv_transit,
                CAST(a.prd_cd as varchar(6)) prd_cd,
                CAST(a.currency_cd as varchar(3)) currency_cd,
                CAST(a.heloc_ind as varchar(1)) heloc_ind,
                CAST(a.bsl_ltv_bckt_cd as varchar(30)) ltv_bckt_cd ,
                CAST(a.bsl_ltv_rto as decimal(15,8)) ltv_rto,
                CAST(a.loan_amt_at_insured_date as decimal(22,2))  loan_amt_at_insured_date ,
                CAST(a.bsl_rntl_incm_dpndcy_f as varchar(1)) rntl_incm_dpndcy_f,
                CAST(a.bsl_trnsctr_ind as varchar(1)) trnsctr_ind,
                CAST(a.bsl_currency_mismatch_f as varchar(1)) currency_mismatch_f,
                CAST(a.bsl_tot_expsr_above_1500k_lmt_f as varchar(1)) tot_expsr_above_1500k_lmt_f,
                CAST(a.bsl_rntl_incm_amt as decimal(22,2)) rntl_incm_amt,
                CAST(a.bsl_grs_incm_amt as decimal(22,2)) grs_incm_amt,
                CAST(a.bsl_prch_prc_amt as decimal(22,2)) prch_prc_amt,
                CAST(a.bsl_aprsd_val_amt as decimal(22,2)) aprsd_val_amt,
                CAST(a.bsl_prpty_val_amt as decimal(22,2)) prpty_val_amt,
                CAST(a.bsl_undrawn_amt as decimal(22,2)) undrawn_amt,
                CAST(a.crnt_auth_lmt_amt as decimal(22,2)) crnt_auth_lmt_amt,
                CAST(a.occupancy_type_cd as varchar(1)) occupancy_type_cd,
                CAST(a.os_bal_amt as decimal(22,2)) os_bal_amt,
                CAST(a.crnt_ltv_rto as decimal(15,8) ) crnt_ltv_rto,
                CAST(a.clp_flag as char(1)) clp_flag,
                CAST(b.CRNT_PRPTY_VAL_AMT as decimal(22,2))  CRNT_PRPTY_VAL_AMT,
                CAST(b.lnk_to_step as char(1)) LNK_TO_STEP,
                date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS INSRT_PROCESS_TMSTMP,
                date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS UPDT_PROCESS_TMSTMP
            FROM 
                prod_rcrr1.BASEL_ACCT_PRFM_FACT a
            LEFT JOIN prod_rcrr1.acct_prfm_fact b
            ON a.mth_end_dt=b.mth_end_dt 
            AND a.src_sys_cd=b.src_sys_cd 
            AND a.acct_num=b.acct_num
            LEFT JOIN (
                SELECT 
                    distinct bcm_acct_num, tsys_acct_id
                FROM
                    tsz.kq_tkq_ks_tsys_xref
                WHERE businesseffectivedate IN ('2024-11-09', '2025-08-16')
                AND end_of_chain_indicator = 'Y'
            ) AS c 
            ON c.tsys_acct_id = a.acct_num
            WHERE a.src_sys_cd in ('KQ', 'GZ', 'SL', 'TNG_MTG', 'TNG_MCAP', 'KQ_TSYS')
            AND a.MTH_END_DT= '{{ var.value.MTH_END_DT }}'
        """,
        schema=pa.schema([
            ('mth_tm_id', pa.decimal128(11, 0)),
            ('mth_end_dt', pa.date32()),
            ('src_sys_cd', pa.string()),
            ('basel_acct_id', pa.decimal128(20, 0)),
            ('acct_num', pa.string()),
            ('proc_transit', pa.string()),
            ('serv_transit', pa.string()),
            ('prd_cd', pa.string()),
            ('currency_cd', pa.string()),
            ('heloc_ind', pa.string()),
            ('ltv_bckt_cd', pa.string()),
            ('ltv_rto', pa.decimal128(15, 8)),
            ('loan_amt_at_insured_date', pa.decimal128(22, 2)),
            ('rntl_incm_dpndcy_f', pa.string()),
            ('trnsctr_ind', pa.string()),
            ('currency_mismatch_f', pa.string()),
            ('tot_expsr_above_1500k_lmt_f', pa.string()),
            ('rntl_incm_amt', pa.decimal128(22, 2)),
            ('grs_incm_amt', pa.decimal128(22, 2)),
            ('prch_prc_amt', pa.decimal128(22, 2)),
            ('aprsd_val_amt', pa.decimal128(22, 2)),
            ('prpty_val_amt', pa.decimal128(22, 2)),
            ('undrawn_amt', pa.decimal128(22, 2)),
            ('crnt_auth_lmt_amt', pa.decimal128(22, 2)),
            ('occupancy_type_cd', pa.string()),
            ('os_bal_amt', pa.decimal128(22, 2)),
            ('crnt_ltv_rto', pa.decimal128(15, 8)),
            ('clp_flag', pa.string()),
            ('CRNT_PRPTY_VAL_AMT', pa.decimal128(22, 2)),
            ('LNK_TO_STEP', pa.string()),
            ('INSRT_PROCESS_TMSTMP', pa.timestamp('us')),
            ('UPDT_PROCESS_TMSTMP', pa.timestamp('us'))
        ])
    )

    # 2. Set MTH_TM_ID column to XCom value
    UPDATE_MTH_TM_ID = DuckDbUpdateParquetOperator(
        task_id='UPDATE_MTH_TM_ID',
        source='BASEL_ACCT_PRFM_FACT.parquet',
        target='UPDATE_MTH_TM_ID.parquet',
        sql="""
            SET mth_tm_id = {{ var.value.MTH_TM_ID }}
        """
    )

    # 3. Extract mortgage basel acct ID's and update where necessary
    #bai_mor_sql = f"""UPDATE {update_table_schema}.{update_table} a 
    #        SET a.basel_acct_id = (SELECT gz.basel_acct_id FROM EDRTLRP1D.BASEL_MORT_MTH_SNAPSHOT gz 
    #            WHERE gz.mth_tm_id = a.mth_tm_id AND TRIM(gz.mort_num) = a.acct_num) 
    #        WHERE a.mth_end_dt = '{run_mth_end_dt}' AND a.src_sys_cd = 'GZ';"""
    
    EXTRACT_MORT_BASEL_ACCT_ID = Db2ParquetExportOperator(
        task_id='EXTRACT_MORT_BASEL_ACCT_ID',
        db2_conn_id='db2-conn',
        target='EXTRACT_MORT_BASEL_ACCT_ID.parquet',
        sql="""
            SELECT
                gz.basel_acct_id,
                gz.mort_num
            FROM
                {{ params.EDW_schema_EDRTLRP1D }}.BASEL_MORT_MTH_SNAPSHOT gz
            WHERE gz.mth_tm_id = '{{ var.value.MTH_TM_ID }}';
        """,
        schema=pa.schema([
            ('basel_acct_id', pa.decimal128(20, 0)),
            ('mort_num', pa.string()),
        ])
    )

    UPDATE_MOR_BSL_ACCT_ID = DuckDbUpdateParquetOperator(
        task_id='UPDATE_MOR_BSL_ACCT_ID',
        source='UPDATE_MTH_TM_ID.parquet',
        target='UPDATE_MOR_BSL_ACCT_ID.parquet',
        sql=f"""
            SET 
                basel_acct_id = r.basel_acct_id
            FROM
                'EXTRACT_MORT_BASEL_ACCT_ID.parquet' r
            WHERE src_sys_cd = 'GZ'
            AND acct_num = TRIM(r.mort_num)
        """
    )

    # 4. Extract tangerine basel acct ID's and update where necessary
    #bai_tng_sql = f"""UPDATE {update_table_schema}.{update_table} a 
    #    SET a.basel_acct_id = (SELECT tng.basel_acct_id FROM EDRTLRP1D.BASEL_ACCT_DIM tng   
    #        WHERE TRIM(tng.src_app_cd) = 'TNG-MOR' AND tng.src_sys_del_f = 'N' AND tng.src_app_id = a.acct_num)
    #    WHERE a.mth_end_dt='{run_mth_end_dt}' AND a.src_sys_cd in ('TNG_MTG','TNG_MCAP');"""
    
    EXTRACT_TNG_BASEL_ACCT_ID = Db2ParquetExportOperator(
        task_id='EXTRACT_TNG_BASEL_ACCT_ID',
        db2_conn_id='db2-conn',
        target='EXTRACT_TNG_BASEL_ACCT_ID.parquet',
        sql="""
            SELECT 
                tng.basel_acct_id ,
                tng.src_app_id
            FROM 
                {{ params.EDW_schema_EDRTLRP1D }}.BASEL_ACCT_DIM tng
            WHERE TRIM(tng.src_app_cd) = 'TNG-MOR' 
            AND tng.src_sys_del_f = 'N';
        """,
        schema=pa.schema([
            ('basel_acct_id', pa.decimal128(20, 0)),
            ('src_app_id', pa.string()),
        ])
    )

    UPDATE_TNG_BSL_ACCT_ID = DuckDbUpdateParquetOperator(
        task_id='UPDATE_TNG_BSL_ACCT_ID',
        source='UPDATE_MOR_BSL_ACCT_ID.parquet',
        target='UPDATE_TNG_BSL_ACCT_ID.parquet',
        sql="""
            SET 
                basel_acct_id = r.basel_acct_id
            FROM 
                'EXTRACT_TNG_BASEL_ACCT_ID.parquet' r
            WHERE src_sys_cd in ('TNG_MTG','TNG_MCAP')
            AND TRIM(acct_num) = TRIM(r.src_app_id)
        """
    )

    # 5. Extract the first set of SPL basel acct ID's and update where necessary
    #bai_spl1_sql = f"""UPDATE {update_table_schema}.{update_table} E
    #    SET E.BASEL_ACCT_ID=D.ORIG_BASEL_ACCT_ID
    #    FROM
    #        (
    #            SELECT DISTINCT A.RLP_LOAN_NO AS ACCT_NUM ,A.ORIG_BASEL_ACCT_ID AS ORIG_BASEL_ACCT_ID
    #            FROM EDRTLRP1D.RLP_TO_SL_ACCT_LIST a
    #        ) D WHERE TRIM(L '0' FROM (E.ACCT_NUM))=TRIM(L '0' FROM D.ACCT_NUM)
    #        AND E.MTH_TM_ID={run_mth_tm_id};"""
    
    EXTRACT_SPL_BASEL_ACCT_ID = Db2ParquetExportOperator(
        task_id='EXTRACT_SPL_BASEL_ACCT_ID',
        db2_conn_id='db2-conn',
        target='EXTRACT_SPL_BASEL_ACCT_ID.parquet',
        sql="""
            SELECT 
                DISTINCT A.RLP_LOAN_NO AS ACCT_NO,
                A.ORIG_BASEL_ACCT_ID AS ORIG_BASEL_ACCT_ID
            FROM 
                {{ params.EDW_schema_EDRTLRP1D }}.RLP_TO_SL_ACCT_LIST a;
        """,
        schema=pa.schema([
            ('ACCT_NO', pa.string()),
            ('ORIG_BASEL_ACCT_ID', pa.decimal128(20, 0)),
        ])
    )

    UPDATE_SPL_BSL_ACCT_ID = DuckDbUpdateParquetOperator(
        task_id='UPDATE_SPL_BSL_ACCT_ID',
        source='UPDATE_TNG_BSL_ACCT_ID.parquet',
        target='UPDATE_SPL_BSL_ACCT_ID.parquet',
        sql="""
            SET 
                BASEL_ACCT_ID = r.ORIG_BASEL_ACCT_ID
            FROM 
                'EXTRACT_SPL_BASEL_ACCT_ID.parquet' r
            WHERE LTRIM(ACCT_NUM, '0') = LTRIM(r.ACCT_NO, '0')
        """
    )

    # 6. Extract the second set of SPL basel acct ID's and update where necessary
    #bai_spl2_sql = f"""UPDATE {update_table_schema}.{update_table} a 
    #    SET a.basel_acct_id = (SELECT sl.basel_acct_id FROM EDRTLRP1D.BASEL_PSNL_LOAN_MTH_SNAPSHOT sl  
    #                            WHERE sl.mth_tm_id = a.mth_tm_id 
    #                            AND CONCAT(LPAD(sl.crnt_br_loctn_trnst,5,'0'), LPAD(TRIM(sl.loan_num),7,'0')) = a.acct_num)
    #    WHERE a.mth_end_dt='{run_mth_end_dt}' 
    #    AND a.src_sys_cd ='SL' and 
    #    (a.basel_acct_id is null or 
    #        a.BASEL_ACCT_ID NOT IN (SELECT DISTINCT BASEL_ACCT_ID FROM EDRTLRP1D.BASEL_PSNL_LOAN_MTH_SNAPSHOT WHERE MTH_TM_ID={run_mth_tm_id}));"""
    
    EXTRACT_SPL_BASEL_ACCT_ID_2 = Db2ParquetExportOperator(
        task_id='EXTRACT_SPL_BASEL_ACCT_ID_2',
        db2_conn_id='db2-conn',
        target='EXTRACT_SPL_BASEL_ACCT_ID_2.parquet',
        sql="""
            SELECT 
                sl.basel_acct_id,
                sl.crnt_br_loctn_trnst,
                sl.loan_num
            FROM 
                {{ params.EDW_schema_EDRTLRP1D }}.BASEL_PSNL_LOAN_MTH_SNAPSHOT sl
            WHERE sl.mth_tm_id = '{{ var.value.MTH_TM_ID }}';
        """,
        schema=pa.schema([
            ('acct_id', pa.decimal128(20, 0)),
            ('crnt_br_loctn_trnst', pa.string()),
            ('loan_num', pa.string()),
        ])
    )

    UPDATE_SPL_2_BSL_ACCT_ID = DuckDbUpdateParquetOperator(
        task_id='UPDATE_SPL_2_BSL_ACCT_ID',
        source='UPDATE_SPL_BSL_ACCT_ID.parquet',
        target='UPDATE_SPL_2_BSL_ACCT_ID.parquet',
        sql="""
            SET basel_acct_id = r.acct_id
            FROM 
                'EXTRACT_SPL_BASEL_ACCT_ID_2.parquet' r
            WHERE 
                acct_num = CONCAT(LPAD(r.crnt_br_loctn_trnst,5,'0'), LPAD(TRIM(r.loan_num),7,'0'))
            AND src_sys_cd = 'SL' 
            AND (
                basel_acct_id IS NULL
                OR
                basel_acct_id NOT IN (
                    SELECT DISTINCT acct_id
                    FROM 'EXTRACT_SPL_BASEL_ACCT_ID_2.parquet'
                    
                )
            )
        """
    )

    # 7. Extract the KS basel acct ID's and update where necessary
    #bai_ks_sql = f"""UPDATE {update_table_schema}.{update_table} a 
    #    SET a.basel_acct_id = (SELECT kq.basel_acct_id FROM EDRTLRP1D.BASEL_REVLVNG_CR_MTH_SNAPSHOT kq 
    #        WHERE kq.mth_tm_id = a.mth_tm_id AND TRIM(L '0' FROM kq.acct_num) = a.acct_num)
    #    WHERE a.mth_end_dt ='{run_mth_end_dt}' AND a.src_sys_cd = 'KQ';"""

    EXTRACT_KS_BASEL_ACCT_ID = Db2ParquetExportOperator(
        task_id='EXTRACT_KS_BASEL_ACCT_ID',
        db2_conn_id='db2-conn',
        target='EXTRACT_KS_BASEL_ACCT_ID.parquet',
        sql="""
            SELECT 
                kq.basel_acct_id,
                kq.acct_num
            FROM 
                {{ params.EDW_schema_EDRTLRP1D }}.BASEL_REVLVNG_CR_MTH_SNAPSHOT kq
            WHERE kq.mth_tm_id = '{{ var.value.MTH_TM_ID }}';
        """,
        schema=pa.schema([
            ('acct_id', pa.decimal128(20, 0)),
            ('acct_no', pa.string()),
        ])
    )

    UPDATE_KS_BSL_ACCT_ID = DuckDbUpdateParquetOperator(
        task_id='UPDATE_KS_BSL_ACCT_ID',
        source='UPDATE_SPL_2_BSL_ACCT_ID.parquet',
        target='UPDATE_KS_BSL_ACCT_ID.parquet',
        sql=f"""
            SET basel_acct_id = r.acct_id
            FROM 
                'EXTRACT_KS_BASEL_ACCT_ID.parquet' r
            WHERE src_sys_cd = 'KQ'
            AND LTRIM(acct_num, '0') = LTRIM(r.acct_no, '0')
        """
    )

    # 8. Load enriched BASEL_ACCT_PFRM_FACT parquet into IIAS
    LOAD_BASEL_ACCT_PFRM_FACT = PythonOperator(
        task_id='LOAD_BASEL_ACCT_PFRM_FACT',
        python_callable=_load_basel_acct_pfrm_fact,
    )

    DUPE_CHECK_BASEL_ACCT_PFRM_FACT = DuplicateCheckOperator(
        task_id='DUPE_CHECK_BASEL_ACCT_PFRM_FACT',
        db2_conn_id="db2-conn",
        table_name='{{ params.EDW_schema_EDRTLRP1D }}.BASEL_ACCT_PRFM_FACT',
        dupe_checks=[
            ("BASEL_ACCT_ID",),
            ("SRC_SYS_CD", "ACCT_NUM")
        ]
    )

    # 9. Load BASEL_KS_ACCT_TRANSACTOR_ROLE using BASEL_ACCT_PRFM_FACT
    # basel_ks_acct_transactor_role_load_sql = f"""INSERT INTO
	# EDRTLRP1D.BASEL_KS_ACCT_TRANSACTOR_ROLE
    # SELECT
    #     DISTINCT SNAP.MTH_TM_ID,
    #     SNAP.BASEL_ACCT_ID,
    #     CASE WHEN MAX(0, SNAP.BNS_DLQNT_DAY -30) > 0 THEN 'D' WHEN FACT.TRNSCTR_IND = 'T' THEN 'T' WHEN FACT.TRNSCTR_IND = 'N' THEN 'R' ELSE '' END AS ROLE_IND,
    #     CURRENT_TIMESTAMP,
    #     CURRENT_TIMESTAMP
    # FROM
    #     EDRTLRP1D.BASEL_REVLVNG_CR_MTH_SNAPSHOT SNAP
    #     LEFT JOIN (
    #         SELECT
    #             DISTINCT tm_id AS mth_tm_id,
    #             LPAD(a.ACCT_NUM, 23, '0') AS acct_num,
    #             TRNSCTR_IND
    #         FROM
    #             EDRTLRP1D.BASEL_ACCT_PFRM_FACT a,
    #             EDRTLRP1D.TM_DIM b
    #         WHERE
    #             a.mth_end_dt = b.TM_LVL_END_DT
    #             AND tm_lvl = 'Month'
    #             AND tm_id = {run_mth_tm_id}
    #             AND src_sys_cd = 'KQ'
    #     ) FACT ON SNAP.MTH_TM_ID = FACT.MTH_TM_ID
    #     AND SNAP.ACCT_NUM = FACT.ACCT_NUM
    # WHERE
    #     SNAP.MTH_TM_ID = {run_mth_tm_id};"""

    INSERT_BASEL_KS_ACCT_TRANSACTOR_ROLE = PythonOperator(
        task_id='INSERT_BASEL_KS_ACCT_TRANSACTOR_ROLE',
        python_callable=_insert_basel_ks_acct_transactor_role,
    )

    # Setup dependencies
    CLEAR_DATA_ON_RERUN >> [
        EXTRACT_BASEL_ACCT_PRFM_FACT,
        EXTRACT_MORT_BASEL_ACCT_ID,
        EXTRACT_TNG_BASEL_ACCT_ID,
        EXTRACT_SPL_BASEL_ACCT_ID,
        EXTRACT_SPL_BASEL_ACCT_ID_2,
        EXTRACT_KS_BASEL_ACCT_ID
    ]
    EXTRACT_BASEL_ACCT_PRFM_FACT >> UPDATE_MTH_TM_ID
    [ UPDATE_MTH_TM_ID, EXTRACT_MORT_BASEL_ACCT_ID ] >> UPDATE_MOR_BSL_ACCT_ID
    [ UPDATE_MOR_BSL_ACCT_ID, EXTRACT_TNG_BASEL_ACCT_ID ] >> UPDATE_TNG_BSL_ACCT_ID
    [ UPDATE_TNG_BSL_ACCT_ID, EXTRACT_SPL_BASEL_ACCT_ID ] >> UPDATE_SPL_BSL_ACCT_ID
    [ UPDATE_SPL_BSL_ACCT_ID, EXTRACT_SPL_BASEL_ACCT_ID_2 ] >> UPDATE_SPL_2_BSL_ACCT_ID
    [ UPDATE_SPL_2_BSL_ACCT_ID, EXTRACT_KS_BASEL_ACCT_ID ] >> UPDATE_KS_BSL_ACCT_ID
    UPDATE_KS_BSL_ACCT_ID >> LOAD_BASEL_ACCT_PFRM_FACT
    LOAD_BASEL_ACCT_PFRM_FACT >> INSERT_BASEL_KS_ACCT_TRANSACTOR_ROLE
    LOAD_BASEL_ACCT_PFRM_FACT >> DUPE_CHECK_BASEL_ACCT_PFRM_FACT 