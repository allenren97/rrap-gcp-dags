from airflow.sdk import task


@task.parquet(
    task_id="lookup_67",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/lookup_67.parquet",
    sql="""
        SELECT tm.tm_id as mth_tm_id, airb.* 
        FROM 
            '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/airb_revlvng_cr_mth_snapshot/*.parquet' as airb 
        JOIN
            ingestion.TM_DIM as tm 
        ON (airb.mth_end_dt = tm.tm_lvl_end_dt)
    """,
    export_params={},
    clear_before_write=True,
)
def lookup_67() -> None:
    """
    This task joins '/bns/rrap/data/<YYYY-MM-DD>/jb0071_airb_revlvng_cr_mth_snapshot/*.parquet' 
    contents with 'jb0072_ODBC_RCRR1_TIM_DIM.parquet' to obtain mth_tm_id column.
    """
    pass


@task.parquet(
    task_id="join_step_pln",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_step_pln.parquet",
    sql="""
        SELECT pln.STEP_PLN_SNAPSHOT_ID, l67.*
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/lookup_67.parquet' as l67 
        LEFT OUTER JOIN
        ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT as pln 
        USING (STEP_PLN_AGRMNT_NUM)
    """,
    export_params={},
    clear_before_write=True,
)
def join_step_pln() -> None:
    """
    This task joins 'lookup_67.parquet' contents with ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT DuckLake table
    on STEP_PLN_AGRMNT_NUM to obtain STEP_PLN_SNAPSHOT_ID column.
    """
    pass


@task.parquet(
    task_id="join_cust_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_cust_id.parquet",
    sql="""
        SELECT bc.basel_cust_id, js.*
        FROM 
            '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_step_pln.parquet' as js 
        LEFT JOIN
            ingestion.BASEL_CUST_DIM as bc 
        ON 
            lpad(trim(js.acct_num), 23, '0') = lpad(trim(bc.acct_num), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def join_cust_id() -> None:
    """
    This task joins 'join_step_pln.parquet' contents with 'jb0072_BASEL_CUST_DIM.parquet' on trimmed and 
    left padded account numbers in order to obtain basel_cust_id column. 
    Output file is '/bns/rrap/data/<YYYY-MM-DD>/jb0072_Join_cust_id/*.parquet'.
    """
    pass


@task.parquet(
    task_id="join_acct_num",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_acct_num.parquet",
    sql="""
        SELECT b.basel_acct_id, j.*
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_cust_id.parquet' as j 
        LEFT JOIN
        ingestion.BASEL_ACCT_DIM as b 
        ON 
            lpad(trim(j.acct_num), 23, '0') = lpad(trim(b.acct_num), 23, '0')

    """,
    export_params={},
    clear_before_write=True,
)
def join_acct_num() -> None:
    """
    This task joins 'join_cust_id.parquet' contents with 'jb0072_BASEL_ACCT_DIM.parquet' on trimmed 
    and left padded account numbers in order to obtain basel_acct_id column. 
    Output file is '/bns/rrap/data/<YYYY-MM-DD>/jb0072_Join_acct_num/*.parquet'.
    """
    pass


@task.parquet(
    task_id="join_72",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_72.parquet",
    sql="""
        SELECT o.org_unit_id, j.*
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_acct_num.parquet' as j LEFT JOIN
        ingestion.ORG_UNIT_DIM as o
        ON (o.trnst_num = j.trnst_num::varchar)
    """,
    export_params={},
    clear_before_write=True,
)
def join_72() -> None:
    """
    This task joins 'join_acct_num.parquet' contents with 'jb0072_ORG_UNIT_DIM.parquet' on transit numbers in order 
    to obtain org unit ID column.
    Output file is '/bns/rrap/data/<YYYY-MM-DD>/jb0072_Join_72/*.parquet'.
    """
    pass


@task.parquet(
    task_id="transformer_58",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_58.parquet",
    sql="""
    SELECT
        last_prch_dt ,
        last_pymt_dt ,
        acct_cls_rsn_cd as acct_cls_rsn_cd ,
        acct_stat_cd as acct_stat_cd ,
        (anul_fee_cd::int)::text as anul_fee_cd,
        anul_fee_lnk_f as anul_fee_lnk_f ,
        bal_hist_csh_adv_amt as bal_hist_csh_adv_amt ,
        bal_hist_prchs_amt as bal_hist_prchs_amt ,
        bal_hist_pymts_amt as bal_hist_pymts_amt ,
        bns_dlqnt_days as bns_dlqnt_day ,
        chip_f,
        chrg_off_cd as chrg_off_cd ,
        cmpgn_notified_cd as cmpgn_notified_cd ,
        cmpgn_stat_cd as cmpgn_stat_cd ,
        lpad(colctn_ctr_cd::text, 2, '0') AS colctn_ctr_cd,
        corp_rtl_f as corp_rtl_f ,
        cr_lmt_amt as cr_lmt_amt ,
        cr_score as cr_score ,
        csh_advnc_1_cycl_ago_bal_amt as csh_advnc_1_cycl_ago_bal_amt ,
        csh_advnc_2_cycl_ago_bal_amt as csh_advnc_2_cycl_ago_bal_amt ,
        csh_advnc_crnt_cycl_bal_amt as csh_advnc_crnt_cycl_bal_amt ,
        csh_advnc_rcvry_intr_amt as csh_advnc_rcvry_intr_amt ,
        divdd_rbt_last_yr_amt as divdd_rbt_last_yr_amt ,
        dlqnt_hist_1_12 as dlqnt_hist_1_12 ,
        dlqnt_hist_13_24 as dlqnt_hist_13_24 ,
        final_cr_score as final_cr_score ,
        fscl_ytd_prchs_amt as fncl_ytd_prchs_amt ,
        fscl_ytd_rtrns_amt as fncl_ytd_rtrns_amt ,
        full_pymt_cd as full_pymt_cd ,
        inact_cd as inact_cd ,
        last_pymt_amt as last_pymt_amt ,
        last_yr_cr_intr_pd_amt as last_yr_cr_intr_pd_amt ,
        mdl_cd_3 as mdl_cd_3 ,
        mdl_cd_4 as mdl_cd_4 ,
        mdl_citifone as mdl_citifone ,
        mdl_compaction as mdl_compaction ,
        mths_dlqnt_cnt as mth_dlqnt_cnt ,
        non_responder_f as non_responder_f ,
        oprtr_last_onlne_updt as oprtr_last_online_updt ,
        orig_chrg_off_amt as orig_chrg_off_amt ,
        pad_oth_fncl_inst_f as pad_oth_fncl_inst_f ,
        prch_1_cycl_ago_bal_amt as prch_1_cycl_ago_bal_amt ,
        prch_2_cycl_ago_bal_amt as prch_2_cycl_ago_bal_amt ,
        prch_crnt_cycl_bal_amt as prch_crnt_cycl_bal_amt ,
        prch_rcvry_intr_amt as prch_rcvry_intr_amt ,
        prd_cd as prd_cd ,
        prev_sub_prd_cd as prev_sub_prd_cd ,
        proc_tp_cd as proc_tp_cd ,
        reqst_cr_lmt_amt as reqst_cr_lmt_amt ,
        risk_bal_rto as risk_bal_rto ,
        rtn_envelope as rtn_envelope ,
        scrd_tp_cd as scrd_tp_cd ,
        lpad(scrty_tp_cd::text, 2, '0') AS scrty_tp_cd,
        scrty_val_amt as scrty_val_amt ,
        lpad(setp_trnst_num::text, 5,'0') as setp_trnst_num,
        solctn_cd as solctn_cd ,
        nullif(spsp_ccli, '') as spsp_ccli ,
        step_pln_agrmnt_num as step_pln_agrmnt_num ,
        sub_prd_cd as sub_prd_cd ,
        switch_cd as switch_cd ,
        tot_cycl_to_dt_fncl_chrg_amt as tot_cycl_to_dt_fncl_chrg_amt ,
        tot_new_bal_amt as tot_new_bal_amt ,
        tot_rcvry_intr_amt as tot_rcvry_intr_amt ,
        tot_unpaid_fncl_chrg_amt as tot_unpaid_fncl_chrg_amt ,
        tot_ytd_cr_intr_pd_amt as tot_ytd_cr_intr_pd_amt ,
        lpad(trnst_num::text, 5, '0') as trnst_num,
        visa_plastic_out as visa_plastic_out ,
        ytd_csh_advnc_intr_chrgd_amt as ytd_csh_advnc_intr_chrgd_amt ,
        ytd_csh_advnc_intr_pd as ytd_csh_advnc_intr_pd ,
        ytd_prchs_cnt as ytd_prchs_cnt ,
        ytd_prchs_intr_chrgd_amt as ytd_prchs_intr_chrgd_amt ,
        ytd_prchs_intr_pd_amt as ytd_prchs_intr_pd_amt ,
        now() as insrt_process_tmstmp ,
        now() as updt_process_tmstmp ,
        src_cd::text as src_cd ,            -- comes from transformer_30 as acct1_src from ODBC_rcrr_revlvng_credit_mth_snapshot as an integer (which trims 0s)
        switch_xref::text as switch_xref ,  -- Also from transformer_30 as acct1_switch_xref from ODBC_rcrr_revlvng_credit_mth_snapshot as an integer
        rtrim(crnt_bill_cd) as crnt_bill_cd ,
        ifnull(rtrim(prim_cust_id), '') as prim_cust_cid ,
        trim(block_recl_cd) as block_recl_cd ,
        CASE WHEN basel_cust_id IS NULL THEN -1 ELSE  basel_cust_id END as prim_basel_cust_id,
        CASE WHEN basel_cust_id IS NULL THEN -1 ELSE  basel_cust_id END as basel_cust_id,
        CASE WHEN step_pln_snapshot_id IS NULL THEN -1 ELSE step_pln_snapshot_id END as step_pln_snapshot_id,
        CASE WHEN org_unit_id IS NULL THEN -1 ELSE org_unit_id END as trnst_ou_id,
        CASE WHEN acct_opnd_dt IS NULL THEN '9999-12-31' ELSE acct_opnd_dt END as acct_opnd_dt,
        CASE WHEN last_acty_dt IS NULL THEN '9999-12-31' ELSE last_acty_dt END as last_acty_dt,
        CASE WHEN chrg_off_dt IS NULL THEN '9999-12-31' ELSE chrg_off_dt END as chrg_off_dt,
        CASE WHEN frst_use_dt IS NULL THEN '9999-12-31' ELSE frst_use_dt END as frst_use_dt,
        CASE WHEN non_accrl_dt IS NULL THEN '9999-12-31' ELSE non_accrl_dt END as non_accrl_dt,
        CASE WHEN write_off_dt IS NULL THEN '9999-12-31' ELSE write_off_dt END as write_off_dt,
        CASE WHEN last_blocked_dt IS NULL THEN '9999-12-31' ELSE last_blocked_dt END as last_blocked_dt,
        CASE WHEN next_rnew_fee_dt IS NULL THEN '9999-12-31' ELSE next_rnew_fee_dt END as next_rnew_fee_dt,
        CASE WHEN switch_dt IS NULL THEN '9999-12-31' ELSE switch_dt END as switch_dt,
        CASE WHEN sub_prd_tfr_dt IS NULL THEN '9999-12-31' ELSE sub_prd_tfr_dt END as sub_prd_tfr_dt,
        CASE WHEN last_onlne_updt_dt IS NULL THEN '9999-12-31' ELSE last_onlne_updt_dt END as last_online_updt_dt,
        CASE WHEN last_addr_chng_dt IS NULL THEN '9999-12-31' ELSE last_addr_chng_dt END last_addr_chng_dt,
        crncy_cd,
        crnt_bill_cd,
        basel_acct_id,
        lpad(trim(acct_num), 23, '0') as acct_num, -- no need to lpad since this is now done at the input of each table
        gl_acct_num,
        gl_trnst_num,
        mth_tm_id
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_72.parquet'
    """,
    export_params={},
    clear_before_write=True,
)
def transformer_58() -> None:
    """
    This task transforms and renames column names from 'join_72.parquet' and writes them to 
    '/bns/rrap/data/<YYYY-MM-DD>/sq007/transformer_58.parquet'.
    """
    pass


@task.parquet(
    task_id="rows_requiring_update",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/rows_requiring_update.parquet",
    sql="""
    WITH prev_month AS (
        SELECT basel_acct_id, 
        mth_dlqnt_cnt,
        bns_dlqnt_day,
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE mth_tm_id = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}'
    ),
    curr_month AS (
        SELECT basel_acct_id,
        mth_dlqnt_cnt,
        bns_dlqnt_day
        FROM {{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_58.parquet
        WHERE mth_tm_id = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }}'
    )
    SELECT 
        curr_month.BASEL_ACCT_ID AS curr_acct_id, 
        curr_month.MTH_DLQNT_CNT AS curr_mth_dlqnt_cnt, 
        curr_month.bns_dlqnt_day AS curr_dlqnt_day,
        prev_month.BASEL_ACCT_ID AS prev_acct_id, 
        prev_month.MTH_DLQNT_CNT AS prev_mth_dlqnt_cnt, 
        prev_month.bns_dlqnt_day AS prev_bns_dlqnt_day
    FROM curr_month
    LEFT OUTER JOIN prev_month
    ON curr_month.BASEL_ACCT_ID = prev_month.BASEL_ACCT_ID
    """,
    export_params={},
    clear_before_write=True,
)
def rows_requiring_update() -> None:
    """
    This task identifies rows that require updates and writes them to a Parquet file.
    """
    pass


@task.update(
    task_id="set_mth_dlqnt_cnt",
    duckdb_conn_id="duckdb-conn",
    sql="SET PREV_ACCT_ID = CURR_ACCT_ID, PREV_MTH_DLQNT_CNT = 0 WHERE PREV_MTH_DLQNT_CNT IS NULL",
    source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/rows_requiring_update.parquet",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_mth_dlqnt_cnt",
    export_params={
        'PER_THREAD_OUTPUT': 'TRUE',
        'file_size_bytes':  '100000000',
    },
    clear_before_write=True,
)
def set_mth_dlqnt_cnt() -> None:
    """ This task sets previous month's 'month delinquent count' to 0 where it is null. """
    pass


@task.update(
    task_id="set_net_new_account",
    duckdb_conn_id="duckdb-conn",
    sql="SET CURR_MTH_DLQNT_CNT = PREV_MTH_DLQNT_CNT",
    source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_mth_dlqnt_cnt/*.parquet",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_net_new_account",
    export_params={
        'PER_THREAD_OUTPUT': 'TRUE',
        'file_size_bytes':  '100000000',
    },
    clear_before_write=True,
)
def set_net_new_account() -> None:
    """ This task sets current month's 'month delinquent count' to previous month's value for all records. """
    pass


@task.update(
    task_id="update_mth_dlqnt_cnt",
    duckdb_conn_id="duckdb-conn",
    sql="SET CURR_MTH_DLQNT_CNT = PREV_MTH_DLQNT_CNT + 1 where curr_dlqnt_day >= 30",
    source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_net_new_account/*.parquet",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_mth_dlqnt_cnt",
    export_params={
        'PER_THREAD_OUTPUT': 'TRUE',
        'file_size_bytes':  '100000000',
    },
    clear_before_write=True,
)
def update_mth_dlqnt_cnt() -> None:
    """ 
    This task sets current month's 'month delinquent count' to previous month's value plus 1 
    where current month's 'days delinquent' is >= 30. 
    """
    pass


@task.parquet(
    task_id="update_basel_revlvng_cr_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot",
    sql= f"""
        SELECT DISTINCT ON (a.BASEL_ACCT_ID) 
            a.* EXCLUDE(MTH_DLQNT_CNT), 
            b.curr_mth_dlqnt_cnt MTH_DLQNT_CNT
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_58.parquet' as a
        JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_mth_dlqnt_cnt/*.parquet' as b
            ON a.BASEL_ACCT_ID = b.curr_acct_id
        WHERE a.MTH_TM_ID = (SELECT TM_ID FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/TM_DIM.parquet')
    """,
    export_params={
        'PER_THREAD_OUTPUT': 'TRUE',
        'file_size_bytes':  '100000000',
    },
    clear_before_write=True,
)
def update_basel_revlvng_cr_mth_snapshot() -> None:
    """
    This task pulls a distinct list of BASEL_ACCT_IDs and all other columns from 'transformer_58.parquet' 
    along with their corresponding 'delinquent month count' from 'update_mth_dlqnt_cnt'.
    """


@task.parquet(
    task_id="src_prd_stdnt_loan_lkp",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/src_prd_stdnt_loan_lkp.parquet",
    sql="""
        SELECT PRD_SYS_CD, SRC_PRD_CD, SRC_SUB_PRD_CD, BILL_CD_CHAR, EFF_TO_YR_MTH, BASEL_PRD_CD 
        FROM ingestion.SRC_PRD_STDNT_LOAN_LKP
        WHERE EFF_TO_YR_MTH = '999912'
    """,
    export_params={},
    clear_before_write=True,
)
def src_prd_stdnt_loan_lkp() -> None:
    """
    This task pulls data from ingestion.SRC_PRD_STDNT_LOAN_LKP DuckLake table and writes it to a Parquet file.
    """
    pass


@task.parquet(
    task_id="sslb_account_ids",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_account_ids.parquet",
    sql="""
        SELECT a.basel_acct_id
        FROM
        '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot/*.parquet' a,
        '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/src_prd_stdnt_loan_lkp.parquet' b
        WHERE 
        a.PRD_CD  = trim(b.SRC_PRD_CD)
        AND a.SUB_PRD_CD = trim(b.SRC_SUB_PRD_CD)
        AND SUBSTR(trim(a.CRNT_BILL_CD), 3, 1) = trim(b.BILL_CD_CHAR)
        AND trim(a.PRD_CD) = 'SSL'
        AND trim(b.BILL_CD_CHAR) = 'B'
    """,
    export_params={},
    clear_before_write=True,
)
def sslb_account_ids() -> None:
    """
    This task identifies SSL B accounts by joining 'update_basel_revlvng_cr_mth_snapshot' with 'src_prd_stdnt_loan_lkp' on product code, 
    sub product code and billing code conditions.
    Output is a Parquet file with a list of basel_acct_ids that are SSL B accounts.
    """
    pass


@task.parquet(
    task_id="non_sslb_accounts",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/non_sslb_accounts.parquet",
    sql=f"""
        SELECT a.*
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot/*.parquet' a
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_account_ids.parquet' b 
        ON a.basel_acct_id = b.basel_acct_id
        WHERE b.basel_acct_id is null
    """,
    export_params={},
    clear_before_write=True,
)
def non_sslb_accounts() -> None:
    """
    This task identifies non SSL B accounts by left joining 'update_basel_revlvng_cr_mth_snapshot' with 'sslb_account_ids' on basel_acct_id and filtering for nulls.
    Output is a Parquet file with records of non SSL B accounts.
    """
    pass


@task.parquet(
    task_id="sslb_accounts",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_accounts.parquet",
    sql=f"""
        SELECT a.* REPLACE(
            CASE
                WHEN CR_LMT_AMT = 0 AND TOT_NEW_BAL_AMT < 0
                THEN CR_LMT_AMT
                ELSE TOT_NEW_BAL_AMT
            END
            as CR_LMT_AMT
        )
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot/*.parquet' a
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_account_ids.parquet' b 
        ON a.basel_acct_id = b.basel_acct_id
        WHERE b.basel_acct_id is not null
    """,
    export_params={},
    clear_before_write=True,
)
def sslb_accounts() -> None:
    """
    This task applies the SSLB patch by setting CR_LMT_AMT to TOT_NEW_BAL_AMT for SSL B accounts where CR_LMT_AMT is 0 and TOT_NEW_BAL_AMT is negative.
    Output is a Parquet file with records of SSL B accounts with the patch applied.
    """
    pass


@task.parquet(
    task_id="merge_final_output",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/basel_revlvng_cr_mth_snapshot",
    sql=f"""
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_accounts.parquet'
        UNION ALL
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/non_sslb_accounts.parquet'
    """,
    export_params={},
    clear_before_write=True,
)
def merge_final_output() -> None:
    """
    This task merges the SSL B accounts with the non SSL B accounts to produce the final output Parquet file that will be used for downstream processing.
    Output is a Parquet file with all accounts, where SSL B accounts have the patch applied.
    """
    pass


@task.duckdb(
    task_id="load_basel_revlvng_cr_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT BY NAME
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/basel_revlvng_cr_mth_snapshot/*.parquet'
    """,
)
def load_basel_revlvng_cr_mth_snapshot() -> None:
    """
    This task loads the final Parquet output into the ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT DuckLake table.
    """
    pass


""" TaskFlow function calls """
# Original DataStage steps

lookup_67 = lookup_67()
join_step_pln = join_step_pln()
join_cust_id = join_cust_id()
join_acct_num = join_acct_num()
join_72 = join_72()
transformer_58 = transformer_58()
# TSYS updates
rows_requiring_update = rows_requiring_update()
set_mth_dlqnt_cnt = set_mth_dlqnt_cnt()
set_net_new_account = set_net_new_account()
update_mth_dlqnt_cnt = update_mth_dlqnt_cnt()
update_basel_revlvng_cr_mth_snapshot = update_basel_revlvng_cr_mth_snapshot()
# SSLB patch
src_prd_stdnt_loan_lkp = src_prd_stdnt_loan_lkp()
sslb_account_ids = sslb_account_ids()
non_sslb_accounts = non_sslb_accounts()
sslb_accounts = sslb_accounts()
merge_final_output = merge_final_output()
# Load to DuckLake
load_basel_revlvng_cr_mth_snapshot = load_basel_revlvng_cr_mth_snapshot()


""" Dependency chaining """
lookup_67 >> join_step_pln
join_step_pln >> join_cust_id
join_cust_id >> join_acct_num
join_acct_num >> join_72
join_72 >> transformer_58
transformer_58 >> rows_requiring_update 
rows_requiring_update >> set_mth_dlqnt_cnt 
set_mth_dlqnt_cnt >> set_net_new_account 
set_net_new_account >> update_mth_dlqnt_cnt 
update_mth_dlqnt_cnt >> update_basel_revlvng_cr_mth_snapshot
update_basel_revlvng_cr_mth_snapshot >> src_prd_stdnt_loan_lkp
src_prd_stdnt_loan_lkp >> sslb_account_ids
sslb_account_ids >> [
    non_sslb_accounts,
    sslb_accounts
] >> merge_final_output >> load_basel_revlvng_cr_mth_snapshot

