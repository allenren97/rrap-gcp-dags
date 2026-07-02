from airflow.sdk import task


@task.parquet(
    task_id="join_step_pln_snapshot_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_step_pln_snapshot_id.parquet",
    sql="""
    SELECT * EXCLUDE(STEP_PLN_SNAPSHOT_ID),
    (
        CASE
            WHEN STEP_PLN_SNAPSHOT_ID IS NULL OR STEP_PLN_SNAPSHOT_ID = 0 THEN -1
            ELSE STEP_PLN_SNAPSHOT_ID
        END
    ) AS STEP_PLN_SNAPSHOT_ID
    FROM (
    SELECT j.*, s.STEP_PLN_SNAPSHOT_ID
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_2.parquet' j
    LEFT JOIN ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT s
    ON j.MTH_TM_ID = s.MTH_TM_ID
    AND j.STEP_PLN_AGRMNT_NUM = TRIM(s.STEP_PLN_AGRMNT_NUM)
    )
    """,
    export_params={},
    clear_before_write=True,
)
def join_step_pln_snapshot_id():
    pass


@task.parquet(
    task_id="join_basel_cust_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_cust_id.parquet",
    sql="""
    SELECT a.ACCT_NUM as MORT_NUM, b.BASEL_CUST_ID
    FROM ingestion.BASEL_ACCT_DIM a
    INNER JOIN ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT b
    ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    """,
    export_params={},
    clear_before_write=True,
)
def join_basel_cust_id():
    pass


@task.parquet(
    task_id="join_prim_basel_cust_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_prim_basel_cust_id.parquet",
    sql="""
    SELECT j.*, b.BASEL_CUST_ID as PRIM_BASEL_CUST_ID
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_step_pln_snapshot_id.parquet' j
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_cust_id.parquet' b
    ON j.MORT_NUM = b.MORT_NUM
    """,
    export_params={},
    clear_before_write=True,
)
def join_prim_basel_cust_id():
    pass


@task.parquet(
    task_id="join_basel_acct_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_acct_id.parquet",
    sql="""
    SELECT j.*, b.BASEL_ACCT_ID
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_prim_basel_cust_id.parquet' j
    LEFT JOIN ingestion.BASEL_ACCT_DIM b
    ON j.MORT_NUM = b.ACCT_NUM
    """,
    export_params={},
    clear_before_write=True,
)
def join_basel_acct_id():
    pass


@task.parquet(
    task_id="join_serv_br_ou_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_serv_br_ou_id.parquet",
    sql="""
    SELECT * EXCLUDE(SERV_BR_OU_ID),
    (CASE
        WHEN SERV_BR_OU_ID IS NULL OR SERV_BR_OU_ID = 0 THEN -1
        ELSE SERV_BR_OU_ID
    END) AS SERV_BR_OU_ID,
    FROM (
    SELECT j.*, o.ORG_UNIT_ID as SERV_BR_OU_ID
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_acct_id.parquet' j
    LEFT JOIN ingestion.ORG_UNIT_DIM o
    ON o.TRNST_NUM = j.SERV_BR_TRNST_NUM
    )
    """,
    export_params={},
    clear_before_write=True,
)
def join_serv_br_ou_id():
    pass


@task.parquet(
    task_id="join_proc_br_ou_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_proc_br_ou_id.parquet",
    sql="""
    SELECT * EXCLUDE(PROC_BR_OU_ID),
    (CASE
        WHEN PROC_BR_OU_ID IS NULL OR PROC_BR_OU_ID = 0 THEN -1
        ELSE PROC_BR_OU_ID
    END) AS PROC_BR_OU_ID,
    FROM (
    SELECT j.*, o.ORG_UNIT_ID as PROC_BR_OU_ID
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_serv_br_ou_id.parquet' j
    LEFT JOIN ingestion.ORG_UNIT_DIM o
    ON o.TRNST_NUM = j.PROC_BR_TRNST_NUM
    )
    """,
    export_params={},
    clear_before_write=True,
)
def join_proc_br_ou_id():
    pass


@task.parquet(
    task_id="make_basel_mort_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/initial_basel_mort_mth_snapshot.parquet",
    sql="""
    SELECT
        CONCAT(MORT_NUM, MTH_TM_ID) AS BASEL_MORT_MTH_SNAPSHOT_ID,
        MTH_TM_ID,
        STEP_PLN_SNAPSHOT_ID,
        PRIM_BASEL_CUST_ID,
        BASEL_ACCT_ID,
        FLOAT_CD,
        left(cast(MORT_NUM as character(10)), 7) as MORT_NUM,
        PD_OFF_F,
        FRCLSR_F,
        MTH_IN_ARRS_CNT,
        INSUR_CL_CD,
        PRPTY_CD,
        FUND_CD,
        INTR_ADJ_DT,
        CRNT_BAL_AMT,
        SERV_BR_TRNST_NUM,
        SERV_BR_OU_ID,
        PROC_BR_TRNST_NUM,
        PROC_BR_OU_ID,
        cast(MORT_AUTH_DT as date) as MORT_AUTH_DT,
        cast(PD_OFF_DT as date) as PD_OFF_DT,
        cast(CRNT_TERM_MAT_DT as date) as CRNT_TERM_MAT_DT,
        cast(LAST_RNEW_DT as date) as LAST_RNEW_DT,
        AUTH_AMT,
        INTR_DUE_AMT,
        LND_VAL,
        TAX_CRNT_BAL_AMT,
        cast(INTR_ACCR_AMT AS DECIMAL(17,3)) as INTR_ACCR_AMT,
        TOT_ADVNC_AMT,
        BRWER_CD,
        APRSD_LAST_BUILDING_VAL,
        APRSD_LAND_VAL,
        APRSD_LAST_LAND_VAL,
        APRSD_BUILDING_VAL,
        APRSD_ORIG_LAND_VAL,
        APRSD_ORIG_BUILDING_VAL,
        cast(FINAL_ADVNC_DT as date) as FINAL_ADVNC_DT,
        LIFE_INSUR_CD,
        cast(WK_FRST_UNPAID_DT as date) as WK_FRST_UNPAID_DT,
        BUS_SRC_CD,
        SCRTY_TP_2,
        FRST_UNPAID_DT,
        MONTREAL_TRUST_DSBLTY_STAT_CD,
        SALE_DT_VAL,
        MRKTING_1_CD,
        MRKTING_2_CD,
        MRKTING_3_CD,
        MRKTING_4_CD,
        MRKTING_5_CD,
        CRI_CD,
        ARI_CD,
        PVSN_AMT,
        SCOTIA_TOT_EQTY_PLN_F,
        cast(LOAN_AUTH_DT as date) as LOAN_AUTH_DT,
        HLTH_CRSIS_PRTCTN_INSUR_STAT_CD,
        TOT_SUSP_BAL_AMT,
        ACCT_TP_CD,
        PRPTY_PROV_CD,
        cast(RENEWED_DT as date) as RENEWED_DT,
        MORT_TERM_MTH,
        MAX_INTR_RT,
        YR_5_MTH_6_F,
        rpad(trim(CUST_ID), 20, ' ') as PRIM_CUST_CID,
        STEP_PLN_AGRMNT_NUM,
        current_timestamp as INSRT_PROCESS_TMSTMP,
        current_timestamp as UPDT_PROCESS_TMSTMP,
        cast(YTD_PRPY_AMT as DECIMAL(17,3)) as YTD_PRPY_AMT,
        AMORT_MTH,
        PRPTY_ADDR,
        GL_ACCT_NUM,
        GL_TRNST_NUM,
        PRD_GRP_CD,
        UNIT_CNT,
        COMM_TP
    from '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_proc_br_ou_id.parquet'
    """,
    export_params={},
    clear_before_write=True,
)
def make_basel_mort_mth_snapshot():
    pass


@task.update(
    task_id="update_basel_mort_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/basel_mort_mth_snapshot.parquet",
    source="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/initial_basel_mort_mth_snapshot.parquet",
    sql="""
    SET
        FRCLSR_F = (CASE WHEN TRIM(FRCLSR_F) = '' THEN NULL ELSE FRCLSR_F END),
        PRPTY_CD = (CASE WHEN TRIM(PRPTY_CD) = '' THEN NULL ELSE PRPTY_CD END),
        PRPTY_PROV_CD = (CASE WHEN TRIM(PRPTY_PROV_CD) = '' THEN NULL ELSE PRPTY_PROV_CD END),
        SCRTY_TP_2 = (CASE WHEN TRIM(SCRTY_TP_2) = '' THEN NULL ELSE SCRTY_TP_2 END),
        MONTREAL_TRUST_DSBLTY_STAT_CD = (CASE WHEN TRIM(MONTREAL_TRUST_DSBLTY_STAT_CD) = '' THEN NULL ELSE MONTREAL_TRUST_DSBLTY_STAT_CD END),
        MRKTING_1_CD = (CASE WHEN TRIM(MRKTING_1_CD) = '' THEN NULL ELSE MRKTING_1_CD END),
        MRKTING_2_CD = (CASE WHEN TRIM(MRKTING_2_CD) = '' THEN NULL ELSE MRKTING_2_CD END),
        MRKTING_3_CD = (CASE WHEN TRIM(MRKTING_3_CD) = '' THEN NULL ELSE MRKTING_3_CD END),
        MRKTING_4_CD = (CASE WHEN TRIM(MRKTING_4_CD) = '' THEN NULL ELSE MRKTING_4_CD END),
        MRKTING_5_CD = (CASE WHEN TRIM(MRKTING_5_CD) = '' THEN NULL ELSE MRKTING_5_CD END),
        CRI_CD = COALESCE(CRI_CD, '          '),
        ARI_CD = COALESCE(ARI_CD, '          '),
        YR_5_MTH_6_F = (CASE WHEN TRIM(YR_5_MTH_6_F) = '' THEN NULL ELSE YR_5_MTH_6_F END),
        COMM_TP = RPAD(COMM_TP, 20, ' '),
        GL_TRNST_NUM = (CASE WHEN TRIM(GL_TRNST_NUM) = '' THEN NULL ELSE GL_TRNST_NUM END)
    WHERE
        MTH_TM_ID = {{ task_instance.xcom_pull(task_ids='handle_month_context', 'MTH_TM_ID') }}
    """,
    export_params={},
    clear_before_write=True,
)
def update_basel_mort_mth_snapshot():
    pass


@task.duckdb(
    task_id="load_basel_mort_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.BASEL_MORT_MTH_SNAPSHOT BY NAME
    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/basel_mort_mth_snapshot.parquet'
    """,
)
def load_basel_mort_mth_snapshot():
    pass


""" TaskFlow function definitions """
join_step_pln_snapshot_id = join_step_pln_snapshot_id()
join_basel_cust_id = join_basel_cust_id()
join_prim_basel_cust_id = join_prim_basel_cust_id()
join_basel_acct_id = join_basel_acct_id()
join_serv_br_ou_id = join_serv_br_ou_id()
join_proc_br_ou_id = join_proc_br_ou_id()
make_basel_mort_mth_snapshot = make_basel_mort_mth_snapshot()
update_basel_mort_mth_snapshot = update_basel_mort_mth_snapshot()
load_basel_mort_mth_snapshot = load_basel_mort_mth_snapshot()

""" Dependency chaining """
[
    join_step_pln_snapshot_id,
    join_basel_cust_id
] >> join_prim_basel_cust_id
join_prim_basel_cust_id >> join_basel_acct_id
join_basel_acct_id >> join_serv_br_ou_id
join_serv_br_ou_id >> join_proc_br_ou_id
join_proc_br_ou_id >> make_basel_mort_mth_snapshot
make_basel_mort_mth_snapshot >> update_basel_mort_mth_snapshot
update_basel_mort_mth_snapshot >> load_basel_mort_mth_snapshot
