import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq003_rundir():
    """
    Task to create RUNDIR for sequence sq003.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq003_rundir = f"{rundir}/sq003"
    os.makedirs(sq003_rundir, exist_ok=True)


@task.beeline(
    task_id="get_cust_acct_rltnp",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.EZ1_SCHEMA }};
    select distinct
        mth_end_dt,
        cast(cast(acct_num as bigint) as string) as acct_num,
        src_sys_cd
    from cust_acct_rltnp
    where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ("mth_end_dt", pa.date64()),
        ("acct_num", pa.string()),
        ("src_sys_cd", pa.string()),
    ]),
    target="ez1_cust_acct_rltnp.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_cust_acct_rltnp():
    pass


@task.beeline(
    task_id="get_mortgage_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    select
        mth_end_dt,
        cast(mort_num as varchar(80)) as acct_num,
        src_sys_cd
    from mortgage_mth_snapshot
    where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ("mth_end_dt", pa.date64()),
        ("acct_num", pa.string()),
        ("src_sys_cd", pa.string()),
    ]),
    target="mortgage_mth_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_mortgage_mth_snapshot():
    pass


@task.beeline(
    task_id="get_tsys_revlvng_credit_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    select
        mth_end_dt,
        cast(acct_num as varchar(80)) as acct_num,
        src_sys_cd
    from tsys_revlvng_credit_mth_snapshot
    where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ("mth_end_dt", pa.date64()),
        ("acct_num", pa.string()),
        ("src_sys_cd", pa.string()),
    ]),
    target="tsys_revlvng_credit_mth_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_tsys_revlvng_credit_mth_snapshot():
    pass


@task.beeline(
    task_id="get_psnl_loan_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    select
        mth_end_dt,
        cast(acct_num as varchar(80)) as acct_num,
        src_sys_cd
    from psnl_loan_mth_snapshot
    where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    group by mth_end_dt, acct_num, src_sys_cd;
    """,
    schema=pa.schema([
        ("mth_end_dt", pa.date64()),
        ("acct_num", pa.string()),
        ("src_sys_cd", pa.string()),
    ]),
    target="psnl_loan_mth_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_psnl_loan_mth_snapshot():
    pass


@task.beeline(
    task_id="get_tng_mort_acct_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    select
        mth_end_dt,
        cast(acct_id as varchar(80)) as acct_num,
        src_sys_cd
    from tng_mort_acct_mth_snapshot
    where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    group by mth_end_dt, acct_id, src_sys_cd;
    """,
    schema=pa.schema([
        ("mth_end_dt", pa.date64()),
        ("acct_num", pa.string()),
        ("src_sys_cd", pa.string()),
    ]),
    target="tng_mort_acct_mth_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_tng_mort_acct_mth_snapshot():
    pass


@task.parquet(
    task_id="odbc_ez_cust_acct_rltnp",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/odbc_ez_cust_acct_rltnp.parquet",
    sql="""
    SELECT
        mth_end_dt,
        acct_num,
        CASE trim(src_sys_cd)
            WHEN 'KQ_TSYS' THEN cast('TSYS-rev' as varchar(20))
            WHEN 'TSYS' THEN cast('TSYS-rev' as varchar(20))
            ELSE trim(src_sys_cd)
        END AS src_sys_cd
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_cust_acct_rltnp', key='parquet') }}'

    UNION

    SELECT
        mth_end_dt,
        acct_num,
        cast('GZ' as varchar(20)) as src_sys_cd
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_mortgage_mth_snapshot', key='parquet') }}'

    UNION

    SELECT
        mth_end_dt,
        acct_num,
        CASE trim(src_sys_cd)
            WHEN 'KQ_TSYS' THEN cast('TSYS-rev' as varchar(20))
            WHEN 'TSYS' THEN cast('TSYS-rev' as varchar(20))
            ELSE cast('KQ' as varchar(20))
        END AS src_sys_cd
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_tsys_revlvng_credit_mth_snapshot', key='parquet') }}'

    UNION

    SELECT
        mth_end_dt,
        acct_num,
        cast('SL' as varchar(20)) as src_sys_cd
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_psnl_loan_mth_snapshot', key='parquet') }}'

    UNION

    SELECT
        mth_end_dt,
        acct_num,
        cast('TNG_MTG' as varchar(20)) as src_sys_cd
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_tng_mort_acct_mth_snapshot', key='parquet') }}'
    """,
    export_params={},
    clear_before_write=True,
)
def odbc_ez_cust_acct_rltnp():
    pass


@task.parquet(
    task_id="remove_tsys_dupes",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/remove_tsys_dupes.parquet",
    sql="""
    SELECT DISTINCT
        LPAD(TRIM(acct_num), 23, '0') AS acct_num,
        src_sys_cd,
        mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.odbc_ez_cust_acct_rltnp', key='parquet') }}'
    WHERE src_sys_cd = 'TSYS-rev'
    """,
    export_params={},
    clear_before_write=True,
)
def remove_tsys_dupes():
    pass


@task.parquet(
    task_id="select_other_sources",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/select_other_sources.parquet",
    sql="""
    SELECT *
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.odbc_ez_cust_acct_rltnp', key='parquet') }}'
    WHERE src_sys_cd != 'TSYS-rev'
    """,
    export_params={},
    clear_before_write=True,
)
def select_other_sources():
    pass


@task.parquet(
    task_id="xfm",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/airb_acct_dim.parquet",
    sql="""
    SELECT
        ANY_VALUE(acct_num) as acct_num,
        src_sys_cd
    FROM (
        SELECT
            acct_num,
            CASE src_sys_cd
                WHEN 'TSYS-rev' THEN 'KQ'
                ELSE src_sys_cd
            END AS src_sys_cd
        FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.remove_tsys_dupes', key='parquet') }}'

        UNION

        SELECT
            acct_num,
            src_sys_cd
        FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.select_other_sources', key='parquet') }}'
    )
    GROUP BY LPAD(TRIM(acct_num), 23, '0'), src_sys_cd
    """,
    export_params={},
    clear_before_write=True,
)
def xfm():
    pass


@task.beeline(
    task_id="kq_tkq_ks_tsys_xref",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.TSZ_SCHEMA }};
    select
        bcm_acct_num,
        tsys_acct_id,
        client_prod_cd,
        tsys_prod_cd,
        emp_cd,
        ks_plastic_card_num,
        bcm_prod_cd,
        bcm_sub_prod_cd,
        bcm_block_reclass,
        tsys_cust_id,
        tsys_cust_type_cd,
        tsys_plastic_card_num,
        transfer_from_acct_num,
        bns_cust_id,
        conversion_dt,
        end_of_chain_indicator,
        businesseffectivedate
    from kq_tkq_ks_tsys_xref
    where businesseffectivedate = '2025-08-16' and end_of_chain_indicator = 'Y';
    """,
    schema=pa.schema([
        ("bcm_acct_num", pa.string()),
        ("tsys_acct_id", pa.string()),
        ("client_prod_cd", pa.string()),
        ("tsys_prod_cd", pa.string()),
        ("emp_cd", pa.string()),
        ("ks_plastic_card_num", pa.string()),
        ("bcm_prod_cd", pa.string()),
        ("bcm_sub_prod_cd", pa.string()),
        ("bcm_block_reclass", pa.string()),
        ("tsys_cust_id", pa.string()),
        ("tsys_cust_type_cd", pa.string()),
        ("tsys_plastic_card_num", pa.string()),
        ("transfer_from_acct_num", pa.string()),
        ("bns_cust_id", pa.string()),
        ("conversion_dt", pa.string()),
        ("end_of_chain_indicator", pa.string()),
        ("businesseffectivedate", pa.date64()),
    ]),
    target="kq_tkq_ks_tsys_xref.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def kq_tkq_ks_tsys_xref():
    pass



@task.parquet(
    task_id="join_basel_acct_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_basel_acct_id.parquet",
    sql="""
    SELECT
        airb.*, iias.basel_acct_id
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.odbc_ez_cust_acct_rltnp', key='parquet') }}' as airb
    LEFT JOIN ingestion.BASEL_ACCT_DIM as iias
        ON lpad(trim(iias.app_id), 23, '0') = lpad(trim(airb.acct_num), 23, '0')
        AND trim(iias.app_cd) = trim(
            CASE airb.src_sys_cd
                WHEN 'KQ_TSYS' THEN 'KS'
                WHEN 'TSYS-rev' THEN 'KS'
            END
        )
    WHERE iias.basel_acct_id is null and src_sys_cd in ('KQ_TSYS', 'TSYS-rev')
    """,
    export_params={},
    clear_before_write=True,
)
def join_basel_acct_id():
    pass


@task.parquet(
    task_id="join_xref",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_xref.parquet",
    sql="""
    SELECT j.*
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.join_basel_acct_id', key='parquet') }}' as j
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.kq_tkq_ks_tsys_xref', key='parquet') }}' as m1
        ON lpad(trim(j.acct_num), 23, '0') = lpad(trim(m1.bcm_acct_num), 23, '0')
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.kq_tkq_ks_tsys_xref', key='parquet') }}' as m2
        ON lpad(trim(j.acct_num), 23, '0') = lpad(trim(m2.tsys_acct_id), 23, '0')
    WHERE m1.tsys_acct_id is null and m2.tsys_acct_id is null
    """,
    export_params={},
    clear_before_write=True,
)
def join_xref():
    pass


@task.parquet(
    task_id="tsys_net_new_report",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/tsys_net_new_report.parquet",
    sql="""
    SELECT
        ANY_VALUE(acct_num) as acct_num,
        CASE
            WHEN COUNT(CASE WHEN src_sys_cd = 'TSYS-rev' THEN 1 END) > 0
                AND COUNT(CASE WHEN src_sys_cd = 'KQ_TSYS' THEN 1 END) > 0 THEN 'both'
            WHEN COUNT(CASE WHEN src_sys_cd = 'TSYS-rev' THEN 1 END) > 0 THEN 'prod_rcrr1.tsys_revlvng_credit_mth_snapshot'
            WHEN COUNT(CASE WHEN src_sys_cd = 'KQ_TSYS' THEN 1 END) > 0 THEN 'ez1.cust_acct_rltnp'
        END AS tsys_cd_origin,
        now() AS date_converted,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' AS mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.join_xref', key='parquet') }}'
    WHERE src_sys_cd in ('TSYS-rev', 'KQ_TSYS')
    GROUP BY lpad(trim(acct_num), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def tsys_net_new_report():
    pass


create_sq003_rundir = create_sq003_rundir()
get_cust_acct_rltnp = get_cust_acct_rltnp()
get_mortgage_mth_snapshot = get_mortgage_mth_snapshot()
get_tsys_revlvng_credit_mth_snapshot = get_tsys_revlvng_credit_mth_snapshot()
get_psnl_loan_mth_snapshot = get_psnl_loan_mth_snapshot()
get_tng_mort_acct_mth_snapshot = get_tng_mort_acct_mth_snapshot()
odbc_ez_cust_acct_rltnp = odbc_ez_cust_acct_rltnp()
remove_tsys_dupes = remove_tsys_dupes()
select_other_sources = select_other_sources()
xfm = xfm()
kq_tkq_ks_tsys_xref = kq_tkq_ks_tsys_xref()
join_basel_acct_id = join_basel_acct_id()
join_xref = join_xref()
tsys_net_new_report = tsys_net_new_report()


create_sq003_rundir >> [
    get_cust_acct_rltnp,
    get_mortgage_mth_snapshot,
    get_tsys_revlvng_credit_mth_snapshot,
    get_psnl_loan_mth_snapshot,
    get_tng_mort_acct_mth_snapshot,
    kq_tkq_ks_tsys_xref,
]

[
    get_cust_acct_rltnp,
    get_mortgage_mth_snapshot,
    get_tsys_revlvng_credit_mth_snapshot,
    get_psnl_loan_mth_snapshot,
    get_tng_mort_acct_mth_snapshot,
] >> odbc_ez_cust_acct_rltnp

odbc_ez_cust_acct_rltnp >> [remove_tsys_dupes, select_other_sources] >> xfm

odbc_ez_cust_acct_rltnp >> join_basel_acct_id
[join_basel_acct_id, kq_tkq_ks_tsys_xref] >> join_xref >> tsys_net_new_report
