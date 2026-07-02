from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task, task_group, get_current_context

from bns.rrap.helpers.batch_utils import batch_insert
from bns.rrap.hooks.duckdb import DuckLakeHook

import logging
import pendulum


MSSQL_CONN_ID = "airb_recon"

def _BASEL_SEC_ADJ_FACTR_MTH_SNAP(rundate: str, mth_tm_id: int):
    batch_insert(
        ddb_query=f"""
WITH
t1 AS (
    SELECT 'AUTO' AS SECRTZTN_TP_CD, 'AUTO' AS SECRTZTN_TP_DESC, {mth_tm_id} AS MTH_TM_ID, '{rundate}'::DATE AS EFFECTIVE_DATE
    UNION
    SELECT 'CC' AS SECRTZTN_TP_CD, 'CREDIT CARD' AS SECRTZTN_TP_DESC, {mth_tm_id} AS MTH_TM_ID, '{rundate}'::DATE AS EFFECTIVE_DATE
    UNION
    SELECT 'CL' AS SECRTZTN_TP_CD, 'CREDIT LINE' AS SECRTZTN_TP_DESC, {mth_tm_id} AS MTH_TM_ID, '{rundate}'::DATE AS EFFECTIVE_DATE
)
SELECT
    t1.SECRTZTN_TP_CD,
    t1.MTH_TM_ID,
    t1.SECRTZTN_TP_DESC,
    CAST(t2.TOT_SECRTZTN_AMT_TO_ADJUST AS DECIMAL(18,2)) AS TOT_SECRTZTN_AMT_TO_ADJUST,
    CAST(t3.TOT_ADJUSTED_OS_BAL_IN_CCAR AS DECIMAL(17,2)) AS TOT_ADJUSTED_OS_BAL_IN_CCAR,
    CAST(t4.SECRTZTN_OS_ADJ_FACTR AS DECIMAL(17,10)) AS SECRTZTN_OS_ADJ_FACTR,
    null AS SECRTZTN_CR_LMT_ADJ_FACTR,
    CURRENT_TIME AS INSRT_PROCESS_TMSTMP,
    CURRENT_TIME AS UPDT_PROCESS_TMSTMP
FROM t1
INNER JOIN features.TOT_SECRTZTN_AMT_TO_ADJUST t2 ON
    t1.EFFECTIVE_DATE = t2.OBSN_DT
    AND t1.SECRTZTN_TP_CD = t2.SECRTZTN_TP_CD
INNER JOIN features.TOT_ADJUSTED_OS_BAL_IN_CCAR t3 ON
    t1.EFFECTIVE_DATE = t3.OBSN_DT
    AND t1.SECRTZTN_TP_CD = t3.SECRTZTN_TP_CD
INNER JOIN features.SECRTZTN_OS_ADJ_FACTR t4 ON
    t1.EFFECTIVE_DATE = t4.OBSN_DT
    AND t1.SECRTZTN_TP_CD = t4.SECRTZTN_TP_CD
        """,
        mssql_schema="EDRRAPT",
        mssql_table="BASEL_SEC_ADJ_FACTR_MTH_SNAP",
    )


def _BASEL_CC_SEC_ACCT_MTH_SNAP(rundate: str, mth_tm_id: int):
    batch_insert(
        ddb_query=f"""
select
    {mth_tm_id} AS MTH_TM_ID,
    TRIM(CAST(t1.ACCOUNT_NUMBER AS VARCHAR)) AS ACCT_NUM_RECVD,
    t2.BASEL_ACCT_ID AS BASEL_ACCT_ID_CCAR_MATCHED,
    CURRENT_TIME AS INSRT_PROCESS_TMSTMP,
    CURRENT_TIME AS UPDT_PROCESS_TMSTMP,
    'AUTO' AS SECRTZTN_TP_CD,
    t1.SECURITIZATION_DATE
from ingestion.SPL_SOURCE_FILE_ACCOUNTS t1
left outer join (
    SELECT
        MTH_TM_ID,
        BASEL_ACCT_ID,
        TRY_CAST(
            COALESCE(TRIM(CRNT_BR_LOCTN_TRNST), '') || COALESCE(TRIM(LOAN_NUM), '') AS BIGINT
        ) AS UNIQUE_ACCOUNTS
    FROM (
        SELECT
            ss.MTH_TM_ID,
            ss.BASEL_ACCT_ID,
            ss.CRNT_BR_LOCTN_TRNST,
            ss.LOAN_NUM
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT ss
        LEFT OUTER JOIN (select * from features.TREATMENT_F where OBSN_DT = '{rundate}') t1 ON
            ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.PIT_STATUS_ACCOUNT where OBSN_DT = '{rundate}') t2 ON
            ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.TRNST_EXCLSN_F where OBSN_DT = '{rundate}') t3 ON
            ss.BASEL_ACCT_ID = t3.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.PRD_ID where OBSN_DT = '{rundate}') t4 ON
            ss.BASEL_ACCT_ID = t4.BASEL_ACCT_ID
        WHERE
            ss.MTH_TM_ID = {mth_tm_id}
            AND ss.TOT_CRNT_BAL_AMT >= 0
            AND t1.TREATMENT_F = 'A'
            AND t2.PIT_STATUS_ACCOUNT IN ('CUR','DEF')
            AND t3.TRNST_EXCLSN_F = 'N'
            AND t4.PRD_ID IN ('S09', 'S10')
    )
) t2 ON
    t1.ACCOUNT_NUMBER = t2.UNIQUE_ACCOUNTS
where 1=1
    and effective_date = '{rundate}'
    and coalesce(account_number,'') != ''
union
select
    {mth_tm_id} AS MTH_TM_ID,
    CAST(t1.VISA_ACCT_NUM AS VARCHAR) AS ACCT_NUM_RECVD,
    t2.BASEL_ACCT_ID AS BASEL_ACCT_ID_CCAR_MATCHED,
    CURRENT_TIME AS INSRT_PROCESS_TMSTMP,
    CURRENT_TIME AS UPDT_PROCESS_TMSTMP,
    'CC' AS SECRTZTN_TP_CD,
    null AS SECURITIZATION_DATE
from ingestion.CC_SOURCE_FILE_ACCOUNTS t1
left outer join (
    SELECT
        MTH_TM_ID,
        BASEL_ACCT_ID,
        TRY_CAST(
            SUBSTR(CONCAT('CA','0201', COALESCE(ACCT_NUM,'')), 17, 13) AS BIGINT
        ) AS UNIQUE_ACCOUNTS
    FROM (
        SELECT
            ss.MTH_TM_ID,
            ss.BASEL_ACCT_ID,
            ss.ACCT_NUM
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
        LEFT OUTER JOIN (select * from features.CONSM_PRD_TREATMNT_CD where OBSN_DT = '{rundate}') t1 ON
            ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.SML_BUS_F where OBSN_DT = '{rundate}') t2 ON
            ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.PIT_STATUS_ACCOUNT where OBSN_DT = '{rundate}') t3 ON
            ss.BASEL_ACCT_ID = t3.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.TRNST_EXCLSN_F where OBSN_DT = '{rundate}') t4 ON
            ss.BASEL_ACCT_ID = t4.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.BASEL_PRD_TP_CD where OBSN_DT = '{rundate}') t5 ON
            ss.BASEL_ACCT_ID = t5.BASEL_ACCT_ID
        WHERE
            ss.MTH_TM_ID = {mth_tm_id}
            AND ss.TOT_NEW_BAL_AMT >= 0
            AND t1.CONSM_PRD_TREATMNT_CD = 'A'
            AND t2.SML_BUS_F = 'N'
            AND t3.PIT_STATUS_ACCOUNT IN ('CUR','DEF')
            AND t4.TRNST_EXCLSN_F ='N'
            AND t5.BASEL_PRD_TP_CD = 'CARD'
    )
) t2 ON
    t1.VISA_ACCT_NUM = t2.UNIQUE_ACCOUNTS
where 1=1
    and effective_date = '{rundate}'
    and coalesce(visa_acct_num,'') != ''
union
select
    {mth_tm_id} AS MTH_TM_ID,
    CAST(t1.VISA_ACCT_NUM AS VARCHAR) AS ACCT_NUM_RECVD,
    t2.BASEL_ACCT_ID AS BASEL_ACCT_ID_CCAR_MATCHED,
    CURRENT_TIME AS INSRT_PROCESS_TMSTMP,
    CURRENT_TIME AS UPDT_PROCESS_TMSTMP,
    'CL' AS SECRTZTN_TP_CD,
    null AS SECURITIZATION_DATE
from ingestion.CL_SOURCE_FILE_ACCOUNTS t1
left outer join (
    SELECT
        MTH_TM_ID,
        BASEL_ACCT_ID,
        TRY_CAST(
            SUBSTR(CONCAT('CA','0201', COALESCE(ACCT_NUM,'')), 17, 13) AS BIGINT
        ) AS UNIQUE_ACCOUNTS
    FROM (
        SELECT
            ss.MTH_TM_ID,
            ss.BASEL_ACCT_ID,
            ss.ACCT_NUM
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
        LEFT OUTER JOIN (select * from features.CONSM_PRD_TREATMNT_CD where OBSN_DT = '{rundate}') t1 ON
            ss.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.SML_BUS_F where OBSN_DT = '{rundate}') t2 ON
            ss.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.PIT_STATUS_ACCOUNT where OBSN_DT = '{rundate}') t3 ON
            ss.BASEL_ACCT_ID = t3.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.TRNST_EXCLSN_F where OBSN_DT = '{rundate}') t4 ON
            ss.BASEL_ACCT_ID = t4.BASEL_ACCT_ID
        LEFT OUTER JOIN (select * from features.PRD_ID where OBSN_DT = '{rundate}') t5 ON
            ss.BASEL_ACCT_ID = t5.BASEL_ACCT_ID
        WHERE
            ss.MTH_TM_ID = {mth_tm_id}
            AND ss.TOT_NEW_BAL_AMT >= 0
            AND t1.CONSM_PRD_TREATMNT_CD = 'A'
            AND t2.SML_BUS_F = 'N'
            AND t3.PIT_STATUS_ACCOUNT IN ('CUR','DEF')
            AND t4.TRNST_EXCLSN_F ='N'
            AND t5.PRD_ID IN ('KS33','KS35','KS123','KS125')
    )
) t2 ON
    t1.VISA_ACCT_NUM = t2.UNIQUE_ACCOUNTS
where 1=1
    and effective_date = '{rundate}'
    and coalesce(visa_acct_num,'') != ''
        """,
        mssql_schema="EDRRAPT",
        mssql_table="BASEL_CC_SEC_ACCT_MTH_SNAP",
    )

def log_sql(sql):
    logging.warning(f"executing:\n{sql}")
    return sql


@dag(
    dag_id="2.Life_Raft",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Toronto"),
    catchup=False,
    #tags=["sequence", "source", "ingestion"],
    params={},
    user_defined_macros={
        "log_sql": log_sql
    },
)
def life_raft():
    """
    life raft
    """

    @task()
    def handle_month_context():
        """
        Task to create XComs (mth_tm_id, rundate, etc.) for the DAG run.
        """
        context = get_current_context()
        rundate = context['logical_date'].subtract(months=1).end_of('month').\
                    strftime('%Y-%m-%d')

        hook = DuckLakeHook(duckdb_conn_id='duckdb-conn')
        mth_tm_id = hook.duckdb.sql(f"""
            SELECT TM_ID FROM ingestion.TM_DIM
            WHERE TM_LVL = 'Month' AND TM_LVL_END_DT = '{rundate}'
        """).fetchone()[0]

        logging.warning(f"Rundate: {rundate}, MTH_TM_ID: {mth_tm_id}")

        prev_mth_tm_id = mth_tm_id - 40
        popn_dt = context['logical_date'].strftime('%Y-%m-15')
#         rundir = f"/bns/rrap/data/source_ingestion/{rundate}"
#         os.makedirs(rundir, exist_ok=True)

        context['ti'].xcom_push(key='MTH_TM_ID', value=mth_tm_id)
        context['ti'].xcom_push(key='PREV_MTH_TM_ID', value=prev_mth_tm_id)
        context['ti'].xcom_push(key='RUNDATE', value=rundate)
        context['ti'].xcom_push(key='POPN_DT', value=popn_dt)
#         context['ti'].xcom_push(key='RUNDIR', value=rundir)


    @task_group(group_id="BASEL_CC_SEC_ACCT_MTH_SNAP")
    def BASEL_CC_SEC_ACCT_MTH_SNAP():

        delete = SQLExecuteQueryOperator(
            task_id="delete",
            conn_id=MSSQL_CONN_ID,
            sql="""
            {{ log_sql(
                "DELETE FROM EDRRAPT.BASEL_CC_SEC_ACCT_MTH_SNAP "
                ~ "WHERE MTH_TM_ID = "
                ~ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID')
            ) }}
            """,
        )

        stitch = PythonOperator(
            task_id='stitch',
            python_callable=_BASEL_CC_SEC_ACCT_MTH_SNAP,
            op_kwargs={
                "rundate": '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDATE") }}',
                "mth_tm_id": '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }}'
            }
        )

        delete >> stitch


    @task_group(group_id="BASEL_SEC_ADJ_FACTR_MTH_SNAP")
    def BASEL_SEC_ADJ_FACTR_MTH_SNAP():

        delete = SQLExecuteQueryOperator(
            task_id="delete",
            conn_id=MSSQL_CONN_ID,
            sql="""
            {{ log_sql(
                "DELETE FROM EDRRAPT.BASEL_SEC_ADJ_FACTR_MTH_SNAP "
                ~ "WHERE MTH_TM_ID = "
                ~ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID')
            ) }}
            """,
        )

        stitch = PythonOperator(
            task_id='stitch',
            python_callable=_BASEL_SEC_ADJ_FACTR_MTH_SNAP,
            op_kwargs={
                "rundate": '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDATE") }}',
                "mth_tm_id": '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }}'
            }
        )

        delete >> stitch

    handle_month_context() >> [ BASEL_CC_SEC_ACCT_MTH_SNAP(), BASEL_SEC_ADJ_FACTR_MTH_SNAP() ]

life_raft()
