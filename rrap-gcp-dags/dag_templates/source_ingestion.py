import pendulum
import logging
import os

from airflow.sdk import dag, get_current_context, task

from bns.rrap.hooks.duckdb import DuckLakeHook
from bns.rrap.helpers.dependency_utilities import _auto_wire_dependencies

deduped_imports("taskgroups/sequences/**/*.py")


logger = logging.getLogger(__name__)

@dag(
    dag_id="source_ingestion",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Toronto"),
    catchup=False,
    tags=["sequence", "source", "ingestion"],
    params={}
)
def source_ingestion():
    """
    Source ingestion DAG template.
    Each sequence previously migrated as part of TSYS to be re-written and converted
    to using a generic SQL adapter for IIAS replacement. The adapter can point to either
    MSSQL or DuckLake as the tables for enriching source data being ingested.
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

        logger.warning(f"Rundate: {rundate}, MTH_TM_ID: {mth_tm_id}")

        prev_mth_tm_id = mth_tm_id - 40
        popn_dt = context['logical_date'].strftime('%Y-%m-15')
        rundir = f"/bns/rrap/data/source_ingestion/{rundate}"
        os.makedirs(rundir, exist_ok=True)

        context['ti'].xcom_push(key='MTH_TM_ID', value=mth_tm_id)
        context['ti'].xcom_push(key='PREV_MTH_TM_ID', value=prev_mth_tm_id)
        context['ti'].xcom_push(key='RUNDATE', value=rundate)
        context['ti'].xcom_push(key='POPN_DT', value=popn_dt)
        context['ti'].xcom_push(key='RUNDIR', value=rundir)
        context['ti'].xcom_push(key='MTH_END_DT', value=rundate)


    handle_month_context = handle_month_context()

    """
    Import TaskGroups for each sequence. Wire dependencies between each sequence based 
    on the order of execution and create manual fail tasks before each sequence.
    """
    import_contents("taskgroups/sequences/sq001/group.py")
    import_contents("taskgroups/sequences/sq002/group.py")
    import_contents("taskgroups/sequences/sq003/group.py")
    import_contents("taskgroups/sequences/sq004/group.py")
    import_contents("taskgroups/sequences/sq005/group.py")
    import_contents("taskgroups/sequences/sq006/group.py")
    import_contents("taskgroups/sequences/sq007/group.py")
    import_contents("taskgroups/sequences/sq008/group.py")
    import_contents("taskgroups/sequences/sq011/group.py")
    import_contents("taskgroups/sequences/sq015/group.py")
    import_contents("taskgroups/sequences/sq016/group.py")
    import_contents("taskgroups/sequences/sq018/group.py")
    import_contents("taskgroups/sequences/sq019/group.py")
    import_contents("taskgroups/sequences/sq020/group.py")
    import_contents("taskgroups/sequences/sq023/group.py")
    import_contents("taskgroups/sequences/sq033/group.py")
    import_contents("taskgroups/sequences/sq034/group.py")
    import_contents("taskgroups/sequences/sq035/group.py")
    import_contents("taskgroups/sequences/sq036/group.py")
    import_contents("taskgroups/sequences/sq037/group.py")
    import_contents("taskgroups/sequences/sq043/group.py")
    import_contents("taskgroups/sequences/sq044/group.py")
    import_contents("taskgroups/sequences/sq0051/group.py")
    import_contents("taskgroups/sequences/sq083/group.py")

    """
    Dependencies from handle_month_context to the first sequence
    """
    handle_month_context >> [
        sq0051, sq035, sq083, sq043,
        sq044, sq037, sq036, sq002, sq003,
        sq004, sq015, sq033, sq034, sq018,
        sq011, sq020, sq019, sq006, sq016,
        sq005, sq023, sq001, sq008, sq007
    ]

    """ Actual dependencies between sequences based on data."""
    basel_cust_dim >> [
        sq004, 
        sq005,
        sq006,
        sq007
    ]
    basel_acct_dim >> [
        sq004,
        sq006,
        sq007,
        sq008,
        sq016,
        sq0051
    ]
    org_unit_dim >> [
        sq005,
        sq006,
        sq007,
        sq008
    ]
    basel_step_pln_mth_snapshot >> [
        sq006,
        sq007,
        sq008
    ]
    basel_cust_acct_rltnp_snapshot >> [
        sq008
    ]


source_ingestion()