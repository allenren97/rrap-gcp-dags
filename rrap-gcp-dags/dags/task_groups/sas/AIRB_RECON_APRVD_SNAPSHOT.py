import pyarrow as pa
import os
import duckdb as ddb
import logging

from airflow.sdk import task, task_group
from airflow.sdk import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowException

# from bns.rrap.operators.beeline import BeelineParquetExportOperator
# from bns.rrap.hooks.db2 import Db2ClpHook
# from bns.rrap.operators.db2 import Db2ParquetExportOperator
# from bns.rrap.operators.duckdb import DuckDbUpdateParquetOperator

from bns.rrap.operators.empty import BeelineParquetExportOperator
from bns.rrap.operators.empty import Db2ClpHook
from bns.rrap.operators.empty import Db2ParquetExportOperator
from bns.rrap.operators.empty import DuckDbUpdateParquetOperator

#from task_groups.gl_recon.utils import bulk_export_to_parquet

@task_group(group_id="AIRB_RECON_APRVD_SNAPSHOT")
def AIRB_RECON_APRVD_SNAPSHOT(**kwargs):
    MSSQL_CONN_ID = "airb_recon"
    DB2_CONN_ID = "db2-conn"


    @task(task_id="check_airb_approvals")
    def check_airb_approvals(**kwargs):
        pass
#         MTH_END_DT = kwargs["var"]["value"].get("MTH_END_DT")
#
#         sql_query = f"""
#             SELECT COUNT(DISTINCT NTZA_Src_Sys_Cd) AS cnt
#             FROM dbo.AIRB_RECON_APRVD_STATUS
#             WHERE Approval_For_Month_Of = '{MTH_END_DT}'
#                 AND NTZA_Src_Sys_Cd IN ('MO','SPL','KS')
#         """
#
#         hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
#         records = hook.get_records(sql=sql_query)
#
#         cnt = records[0][0] if records and records[0] else 0
#
#         if cnt == 3:
#             logging.warning("All approvals are in place. Cleared to proceed.")
#             return
#
#         raise AirflowException(
#             f"Approvals not ready for {MTH_END_DT}. "
#             f"Found {cnt}/3 source systems. Needs retry."
#         )
    
    @task(task_id="extract_snapshot")
    def extract_snapshot(**kwargs):
        pass
#         MTH_END_DT = kwargs["var"]["value"].get("MTH_END_DT")
#
#         sql = f"""
#             WITH tmp AS (
#                 SELECT
#                     CAST(MTH_END_DT AS DATE) AS MTH_END_DT,
#                     GL_ACCT_NUM,
#                     GL_TRNST_NUM,
#                     NTZA_SRC_SYS_CD AS SRC_SYS_CD,
#                     CRNCY_CD,
#                     BATCH_ID,
#                     APPROVED_BY AS APRVL_USR_ID,
#                     APPROVED_TIMESTAMP AS APRVL_TMSTMP,
#                     AIRB_ADJ_COA_AMT
#                 FROM dbo.AIRB_RECON_APRVD_SNAPSHOT
#                 WHERE CAST(MTH_END_DT AS DATE) = '{MTH_END_DT}'
#             )
#             -- deduplicate
#             SELECT
#                 MTH_END_DT,
#                 GL_ACCT_NUM,
#                 GL_TRNST_NUM,
#                 SRC_SYS_CD,
#                 CRNCY_CD,
#                 BATCH_ID,
#                 APRVL_USR_ID,
#                 APRVL_TMSTMP,
#                 AIRB_ADJ_COA_AMT
#             FROM tmp
#             GROUP BY
#                 MTH_END_DT,
#                 GL_ACCT_NUM,
#                 GL_TRNST_NUM,
#                 SRC_SYS_CD,
#                 CRNCY_CD,
#                 BATCH_ID,
#                 APRVL_USR_ID,
#                 APRVL_TMSTMP,
#                 AIRB_ADJ_COA_AMT
#         """
#
#         schema = pa.schema(
#             [
#                 ("MTH_END_DT", pa.date64()),
#                 ("GL_ACCT_NUM", pa.string()),
#                 ("GL_TRNST_NUM", pa.string()),
#                 ("SRC_SYS_CD", pa.string()),
#                 ("CRNCY_CD", pa.string()),
#                 ("BATCH_ID", pa.int64()),
#                 ("APRVL_USR_ID", pa.string()),
#                 ("APRVL_TMSTMP", pa.timestamp("ns")),
#                 ("AIRB_ADJ_COA_AMT", pa.float64())
#                 ]
#         )
#
#         bulk_export_to_parquet(
#             conn_id=MSSQL_CONN_ID,
#             sql=sql,
#             target_parquet="AIRB_RECON_APRVD_SNAPSHOT.parquet",
#             schema=schema
#         )

    @task(task_id="load_snapshot")
    def load_snapshot(**kwargs):
        pass
#         MTH_END_DT = kwargs["var"]["value"].get("MTH_END_DT")
#         wd = kwargs["var"]["value"].get("RUNDIR")
#
#         tsv = os.path.join(wd, f"AIRB_RECON_APRVD_SNAPSHOT.tsv")
#
#         db2hook = Db2ClpHook(db2_conn_id=DB2_CONN_ID)
#
#         try:
#             db2hook.exec_sql(
#                 f"""DELETE FROM {kwargs['params']['EDW_schema_EDRTLRP1D']}.AIRB_RECON_APRVD_SNAPSHOT
#                     WHERE MTH_END_DT = '{MTH_END_DT}';"""
#             )
#         except RuntimeError as e:
#             logging.warning(
#                 f"No previous records were found in { kwargs['params']['EDW_schema_EDRTLRP1D'] }.AIRB_RECON_APRVD_SNAPSHOT to remove"
#             )
#
#
#         columns = [
#             "MTH_END_DT",
#             "GL_ACCT_NUM",
#             "GL_TRNST_NUM",
#             "SRC_SYS_CD",
#             "CRNCY_CD",
#             "BATCH_ID",
#             "APRVL_USR_ID",
#             "APRVL_TMSTMP",
#             "AIRB_ADJ_COA_AMT"
#         ]
#
#         query = f"""
#                 COPY (SELECT
#                         MTH_END_DT,
#                         GL_ACCT_NUM,
#                         GL_TRNST_NUM,
#                         SRC_SYS_CD,
#                         CRNCY_CD,
#                         BATCH_ID,
#                         APRVL_USR_ID,
#                         strftime(APRVL_TMSTMP, '%Y-%m-%d %H:%M:%S') AS APRVL_TMSTMP,
#                         AIRB_ADJ_COA_AMT
#                 FROM '{wd}/AIRB_RECON_APRVD_SNAPSHOT.parquet')
#                 TO '{tsv}'
#                 (DELIMITER '\t', HEADER FALSE);
#         """
#         logging.warning("Executing duckdb statement: %s", query)
#         ddb.query(query)
#
#         db2hook.from_tsv(
#             tsv,
#             f"{kwargs['params']['EDW_schema_EDRTLRP1D']}.AIRB_RECON_APRVD_SNAPSHOT",
#             columns,
#             timestampformat="YYYY-MM-DD HH:MM:SS:UUUUUU"
#         )


    check_airb_approvals() >> extract_snapshot() >> load_snapshot()



