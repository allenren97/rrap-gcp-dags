from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.METRPL_SCRI_VAL",
    "reference.DLGD_METRPL_THRSHD_VAL_DIM"]

DOWNSTREAM_ASSET = "features.METRPL_BREACH_F"
DEPENDENCIES = {
    "duckdb_clear": ["duckdb_load"]
}


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME FROM(
      SELECT
            m.METRPL_AREA_NM,
            CASE
                WHEN METRPL_SCRI_VAL > THRSHD_VAL THEN 'Y'
                else 'N'
            END AS METRPL_BREACH_F,
            OBSN_DT
        
        FROM {UPSTREAM_ASSET[0]} m
        LEFT JOIN {UPSTREAM_ASSET[1]} thresh
            ON trim(
                regexp_replace(
                translate(
                    lower(coalesce(thresh.METRPL_AREA_NM, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
            ) = m.METRPL_AREA_NM
            AND YRMTH >= thresh.EFF_FROM_YR_MTH and YRMTH < thresh.EFF_TO_YR_MTH
            AND thresh.CRNT_F = 'Y'
            )
    """
):
  pass