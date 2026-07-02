import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    "models.heloc_lgdd_segment",
    "models.heloc_lgdnd_segment",
    "models.mor_lgdd_segment",
    "models.mor_lgdnd_segment",
    "models.itl_lgdd_segment",
    "models.itl_lgdnd_segment",
    "models.cc_lgdd_segment",
    "models.cc_lgdnd_segment",
    "models.loc_lgdd_segment",
    "models.loc_lgdnd_segment",
    "models.dtl_lgdd_segment",
    "models.dtl_lgdnd_segment",
    "models.tng_mor_lgdd_segment",
    "models.tng_mor_lgdnd_segment",
    "models.ssla_lgdd_segment",
    "models.ssla_lgdnd_segment",
    "models.sslb_lgdd_segment",
    "models.sslb_lgdnd_segment",
    "models.step_heloc_lgdd_segment",
    "models.step_heloc_lgdnd_segment",
    "models.step_mix_mor_lgdd_segment",
    "models.step_mix_mor_lgdnd_segment",
    "models.standalone_mor_lgdd_segment",
    "models.standalone_mor_lgdnd_segment",
    "models.standalone_heloc_lgdd_segment",
    "models.standalone_heloc_lgdnd_segment",
    "features.BASEL_ACCT_ID",
    ]
DOWNSTREAM_ASSET = "instruments.LGD_BASEL_SEG_NUM"
DEPENDENCIES = {	
    "duckdb_clear": ["export_result"],
    "export_result": ["duckdb_load"],
}

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET }
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """
):
    pass

def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="lgd_basel_seg_num.export_result.sql",
    config_type="instrument",
):
    pass

"""
code for pulling benchmark for resl stream

SELECT 
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    BASEL_ACCT_ID,
    CASE
        WHEN LGD_MODEL_NM = 'BNS STEP HELOC LGDD' THEN 'step_heloc_lgdd'
        WHEN LGD_MODEL_NM = 'BNS STEP HELOC LGDND' THEN 'step_heloc_lgdnd'

        WHEN LGD_MODEL_NM = 'BNS BNS STP MORMIX LGDD' THEN 'step_mix_mor_lgdd'
        WHEN LGD_MODEL_NM = 'BNS BNS STP MORMIX LGDND' THEN 'step_mix_mor_lgdnd'

        WHEN LGD_MODEL_NM = 'BNS STP MORMIX LGDD' THEN 'step_mix_mor_lgdd'
        WHEN LGD_MODEL_NM = 'BNS STP MORMIX LGDND' THEN 'step_mix_mor_lgdnd'
        
        WHEN LGD_MODEL_NM = 'BNS MOR LGD-D' THEN 'standalone_mor_lgdd'
        WHEN LGD_MODEL_NM = 'BNS MOR LGD-ND' THEN 'standalone_mor_lgdnd'

        WHEN LGD_MODEL_NM = 'CC LGD-D' THEN 'cc_lgdd'
        WHEN LGD_MODEL_NM = 'CC LGD-ND' THEN 'cc_lgdnd'
        WHEN LGD_MODEL_NM = 'DTL LGD-D' THEN 'dtl_lgdd'
        WHEN LGD_MODEL_NM = 'DTL LGD-ND' THEN 'dtl_lgdnd'

        WHEN LGD_MODEL_NM = 'HELOC LGD-D' THEN 'standalone_heloc_lgdd'
        WHEN LGD_MODEL_NM = 'HELOC LGD-ND' THEN 'standalone_heloc_lgdnd'

        WHEN LGD_MODEL_NM = 'ITL LGD-D' THEN 'itl_lgdd'
        WHEN LGD_MODEL_NM = 'ITL LGD-ND' THEN 'itl_lgdnd'
        WHEN LGD_MODEL_NM = 'LOC LGD-D' THEN 'loc_lgdd'
        WHEN LGD_MODEL_NM = 'LOC LGD-ND' THEN 'loc_lgdnd'
        WHEN LGD_MODEL_NM = 'SL Type A LGD-D' THEN 'ssla_lgdd'
        WHEN LGD_MODEL_NM = 'SL Type A LGD-ND' THEN 'ssla_lgdnd'
        WHEN LGD_MODEL_NM = 'SL Type B LGD-D' THEN 'sslb_lgdd'
        WHEN LGD_MODEL_NM = 'SL Type B LGD-ND' THEN 'sslb_lgdnd'
        WHEN LGD_MODEL_NM = 'TNG MOR LGD-D' THEN 'tng_mor_lgdd'
        WHEN LGD_MODEL_NM = 'TNG MOR LGD-ND' THEN 'tng_mor_lgdnd'
    END AS MODEL,
    LGD_BASEL_SEG_NUM,
    'RESL' AS STREAM
FROM "/bns/rrap/data/historical_snapshot/DEV2_EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT_21076.parquet"
"""

"""
code for pulling benchmark data for non_resl stream

SELECT 
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    BASEL_ACCT_ID,
    CASE
        WHEN LGD_MODEL_NM = 'BNS MOR LGD-D' THEN 'mor_lgdd'
        WHEN LGD_MODEL_NM = 'BNS MOR LGD-ND' THEN 'mor_lgdnd'
        WHEN LGD_MODEL_NM = 'CC LGD-D' THEN 'cc_lgdd'
        WHEN LGD_MODEL_NM = 'CC LGD-ND' THEN 'cc_lgdnd'
        WHEN LGD_MODEL_NM = 'DTL LGD-D' THEN 'dtl_lgdd'
        WHEN LGD_MODEL_NM = 'DTL LGD-ND' THEN 'dtl_lgdnd'
        WHEN LGD_MODEL_NM = 'HELOC LGD-D' THEN 'heloc_lgdd'
        WHEN LGD_MODEL_NM = 'HELOC LGD-ND' THEN 'heloc_lgdnd'
        WHEN LGD_MODEL_NM = 'ITL LGD-D' THEN 'itl_lgdd'
        WHEN LGD_MODEL_NM = 'ITL LGD-ND' THEN 'itl_lgdnd'
        WHEN LGD_MODEL_NM = 'LOC LGD-D' THEN 'loc_lgdd'
        WHEN LGD_MODEL_NM = 'LOC LGD-ND' THEN 'loc_lgdnd'
        WHEN LGD_MODEL_NM = 'SL Type A LGD-D' THEN 'ssla_lgdd'
        WHEN LGD_MODEL_NM = 'SL Type A LGD-ND' THEN 'ssla_lgdnd'
        WHEN LGD_MODEL_NM = 'SL Type B LGD-D' THEN 'sslb_lgdd'
        WHEN LGD_MODEL_NM = 'SL Type B LGD-ND' THEN 'sslb_lgdnd'
        WHEN LGD_MODEL_NM = 'TNG MOR LGD-D' THEN 'tng_mor_lgdd'
        WHEN LGD_MODEL_NM = 'TNG MOR LGD-ND' THEN 'tng_mor_lgdnd'
    END AS MODEL,
    LGD_BASEL_SEG_NUM,
    'NON_RESL' AS STREAM
FROM "/bns/rrap/data/historical_snapshot/BASEL_ANALYTCL_BL_INSTRMNT_FACT_21076.parquet"
"""
    
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        select
            *
        from '{{{{ task_instance.xcom_pull(task_ids="fact__lgd_basel_seg_num.export_result", key="parquet") }}}}'
        )
    """,
):
    pass