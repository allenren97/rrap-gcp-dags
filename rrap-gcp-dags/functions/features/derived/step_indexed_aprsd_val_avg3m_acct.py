from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.STEP_INDEXED_APRSD_VAL", ]
DOWNSTREAM_ASSET = "features.STEP_INDEXED_APRSD_VAL_AVG3M_ACCT"
DEPENDENCIES = {
    "duckdb_delete_step_indexed_aprsd_val_avg3m_acct": ["duckdb_load_step_indexed_aprsd_val_avg3m_acct"],
}


def duckdb_delete_step_indexed_aprsd_val_avg3m_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_step_indexed_aprsd_val_avg3m_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME
    WITH base AS (
        SELECT 
            STEP_PLN_AGRMNT_NUM,
            AVG(STEP_INDEXED_APRSD_VAL) AS STEP_INDEXED_APRSD_VAL_AVG3M_ACCT
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 2 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            STEP_PLN_AGRMNT_NUM
    )
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        STEP_PLN_AGRMNT_NUM, 
        STEP_INDEXED_APRSD_VAL_AVG3M_ACCT 
    FROM base
    """,
):
    pass

