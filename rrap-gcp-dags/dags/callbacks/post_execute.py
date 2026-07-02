import logging, os
import duckdb as ddb
from pathlib import Path
from functools import partial
from yaml import safe_load

from airflow.models import Variable

VALIDATE_OUTPUT_YAML = '/bns/rrap/apps/rrap-airflow/config/validate_output.yaml'

def validate_non_empty_output(context, result=None, raise_exception=True):
    """This checks the 'target' attribute of the task to ensure that the output file is not empty and exists."""
    ti = context['task_instance']
    target_arg = None
    if 'target' in context and type(context['target']) is str:
        target_arg = context['target']
    elif hasattr(ti.task, 'target') and type(ti.task.target) is str:
        target_arg = ti.task.target
    else:
        logging.warning("'validate_non_empty_output' check is skipped since this task has no 'target' attribute")
        return
    
    # basedir/mth_end_dt is the default output path across all operators
    wd = Variable.get('RUNDIR')
    target = os.path.join(wd, target_arg)
    logging.warning(f"Checking output of {target}")
    
    # empty suffix = directory of parquets
    if Path(target).suffix == '': 
        target = f"{target}/*.parquet"

    # duckdb will handle file types for .tsv as well
    n_rows = ddb.sql(f"SELECT count(*) FROM '{target}';").fetchone()[0]
    
    # Check if task and/or DAG is allowed to pass with an empty parquet file
    with open(VALIDATE_OUTPUT_YAML, 'r') as f:
        validate_output_cfg = safe_load(f)
        pass_with_empty = validate_output_cfg["PASS_WITH_EMPTY_PARQUET"]

    if n_rows == 0 and raise_exception:
        if ti.dag_id not in pass_with_empty or ti.task_id not in pass_with_empty[ti.dag_id]:
            raise Exception(f"No records found in output file {target}")
    
    logging.warning(f"\n{n_rows} records found in {target}")

log_table_row_count = partial(validate_non_empty_output, raise_exception=False)
