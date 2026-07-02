import logging, re
from airflow.exceptions import AirflowSkipException

def skip_task_check(context):
    task_id = context['task_instance'].task_id

    for skip_re in context['params']['skip']:
        skip_re = skip_re.strip()
        if skip_re == task_id:
            logging.warning(f"This task '{task_id}' was VERBATIM found in skip list. Skipping...")
            raise AirflowSkipException
        
        if re.fullmatch(skip_re, task_id):
            logging.warning(f"This task '{task_id}' matched with '{skip_re}' in the skip list. Skipping...")
            raise AirflowSkipException

    logging.warning(f"This task is NOT a skipped task - task_id={task_id}")
    logging.warning(f"List of skipped tasks include: {context['params']['skip']}")