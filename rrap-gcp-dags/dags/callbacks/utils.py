from functools import partial
from airflow.exceptions import AirflowException

def chain_callbacks(callbacks:list):
    """This chains callbacks to run one after the other"""
    def _multi_call_partial(*args, callbacks=None):
        for f in callbacks:
            f(*args)
    return partial(_multi_call_partial, callbacks=callbacks)


def require_approval():
    """This function will set a task to fail immediately to wait for manual approval"""
    raise AirflowException("Please mark this task successful when approval to proceed has been provided.")
