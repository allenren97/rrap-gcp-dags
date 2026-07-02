#!/usr/bin/env python3

import argparse
import pendulum
import json
import subprocess
import re
import requests
from time import sleep
import logging
import os

DATE_FORMAT = "YYYY-MM-DD"

logger = logging.getLogger("backfill-runs")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler("/bns/rrap/apps/rebuild-airflow/logs/backfill-runs.log")
fh.setFormatter(formatter)
logger.addHandler(fh)


def _is_valid_date(date: str):
    if re.match(r'^\d{4}-\d{2}-\d{2}', date):
        return
    else:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")


def _get_queued_dags(dag_id: str):
    """
    Takes a dag id
    Returns a list of queued run ids
    """

    cmd = [ 'airflow', 'dags', 'list-runs', dag_id, '--output', 'json', '--state', 'queued' ]

    result = subprocess.run(cmd, capture_output=True)
    output = json.loads(result.stdout)

    return [ o['run_id'] for o in output ]


def _get_running_dags(dag_id: str):
    """
    Takes a dag id
    Returns a list of running run ids
    """

    cmd = [ 'airflow', 'dags', 'list-runs', dag_id, '--output', 'json', '--state', 'running' ]

    result = subprocess.run(cmd, capture_output=True)
    output = json.loads(result.stdout)

    return [ o['run_id'] for o in output ]


def _get_dagrun_ids(from_date: str, to_date: str, dag_id: str):
    """
    Takes from_date, to_date, dag_id
    Returns a list of run_ids
    """

    # Get range of dates
    start = pendulum.from_format(from_date, DATE_FORMAT)
    end = pendulum.from_format(to_date, DATE_FORMAT)
    months = start.diff(end).in_months()
    date_range = [ start.add(months=i).start_of('month').to_date_string() for i in range(1, months+1) ]
    logger.info(f"Date Range: {date_range}")

    # Get run ids for logical dates
    cmd = [ 'airflow', 'dags', 'list-runs', dag_id, '--output', 'json' ]

    result = subprocess.run(cmd, capture_output=True)
    output = json.loads(result.stdout)

    run_ids = []
    for run in output:
        if run['logical_date'].split('T')[0] in date_range:
            run_ids.append(run['run_id'])

    return run_ids


def _backfill_dag(from_date: str, to_date: str, dag_id: str):
    """
    Takes from_date, to_date and dag_id to backfill
    Returns nothing
    """
    cmd = ['airflow', 'backfill', 'create', '--dag-id', dag_id, '--from-date', from_date, '--to-date', to_date,  '--max-active-runs', '1']
    subprocess.run(cmd)

    return


def _mark_dagrun_success(run_id: str, dag_id: str):
    """
    Takes a run_id and dag_id and marks entire dag successful
    Returns nothing
    """

    response = requests.patch(
        f"{HOST}/api/v2/dags/{dag_id}/dagRuns/{run_id}",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
        },
        json={
            "state": "success",
            "note": "",
        },
        verify=False
    )
    logger.info(f"_mark_dagrun_succes return code: {response}")

    return response


def _get_tasks(taskgroup: str, dag_id: str):
    """
    Tasks a taskgroup and dag_id
    Returns a list of matching task_ids
    """
    group_id = f"{taskgroup}."
    response = requests.get(
        f"{HOST}/api/v2/dags/{dag_id}/tasks",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
        },
        verify=False
    )
    logger.info(f"_clear_from_handle_month_context return code: {response}")
    tasklist = json.loads(response.text)

    task_ids = []
    for task in tasklist['tasks']:
        if group_id in task['task_id']:
            task_ids.append(task['task_id'])

    return task_ids


def _mark_task_failed(task_id: str, run_id: str, dag_id: str):
    """
    Takes a run_id, task_id, and dag_id and marks that task as failed
    """

    response = requests.patch(
        f"{HOST}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
        },
        json={
            "new_state": "failed",
            "note": "",
            "include_upstream": False,
            "include_downstream": False,
            "include_future": False,
            "include_past": False,
        },
        verify=False
    )
    logger.info(f"_mark_task_failed return code: {response}")

    return response


def _clear_tasks(tasks: list[str], run_id: str, include_downstream: bool, only_failed: bool, dag_id: str):
    """
    Takes a task_id, run_id, include_downstream, and dag id
    Clears task id for specific dagrun
    """

    response = requests.post(
        f"{HOST}/api/v2/dags/{dag_id}/clearTaskInstances",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
        },
        json={
            "dry_run": False,
            "dag_run_id": run_id,
            "task_ids": tasks,
            "include_downstream": include_downstream,
            "include_upstream": False,
            "include_future": False,
            "include_past": False,
            "only_failed": only_failed,
            "only_running": False,
            "reset_dag_runs": True,
            "run_on_latest_version": True,
        },
        verify=False
    )
    logger.info(f"_clear_tasks return code: {response}")

    return


if __name__ == "__main__":
    """Handle arguments and backfill/clear necessary runs"""

    p = argparse.ArgumentParser()
    p.add_argument("--from-date", required=True, help="Date to start from in YYYY-MM-DD format (instance timezone)")
    p.add_argument("--to-date", required=True, help="Date to end at in YYYY-MM-DD format (instance timezone)")
    p.add_argument("--dag-id", required=True, help="Airflow DAG ID")
    p.add_argument("--host", required=True, help="api-server host including http and port ex. http://localhost:8080")
    p.add_argument("--api-token", required=True, help="API token for accessing Airflow api-server")
    p.add_argument("--taskgroup", default=None, help="Optional: TaskGroup to be cleared")
    p.add_argument("--only-failed", default=True, help="Clear only failed tasks")

    args = p.parse_args()
    logger.info(f"Arugments: {args}")

    only_failed = args.only_failed
    if args.only_failed == "true":
        only_failed = True
    elif args.only_failed == "false":
        only_failed = False

    logger.info(f"only_failed: {only_failed}")

    # Setup global host and api token
    global API_TOKEN
    global HOST
    HOST = f"{args.host}"

    token = json.loads(args.api_token)
    API_TOKEN = f"{token['access_token']}"

    # Check from_date and to_date to ensure valid dates
    _is_valid_date(args.from_date)
    _is_valid_date(args.to_date)
    logger.info(f"from_date: {args.from_date} - to_date: {args.to_date}")

    # Backfill dag for dates
    _backfill_dag(from_date=args.from_date, to_date=args.to_date, dag_id=args.dag_id)

    # Check if queued dagrun
    # If queued dagrun, get running dagrun run id
    # Mark running dagrun successful
    queued = _get_queued_dags(args.dag_id)
    logger.info(f"Queued: {queued}")
    while len(queued) > 0:
        running = _get_running_dags(args.dag_id)
        logger.info(f"Running: {running}")

        for dagrun in running:
            logger.info(f"Marking {dagrun} successful..")
            result = _mark_dagrun_success(dagrun, args.dag_id)
            sleep(5)

        queued = _get_queued_dags(args.dag_id)

    # If no queued, dags assume dags may be running
    running = _get_running_dags(args.dag_id)
    logger.info(f"Running: {running}")

    if len(running) > 0:
        for dagrun in running:
                logger.info(f"Marking {dagrun} successful..")
                result = _mark_dagrun_success(dagrun, args.dag_id)
                sleep(5)

    # Get run ids for dag in range, returns in newest to oldest order
    run_ids = _get_dagrun_ids(args.from_date, args.to_date, args.dag_id)
    logger.info(f"Run IDs to start: {run_ids}")

    # Wait for the backfill to resolve
    sleep(30)

    if args.taskgroup:
        # Will run only the taskgroup specified,
        # Clear handle_month_context with no downstream
        # and clear taskgroup with no downstream
        task_ids = _get_tasks(args.taskgroup, args.dag_id)
        task_ids.append("handle_month_context")
        logger.info(f"Task ids: {task_ids}")

        # Run oldest dagruns first
        for run in reversed(run_ids):
            if only_failed:
                logger.info("Marking handle_month_context failed..")
                _mark_task_failed("handle_month_context", run, args.dag_id)
                sleep(15)

            _clear_tasks(task_ids, run, False, only_failed, args.dag_id)
            sleep(15)

            # Wait for dagrun to complete before continuing
            dags_running = _get_running_dags(args.dag_id)
            while len(dags_running) > 0:
                dags_running = _get_running_dags(args.dag_id)
                if run in dags_running:
                    logger.info(f"Waiting for dagrun to complete.. run id - {run}")
                    sleep(15)
                else:
                    break

    else:
        # Will run everything in the dag, clear handle_month_context
        # and all its downstream, plus wait for the Dag to complete before continuing on
        for run in reversed(run_ids):
            task_ids = [ "handle_month_context" ]

            if only_failed:
                logger.info("Marking handle_month_context failed..")
                _mark_task_failed("handle_month_context", run, args.dag_id)
                sleep(15)

            _clear_tasks(task_ids, run, True, only_failed, args.dag_id)

            # Wait for dagrun to complete before continuing
            dags_running = _get_running_dags(args.dag_id)
            while len(dags_running) > 0:
                dags_running = _get_running_dags(args.dag_id)
                if run in dags_running:
                    logger.info(f"Waiting for dagrun to complete.. run id - {run}")
                    sleep(15)
                else:
                    break

