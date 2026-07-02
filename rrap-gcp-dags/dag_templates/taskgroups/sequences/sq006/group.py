from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq006_start():
    """ Manual approval task to start sq006 """
    raise AirflowException("Please mark this task successful to start sequence sq006.")


@task_group(group_id="sq006")
def sq006_group():
    """
    TaskGroup for sequence sq006.
    """

    @task_group(group_id="sq006_source")
    def sq006_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq006.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq006/source_group.py")


    @task_group(group_id="sq006_enrichment")
    def sq006_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq006.
        """
        import_contents("taskgroups/sequences/sq006/enrichment_group.py")


    sq006_source_group = sq006_source_group()
    sq006_enrichment_group = sq006_enrichment_group()

    sq006_source_group >> sq006_enrichment_group


@task(outlets=[AssetAlias("basel_step_psnl_loan_mth_snapshot")])
def basel_step_psnl_loan_mth_snapshot(*, outlet_events):
    outlet_events[AssetAlias("basel_step_psnl_loan_mth_snapshot")].add(
        Asset("ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT", extra={})
    )


sq006 = sq006_group()
sq006_start = sq006_start()
basel_step_psnl_loan_mth_snapshot = basel_step_psnl_loan_mth_snapshot()

sq006_start >> sq006 >> basel_step_psnl_loan_mth_snapshot
