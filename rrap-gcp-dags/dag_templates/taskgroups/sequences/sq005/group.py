from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq005_start():
    """ Manual approval task to start sq005 """
    raise AirflowException("Please mark this task successful to start sequence sq005.")


@task_group(group_id="sq005")
def sq005_group():
    """
    TaskGroup for sequence sq005.
    """

    @task_group(group_id="sq005_source")
    def sq005_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq005.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq005/source_group.py")


    @task_group(group_id="sq005_enrichment")
    def sq005_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq005.
        """
        import_contents("taskgroups/sequences/sq005/enrichment_group.py")


    sq005_source_group = sq005_source_group()
    sq005_enrichment_group = sq005_enrichment_group()

    sq005_source_group >> sq005_enrichment_group


@task(outlets=[AssetAlias("basel_step_pln_mth_snapshot")])
def basel_step_pln_mth_snapshot(*, outlet_events):
    outlet_events[AssetAlias("basel_step_pln_mth_snapshot")].add(
        Asset("ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT", extra={})
    )


sq005 = sq005_group()
sq005_start = sq005_start()
basel_step_pln_mth_snapshot = basel_step_pln_mth_snapshot()

sq005_start >> sq005 >> basel_step_pln_mth_snapshot
