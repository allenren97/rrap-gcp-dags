from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq008_start():
    """ Manual approval task to start sq008 """
    raise AirflowException("Please mark this task successful to start sequence sq008.")


@task_group(group_id="sq008")
def sq008_group():
    """
    TaskGroup for sequence sq008.
    """

    @task_group(group_id="sq008_source")
    def sq008_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq008.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq008/source_group.py")


    @task_group(group_id="sq008_enrichment")
    def sq008_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq008.
        """
        import_contents("taskgroups/sequences/sq008/enrichment_group.py")

    sq008_source_group = sq008_source_group()
    sq008_enrichment_group = sq008_enrichment_group()

    sq008_source_group >> sq008_enrichment_group


@task(outlets=[AssetAlias("basel_mort_mth_snapshot")])
def basel_mort_mth_snapshot(*, outlet_events):
    outlet_events[AssetAlias("basel_mort_mth_snapshot")].add(
        Asset("ingestion.BASEL_MORT_MTH_SNAPSHOT", extra={})
    )


sq008 = sq008_group()
sq008_start = sq008_start()
basel_mort_mth_snapshot = basel_mort_mth_snapshot()

sq008_start >> sq008 >> basel_mort_mth_snapshot
