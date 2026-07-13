from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq001_start():
    """ Manual approval task to start sq001 """
    raise AirflowException("Please mark this task successful to start sequence sq001.")


@task_group(group_id="sq001")
def sq001_group():
    """
    TaskGroup for sequence sq001.
    """

    @task_group(group_id="sq001_source")
    def sq001_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq001.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq001/source_group.py")


    @task_group(group_id="sq001_enrichment")
    def sq001_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq001.
        """
        import_contents("taskgroups/sequences/sq001/enrichment_group.py")
    

    sq001_source_group = sq001_source_group()
    sq001_enrichment_group = sq001_enrichment_group()

    sq001_source_group >> sq001_enrichment_group


@task(outlets=[AssetAlias("airb_mort_mth_snapshot")])
def airb_mort_mth_snapshot(*, outlet_events):
    outlet_events[AssetAlias("airb_mort_mth_snapshot")].add(
        Asset("ingestion.AIRB_MORT_MTH_SNAPSHOT", extra={})
    )


sq001 = sq001_group()
sq001_start = sq001_start()
airb_mort_mth_snapshot = airb_mort_mth_snapshot()

sq001_start >> sq001 >> airb_mort_mth_snapshot
