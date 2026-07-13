from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq015_start():
    """ Manual approval task to start sq015 """
    raise AirflowException("Please mark this task successful to start sequence sq015.")


@task_group(group_id="sq015")
def sq015_group():
    """
    TaskGroup for sequence sq015.
    """

    @task_group(group_id="sq015_source")
    def sq015_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq015.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq015/source_group.py")


    @task_group(group_id="sq015_enrichment")
    def sq015_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq015.
        """
        import_contents("taskgroups/sequences/sq015/enrichment_group.py")


    sq015_source_group = sq015_source_group()
    sq015_enrichment_group = sq015_enrichment_group()

    sq015_source_group >> sq015_enrichment_group


@task(outlets=[AssetAlias("org_unit_dim")])
def org_unit_dim(*, outlet_events):
    outlet_events[AssetAlias("org_unit_dim")].add(
        Asset("ingestion.ORG_UNIT_DIM", extra={})
    )


sq015 = sq015_group()
sq015_start = sq015_start()
org_unit_dim = org_unit_dim()

sq015_start >> sq015 >> org_unit_dim
