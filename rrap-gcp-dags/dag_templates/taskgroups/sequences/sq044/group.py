from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq044_start():
    """ Manual approval task to start sq044 """
    raise AirflowException("Please mark this task successful to start sequence sq044.")


@task_group(group_id="sq044")
def sq044_group():
    """
    TaskGroup for sequence sq044.
    """

    @task_group(group_id="sq044_source")
    def sq044_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq044.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq044/source_group.py")


    @task_group(group_id="sq044_enrichment")
    def sq044_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq044.
        """
        import_contents("taskgroups/sequences/sq044/enrichment_group.py")


    sq044_source_group = sq044_source_group()
    sq044_enrichment_group = sq044_enrichment_group()

    sq044_source_group >> sq044_enrichment_group


@task(outlets=[AssetAlias("candn_popn_mth_snapshot")])
def candn_popn_mth_snapshot(*, outlet_events):
    outlet_events[AssetAlias("candn_popn_mth_snapshot")].add(
        Asset("ingestion.CANDN_POPN_MTH_SNAPSHOT", extra={})
    )


sq044 = sq044_group()
sq044_start = sq044_start()
candn_popn_mth_snapshot = candn_popn_mth_snapshot()

sq044_start >> sq044 >> candn_popn_mth_snapshot
