from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq035_start():
    """ Manual approval task to start sq035 """
    raise AirflowException("Please mark this task successful to start sequence sq035.")


@task_group(group_id="sq035")
def sq035_group():
    """
    TaskGroup for sequence sq035.
    """

    @task_group(group_id="sq035_source")
    def sq035_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq035.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq035/source_group.py")


    @task_group(group_id="sq035_enrichment")
    def sq035_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq035.
        """
        import_contents("taskgroups/sequences/sq035/enrichment_group.py")


    sq035_source_group = sq035_source_group()
    sq035_enrichment_group = sq035_enrichment_group()

    sq035_source_group >> sq035_enrichment_group


@task(outlets=[AssetAlias("asset_src_curr")])
def asset_src_curr(*, outlet_events):
    outlet_events[AssetAlias("asset_src_curr")].add(
        Asset("ingestion.ASSET_SRC_CURR", extra={})
    )


sq035 = sq035_group()
sq035_start = sq035_start()
asset_src_curr = asset_src_curr()

sq035_start >> sq035 >> asset_src_curr
