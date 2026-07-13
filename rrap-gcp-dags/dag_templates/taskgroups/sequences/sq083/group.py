from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq083_start():
    """ Manual approval task to start sq083 """
    raise AirflowException("Please mark this task successful to start sequence sq083.")


@task_group(group_id="sq083")
def sq083_group():
    """
    TaskGroup for sequence sq083.
    """

    @task_group(group_id="sq083_source")
    def sq083_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq083.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq083/source_group.py")


    @task_group(group_id="sq083_enrichment")
    def sq083_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq083.
        """
        import_contents("taskgroups/sequences/sq083/enrichment_group.py")


    sq083_source_group = sq083_source_group()
    sq083_enrichment_group = sq083_enrichment_group()

    sq083_source_group >> sq083_enrichment_group


@task(outlets=[AssetAlias("mbr_src_curr")])
def mbr_src_curr(*, outlet_events):
    outlet_events[AssetAlias("mbr_src_curr")].add(
        Asset("ingestion.MBR_SRC_CURR", extra={})
    )


sq083 = sq083_group()
sq083_start = sq083_start()
mbr_src_curr = mbr_src_curr()

sq083_start >> sq083 >> mbr_src_curr
