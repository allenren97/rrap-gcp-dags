from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq002_start():
    """ Manual approval task to start sq002 """
    raise AirflowException("Please mark this task successful to start sequence sq002.")


@task_group(group_id="sq002")
def sq002_group():
    """
    TaskGroup for sequence sq002.
    """

    @task_group(group_id="sq002_source")
    def sq002_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq002.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq002/source_group.py")


    @task_group(group_id="sq002_enrichment")
    def sq002_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq002.
        """
        import_contents("taskgroups/sequences/sq002/enrichment_group.py")
    

    sq002_source_group = sq002_source_group()
    sq002_enrichment_group = sq002_enrichment_group()

    sq002_source_group >> sq002_enrichment_group


@task(outlets=[AssetAlias("basel_cust_dim")])
def basel_cust_dim(*, outlet_events):
    outlet_events[AssetAlias("basel_cust_dim")].add(
        Asset("ingestion.BASEL_CUST_DIM", extra={})
    )


sq002 = sq002_group()
sq002_start = sq002_start()
basel_cust_dim = basel_cust_dim()

sq002_start >> sq002 >> basel_cust_dim
