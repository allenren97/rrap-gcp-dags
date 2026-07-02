from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq011_start():
    """ Manual approval task to start sq011 """
    raise AirflowException("Please mark this task successful to start sequence sq011.")


@task_group(group_id="sq011")
def sq011_group():
    """
    TaskGroup for sequence sq011.
    """

    @task_group(group_id="sq011_source")
    def sq011_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq011.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq011/source_group.py")


    @task_group(group_id="sq011_enrichment")
    def sq011_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq011.
        """
        import_contents("taskgroups/sequences/sq011/enrichment_group.py")
        

    sq011_source_group = sq011_source_group()
    sq011_enrichment_group = sq011_enrichment_group()

    sq011_source_group >> sq011_enrichment_group


@task(outlets=[AssetAlias("tng_cust_mo")])
def tng_cust_mo(*, outlet_events):
    outlet_events[AssetAlias("tng_cust_mo")].add(
        Asset("ingestion.TNG_CUST_MO", extra={})
    )


sq011 = sq011_group()
sq011_start = sq011_start()
tng_cust_mo = tng_cust_mo()

sq011_start >> sq011 >> tng_cust_mo
