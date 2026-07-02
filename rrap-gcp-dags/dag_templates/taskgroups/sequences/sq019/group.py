from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq019_start():
    """ Manual approval task to start sq019 """
    raise AirflowException("Please mark this task successful to start sequence sq019.")


@task_group(group_id="sq019")
def sq019_group():
    """
    TaskGroup for sequence sq019.
    """

    @task_group(group_id="sq019_source")
    def sq019_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq019.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq019/source_group.py")


    @task_group(group_id="sq019_enrichment")
    def sq019_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq019.
        """
        import_contents("taskgroups/sequences/sq019/enrichment_group.py")


    sq019_source_group = sq019_source_group()
    sq019_enrichment_group = sq019_enrichment_group()

    sq019_source_group >> sq019_enrichment_group


@task(outlets=[AssetAlias("tng_cust_tu")])
def tng_cust_tu(*, outlet_events):
    outlet_events[AssetAlias("tng_cust_tu")].add(
        Asset("ingestion.TNG_CUST_TU", extra={})
    )


sq019 = sq019_group()
sq019_start = sq019_start()
tng_cust_tu = tng_cust_tu()

sq019_start >> sq019 >> tng_cust_tu
