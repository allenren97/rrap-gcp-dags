from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq037_start():
    """ Manual approval task to start sq037 """
    raise AirflowException("Please mark this task successful to start sequence sq037.")


@task_group(group_id="sq037")
def sq037_group():
    """
    TaskGroup for sequence sq037.
    """

    @task_group(group_id="sq037_source")
    def sq037_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq037.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq037/source_group.py")


    @task_group(group_id="sq037_enrichment")
    def sq037_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq037.
        """
        import_contents("taskgroups/sequences/sq037/enrichment_group.py")        


    sq037_source_group = sq037_source_group()
    sq037_enrichment_group = sq037_enrichment_group()

    sq037_source_group >> sq037_enrichment_group


@task(outlets=[AssetAlias("unemp_rate")])
def unemp_rate(*, outlet_events):
    outlet_events[AssetAlias("unemp_rate")].add(
        Asset("ingestion.UNEMP_RATE", extra={})
    )


sq037 = sq037_group()
sq037_start = sq037_start()
unemp_rate = unemp_rate()


sq037_start >> sq037 >> unemp_rate
