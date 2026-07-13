from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq0051_start():
    """ Manual approval task to start sq0051 """
    raise AirflowException("Please mark this task successful to start sequence sq0051.")


@task_group(group_id="sq0051")
def sq0051_group():
    """
    TaskGroup for sequence sq0051.
    """

    @task_group(group_id="sq0051_source")
    def sq0051_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq0051.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq0051/source_group.py")


    @task_group(group_id="sq0051_enrichment")
    def sq0051_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq0051.
        """
        pass

    sq0051_source_group = sq0051_source_group()
    sq0051_enrichment_group = sq0051_enrichment_group()

    sq0051_source_group >> sq0051_enrichment_group


@task(outlets=[AssetAlias("basel_ifrs9_ecl_profile_fact")])
def basel_ifrs9_ecl_profile_fact(*, outlet_events):
    outlet_events[AssetAlias("basel_ifrs9_ecl_profile_fact")].add(
        Asset("ingestion.BASEL_IFRS9_ECL_PROFILE_FACT", extra={})
    )


sq0051 = sq0051_group()
sq0051_start = sq0051_start()
basel_ifrs9_ecl_profile_fact = basel_ifrs9_ecl_profile_fact()

sq0051_start >> sq0051 >> basel_ifrs9_ecl_profile_fact
