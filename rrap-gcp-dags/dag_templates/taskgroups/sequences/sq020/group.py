from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq020_start():
    """ Manual approval task to start sq020 """
    raise AirflowException("Please mark this task successful to start sequence sq020.")


@task_group(group_id="sq020")
def sq020_group():
    """
    TaskGroup for sequence sq020.
    """

    @task_group(group_id="sq020_source")
    def sq020_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq020.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq020/source_group.py")


    @task_group(group_id="sq020_enrichment")
    def sq020_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq020.
        """
        import_contents("taskgroups/sequences/sq020/enrichment_group.py")
    

    sq020_source_group = sq020_source_group()
    sq020_enrichment_group = sq020_enrichment_group()

    sq020_source_group >> sq020_enrichment_group


@task(outlets=[AssetAlias("tng_acct_indcost")])
def tng_acct_indcost(*, outlet_events):
    outlet_events[AssetAlias("tng_acct_indcost")].add(
        Asset("ingestion.TNG_ACCT_INDCOST", extra={})
    )


sq020 = sq020_group()
sq020_start = sq020_start()
tng_acct_indcost = tng_acct_indcost()

sq020_start >> sq020 >> tng_acct_indcost
