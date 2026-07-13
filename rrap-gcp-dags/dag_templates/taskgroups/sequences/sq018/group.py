from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq018_start():
    """ Manual approval task to start sq018 """
    raise AirflowException("Please mark this task successful to start sequence sq018.")


@task_group(group_id="sq018")
def sq018_group():
    """
    TaskGroup for sequence sq018.
    """

    @task_group(group_id="sq018_source")
    def sq018_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq018.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq018/source_group.py")


    @task_group(group_id="sq018_enrichment")
    def sq018_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq018.
        """
        import_contents("taskgroups/sequences/sq018/enrichment_group.py")


    sq018_source_group = sq018_source_group()
    sq018_enrichment_group = sq018_enrichment_group()

    sq018_source_group >> sq018_enrichment_group


@task(outlets=[AssetAlias("tng_acct_writeoff")])
def tng_acct_writeoff(*, outlet_events):
    outlet_events[AssetAlias("tng_acct_writeoff")].add(
        Asset("ingestion.TNG_ACCT_WRITEOFF", extra={})
    )


sq018 = sq018_group()
sq018_start = sq018_start()
tng_acct_writeoff = tng_acct_writeoff()

sq018_start >> sq018 >> tng_acct_writeoff
