from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq033_start():
    """ Manual approval task to start sq033 """
    raise AirflowException("Please mark this task successful to start sequence sq033.")


@task_group(group_id="sq033")
def sq033_group():
    """
    TaskGroup for sequence sq033.
    """

    @task_group(group_id="sq033_source")
    def sq033_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq033.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq033/source_group.py")


    @task_group(group_id="sq033_enrichment")
    def sq033_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq033.
        """
        import_contents("taskgroups/sequences/sq033/enrichment_group.py")

    sq033_source_group = sq033_source_group()
    sq033_enrichment_group = sq033_enrichment_group()

    sq033_source_group >> sq033_enrichment_group


@task(outlets=[AssetAlias("tng_acct_collecttrst")])
def tng_acct_collecttrst(*, outlet_events):
    outlet_events[AssetAlias("tng_acct_collecttrst")].add(
        Asset("ingestion.TNG_ACCT_COLLECTTRST", extra={})
    )


sq033 = sq033_group()
sq033_start = sq033_start() 
tng_acct_collecttrst = tng_acct_collecttrst()

sq033_start >> sq033 >> tng_acct_collecttrst