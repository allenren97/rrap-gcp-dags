from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq003_start():
    """ Manual approval task to start sq003 """
    raise AirflowException("Please mark this task successful to start sequence sq003.")


@task_group(group_id="sq003")
def sq003_group():
    """
    TaskGroup for sequence sq003.
    """

    @task_group(group_id="sq003_source")
    def sq003_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq003.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq003/source_group.py")


    @task_group(group_id="sq003_enrichment")
    def sq003_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq003.
        """
        import_contents("taskgroups/sequences/sq003/enrichment_group.py")
    

    sq003_source_group = sq003_source_group()
    sq003_enrichment_group = sq003_enrichment_group()

    sq003_source_group >> sq003_enrichment_group


@task(outlets=[AssetAlias("basel_acct_dim")])
def basel_acct_dim(*, outlet_events):
    outlet_events[AssetAlias("basel_acct_dim")].add(
        Asset("ingestion.BASEL_ACCT_DIM", extra={})
    )


sq003 = sq003_group()
sq003_start = sq003_start()
basel_acct_dim = basel_acct_dim()

sq003_start >> sq003 >> basel_acct_dim
