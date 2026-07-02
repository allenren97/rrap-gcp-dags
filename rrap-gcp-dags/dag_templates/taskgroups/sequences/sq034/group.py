from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq034_start():
    """ Manual approval task to start sq034 """
    raise AirflowException("Please mark this task successful to start sequence sq034.")


@task_group(group_id="sq034")
def sq034_group():
    """
    TaskGroup for sequence sq034.
    """

    @task_group(group_id="sq034_source")
    def sq034_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq034.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq034/source_group.py")


    @task_group(group_id="sq034_enrichment")
    def sq034_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq034.
        """
        import_contents("taskgroups/sequences/sq034/enrichment_group.py")
    

    sq034_source_group = sq034_source_group()
    sq034_enrichment_group = sq034_enrichment_group()

    sq034_source_group >> sq034_enrichment_group


@task(outlets=[AssetAlias("tng_acct_mo")])
def tng_acct_mo(*, outlet_events):
    outlet_events[AssetAlias("tng_acct_mo")].add(
        Asset("ingestion.TNG_ACCT_MO ", extra={})
    )


sq034 = sq034_group()
sq034_start = sq034_start()
tng_acct_mo = tng_acct_mo()

sq034_start >> sq034 >> tng_acct_mo
