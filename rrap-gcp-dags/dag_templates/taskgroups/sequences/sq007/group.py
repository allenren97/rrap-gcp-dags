from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq007_start():
    """ Manual approval task to start sq007 """
    raise AirflowException("Please mark this task successful to start sequence sq007.")


@task_group(group_id="sq007")
def sq007_group():
    """
    TaskGroup for sequence sq007
    """

    @task_group(group_id="sq007_source")
    def sq007_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq007
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq007/source_group.py")


    @task_group(group_id="sq007_enrichment")
    def sq007_enrichment_group():
        """
        TaskGroup for enrichment tasksin sequence sq007
        Currently, IIAS data used to enrich EDL data
        Future, DuckLake / MSSQL data used to enrich EDL data
        """
        # Implementation for enrichment tasks goes here
        import_contents("taskgroups/sequences/sq007/enrichment_group.py")

    sq007_source_group = sq007_source_group()
    sq007_enrichment_group = sq007_enrichment_group() 

    sq007_source_group >> sq007_enrichment_group


@task(outlets=[AssetAlias("basel_revlvng_cr_mth_snapshot")])
def basel_revlvng_cr_mth_snapshot(*, outlet_events):
    outlet_events[AssetAlias("basel_revlvng_cr_mth_snapshot")].add(
        Asset("ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT", extra={})
    )


sq007_start = sq007_start()
sq007 = sq007_group()
basel_revlvng_cr_mth_snapshot = basel_revlvng_cr_mth_snapshot()

sq007_start >> sq007 >> basel_revlvng_cr_mth_snapshot