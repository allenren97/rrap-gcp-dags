from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq023_start():
    """ Manual approval task to start sq023 """
    raise AirflowException("Please mark this task successful to start sequence sq023.")


@task_group(group_id="sq023")
def sq023_group():
    """
    TaskGroup for sequence sq023.
    """

    @task_group(group_id="sq023_source")
    def sq023_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq023.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq023/source_group.py")


    @task_group(group_id="sq023_enrichment")
    def sq023_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq023.
        """
        import_contents("taskgroups/sequences/sq023/enrichment_group.py")


    sq023_source_group = sq023_source_group()
    sq023_enrichment_group = sq023_enrichment_group()

    sq023_source_group >> sq023_enrichment_group


@task(outlets=[AssetAlias("baselayer_mor")])
def baselayer_mor(*, outlet_events):
    outlet_events[AssetAlias("baselayer_mor")].add(
        Asset("ingestion.BASELAYER_MOR", extra={})
    )


sq023 = sq023_group()
sq023_start = sq023_start()
baselayer_mor = baselayer_mor() 

sq023_start >> sq023 >> baselayer_mor
