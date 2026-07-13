from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq036_start():
    """ Manual approval task to start sq036 """
    raise AirflowException("Please mark this task successful to start sequence sq036.")


@task_group(group_id="sq036")
def sq036_group():
    """
    TaskGroup for sequence sq036.
    """

    @task_group(group_id="sq036_source")
    def sq036_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq036.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq036/source_group.py")


    @task_group(group_id="sq036_enrichment")
    def sq036_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq036.
        """
        import_contents("taskgroups/sequences/sq036/enrichment_group.py")

    sq036_source_group = sq036_source_group()
    sq036_enrichment_group = sq036_enrichment_group()

    sq036_source_group >> sq036_enrichment_group


@task(outlets=[AssetAlias("teranet_house_prc_index")])
def teranet_house_prc_index(*, outlet_events):
    outlet_events[AssetAlias("teranet_house_prc_index")].add(
        Asset("ingestion.TERANET_HOUSE_PRC_INDEX", extra={})
    )


@task(outlets=[AssetAlias("teranet_house_prc_index_cma")])
def teranet_house_prc_index_cma(*, outlet_events):
    outlet_events[AssetAlias("teranet_house_prc_index_cma")].add(
        Asset("ingestion.TERANET_HOUSE_PRC_INDEX_CMA", extra={})
    )


@task(outlets=[AssetAlias("rrap_teranet_consolidated_cma_data")])
def rrap_teranet_consolidated_cma_data(*, outlet_events):
    outlet_events[AssetAlias("rrap_teranet_consolidated_cma_data")].add(
        Asset("ingestion.RRAP_TERANET_CONSOLIDATED_CMA_DATA", extra={})
    )


sq036 = sq036_group()
sq036_start = sq036_start()
teranet_house_prc_index = teranet_house_prc_index()
teranet_house_prc_index_cma = teranet_house_prc_index_cma()
rrap_teranet_consolidated_cma_data = rrap_teranet_consolidated_cma_data()


sq036_start >> sq036 >> [
    teranet_house_prc_index,
    teranet_house_prc_index_cma,
    rrap_teranet_consolidated_cma_data
]
