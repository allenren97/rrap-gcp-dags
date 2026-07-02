from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq016_start():
    """ Manual approval task to start sq016 """
    raise AirflowException("Please mark this task successful to start sequence sq016.")


@task_group(group_id="sq016")
def sq016_group():
    """
    TaskGroup for sequence sq016.
    """

    @task_group(group_id="sq016_source")
    def sq016_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq016.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq016/source_group.py")


    @task_group(group_id="sq016_enrichment")
    def sq016_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq016.
        """
        import_contents("taskgroups/sequences/sq016/enrichment_group.py")
    

    sq016_source_group = sq016_source_group()
    sq016_enrichment_group = sq016_enrichment_group()

    sq016_source_group >> sq016_enrichment_group


@task(outlets=[AssetAlias("basel_psnl_ln_subv_mst_snapsht_new")])
def basel_psnl_ln_subv_mst_snapsht_new(*, outlet_events):
    outlet_events[AssetAlias("basel_psnl_ln_subv_mst_snapsht_new")].add(
        Asset("ingestion.BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW", extra={})
    )


sq016 = sq016_group()
sq016_start = sq016_start()
basel_psnl_ln_subv_mst_snapsht_new = basel_psnl_ln_subv_mst_snapsht_new()

sq016_start >> sq016 >> basel_psnl_ln_subv_mst_snapsht_new
