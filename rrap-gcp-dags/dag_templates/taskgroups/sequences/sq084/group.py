from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq084_start():
    """ Manual approval task to start sq084 """
    raise AirflowException("Please mark this task successful to start sequence sq084.")


@task_group(group_id="sq084")
def sq084_group():
    """
    TaskGroup for sequence sq084.
    """

    @task_group(group_id="sq084_source")
    def sq084_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq084.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq084/source_group.py")


    @task_group(group_id="sq084_enrichment")
    def sq084_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq084.
        """
        import_contents("taskgroups/sequences/sq084/enrichment_group.py")


    sq084_source_group = sq084_source_group()
    sq084_enrichment_group = sq084_enrichment_group()

    sq084_source_group >> sq084_enrichment_group


@task(outlets=[AssetAlias("cbs_mdm_flags")])
def cbs_mdm_flags(*, outlet_events):
    outlet_events[AssetAlias("cbs_mdm_flags")].add(
        Asset("emulated.CBS_MDM_FLAGS", extra={})
    )


sq084 = sq084_group()
sq084_start = sq084_start()
cbs_mdm_flags = cbs_mdm_flags()

sq084_start >> sq084 >> cbs_mdm_flags
