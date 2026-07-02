from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq043_start():
    """ Manual approval task to start sq043 """
    raise AirflowException("Please mark this task successful to start sequence sq043.")


@task_group(group_id="sq043")
def sq043_group():
    """
    TaskGroup for sequence sq043.
    """

    @task_group(group_id="sq043_source")
    def sq043_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq043.
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq043/source_group.py")


    @task_group(group_id="sq043_enrichment")
    def sq043_enrichment_group():
        """
        TaskGroup for enrichment tasks in sequence sq043.
        """
        import_contents("taskgroups/sequences/sq043/enrichment_group.py")


    sq043_source_group = sq043_source_group()
    sq043_enrichment_group = sq043_enrichment_group()

    sq043_source_group >> sq043_enrichment_group


@task(outlets=[AssetAlias("hh_dspsbl_incm_qtr")])
def hh_dspsbl_incm_qtr(*, outlet_events):
    outlet_events[AssetAlias("hh_dspsbl_incm_qtr")].add(
        Asset("ingestion.HH_DSPSBL_INCM_QTR", extra={})
    )


sq043 = sq043_group()
sq043_start = sq043_start()
hh_dspsbl_incm_qtr = hh_dspsbl_incm_qtr()

sq043_start >> sq043 >> hh_dspsbl_incm_qtr
