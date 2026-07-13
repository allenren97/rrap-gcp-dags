from airflow.sdk import task_group, AssetAlias, Asset, task
from airflow.exceptions import AirflowException


@task
def sq004_start():
    """ Manual approval task to start sq004 """
    raise AirflowException("Please mark this task successful to start sequence sq004.")


@task_group(group_id="sq004")
def sq004_group():
    """
    TaskGroup for sequence sq004
    """

    @task_group(group_id="sq004_source")
    def sq004_source_group():
        """
        TaskGroup for source tasks from EDL in sequence sq004
        """
        # Import of source_group.py
        import_contents("taskgroups/sequences/sq004/source_group.py")


    @task_group(group_id="sq004_enrichment")
    def sq004_enrichment_group():
        """
        TaskGroup for enrichment tasksin sequence sq004
        Currently, IIAS data used to enrich EDL data
        Future, DuckLake / MSSQL data used to enrich EDL data
        """
        # Implementation for enrichment tasks goes here
        import_contents("taskgroups/sequences/sq004/enrichment_group.py")
    

    sq004_source_group = sq004_source_group()
    sq004_enrichment_group = sq004_enrichment_group() 

    sq004_source_group >> sq004_enrichment_group


@task(outlets=[AssetAlias("basel_cust_acct_rltnp_snapshot")])
def basel_cust_acct_rltnp_snapshot(*, outlet_events):
    outlet_events[AssetAlias("basel_cust_acct_rltnp_snapshot")].add(
        Asset("ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT", extra={})
    )



sq004 = sq004_group()
sq004_start = sq004_start()
basel_cust_acct_rltnp_snapshot = basel_cust_acct_rltnp_snapshot()

sq004_start >> sq004 >> basel_cust_acct_rltnp_snapshot
