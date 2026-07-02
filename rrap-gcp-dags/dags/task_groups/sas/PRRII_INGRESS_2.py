from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
from bns.rrap.operators.empty import SasOperator


@task_group(group_id="INGRESS_2")
def INGRESS_2():

    OW_SAS_20_LGD_INGRESS_AUDIT = SasOperator(
        ssh_conn_id="sas-conn",
        task_id="OW_SAS_20_LGD_INGRESS_AUDIT",
        sas_file="J_RRII_20_LGD_INGRESS_AUDIT.sas",
        # bash_command="echo ^PSCRIPTS^ow_common_call_sasdi.ksh rrap_iias/J_RRII_20_LGD_INGRESS_AUDIT.sas",
    )
    OW_SAS_20_LGD_INGRESS_AUDIT.doc = "Counts number of records to be replicated into GCP for LGD realized values , Original taskID: NEW!"
    
    OW_SAS_20_LGD_INGRESS_AUDIT
    
