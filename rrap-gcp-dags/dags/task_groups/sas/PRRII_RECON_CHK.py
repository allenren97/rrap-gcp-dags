from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.sdk import task_group
# from bns.rrap.operators.sas import SasOperator
# from bns.rrap.operators.infa import InformaticaOperator
from bns.rrap.operators.empty import SasOperator
from bns.rrap.operators.empty import InformaticaOperator

from task_groups.sas.AIRB_RECON_APRVD_SNAPSHOT import AIRB_RECON_APRVD_SNAPSHOT


@task_group(group_id="RECON_CHK")
def RECON_CHK():
    
    OW_DM_RRII_RECON_APPROVE_CHECK = AIRB_RECON_APRVD_SNAPSHOT()
    
    OW_DM_RRII_RECON_APPROVE_CHECK