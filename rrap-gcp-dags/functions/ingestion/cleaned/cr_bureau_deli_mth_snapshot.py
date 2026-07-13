# PLACEHOLDER ASSET FOR CR_BUREAU_DELI_MTH_SNAPSHOT

import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.TM_DIM' ]
DOWNSTREAM_ASSET = "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT"
DEPENDENCIES = {
}



def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    # path = os.path.join("/bns/rrap/data", f"{ rundate }", 'jb0012_AIRB_MORT_MTH_SNAPSHOT.parquet')

    return PokeReturnValue(is_done=True)
    # else:
        # return PokeReturnValue(is_done=False)


