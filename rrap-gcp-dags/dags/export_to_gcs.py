from airflow.sdk import dag, get_current_context, task, task_group
from bns.rrap.hooks.duckdb import DuckLakeHook
from bns.rrap.operators.beeline import BeelineParquetExportOperator
from bns.rrap.operators.gcstool import GcsScopyOperator

import configparser
import json
import logging
import pendulum


EDLR_TO_GCS_CONFIG = "/bns/rrap/apps/rebuild-airflow/conf/edlr_to_gcs.ini"


def load_section_as_kwargs(path, section):
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    config.read(path)

    def resolve(sec):
        return {k: config.get(sec, k) for k in config[sec].keys()}

    global_kwargs = resolve("global")
    section_kwargs = resolve(section)
    kwargs = {**global_kwargs, **section_kwargs}
    for k, v in kwargs.items():
        if isinstance(v, str):
            if k.lower().startswith("json"):
                try:
                    kwargs[k] = json.loads(v)
                    continue
                except json.JSONDecodeError:
                    logging.warning(f"failed to convert value as json for key: {k}\n{v}")

            if v.lower() == "true":
                kwargs[k] = True
            elif v.lower() == "false":
                kwargs[k] = False
            elif v.lower() == "none":
                kwargs[k] = None

    kwargs["_section"] = section
    return kwargs


def load_sections_by_prefix(path, prefix):
    config = configparser.ConfigParser()
    config.read(path)
    return [sec for sec in config.sections() if sec.startswith(prefix)]


def build_sql(schema: str, table: str, json_filter: dict | None = None) -> str:

    def escape_str(v: str) -> str:
        if "{{" in v or "{%" in v:
            return v
        return v.replace("'", "''")

    conditions = []
    if json_filter:
        for k, v in json_filter.items():
            if isinstance(v, list):
                if not v:
                    conditions.append("1 = 0")
                else:
                    values = ", ".join(
                        f"'{escape_str(x)}'" if isinstance(x, str) else str(x)
                        for x in v
                    )
                    conditions.append(f"{k} in ({values})")
            elif isinstance(v, str):
                conditions.append(f"{k} = '{escape_str(v)}'")
            elif isinstance(v, bool):
                conditions.append(f"{k} = {'true' if v else 'false'}")
            elif v is None:
                conditions.append(f"{k} is null")
            else:
                conditions.append(f"{k} = {v}")
    sql = (
        f"select * from {schema}.{table}"
        + (f" where {' and '.join(conditions)}" if conditions else "")
    )
    logging.warning(f"sql={sql}")
    return sql


@dag(
    dag_id="export_to_gcs",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
)
def export_to_gcs():

    @task()
    def handle_month_context():
        context = get_current_context()
        rundate = (
            context["logical_date"]
            .subtract(months=1)
            .end_of("month")
            .strftime("%Y-%m-%d")
        )
        hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")

        mth_tm_id = hook.duckdb.sql(f"""
            SELECT TM_ID FROM ingestion.TM_DIM
            WHERE TM_LVL = 'Month' AND TM_LVL_END_DT = '{rundate}'
        """).fetchone()[0]
        logging.warning(f"Rundate: {rundate}, MTH_TM_ID: {mth_tm_id}")

        context["ti"].xcom_push(key="mth_tm_id", value=mth_tm_id)
        context["ti"].xcom_push(key="prev_mth_tm_id", value=mth_tm_id - 40)
        context["ti"].xcom_push(key="rundate", value=rundate)
        context["ti"].xcom_push(key="popn_dt", value=context["logical_date"].strftime("%Y-%m-15"))



    @task_group(group_id="edlr_to_gcs")
    def edlr_to_gcs(configs):

        @task
        def filter_configs(configs):
            results = []
            for c in configs:
                if not c["_enabled"]:
                    logging.warning(f"section disabled: {c['_section']}")
                    continue
                results.append(c)
            return results

        @task
        def build_beeline_configs(configs):
            results = []
            for c in configs:
                logging.warning(f"section: {c["_section"]}\n{json.dumps(c, indent=2, default=str)}")
                results.append({
                    "beeline_conn_id": c["beeline_conn_id"],
                    "sql": build_sql(c["schema_name"], c["table_name"], c["json_filter"]),
                    "target": c["target"],
                    "schema": c.get("schema"),
                    "rundir": c["rundir"],
                    "to_parquet": c["to_parquet"],
                    "strings_can_be_null": c["strings_can_be_null"],
                })
            return results

        @task
        def build_gcs_configs(configs):
            results = []
            for c in configs:
                logging.warning(f"section: {c["_section"]}\n{json.dumps(c, indent=2, default=str)}")
                results.append({
                    "file_path": c["file_path"],
                    "bucket": c["bucket"],
                    "prefix": c["prefix"],
                })
            return results

        filtered_configs = filter_configs(configs)
        beeline_configs = build_beeline_configs(filtered_configs)
        gcs_configs = build_gcs_configs(filtered_configs)

        run_beeline = BeelineParquetExportOperator.partial(
            task_id="run_beeline"
        ).expand_kwargs(beeline_configs)

        upload_to_gcs = GcsScopyOperator.partial(
            task_id="upload_to_gcs"
        ).expand_kwargs(gcs_configs)

        filtered_configs >> [ beeline_configs, gcs_configs ]
        beeline_configs >> run_beeline
        [ gcs_configs, run_beeline ] >> upload_to_gcs



    @task_group(group_id="ifrs9")
    def ifrs9():

        @task
        def load_configs():
            sections = load_sections_by_prefix(EDLR_TO_GCS_CONFIG, "ifrs9")
            logging.warning(f"results [{len(sections)}]:\n{"\n".join(sections)}")
            return [load_section_as_kwargs(EDLR_TO_GCS_CONFIG, sec) for sec in sections]

        configs = load_configs()
        edlr_to_gcs(configs)

    handle_month_context() >> ifrs9()

export_to_gcs()
