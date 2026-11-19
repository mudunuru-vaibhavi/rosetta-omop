"""Airflow DAG for Rosetta, the multi-site OMOP pipeline.

The DAG is generated from `omop_etl.pipeline.TASKS`, the same task list `omop-etl run` executes, so the
local run and the orchestrated run cannot drift apart. Each Airflow task calls one plain Python
function; Airflow adds scheduling, retries, parallelism (the two sites stage in parallel), logs and
alerting.

Run it locally without a scheduler (Airflow 3):

    uv pip install "apache-airflow>=3.0"
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags AIRFLOW__CORE__LOAD_EXAMPLES=False
    airflow db migrate && airflow dags reserialize
    python dags/omop_pipeline.py          # calls dag.test(): runs every task once, in order

In a deployment, point AIRFLOW__CORE__DAGS_FOLDER at this folder and set OMOP_DATA_DIR,
OMOP_LANDING_URI (s3://...) and, for real vocabularies, OMOP_ATHENA_DIR.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from omop_etl.config import get_paths
from omop_etl.pipeline import TASKS


def _run(task_name: str) -> dict:
    task = next(t for t in TASKS if t.name == task_name)
    return task.fn(get_paths())  # the return value is stored as an XCom for the run log


with DAG(
    dag_id="rosetta_omop",
    description="Clarity + FHIR -> OMOP CDM v5.4 -> data quality -> RWE study -> dashboard",
    schedule="0 3 * * *",  # nightly, after partner drops land
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "owner": "data-engineering"},
    tags=["omop", "etl", "rwe"],
    doc_md=__doc__,
) as dag:
    ops = {t.name: PythonOperator(task_id=t.name, python_callable=_run, op_args=[t.name], doc=t.doc) for t in TASKS}
    for t in TASKS:
        for up in t.upstream:
            ops[up] >> ops[t.name]


if __name__ == "__main__":
    dag.test()
