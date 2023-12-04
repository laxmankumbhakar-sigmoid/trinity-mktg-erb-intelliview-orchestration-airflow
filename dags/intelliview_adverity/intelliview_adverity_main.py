import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.exceptions import AirflowSkipException
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from trinity.config import BlobStorage  # isort:skip


@dag(
    dag_id="intelliview_adverity_main",
    start_date=datetime(2023, 3, 30),
    schedule="0 1,13 * * *",
    catchup=False,
    default_args={"retries": 0},
)
def template_dag():
    source_to_landing = TriggerDagRunOperator(
        task_id="source_to_landing",
        trigger_dag_id="intelliview_adverity_extraction",
        execution_date="{{ dag_run.logical_date }}",
        conf={"data_interval_start": "{{ ds }}", "data_interval_end": "{{ ds }}"},
        reset_dag_run=True,  # default False
        wait_for_completion=True,  # default False
        poke_interval=30,  # default 60
    )

    json_bronze = {
        "job_name": "intelliview_adverity_bronze",
    }

    landing_to_bronze = DatabricksRunNowOperator(
        task_id="landing_to_bronze", json=json_bronze
    )

    json_silver = {
        "job_name": "intelliview_adverity_silver",
    }

    bronze_to_silver = DatabricksRunNowOperator(
        task_id="bronze_to_silver", json=json_silver, trigger_rule="all_success"
    )

    json_cnc = {
        "job_name": "intelliview_cnc_gold"
    }

    cnc_gold = DatabricksRunNowOperator(
        task_id="cnc_gold", json=json_cnc, trigger_rule="all_done"
    )

    def landing_to_archive_logic(ti, **kwargs):
        execution_date = kwargs["logical_date"]

        upstream = kwargs["dag"].get_task("landing_to_bronze")
        tis = TaskInstance(upstream, execution_date)
        landing_to_bronze_state = tis.current_state()

        logging.info(f"landing_to_bronze_state: {landing_to_bronze_state}")

        if landing_to_bronze_state == "upstream_failed":
            raise AirflowSkipException("source to landing failed")

        job_run_id = ti.xcom_pull(key="run_id", task_ids="landing_to_bronze")
        logging.info(f"run_id: {job_run_id}")

        databricks_hook = DatabricksHook()
        gcs_hook = GCSHook()

        res = databricks_hook.get_run(job_run_id)

        archive_bucket = BlobStorage.trinity_archive.bucket
        landing_bucket = BlobStorage.trinity_landing.bucket

        for task in res["tasks"]:
            if task["state"]["result_state"].lower() == "success":
                files = gcs_hook.list(landing_bucket, prefix=f"{task['task_key']}")
                for file in files:
                    logging.info(f"file: {file}")
                    gcs_hook.copy(
                        source_bucket=landing_bucket,
                        source_object=file,
                        destination_bucket=archive_bucket,
                    )
                    gcs_hook.delete(landing_bucket, file)

    landing_to_archive = PythonOperator(
        task_id="landing_to_archive",
        python_callable=landing_to_archive_logic,
        provide_context=True,
        trigger_rule="all_done",
    )

    source_to_landing >> landing_to_bronze >> [landing_to_archive, bronze_to_silver] >> cnc_gold


template_dag()
