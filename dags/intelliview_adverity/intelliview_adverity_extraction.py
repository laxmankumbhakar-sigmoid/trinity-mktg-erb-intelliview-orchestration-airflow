import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from trinity.config import BlobStorage


@dag(
    dag_id="intelliview_adverity_extraction",
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
)
def intelliview_adverity_extraction():
    def acquisition_to_landing_logic():
        gcs_hook = GCSHook()

        acquisition_bucket = BlobStorage.trinity_acquisition_adverity.bucket
        landing_bucket = BlobStorage.trinity_landing.bucket
        files = gcs_hook.list(
            acquisition_bucket,
            prefix="",
        )
        for file in files:
            if file.split('/')[0] != 'temp_landing_bucket': # TODO: Remove this after we get a landing bucket
                logging.info(f"file: {file}")
                source = file.split('-')[-4]
                table = "_".join(file.split('-')[-3:-1])
                date = file.split('-')[-1].split('.')[0]
                filename = file.split('/')[-1]
                gcs_hook.copy(
                    source_bucket=acquisition_bucket,
                    source_object=file,
                    destination_bucket=landing_bucket,
                    destination_object=f"temp_landing_bucket/{source}/{table}/logical_acquisition_date={date}/{filename}",
                )
                # gcs_hook.delete(acquisition_bucket, file)

    acquisition_to_landing = PythonOperator(
        task_id="acquisition_to_landing",
        python_callable=acquisition_to_landing_logic,
        provide_context=True,
        trigger_rule="all_done",
    )

    acquisition_to_landing

intelliview_adverity_extraction()