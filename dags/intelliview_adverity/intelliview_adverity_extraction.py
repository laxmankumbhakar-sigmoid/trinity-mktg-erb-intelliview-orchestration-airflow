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
    def acquisition_to_landing_logic(ds):
        gcs_hook = GCSHook()

        acquisition_bucket = BlobStorage.trinity_acquisition_adverity.bucket
        landing_bucket = BlobStorage.trinity_landing.bucket
        files = gcs_hook.list(acquisition_bucket)
        for file in files:
                logging.info(f"file: {file}")
                platform_id = file.split('-')[-4]
                report_id = file.split('-')[-3]
                periodicity = file.split('-')[-2]
                table_id = f"{report_id}_{periodicity}"
                filename = file.split('/')[-1]
                destination_object = f"{platform_id}/{table_id}/logical_acquisition_date={ds}/{filename}"
                logging.info(f"destination_object: {destination_object}")
                gcs_hook.copy(
                    source_bucket=acquisition_bucket,
                    source_object=file,
                    destination_bucket=landing_bucket,
                    destination_object=destination_object,
                )
                # gcs_hook.delete(acquisition_bucket, file)

    acquisition_to_landing = PythonOperator(
        task_id="acquisition_to_landing",
        python_callable=acquisition_to_landing_logic,
        provide_context=True,
    )

    acquisition_to_landing

intelliview_adverity_extraction()