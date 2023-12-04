import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from trinity.config import BlobStorage


class Config:
    def __init__(self, context):
        keys = context["dag_run"].conf.keys()
        self.platform_id = context["dag_run"].conf["platform_id"]
        self.table_id = context["dag_run"].conf["table_id"]
        self.acquisition_start_date = context["dag_run"].conf["acquisition_start_date"]
        self.reverse = "False"

        if "acquisition_end_date" in keys:
            self.acquisition_end_date = context["dag_run"].conf["acquisition_end_date"]
        else:
            self.acquisition_end_date = self.acquisition_start_date

        if "reverse" in keys:
            self.reverse = context["dag_run"].conf["reverse"]

        self.source_bucket = BlobStorage.trinity_archive.bucket
        self.destination_bucket = BlobStorage.trinity_landing.bucket

        if self.reverse == "True":
            self.source_bucket = BlobStorage.trinity_landing.bucket
            self.destination_bucket = BlobStorage.trinity_archive.bucket

        self.validate_date_string(self.acquisition_start_date)
        self.validate_date_string(self.acquisition_end_date)

    @property
    def source_objects(self):
        dates = self.get_dates_between(
            self.acquisition_start_date, self.acquisition_end_date
        )
        paths = []
        for date in dates:
            path = f"{self.platform_id}/{self.table_id}/logical_acquisition_date={date}"
            paths.append(path)
        return paths

    def validate_date_string(self, date_string):
        try:
            date = datetime.strptime(date_string, "%Y-%m-%d").date()
            if date.strftime("%Y-%m-%d") == date_string:
                return True
            else:
                raise ValueError(
                    f"Input {date_string} doesn't match the expected format (yyyy-mm-dd)"
                )
        except ValueError:
            raise ValueError(
                "Invalid date format! Date should be in the format 'yyyy-mm-dd' and must actually exist."
            )

    def get_dates_between(self, start_date_str, end_date_str):
        if end_date_str < start_date_str:
            raise ValueError(
                "acquisition_end_date should be greater than acquisition_start_date"
            )

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        dates = []

        current_date = start_date
        while current_date <= end_date:
            # Add the current date to the list of dates
            dates.append(current_date.strftime("%Y-%m-%d"))

            # Move to the next day
            current_date += timedelta(days=1)

        return dates


@dag(
    schedule=None,
    start_date=datetime(2023, 5, 11),
    catchup=False,
    default_args={"retries": 0},
)
def archive_to_landing():
    """
     If you want to transfer data from the archive bucket to the landing bucket at your convenience, you may manually initiate the archive_to_landing pipeline. The configuration can be provided in the following format:

    ```
     {
         "platform_id": "platform",
         "table_id": "table_id",
         "acquisition_start_date": "yyyy-mm-dd",
         "acquisition_end_date": "yyyy-mm-dd"
     }
     ```

     `platform_id`: options include 'care_ga', 'care_snipp', 'care_sfsc', and 'care_sfmc'
     `table_id`: refers to the name of your file, e.g., 'member_info' belongs to the 'snipp' platform_id
     `acquisition_start_date`: indicates the start date from which you wish to transfer data
     `acquisition_end_date` (optional): if provided, indicates the end date until which you want to get data; defaults to the acquisition_start_date if not specified.
    """

    @task
    def get_config(**context):
        config = Config(context)
        logging.info(f"source_bucket: {config.source_bucket}")
        logging.info(f"source_objects: {config.source_objects}")
        logging.info(f"destination_bucket: {config.destination_bucket}")
        return {
            "source_bucket": config.source_bucket,
            "source_objects": config.source_objects,
            "destination_bucket": config.destination_bucket,
        }

    config = get_config()

    logging.info(config)

    def archive_to_landing(source_bucket, prefix, destination_bucket):
        gcs_hook = GCSHook()
        files = gcs_hook.list(source_bucket, prefix=prefix)
        for file in files:
            logging.info(f"file: {file}")
            gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=file,
                destination_bucket=destination_bucket
            )

    for prefix in config["source_objects"]:
        archive_to_landing(config["source_bucket"], prefix, config["destination_bucket"])

archive_to_landing()
