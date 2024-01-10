from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    params={
         "target_schema": Param("intelliview_mapping_gold", type="string"),
         "target_table": Param("bronze_silver_mapping", type="string")
     },
)
def update_lookup_table():
    """
    Manually initiate this DAG to upload any lookup csv file to a table.
    The configuration should be provided as follows:

    ```
     {
         "target_schema": "intelliview_mapping_gold",
         "target_table": "bronze_silver_mapping"
     }
     ```
     `target_schema`: e.g. "intelliview_mapping_gold", "cnc_gold" etc.
     `target_table`: e.g. "bronze_silver_mapping", "dim_campaign_exceptions" etc.
    """

    json = {
        "job_name": "update_lookup_table",
    }
    update_table = DatabricksRunNowOperator(
        task_id="update_lookup_table",
        json=json,
        notebook_params={
            "target_schema": "{{ params.target_schema }}",
            "target_table": "{{ params.target_table }}"
        }
    )

    update_table

update_lookup_table()
