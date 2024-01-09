from datetime import datetime

from airflow.decorators import dag
from airflow.operators.email import EmailOperator

@dag(
    schedule_interval=None,
    start_date=datetime(2023, 5, 11),
    catchup=False,
    default_args={"retries": 0},
)
def send_email():

    subject = "Airflow Email Notification for Intelliview Job Completion"
    body = "The gold models have been updated."
    to = ["pawany@sigmoidanalytics.com"]

    send_email_operator = EmailOperator(
        task_id="send_email_operator",
        to=to,
        subject=subject,
        html_content=body,
    )

    send_email_operator

send_email_dag = send_email()
