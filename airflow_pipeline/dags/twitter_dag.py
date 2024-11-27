import sys
sys.path.append("airflow_pipeline")
from operators.twitter_operator import TwitterOperator
from airflow.models import DAG
from os.path import join
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(-50)).date().strftime(TIMESTAMP_FORMAT)
query = "datascience"

with DAG(dag_id="TwitterDag", start_date=days_ago(6), schedule_interval="@daily") as dag:
        to = TwitterOperator(
        file_path=join(
        "datalake/twitter_datascience",
        "extract_data={{ ds }}",
        "datascience_{{ ds_nodash }}.json"),
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            query=query,
            task_id="twitter_datascience",
        )