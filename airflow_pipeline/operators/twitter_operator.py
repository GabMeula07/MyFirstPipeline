import sys

sys.path.append("airflow_pipeline")
from pathlib import Path
from airflow.models import BaseOperator, TaskInstance, DAG
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from os.path import join
import json


class TwitterOperator(BaseOperator):
    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path

        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        self.create_parent_folder()
        with open(self.file_path, "w") as arquivo:
            for pg in TwitterHook(
                end_time=end_time, start_time=start_time, query=query
            ).run():
                json.dump(pg, arquivo, ensure_ascii=False)
                arquivo.write("\n")


if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-50)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    with DAG(dag_id="Twitter_Test", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            file_path=join(
                "datalake/twitter_datascience",
                f"extract_date={datetime.now().date().strftime('%Y%M%D')}",
                f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),
            
            end_time=end_time,
            start_time=start_time,
            query=query,
            task_id="test_run",
        )
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)
