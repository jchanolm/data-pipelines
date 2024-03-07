from . import S3Utils
import os
import logging

dag_path = os.path.join(os.path.dirname(__file__), "..", "dags")

dags = [path for path in os.listdir(dag_path) if ".py" in path]

s3 = S3Utils(bucket_name="abr-data-airflow", no_bucket_prefix=True)
for dag in dags:
    logging.info(f"Uploading: {dag} to S3")
    s3.save_file(os.path.join(dag_path, dag), os.path.join("dags", dag))
